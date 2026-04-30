//! Schema Routing Metrics
//!
//! Provides metrics and statistics for schema-aware routing decisions.
//! Tracks routing patterns, hit rates, and performance characteristics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use super::{
    DataTemperature, WorkloadType, AccessPattern,
    RoutingDecision, AIWorkloadType, RAGStage,
};

/// Schema routing metrics collector
pub struct SchemaRoutingMetrics {
    /// Routing statistics by table
    table_stats: Arc<RwLock<HashMap<String, TableStats>>>,
    /// Routing statistics by workload type
    workload_stats: Arc<RwLock<HashMap<WorkloadType, WorkloadStats>>>,
    /// Routing statistics by temperature
    temperature_stats: Arc<RwLock<HashMap<DataTemperature, TemperatureStats>>>,
    /// AI workload statistics
    ai_stats: Arc<RwLock<AIWorkloadStats>>,
    /// RAG pipeline statistics
    rag_stats: Arc<RwLock<RAGStats>>,
    /// Overall routing statistics
    routing_stats: Arc<RoutingStats>,
    /// Node distribution statistics
    node_stats: Arc<RwLock<HashMap<String, NodeStats>>>,
    /// Shard distribution statistics
    shard_stats: Arc<RwLock<HashMap<u32, ShardStats>>>,
    /// Start time for uptime calculation
    start_time: Instant,
}

/// Statistics for a specific table
#[derive(Debug, Clone, Default)]
pub struct TableStats {
    /// Table name
    pub table_name: String,
    /// Total queries routed
    pub total_queries: u64,
    /// Queries by access pattern
    pub by_access_pattern: HashMap<AccessPattern, u64>,
    /// Queries by workload type
    pub by_workload: HashMap<WorkloadType, u64>,
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    /// P99 latency in microseconds
    pub p99_latency_us: u64,
    /// Shard hit rate (queries with shard key)
    pub shard_hit_rate: f64,
    /// Cache utilization
    pub cache_hit_rate: f64,
    /// Last query time
    pub last_query_time: Option<Instant>,
}

impl TableStats {
    /// Create new table stats
    pub fn new(table_name: &str) -> Self {
        Self {
            table_name: table_name.to_string(),
            ..Default::default()
        }
    }

    /// Record a query
    pub fn record_query(&mut self, pattern: AccessPattern, workload: WorkloadType, latency_us: u64) {
        self.total_queries += 1;
        *self.by_access_pattern.entry(pattern).or_insert(0) += 1;
        *self.by_workload.entry(workload).or_insert(0) += 1;

        // Update average latency (exponential moving average)
        if self.avg_latency_us == 0 {
            self.avg_latency_us = latency_us;
        } else {
            self.avg_latency_us = (self.avg_latency_us * 9 + latency_us) / 10;
        }

        self.last_query_time = Some(Instant::now());
    }
}

/// Statistics by workload type
#[derive(Debug, Clone, Default)]
pub struct WorkloadStats {
    /// Total queries for this workload
    pub total_queries: u64,
    /// Queries routed to primary
    pub routed_to_primary: u64,
    /// Queries routed to replicas
    pub routed_to_replica: u64,
    /// Queries scattered to all nodes
    pub scatter_gather: u64,
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    /// Tables using this workload
    pub tables: Vec<String>,
}

impl WorkloadStats {
    /// Record a routing decision
    pub fn record(&mut self, to_primary: bool, is_scatter: bool, latency_us: u64) {
        self.total_queries += 1;

        if is_scatter {
            self.scatter_gather += 1;
        } else if to_primary {
            self.routed_to_primary += 1;
        } else {
            self.routed_to_replica += 1;
        }

        // Update average latency
        if self.avg_latency_us == 0 {
            self.avg_latency_us = latency_us;
        } else {
            self.avg_latency_us = (self.avg_latency_us * 9 + latency_us) / 10;
        }
    }
}

/// Statistics by temperature
#[derive(Debug, Clone, Default)]
pub struct TemperatureStats {
    /// Total queries
    pub total_queries: u64,
    /// Total tables at this temperature
    pub table_count: u64,
    /// Total size in bytes
    pub total_size_bytes: u64,
    /// Cache hit rate
    pub cache_hit_rate: f64,
    /// Average query latency
    pub avg_latency_us: u64,
}

/// AI workload statistics
#[derive(Debug, Clone, Default)]
pub struct AIWorkloadStats {
    /// Total AI workload queries
    pub total_queries: u64,
    /// Queries by AI workload type
    pub by_type: HashMap<String, u64>,
    /// Embedding retrieval count
    pub embedding_retrieval: u64,
    /// Context lookup count
    pub context_lookup: u64,
    /// Knowledge base queries
    pub knowledge_base: u64,
    /// Tool execution queries
    pub tool_execution: u64,
    /// Average vector dimensions
    pub avg_vector_dimensions: u64,
    /// Average k (top-k results)
    pub avg_top_k: u64,
}

impl AIWorkloadStats {
    /// Record an AI workload query
    pub fn record(&mut self, workload_type: AIWorkloadType, vector_dims: Option<u64>, top_k: Option<u64>) {
        self.total_queries += 1;

        match workload_type {
            AIWorkloadType::EmbeddingRetrieval => {
                self.embedding_retrieval += 1;
                *self.by_type.entry("embedding_retrieval".to_string()).or_insert(0) += 1;
            }
            AIWorkloadType::ContextLookup => {
                self.context_lookup += 1;
                *self.by_type.entry("context_lookup".to_string()).or_insert(0) += 1;
            }
            AIWorkloadType::KnowledgeBase => {
                self.knowledge_base += 1;
                *self.by_type.entry("knowledge_base".to_string()).or_insert(0) += 1;
            }
            AIWorkloadType::ToolExecution => {
                self.tool_execution += 1;
                *self.by_type.entry("tool_execution".to_string()).or_insert(0) += 1;
            }
        }

        if let Some(dims) = vector_dims {
            self.avg_vector_dimensions = (self.avg_vector_dimensions * 9 + dims) / 10;
        }

        if let Some(k) = top_k {
            self.avg_top_k = (self.avg_top_k * 9 + k) / 10;
        }
    }
}

/// RAG pipeline statistics
#[derive(Debug, Clone, Default)]
pub struct RAGStats {
    /// Total RAG queries
    pub total_queries: u64,
    /// Retrieval stage count
    pub retrieval_count: u64,
    /// Fetch stage count
    pub fetch_count: u64,
    /// Rerank stage count
    pub rerank_count: u64,
    /// Generate stage count
    pub generate_count: u64,
    /// Average retrieval latency
    pub avg_retrieval_latency_us: u64,
    /// Average fetch latency
    pub avg_fetch_latency_us: u64,
    /// Average total pipeline latency
    pub avg_pipeline_latency_us: u64,
}

impl RAGStats {
    /// Record a RAG stage execution
    pub fn record_stage(&mut self, stage: RAGStage, latency_us: u64) {
        self.total_queries += 1;

        match stage {
            RAGStage::Retrieval => {
                self.retrieval_count += 1;
                if self.avg_retrieval_latency_us == 0 {
                    self.avg_retrieval_latency_us = latency_us;
                } else {
                    self.avg_retrieval_latency_us = (self.avg_retrieval_latency_us * 9 + latency_us) / 10;
                }
            }
            RAGStage::Fetch => {
                self.fetch_count += 1;
                if self.avg_fetch_latency_us == 0 {
                    self.avg_fetch_latency_us = latency_us;
                } else {
                    self.avg_fetch_latency_us = (self.avg_fetch_latency_us * 9 + latency_us) / 10;
                }
            }
            RAGStage::Rerank => {
                self.rerank_count += 1;
            }
            RAGStage::Generate => {
                self.generate_count += 1;
            }
        }
    }
}

/// Overall routing statistics
pub struct RoutingStats {
    /// Total queries routed
    pub total_queries: AtomicU64,
    /// Schema-aware routing decisions
    pub schema_aware_routes: AtomicU64,
    /// Fallback routing decisions
    pub fallback_routes: AtomicU64,
    /// Shard-targeted queries
    pub shard_targeted: AtomicU64,
    /// Scatter-gather queries
    pub scatter_gather: AtomicU64,
    /// Primary routes
    pub primary_routes: AtomicU64,
    /// Replica routes
    pub replica_routes: AtomicU64,
    /// AI workload routes
    pub ai_routes: AtomicU64,
    /// RAG pipeline routes
    pub rag_routes: AtomicU64,
    /// Vector search routes
    pub vector_routes: AtomicU64,
    /// Classification cache hits
    pub classification_hits: AtomicU64,
    /// Classification cache misses
    pub classification_misses: AtomicU64,
    /// Routing errors
    pub routing_errors: AtomicU64,
}

impl Default for RoutingStats {
    fn default() -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            schema_aware_routes: AtomicU64::new(0),
            fallback_routes: AtomicU64::new(0),
            shard_targeted: AtomicU64::new(0),
            scatter_gather: AtomicU64::new(0),
            primary_routes: AtomicU64::new(0),
            replica_routes: AtomicU64::new(0),
            ai_routes: AtomicU64::new(0),
            rag_routes: AtomicU64::new(0),
            vector_routes: AtomicU64::new(0),
            classification_hits: AtomicU64::new(0),
            classification_misses: AtomicU64::new(0),
            routing_errors: AtomicU64::new(0),
        }
    }
}

impl RoutingStats {
    /// Get total queries count
    pub fn total_queries(&self) -> u64 {
        self.total_queries.load(Ordering::Relaxed)
    }

    /// Get schema-aware routing percentage
    pub fn schema_aware_percentage(&self) -> f64 {
        let total = self.total_queries.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let schema_aware = self.schema_aware_routes.load(Ordering::Relaxed);
        (schema_aware as f64 / total as f64) * 100.0
    }

    /// Get classification cache hit rate
    pub fn classification_hit_rate(&self) -> f64 {
        let hits = self.classification_hits.load(Ordering::Relaxed);
        let misses = self.classification_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            return 0.0;
        }
        hits as f64 / total as f64
    }

    /// Get primary/replica distribution
    pub fn primary_replica_ratio(&self) -> f64 {
        let primary = self.primary_routes.load(Ordering::Relaxed);
        let replica = self.replica_routes.load(Ordering::Relaxed);
        let total = primary + replica;
        if total == 0 {
            return 0.0;
        }
        primary as f64 / total as f64
    }
}

/// Node routing statistics
#[derive(Debug, Clone, Default)]
pub struct NodeStats {
    /// Node identifier
    pub node_id: String,
    /// Total queries routed to this node
    pub total_queries: u64,
    /// Average latency
    pub avg_latency_us: u64,
    /// Error count
    pub error_count: u64,
    /// Load factor (0.0 - 1.0)
    pub load_factor: f64,
    /// Last query time
    pub last_query_time: Option<Instant>,
}

/// Shard routing statistics
#[derive(Debug, Clone, Default)]
pub struct ShardStats {
    /// Shard identifier
    pub shard_id: u32,
    /// Total queries
    pub total_queries: u64,
    /// Tables in this shard
    pub tables: Vec<String>,
    /// Estimated row count
    pub estimated_rows: u64,
    /// Size in bytes
    pub size_bytes: u64,
}

impl SchemaRoutingMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            table_stats: Arc::new(RwLock::new(HashMap::new())),
            workload_stats: Arc::new(RwLock::new(HashMap::new())),
            temperature_stats: Arc::new(RwLock::new(HashMap::new())),
            ai_stats: Arc::new(RwLock::new(AIWorkloadStats::default())),
            rag_stats: Arc::new(RwLock::new(RAGStats::default())),
            routing_stats: Arc::new(RoutingStats::default()),
            node_stats: Arc::new(RwLock::new(HashMap::new())),
            shard_stats: Arc::new(RwLock::new(HashMap::new())),
            start_time: Instant::now(),
        }
    }

    /// Record a routing decision
    pub async fn record_routing(&self, decision: &RoutingDecision, latency_us: u64) {
        self.routing_stats.total_queries.fetch_add(1, Ordering::Relaxed);

        match &decision.target {
            super::RouteTarget::Primary => {
                self.routing_stats.primary_routes.fetch_add(1, Ordering::Relaxed);
            }
            super::RouteTarget::Node(_) => {
                self.routing_stats.replica_routes.fetch_add(1, Ordering::Relaxed);
            }
            super::RouteTarget::Shard(_) => {
                self.routing_stats.shard_targeted.fetch_add(1, Ordering::Relaxed);
            }
            super::RouteTarget::ScatterGather => {
                self.routing_stats.scatter_gather.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update node stats if routed to specific node
        if let super::RouteTarget::Node(node_id) = &decision.target {
            let mut node_stats = self.node_stats.write().await;
            let stats = node_stats.entry(node_id.clone())
                .or_insert_with(|| NodeStats {
                    node_id: node_id.clone(),
                    ..Default::default()
                });
            stats.total_queries += 1;
            stats.avg_latency_us = (stats.avg_latency_us * 9 + latency_us) / 10;
            stats.last_query_time = Some(Instant::now());
        }

        // Update shard stats
        if !decision.shards.is_empty() {
            let mut shard_stats = self.shard_stats.write().await;
            for shard_id in &decision.shards {
                let stats = shard_stats.entry(*shard_id).or_default();
                stats.shard_id = *shard_id;
                stats.total_queries += 1;
            }
        }
    }

    /// Record a table query
    pub async fn record_table_query(
        &self,
        table: &str,
        pattern: AccessPattern,
        workload: WorkloadType,
        latency_us: u64,
    ) {
        let mut table_stats = self.table_stats.write().await;
        let stats = table_stats.entry(table.to_string())
            .or_insert_with(|| TableStats::new(table));
        stats.record_query(pattern, workload, latency_us);
    }

    /// Record a workload routing
    pub async fn record_workload(&self, workload: WorkloadType, to_primary: bool, is_scatter: bool, latency_us: u64) {
        let mut workload_stats = self.workload_stats.write().await;
        let stats = workload_stats.entry(workload).or_default();
        stats.record(to_primary, is_scatter, latency_us);
    }

    /// Record an AI workload query
    pub async fn record_ai_workload(&self, workload_type: AIWorkloadType, vector_dims: Option<u64>, top_k: Option<u64>) {
        self.routing_stats.ai_routes.fetch_add(1, Ordering::Relaxed);
        let mut ai_stats = self.ai_stats.write().await;
        ai_stats.record(workload_type, vector_dims, top_k);
    }

    /// Record a RAG stage execution
    pub async fn record_rag_stage(&self, stage: RAGStage, latency_us: u64) {
        self.routing_stats.rag_routes.fetch_add(1, Ordering::Relaxed);
        let mut rag_stats = self.rag_stats.write().await;
        rag_stats.record_stage(stage, latency_us);
    }

    /// Record a classification cache hit or miss
    pub fn record_classification_lookup(&self, hit: bool) {
        if hit {
            self.routing_stats.classification_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.routing_stats.classification_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a routing error
    pub fn record_error(&self) {
        self.routing_stats.routing_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get overall routing stats
    pub fn get_routing_stats(&self) -> &RoutingStats {
        &self.routing_stats
    }

    /// Get table statistics
    pub async fn get_table_stats(&self, table: &str) -> Option<TableStats> {
        let stats = self.table_stats.read().await;
        stats.get(table).cloned()
    }

    /// Get all table statistics
    pub async fn get_all_table_stats(&self) -> HashMap<String, TableStats> {
        self.table_stats.read().await.clone()
    }

    /// Get workload statistics
    pub async fn get_workload_stats(&self, workload: WorkloadType) -> Option<WorkloadStats> {
        let stats = self.workload_stats.read().await;
        stats.get(&workload).cloned()
    }

    /// Get all workload statistics
    pub async fn get_all_workload_stats(&self) -> HashMap<WorkloadType, WorkloadStats> {
        self.workload_stats.read().await.clone()
    }

    /// Get AI workload statistics
    pub async fn get_ai_stats(&self) -> AIWorkloadStats {
        self.ai_stats.read().await.clone()
    }

    /// Get RAG pipeline statistics
    pub async fn get_rag_stats(&self) -> RAGStats {
        self.rag_stats.read().await.clone()
    }

    /// Get node statistics
    pub async fn get_node_stats(&self, node_id: &str) -> Option<NodeStats> {
        let stats = self.node_stats.read().await;
        stats.get(node_id).cloned()
    }

    /// Get all node statistics
    pub async fn get_all_node_stats(&self) -> HashMap<String, NodeStats> {
        self.node_stats.read().await.clone()
    }

    /// Get shard statistics
    pub async fn get_shard_stats(&self, shard_id: u32) -> Option<ShardStats> {
        let stats = self.shard_stats.read().await;
        stats.get(&shard_id).cloned()
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Reset all metrics
    pub async fn reset(&self) {
        self.table_stats.write().await.clear();
        self.workload_stats.write().await.clear();
        self.temperature_stats.write().await.clear();
        *self.ai_stats.write().await = AIWorkloadStats::default();
        *self.rag_stats.write().await = RAGStats::default();
        self.node_stats.write().await.clear();
        self.shard_stats.write().await.clear();

        // Reset atomic counters
        self.routing_stats.total_queries.store(0, Ordering::Relaxed);
        self.routing_stats.schema_aware_routes.store(0, Ordering::Relaxed);
        self.routing_stats.fallback_routes.store(0, Ordering::Relaxed);
        self.routing_stats.shard_targeted.store(0, Ordering::Relaxed);
        self.routing_stats.scatter_gather.store(0, Ordering::Relaxed);
        self.routing_stats.primary_routes.store(0, Ordering::Relaxed);
        self.routing_stats.replica_routes.store(0, Ordering::Relaxed);
        self.routing_stats.ai_routes.store(0, Ordering::Relaxed);
        self.routing_stats.rag_routes.store(0, Ordering::Relaxed);
        self.routing_stats.vector_routes.store(0, Ordering::Relaxed);
        self.routing_stats.classification_hits.store(0, Ordering::Relaxed);
        self.routing_stats.classification_misses.store(0, Ordering::Relaxed);
        self.routing_stats.routing_errors.store(0, Ordering::Relaxed);
    }

    /// Generate metrics report
    pub async fn generate_report(&self) -> MetricsReport {
        let routing = self.get_routing_stats();
        let tables = self.get_all_table_stats().await;
        let _workloads = self.get_all_workload_stats().await;
        let ai = self.get_ai_stats().await;
        let rag = self.get_rag_stats().await;
        let nodes = self.get_all_node_stats().await;

        MetricsReport {
            uptime: self.uptime(),
            total_queries: routing.total_queries(),
            schema_aware_percentage: routing.schema_aware_percentage(),
            classification_hit_rate: routing.classification_hit_rate(),
            primary_replica_ratio: routing.primary_replica_ratio(),
            table_count: tables.len(),
            active_nodes: nodes.len(),
            ai_query_percentage: if routing.total_queries() == 0 {
                0.0
            } else {
                (ai.total_queries as f64 / routing.total_queries() as f64) * 100.0
            },
            rag_query_percentage: if routing.total_queries() == 0 {
                0.0
            } else {
                (rag.total_queries as f64 / routing.total_queries() as f64) * 100.0
            },
            error_rate: if routing.total_queries() == 0 {
                0.0
            } else {
                (routing.routing_errors.load(Ordering::Relaxed) as f64 / routing.total_queries() as f64) * 100.0
            },
        }
    }
}

impl Default for SchemaRoutingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaRoutingMetrics {
    /// Get table stats for admin API (blocking sync version)
    pub fn get_table_stats_for_admin(&self) -> Vec<(String, TableStatsForAdmin)> {
        // Use futures to block on the async lock
        let table_stats = self.table_stats.clone();
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => {
                let stats = h.block_on(async {
                    let guard = table_stats.read().await;
                    guard.iter().map(|(name, stats)| {
                        (name.clone(), TableStatsForAdmin {
                            query_count: stats.total_queries,
                            avg_latency_ms: stats.avg_latency_us as f64 / 1000.0,
                            cache_hit_rate: stats.cache_hit_rate,
                            temperature: infer_temperature_from_count(stats.total_queries),
                            workload: infer_workload_from_stats(stats),
                        })
                    }).collect::<Vec<_>>()
                });
                stats
            }
            Err(_) => Vec::new(),
        }
    }

    /// Get workload stats for admin API (blocking sync version)
    pub fn get_workload_stats_for_admin(&self) -> Vec<(WorkloadType, WorkloadStatsForAdmin)> {
        let workload_stats = self.workload_stats.clone();
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => {
                h.block_on(async {
                    let guard = workload_stats.read().await;
                    guard.iter().map(|(workload, stats)| {
                        (*workload, WorkloadStatsForAdmin {
                            query_count: stats.total_queries,
                            avg_latency_ms: stats.avg_latency_us as f64 / 1000.0,
                            queries_to_primary: stats.routed_to_primary,
                            queries_to_replica: stats.routed_to_replica,
                        })
                    }).collect::<Vec<_>>()
                })
            }
            Err(_) => Vec::new(),
        }
    }

    /// Get AI workload stats for admin API (blocking sync version)
    pub fn get_ai_workload_stats(&self) -> AIWorkloadStatsForAdmin {
        let ai_stats = self.ai_stats.clone();
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => {
                h.block_on(async {
                    let guard = ai_stats.read().await;
                    AIWorkloadStatsForAdmin {
                        embedding_retrieval_count: guard.embedding_retrieval,
                        context_lookup_count: guard.context_lookup,
                        knowledge_base_count: guard.knowledge_base,
                        tool_execution_count: guard.tool_execution,
                        avg_vector_dimensions: guard.avg_vector_dimensions as f64,
                    }
                })
            }
            Err(_) => AIWorkloadStatsForAdmin::default(),
        }
    }

    /// Get RAG stats for admin API (blocking sync version)
    pub fn get_rag_stats_for_admin(&self) -> RAGStatsForAdmin {
        let rag_stats = self.rag_stats.clone();
        let handle = tokio::runtime::Handle::try_current();
        match handle {
            Ok(h) => {
                h.block_on(async {
                    let guard = rag_stats.read().await;
                    RAGStatsForAdmin {
                        retrieval_count: guard.retrieval_count,
                        avg_retrieval_latency_ms: guard.avg_retrieval_latency_us as f64 / 1000.0,
                        fetch_count: guard.fetch_count,
                        avg_fetch_latency_ms: guard.avg_fetch_latency_us as f64 / 1000.0,
                        total_pipeline_executions: guard.total_queries,
                        avg_total_latency_ms: guard.avg_pipeline_latency_us as f64 / 1000.0,
                    }
                })
            }
            Err(_) => RAGStatsForAdmin::default(),
        }
    }

}

/// Infer temperature from query count
fn infer_temperature_from_count(query_count: u64) -> DataTemperature {
    if query_count > 10000 {
        DataTemperature::Hot
    } else if query_count > 1000 {
        DataTemperature::Warm
    } else if query_count > 100 {
        DataTemperature::Cold
    } else {
        DataTemperature::Frozen
    }
}

/// Infer workload from stats
fn infer_workload_from_stats(stats: &TableStats) -> WorkloadType {
    let olap_count = stats.by_access_pattern.get(&AccessPattern::FullScan).copied().unwrap_or(0);
    let vector_count = stats.by_access_pattern.get(&AccessPattern::VectorSearch).copied().unwrap_or(0);
    let point_count = stats.by_access_pattern.get(&AccessPattern::PointLookup).copied().unwrap_or(0);

    if vector_count > stats.total_queries / 3 {
        WorkloadType::Vector
    } else if olap_count > stats.total_queries / 2 {
        WorkloadType::OLAP
    } else if point_count > stats.total_queries / 2 {
        WorkloadType::OLTP
    } else {
        WorkloadType::Mixed
    }
}

/// Table stats for admin API
#[derive(Debug, Clone)]
pub struct TableStatsForAdmin {
    pub query_count: u64,
    pub avg_latency_ms: f64,
    pub cache_hit_rate: f64,
    pub temperature: DataTemperature,
    pub workload: WorkloadType,
}

/// Workload stats for admin API
#[derive(Debug, Clone)]
pub struct WorkloadStatsForAdmin {
    pub query_count: u64,
    pub avg_latency_ms: f64,
    pub queries_to_primary: u64,
    pub queries_to_replica: u64,
}

/// AI workload stats for admin API
#[derive(Debug, Clone, Default)]
pub struct AIWorkloadStatsForAdmin {
    pub embedding_retrieval_count: u64,
    pub context_lookup_count: u64,
    pub knowledge_base_count: u64,
    pub tool_execution_count: u64,
    pub avg_vector_dimensions: f64,
}

impl AIWorkloadStatsForAdmin {
    /// Get total AI queries
    pub fn total_ai_queries(&self) -> u64 {
        self.embedding_retrieval_count + self.context_lookup_count +
            self.knowledge_base_count + self.tool_execution_count
    }
}

/// RAG stats for admin API
#[derive(Debug, Clone, Default)]
pub struct RAGStatsForAdmin {
    pub retrieval_count: u64,
    pub avg_retrieval_latency_ms: f64,
    pub fetch_count: u64,
    pub avg_fetch_latency_ms: f64,
    pub total_pipeline_executions: u64,
    pub avg_total_latency_ms: f64,
}

/// Summary metrics report
#[derive(Debug, Clone)]
pub struct MetricsReport {
    /// Uptime duration
    pub uptime: Duration,
    /// Total queries routed
    pub total_queries: u64,
    /// Percentage of schema-aware routes
    pub schema_aware_percentage: f64,
    /// Classification cache hit rate
    pub classification_hit_rate: f64,
    /// Primary to replica ratio
    pub primary_replica_ratio: f64,
    /// Number of tracked tables
    pub table_count: usize,
    /// Number of active nodes
    pub active_nodes: usize,
    /// AI query percentage
    pub ai_query_percentage: f64,
    /// RAG query percentage
    pub rag_query_percentage: f64,
    /// Error rate
    pub error_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{RouteTarget, RoutingReason};

    fn sample_decision() -> RoutingDecision {
        RoutingDecision {
            target: RouteTarget::Primary,
            reason: RoutingReason::WriteQuery,
            shards: vec![],
            branch: None,
            node_info: None,
        }
    }

    #[tokio::test]
    async fn test_metrics_new() {
        let metrics = SchemaRoutingMetrics::new();
        assert_eq!(metrics.get_routing_stats().total_queries(), 0);
    }

    #[tokio::test]
    async fn test_record_routing() {
        let metrics = SchemaRoutingMetrics::new();
        let decision = sample_decision();

        metrics.record_routing(&decision, 1000).await;

        assert_eq!(metrics.get_routing_stats().total_queries(), 1);
        assert_eq!(metrics.get_routing_stats().primary_routes.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_record_table_query() {
        let metrics = SchemaRoutingMetrics::new();

        metrics.record_table_query("users", AccessPattern::PointLookup, WorkloadType::OLTP, 500).await;
        metrics.record_table_query("users", AccessPattern::PointLookup, WorkloadType::OLTP, 600).await;

        let stats = metrics.get_table_stats("users").await.unwrap();
        assert_eq!(stats.total_queries, 2);
        assert_eq!(*stats.by_access_pattern.get(&AccessPattern::PointLookup).unwrap(), 2);
    }

    #[tokio::test]
    async fn test_record_workload() {
        let metrics = SchemaRoutingMetrics::new();

        metrics.record_workload(WorkloadType::OLTP, true, false, 100).await;
        metrics.record_workload(WorkloadType::OLTP, false, false, 200).await;

        let stats = metrics.get_workload_stats(WorkloadType::OLTP).await.unwrap();
        assert_eq!(stats.total_queries, 2);
        assert_eq!(stats.routed_to_primary, 1);
        assert_eq!(stats.routed_to_replica, 1);
    }

    #[tokio::test]
    async fn test_record_ai_workload() {
        let metrics = SchemaRoutingMetrics::new();

        metrics.record_ai_workload(AIWorkloadType::EmbeddingRetrieval, Some(1536), Some(10)).await;
        metrics.record_ai_workload(AIWorkloadType::ContextLookup, None, None).await;

        let stats = metrics.get_ai_stats().await;
        assert_eq!(stats.total_queries, 2);
        assert_eq!(stats.embedding_retrieval, 1);
        assert_eq!(stats.context_lookup, 1);
    }

    #[tokio::test]
    async fn test_record_rag_stage() {
        let metrics = SchemaRoutingMetrics::new();

        metrics.record_rag_stage(RAGStage::Retrieval, 5000).await;
        metrics.record_rag_stage(RAGStage::Fetch, 2000).await;

        let stats = metrics.get_rag_stats().await;
        assert_eq!(stats.total_queries, 2);
        assert_eq!(stats.retrieval_count, 1);
        assert_eq!(stats.fetch_count, 1);
    }

    #[tokio::test]
    async fn test_record_node_stats() {
        let metrics = SchemaRoutingMetrics::new();

        let decision = RoutingDecision {
            target: RouteTarget::Node("node1".to_string()),
            reason: RoutingReason::LowLatency,
            shards: vec![],
            branch: None,
            node_info: None,
        };

        metrics.record_routing(&decision, 1000).await;
        metrics.record_routing(&decision, 2000).await;

        let stats = metrics.get_node_stats("node1").await.unwrap();
        assert_eq!(stats.total_queries, 2);
    }

    #[tokio::test]
    async fn test_record_shard_stats() {
        let metrics = SchemaRoutingMetrics::new();

        let decision = RoutingDecision {
            target: RouteTarget::Shard(5),
            reason: RoutingReason::ShardKey,
            shards: vec![5],
            branch: None,
            node_info: None,
        };

        metrics.record_routing(&decision, 1000).await;

        let stats = metrics.get_shard_stats(5).await.unwrap();
        assert_eq!(stats.total_queries, 1);
    }

    #[tokio::test]
    async fn test_classification_lookup() {
        let metrics = SchemaRoutingMetrics::new();

        metrics.record_classification_lookup(true);
        metrics.record_classification_lookup(true);
        metrics.record_classification_lookup(false);

        assert_eq!(metrics.get_routing_stats().classification_hit_rate(), 2.0 / 3.0);
    }

    #[tokio::test]
    async fn test_record_error() {
        let metrics = SchemaRoutingMetrics::new();

        metrics.record_error();
        metrics.record_error();

        assert_eq!(metrics.get_routing_stats().routing_errors.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_reset_metrics() {
        let metrics = SchemaRoutingMetrics::new();

        // Record some data
        metrics.record_routing(&sample_decision(), 1000).await;
        metrics.record_table_query("users", AccessPattern::PointLookup, WorkloadType::OLTP, 500).await;
        metrics.record_ai_workload(AIWorkloadType::EmbeddingRetrieval, None, None).await;

        // Reset
        metrics.reset().await;

        // Verify reset
        assert_eq!(metrics.get_routing_stats().total_queries(), 0);
        assert!(metrics.get_table_stats("users").await.is_none());
        assert_eq!(metrics.get_ai_stats().await.total_queries, 0);
    }

    #[tokio::test]
    async fn test_generate_report() {
        let metrics = SchemaRoutingMetrics::new();

        // Record various metrics
        for _ in 0..10 {
            metrics.record_routing(&sample_decision(), 1000).await;
        }
        metrics.record_table_query("users", AccessPattern::PointLookup, WorkloadType::OLTP, 500).await;
        metrics.record_ai_workload(AIWorkloadType::EmbeddingRetrieval, None, None).await;
        metrics.record_rag_stage(RAGStage::Retrieval, 5000).await;

        let report = metrics.generate_report().await;

        assert_eq!(report.total_queries, 10);
        assert_eq!(report.table_count, 1);
    }

    #[test]
    fn test_routing_stats_percentages() {
        let stats = RoutingStats::default();

        // Empty stats
        assert_eq!(stats.schema_aware_percentage(), 0.0);
        assert_eq!(stats.classification_hit_rate(), 0.0);
        assert_eq!(stats.primary_replica_ratio(), 0.0);

        // With data
        stats.total_queries.store(100, Ordering::Relaxed);
        stats.schema_aware_routes.store(80, Ordering::Relaxed);
        stats.classification_hits.store(90, Ordering::Relaxed);
        stats.classification_misses.store(10, Ordering::Relaxed);
        stats.primary_routes.store(30, Ordering::Relaxed);
        stats.replica_routes.store(70, Ordering::Relaxed);

        assert_eq!(stats.schema_aware_percentage(), 80.0);
        assert_eq!(stats.classification_hit_rate(), 0.9);
        assert_eq!(stats.primary_replica_ratio(), 0.3);
    }

    #[test]
    fn test_table_stats_record() {
        let mut stats = TableStats::new("orders");

        stats.record_query(AccessPattern::PointLookup, WorkloadType::OLTP, 100);
        stats.record_query(AccessPattern::RangeScan, WorkloadType::OLTP, 200);

        assert_eq!(stats.total_queries, 2);
        assert_eq!(*stats.by_access_pattern.get(&AccessPattern::PointLookup).unwrap(), 1);
        assert_eq!(*stats.by_access_pattern.get(&AccessPattern::RangeScan).unwrap(), 1);
    }

    #[test]
    fn test_workload_stats_record() {
        let mut stats = WorkloadStats::default();

        stats.record(true, false, 100);
        stats.record(false, false, 200);
        stats.record(false, true, 300);

        assert_eq!(stats.total_queries, 3);
        assert_eq!(stats.routed_to_primary, 1);
        assert_eq!(stats.routed_to_replica, 1);
        assert_eq!(stats.scatter_gather, 1);
    }

    #[test]
    fn test_ai_stats_record() {
        let mut stats = AIWorkloadStats::default();

        stats.record(AIWorkloadType::EmbeddingRetrieval, Some(1536), Some(10));
        stats.record(AIWorkloadType::KnowledgeBase, None, None);

        assert_eq!(stats.total_queries, 2);
        assert_eq!(stats.embedding_retrieval, 1);
        assert_eq!(stats.knowledge_base, 1);
    }

    #[test]
    fn test_rag_stats_record() {
        let mut stats = RAGStats::default();

        stats.record_stage(RAGStage::Retrieval, 5000);
        stats.record_stage(RAGStage::Fetch, 2000);
        stats.record_stage(RAGStage::Rerank, 1000);

        assert_eq!(stats.total_queries, 3);
        assert_eq!(stats.retrieval_count, 1);
        assert_eq!(stats.fetch_count, 1);
        assert_eq!(stats.rerank_count, 1);
    }
}
