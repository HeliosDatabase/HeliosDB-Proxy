//! Schema Routing Admin API
//!
//! REST API endpoints for managing schema-aware routing.

use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::registry::{ColumnSchema, StorageType, TableSchema};
use super::router::SchemaAwareRouter;
use super::{
    AccessPattern, DataTemperature, LearningClassifier, SchemaDiscovery, SchemaRegistry,
    SchemaRoutingMetrics, WorkloadType,
};

/// Admin API for schema routing
pub struct SchemaRoutingAdmin {
    pub registry: Arc<SchemaRegistry>,
    pub router: Arc<SchemaAwareRouter>,
    pub classifier: Arc<LearningClassifier>,
    pub discovery: Arc<SchemaDiscovery>,
    pub metrics: Arc<SchemaRoutingMetrics>,
}

impl SchemaRoutingAdmin {
    /// Create a new admin API instance
    pub fn new(
        registry: Arc<SchemaRegistry>,
        router: Arc<SchemaAwareRouter>,
        classifier: Arc<LearningClassifier>,
        discovery: Arc<SchemaDiscovery>,
        metrics: Arc<SchemaRoutingMetrics>,
    ) -> Self {
        Self {
            registry,
            router,
            classifier,
            discovery,
            metrics,
        }
    }

    // =========================================================================
    // TABLE ENDPOINTS
    // =========================================================================

    /// GET /schema/tables - List all registered tables
    pub fn list_tables(&self) -> TablesResponse {
        let tables = self.registry.list_tables();
        TablesResponse {
            tables: tables
                .into_iter()
                .map(|t| TableSummary {
                    name: t.name.clone(),
                    temperature: format!("{:?}", t.temperature),
                    workload: format!("{:?}", t.workload),
                    access_pattern: format!("{:?}", t.access_pattern),
                    column_count: t.columns.len(),
                    shard_key: t.shard_key.clone(),
                    row_count_estimate: Some(t.estimated_rows),
                })
                .collect(),
            total: self.registry.table_count(),
        }
    }

    /// GET /schema/tables/:name - Get details for a specific table
    pub fn get_table(&self, name: &str) -> Option<TableDetails> {
        self.registry.get_table(name).map(|t| TableDetails {
            name: t.name.clone(),
            columns: t
                .columns
                .iter()
                .map(|c| ColumnDetails {
                    name: c.name.clone(),
                    data_type: c.data_type.clone(),
                    nullable: c.nullable,
                    is_primary_key: c.is_primary_key,
                    is_indexed: c.is_indexed,
                    default_value: None, // ColumnSchema doesn't have default_value
                    storage_type: Some(format!("{:?}", c.storage_type)),
                })
                .collect(),
            temperature: format!("{:?}", t.temperature),
            workload: format!("{:?}", t.workload),
            access_pattern: format!("{:?}", t.access_pattern),
            primary_key: t.primary_key.clone(),
            shard_key: t.shard_key.clone(),
            row_count_estimate: Some(t.estimated_rows),
            size_bytes: Some(t.avg_row_size as u64 * t.estimated_rows),
            partition_key: t.partition_key.as_ref().map(|p| format!("{:?}", p)),
        })
    }

    /// POST /schema/tables - Register a new table
    pub fn register_table(
        &self,
        request: RegisterTableRequest,
    ) -> Result<TableDetails, AdminError> {
        let temperature = DataTemperature::from_str(&request.temperature).ok_or_else(|| {
            AdminError::InvalidInput(format!("Invalid temperature: {}", request.temperature))
        })?;

        let workload = WorkloadType::from_str(&request.workload).ok_or_else(|| {
            AdminError::InvalidInput(format!("Invalid workload: {}", request.workload))
        })?;

        let access_pattern = parse_access_pattern(&request.access_pattern).ok_or_else(|| {
            AdminError::InvalidInput(format!(
                "Invalid access pattern: {}",
                request.access_pattern
            ))
        })?;

        let columns: Vec<ColumnSchema> = request
            .columns
            .iter()
            .map(|c| ColumnSchema {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
                is_primary_key: c.is_primary_key,
                is_indexed: c.is_indexed.unwrap_or(false),
                storage_type: StorageType::Row,
            })
            .collect();

        let table = TableSchema {
            name: request.name.clone(),
            columns,
            access_pattern,
            temperature,
            workload,
            primary_key: request.primary_key.clone(),
            shard_key: request.shard_key.clone(),
            estimated_rows: request.row_count_estimate.unwrap_or(0),
            avg_row_size: 0,
            partition_key: None,
            preferred_nodes: Vec::new(),
        };

        self.registry.register_table(table);

        self.get_table(&request.name)
            .ok_or_else(|| AdminError::InternalError("Failed to register table".to_string()))
    }

    /// DELETE /schema/tables/:name - Remove a table from routing
    pub fn remove_table(&self, name: &str) -> Result<(), AdminError> {
        if self.registry.get_table(name).is_none() {
            return Err(AdminError::NotFound(format!("Table not found: {}", name)));
        }
        self.registry.remove_table(name);
        Ok(())
    }

    // =========================================================================
    // CLASSIFICATION ENDPOINTS
    // =========================================================================

    /// POST /schema/classify - Manually classify a table
    pub fn classify_table(
        &self,
        request: ClassifyRequest,
    ) -> Result<ClassificationResult, AdminError> {
        let temperature = DataTemperature::from_str(&request.temperature).ok_or_else(|| {
            AdminError::InvalidInput(format!("Invalid temperature: {}", request.temperature))
        })?;

        let workload = WorkloadType::from_str(&request.workload).ok_or_else(|| {
            AdminError::InvalidInput(format!("Invalid workload: {}", request.workload))
        })?;

        // Get existing table
        let mut table = self
            .registry
            .get_table(&request.table_name)
            .ok_or_else(|| {
                AdminError::NotFound(format!("Table not found: {}", request.table_name))
            })?;

        // Update classifications
        let old_temperature = table.temperature;
        let old_workload = table.workload;

        table.temperature = temperature;
        table.workload = workload;

        // Re-register with new classification
        self.registry.register_table(table);

        Ok(ClassificationResult {
            table_name: request.table_name,
            previous_temperature: format!("{:?}", old_temperature),
            new_temperature: format!("{:?}", temperature),
            previous_workload: format!("{:?}", old_workload),
            new_workload: format!("{:?}", workload),
        })
    }

    /// GET /schema/classify/:table - Get classifier suggestions
    pub fn get_classification_suggestion(
        &self,
        table_name: &str,
    ) -> Result<ClassificationSuggestion, AdminError> {
        // Get history from classifier
        let history = self.classifier.get_history(table_name);

        if history.is_none() {
            return Err(AdminError::NotFound(format!(
                "No query history for table: {}",
                table_name
            )));
        }

        let hist = history.expect("history checked above");
        let query_count = hist.count();
        let suggested_temp = self.classifier.suggest_temperature(table_name);
        let suggested_workload = self.classifier.suggest_workload(table_name);
        let confidence = self.classifier.get_confidence(table_name);

        Ok(ClassificationSuggestion {
            table_name: table_name.to_string(),
            query_count,
            suggested_temperature: suggested_temp.map(|t| format!("{:?}", t)),
            suggested_workload: suggested_workload.map(|w| format!("{:?}", w)),
            confidence: confidence.unwrap_or(0.0),
            sample_size_sufficient: query_count >= 100,
        })
    }

    // =========================================================================
    // ANALYSIS ENDPOINTS
    // =========================================================================

    /// POST /schema/analyze - Analyze a query
    pub fn analyze_query(&self, request: AnalyzeRequest) -> AnalysisResult {
        use super::QueryAnalyzer;

        let query = request.query;
        let analyzer = QueryAnalyzer::new(self.registry.clone());
        let analysis = analyzer.analyze(&query);

        // Get primary access pattern from the list
        let access_pattern = analysis
            .access_patterns
            .first()
            .map(|p| format!("{:?}", p))
            .unwrap_or_else(|| "Mixed".to_string());

        let detected_workload = self
            .classifier
            .classify_query(&query)
            .map(|w| format!("{:?}", w));

        AnalysisResult {
            query,
            tables: analysis.tables.iter().map(|t| t.name.clone()).collect(),
            access_pattern,
            shard_keys: analysis
                .shard_keys
                .iter()
                .map(|(k, v)| format!("{}={:?}", k, v))
                .collect(),
            is_read_only: analysis.is_read_only,
            estimated_complexity: analysis.complexity,
            estimated_selectivity: analysis.selectivity,
            has_aggregation: analysis.has_aggregations,
            has_join: analysis.has_joins,
            has_subquery: analysis.has_subqueries,
            columns: Vec::new(), // Not available in QueryAnalysis
            detected_workload,
        }
    }

    /// POST /schema/route - Get routing decision for a query (dry-run)
    pub fn route_query(&self, request: RouteRequest) -> RouteResult {
        let decision = self.router.route(&request.query);

        RouteResult {
            query: request.query,
            target_type: format!("{:?}", decision.target),
            reason: format!("{:?}", decision.reason),
            preferred_node: decision.node_info.as_ref().map(|n| n.name.clone()),
            alternative_nodes: Vec::new(), // Not available in current RoutingDecision
            estimated_latency_ms: decision.node_info.as_ref().map(|n| n.current_latency_ms),
        }
    }

    // =========================================================================
    // ROUTING STATS ENDPOINTS
    // =========================================================================

    /// GET /schema/stats - Get overall routing statistics
    pub fn get_stats(&self) -> RoutingStatsResponse {
        let stats = self.metrics.get_routing_stats();

        RoutingStatsResponse {
            total_queries_routed: stats.total_queries.load(Ordering::Relaxed),
            queries_to_primary: stats.primary_routes.load(Ordering::Relaxed),
            queries_to_replica: stats.replica_routes.load(Ordering::Relaxed),
            queries_scattered: stats.scatter_gather.load(Ordering::Relaxed),
            avg_latency_ms: 0.0, // Not tracked globally in RoutingStats
            cache_hit_rate: stats.classification_hit_rate(),
        }
    }

    /// GET /schema/stats/tables - Get per-table statistics
    pub fn get_table_stats(&self) -> Vec<TableStatsResponse> {
        let stats = self.metrics.get_table_stats_for_admin();

        stats
            .into_iter()
            .map(|(name, s)| TableStatsResponse {
                table_name: name,
                query_count: s.query_count,
                avg_latency_ms: s.avg_latency_ms,
                hit_rate: s.cache_hit_rate,
                temperature: format!("{:?}", s.temperature),
                workload: format!("{:?}", s.workload),
            })
            .collect()
    }

    /// GET /schema/stats/workloads - Get per-workload statistics
    pub fn get_workload_stats(&self) -> Vec<WorkloadStatsResponse> {
        let stats = self.metrics.get_workload_stats_for_admin();

        stats
            .into_iter()
            .map(|(workload, s)| WorkloadStatsResponse {
                workload: format!("{:?}", workload),
                query_count: s.query_count,
                avg_latency_ms: s.avg_latency_ms,
                queries_to_primary: s.queries_to_primary,
                queries_to_replica: s.queries_to_replica,
            })
            .collect()
    }

    // =========================================================================
    // DISCOVERY ENDPOINTS
    // =========================================================================

    /// POST /schema/discover - Trigger schema discovery
    pub async fn trigger_discovery(&self) -> Result<DiscoveryResult, AdminError> {
        let tables = self
            .discovery
            .discover()
            .await
            .map_err(|e| AdminError::DiscoveryError(e.to_string()))?;

        // Register discovered tables
        for table in &tables {
            self.registry.register_table(table.clone());
        }

        Ok(DiscoveryResult {
            tables_discovered: tables.len(),
            table_names: tables.iter().map(|t| t.name.clone()).collect(),
        })
    }

    /// POST /schema/refresh - Refresh schema cache
    pub async fn refresh_schema(&self) -> Result<RefreshResult, AdminError> {
        self.discovery
            .refresh()
            .await
            .map_err(|e| AdminError::DiscoveryError(e.to_string()))?;

        Ok(RefreshResult {
            success: true,
            message: "Schema cache refreshed successfully".to_string(),
        })
    }

    // =========================================================================
    // AI/AGENT ENDPOINTS
    // =========================================================================

    /// GET /schema/ai/workloads - Get AI workload statistics
    pub fn get_ai_workload_stats(&self) -> AIWorkloadStatsResponse {
        let stats = self.metrics.get_ai_workload_stats();

        AIWorkloadStatsResponse {
            embedding_queries: stats.embedding_retrieval_count,
            context_lookups: stats.context_lookup_count,
            knowledge_base_queries: stats.knowledge_base_count,
            tool_executions: stats.tool_execution_count,
            total_ai_queries: stats.total_ai_queries(),
            avg_vector_dimensions: stats.avg_vector_dimensions,
        }
    }

    /// GET /schema/rag/stats - Get RAG pipeline statistics
    pub fn get_rag_stats(&self) -> RAGStatsResponse {
        let stats = self.metrics.get_rag_stats_for_admin();

        RAGStatsResponse {
            retrieval_count: stats.retrieval_count,
            avg_retrieval_latency_ms: stats.avg_retrieval_latency_ms,
            fetch_count: stats.fetch_count,
            avg_fetch_latency_ms: stats.avg_fetch_latency_ms,
            total_pipeline_executions: stats.total_pipeline_executions,
            avg_total_latency_ms: stats.avg_total_latency_ms,
        }
    }
}

// =============================================================================
// REQUEST/RESPONSE TYPES
// =============================================================================

#[derive(Debug, Serialize)]
pub struct TablesResponse {
    pub tables: Vec<TableSummary>,
    pub total: usize,
}

#[derive(Debug, Serialize)]
pub struct TableSummary {
    pub name: String,
    pub temperature: String,
    pub workload: String,
    pub access_pattern: String,
    pub column_count: usize,
    pub shard_key: Option<String>,
    pub row_count_estimate: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct TableDetails {
    pub name: String,
    pub columns: Vec<ColumnDetails>,
    pub temperature: String,
    pub workload: String,
    pub access_pattern: String,
    pub primary_key: Vec<String>,
    pub shard_key: Option<String>,
    pub row_count_estimate: Option<u64>,
    pub size_bytes: Option<u64>,
    pub partition_key: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ColumnDetails {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub is_indexed: bool,
    pub default_value: Option<String>,
    pub storage_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterTableRequest {
    pub name: String,
    pub columns: Vec<ColumnRequest>,
    pub temperature: String,
    pub workload: String,
    pub access_pattern: String,
    pub primary_key: Vec<String>,
    pub shard_key: Option<String>,
    pub row_count_estimate: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ColumnRequest {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub is_indexed: Option<bool>,
    pub default_value: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClassifyRequest {
    pub table_name: String,
    pub temperature: String,
    pub workload: String,
}

#[derive(Debug, Serialize)]
pub struct ClassificationResult {
    pub table_name: String,
    pub previous_temperature: String,
    pub new_temperature: String,
    pub previous_workload: String,
    pub new_workload: String,
}

#[derive(Debug, Serialize)]
pub struct ClassificationSuggestion {
    pub table_name: String,
    pub query_count: u64,
    pub suggested_temperature: Option<String>,
    pub suggested_workload: Option<String>,
    pub confidence: f64,
    pub sample_size_sufficient: bool,
}

#[derive(Debug, Deserialize)]
pub struct AnalyzeRequest {
    pub query: String,
}

#[derive(Debug, Serialize)]
pub struct AnalysisResult {
    pub query: String,
    pub tables: Vec<String>,
    pub access_pattern: String,
    pub shard_keys: Vec<String>,
    pub is_read_only: bool,
    pub estimated_complexity: u32,
    pub estimated_selectivity: f64,
    pub has_aggregation: bool,
    pub has_join: bool,
    pub has_subquery: bool,
    pub columns: Vec<String>,
    pub detected_workload: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RouteRequest {
    pub query: String,
}

#[derive(Debug, Serialize)]
pub struct RouteResult {
    pub query: String,
    pub target_type: String,
    pub reason: String,
    pub preferred_node: Option<String>,
    pub alternative_nodes: Vec<String>,
    pub estimated_latency_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct RoutingStatsResponse {
    pub total_queries_routed: u64,
    pub queries_to_primary: u64,
    pub queries_to_replica: u64,
    pub queries_scattered: u64,
    pub avg_latency_ms: f64,
    pub cache_hit_rate: f64,
}

#[derive(Debug, Serialize)]
pub struct TableStatsResponse {
    pub table_name: String,
    pub query_count: u64,
    pub avg_latency_ms: f64,
    pub hit_rate: f64,
    pub temperature: String,
    pub workload: String,
}

#[derive(Debug, Serialize)]
pub struct WorkloadStatsResponse {
    pub workload: String,
    pub query_count: u64,
    pub avg_latency_ms: f64,
    pub queries_to_primary: u64,
    pub queries_to_replica: u64,
}

#[derive(Debug, Serialize)]
pub struct DiscoveryResult {
    pub tables_discovered: usize,
    pub table_names: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RefreshResult {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct AIWorkloadStatsResponse {
    pub embedding_queries: u64,
    pub context_lookups: u64,
    pub knowledge_base_queries: u64,
    pub tool_executions: u64,
    pub total_ai_queries: u64,
    pub avg_vector_dimensions: f64,
}

#[derive(Debug, Serialize)]
pub struct RAGStatsResponse {
    pub retrieval_count: u64,
    pub avg_retrieval_latency_ms: f64,
    pub fetch_count: u64,
    pub avg_fetch_latency_ms: f64,
    pub total_pipeline_executions: u64,
    pub avg_total_latency_ms: f64,
}

// =============================================================================
// ERRORS
// =============================================================================

#[derive(Debug)]
pub enum AdminError {
    NotFound(String),
    InvalidInput(String),
    DiscoveryError(String),
    InternalError(String),
}

impl std::fmt::Display for AdminError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(msg) => write!(f, "Not found: {}", msg),
            Self::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            Self::DiscoveryError(msg) => write!(f, "Discovery error: {}", msg),
            Self::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for AdminError {}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

fn parse_access_pattern(s: &str) -> Option<AccessPattern> {
    match s.to_uppercase().as_str() {
        "POINTLOOKUP" | "POINT_LOOKUP" => Some(AccessPattern::PointLookup),
        "RANGESCAN" | "RANGE_SCAN" => Some(AccessPattern::RangeScan),
        "FULLSCAN" | "FULL_SCAN" => Some(AccessPattern::FullScan),
        "VECTORSEARCH" | "VECTOR_SEARCH" => Some(AccessPattern::VectorSearch),
        "TIMESERIESAPPEND" | "TIME_SERIES_APPEND" => Some(AccessPattern::TimeSeriesAppend),
        "MIXED" => Some(AccessPattern::Mixed),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_access_pattern() {
        assert_eq!(
            parse_access_pattern("PointLookup"),
            Some(AccessPattern::PointLookup)
        );
        assert_eq!(
            parse_access_pattern("POINT_LOOKUP"),
            Some(AccessPattern::PointLookup)
        );
        assert_eq!(
            parse_access_pattern("RangeScan"),
            Some(AccessPattern::RangeScan)
        );
        assert_eq!(
            parse_access_pattern("VectorSearch"),
            Some(AccessPattern::VectorSearch)
        );
        assert_eq!(parse_access_pattern("Mixed"), Some(AccessPattern::Mixed));
        assert_eq!(parse_access_pattern("Invalid"), None);
    }

    #[test]
    fn test_admin_error_display() {
        let err = AdminError::NotFound("users".to_string());
        assert!(err.to_string().contains("Not found"));

        let err = AdminError::InvalidInput("bad temp".to_string());
        assert!(err.to_string().contains("Invalid input"));
    }
}
