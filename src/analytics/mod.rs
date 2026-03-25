//! Query Analytics & Slow Query Log
//!
//! Comprehensive query analytics at the proxy layer:
//! - Query fingerprinting and normalization
//! - Execution statistics and histograms
//! - Slow query logging
//! - Pattern detection (N+1, bursts)
//! - AI/Agent workload classification

pub mod config;
pub mod fingerprinter;
pub mod statistics;
pub mod slow_log;
pub mod patterns;
pub mod histogram;
pub mod metrics;
pub mod intent;

// Re-exports
pub use config::{AnalyticsConfig, AnalyticsConfigBuilder, SlowQueryConfig, PatternConfig, SamplingConfig};
pub use fingerprinter::{QueryFingerprinter, QueryFingerprint, OperationType};
pub use statistics::{QueryStatistics, QueryExecution, StatisticsStore, QueryStats};
pub use slow_log::{SlowQueryLog, SlowQueryEntry, SlowQueryReader};
pub use patterns::{PatternDetector, NplusOnePattern, QueryBurst, PatternAlert};
pub use histogram::{LatencyHistogram, HistogramBucket, HistogramSnapshot};
pub use metrics::{AnalyticsMetrics, AnalyticsSnapshot, QueryMetricEntry};
pub use intent::{QueryClassifier, QueryIntent, RagAnalytics, WorkflowTracer, WorkflowTrace, CostAttribution};

use std::sync::Arc;
use dashmap::DashMap;
use parking_lot::RwLock;

/// Main analytics engine
pub struct QueryAnalytics {
    /// Configuration
    config: AnalyticsConfig,

    /// Query fingerprinter
    fingerprinter: QueryFingerprinter,

    /// Statistics store (fingerprint -> stats)
    statistics: StatisticsStore,

    /// Slow query log
    slow_log: SlowQueryLog,

    /// Pattern detector
    patterns: PatternDetector,

    /// Metrics
    metrics: AnalyticsMetrics,

    /// Query classifier (AI intent)
    classifier: QueryClassifier,

    /// Workflow tracer
    workflows: WorkflowTracer,

    /// Cost attribution
    costs: CostAttribution,
}

impl QueryAnalytics {
    /// Create new analytics engine
    pub fn new(config: AnalyticsConfig) -> Self {
        let slow_log = SlowQueryLog::new(config.slow_query.clone());
        let patterns = PatternDetector::new(config.patterns.clone());
        let statistics = StatisticsStore::new(config.max_fingerprints);

        Self {
            fingerprinter: QueryFingerprinter::new(),
            statistics,
            slow_log,
            patterns,
            metrics: AnalyticsMetrics::new(),
            classifier: QueryClassifier::new(),
            workflows: WorkflowTracer::new(),
            costs: CostAttribution::new(),
            config,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(AnalyticsConfig::default())
    }

    /// Record query execution
    pub fn record(&self, execution: QueryExecution) {
        if !self.config.enabled {
            return;
        }

        // Apply sampling if configured
        if self.config.sampling.enabled && !self.should_sample() {
            return;
        }

        // Fingerprint the query
        let fingerprint = self.fingerprinter.fingerprint(&execution.query);

        // Record statistics
        self.statistics.record(&fingerprint, &execution);

        // Check for slow query
        self.slow_log.log_if_slow(&execution, &fingerprint);

        // Detect patterns
        if let Some(session) = &execution.session_id {
            self.patterns.record_query(session, &execution, &fingerprint);
        }

        // Classify intent
        let intent = self.classifier.classify(&execution.query);

        // Record metrics
        self.metrics.record(&fingerprint, &execution, intent);

        // Track workflow if applicable
        if let Some(workflow_id) = &execution.workflow_id {
            self.workflows.record_step(workflow_id, &execution);
        }

        // Attribute costs
        self.costs.record(&execution);
    }

    /// Check if we should sample this query
    fn should_sample(&self) -> bool {
        rand::random::<f64>() < self.config.sampling.rate
    }

    /// Get fingerprinter for external use
    pub fn fingerprinter(&self) -> &QueryFingerprinter {
        &self.fingerprinter
    }

    /// Get statistics for a fingerprint
    pub fn get_stats(&self, fingerprint_hash: u64) -> Option<QueryStats> {
        self.statistics.get(fingerprint_hash)
    }

    /// Get top queries by a metric
    pub fn top_queries(&self, order_by: OrderBy, limit: usize) -> Vec<QueryStats> {
        self.statistics.top(order_by, limit)
    }

    /// Get recent slow queries
    pub fn slow_queries(&self, limit: usize) -> Vec<SlowQueryEntry> {
        self.slow_log.recent(limit)
    }

    /// Get detected patterns
    pub fn get_patterns(&self) -> Vec<PatternAlert> {
        self.patterns.get_alerts()
    }

    /// Get metrics snapshot
    pub fn get_metrics(&self) -> AnalyticsSnapshot {
        self.metrics.snapshot()
    }

    /// Get analytics by query intent
    pub fn by_intent(&self) -> std::collections::HashMap<QueryIntent, IntentStats> {
        self.metrics.by_intent()
    }

    /// Get workflow traces
    pub fn get_workflows(&self, limit: usize) -> Vec<WorkflowTrace> {
        self.workflows.recent(limit)
    }

    /// Get cost attribution
    pub fn get_costs(&self) -> CostReport {
        self.costs.report()
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.statistics.reset();
        self.metrics.reset();
        self.workflows.reset();
        self.costs.reset();
    }
}

/// Order by options for top queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBy {
    TotalTime,
    AvgTime,
    Calls,
    Errors,
    P99Time,
    Rows,
}

impl std::str::FromStr for OrderBy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "total_time" | "totaltime" => Ok(OrderBy::TotalTime),
            "avg_time" | "avgtime" => Ok(OrderBy::AvgTime),
            "calls" | "count" => Ok(OrderBy::Calls),
            "errors" => Ok(OrderBy::Errors),
            "p99" | "p99_time" => Ok(OrderBy::P99Time),
            "rows" => Ok(OrderBy::Rows),
            _ => Err(format!("Unknown order by: {}", s)),
        }
    }
}

/// Intent statistics
#[derive(Debug, Clone)]
pub struct IntentStats {
    pub calls: u64,
    pub total_time_ms: u64,
    pub avg_time_ms: f64,
    pub cache_hit_ratio: f64,
}

/// Cost report
#[derive(Debug, Clone)]
pub struct CostReport {
    pub total_queries: u64,
    pub total_time_seconds: f64,
    pub estimated_cost_usd: f64,
    pub by_user: Vec<UserCost>,
    pub by_agent: Vec<AgentCost>,
}

/// Per-user cost
#[derive(Debug, Clone)]
pub struct UserCost {
    pub user: String,
    pub queries: u64,
    pub time_seconds: f64,
    pub cost_usd: f64,
}

/// Per-agent cost
#[derive(Debug, Clone)]
pub struct AgentCost {
    pub agent_id: String,
    pub queries: u64,
    pub time_seconds: f64,
    pub cost_usd: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_analytics_basic() {
        let analytics = QueryAnalytics::with_defaults();

        let execution = QueryExecution {
            query: "SELECT * FROM users WHERE id = 1".to_string(),
            duration: Duration::from_millis(5),
            rows: 1,
            error: None,
            user: "test_user".to_string(),
            client_ip: "127.0.0.1".to_string(),
            database: "test_db".to_string(),
            node: "primary".to_string(),
            session_id: Some("session_1".to_string()),
            workflow_id: None,
            parameters: None,
        };

        analytics.record(execution);

        let top = analytics.top_queries(OrderBy::Calls, 10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].calls, 1);
    }

    #[test]
    fn test_order_by_parse() {
        assert_eq!("total_time".parse::<OrderBy>().unwrap(), OrderBy::TotalTime);
        assert_eq!("calls".parse::<OrderBy>().unwrap(), OrderBy::Calls);
        assert_eq!("p99".parse::<OrderBy>().unwrap(), OrderBy::P99Time);
    }
}
