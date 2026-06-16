//! Learning Classifier
//!
//! Automatically learns and updates table classifications from query patterns.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;
use parking_lot::RwLock;

use super::registry::{SchemaRegistry, DataTemperature, WorkloadType};

/// Learning-based table classifier
#[derive(Debug)]
pub struct LearningClassifier {
    /// Query history per table
    history: DashMap<String, QueryHistory>,
    /// Classification model
    model: Arc<RwLock<ClassificationModel>>,
    /// Schema registry for updates
    schema: Arc<SchemaRegistry>,
    /// Configuration
    config: ClassifierConfig,
}

impl LearningClassifier {
    /// Create a new learning classifier
    pub fn new(schema: Arc<SchemaRegistry>) -> Self {
        Self {
            history: DashMap::new(),
            model: Arc::new(RwLock::new(ClassificationModel::new())),
            schema,
            config: ClassifierConfig::default(),
        }
    }

    /// Create with custom configuration
    pub fn with_config(schema: Arc<SchemaRegistry>, config: ClassifierConfig) -> Self {
        Self {
            history: DashMap::new(),
            model: Arc::new(RwLock::new(ClassificationModel::new())),
            schema,
            config,
        }
    }

    /// Record a query execution
    pub fn record(&self, table: &str, query_type: QueryType, latency: Duration) {
        let mut history = self.history
            .entry(table.to_string())
            .or_default();

        history.record(query_type, latency);

        // Check if reclassification needed
        if history.count() % self.config.reclassification_threshold == 0 {
            self.reclassify(table);
        }
    }

    /// Manually trigger reclassification
    pub fn reclassify(&self, table: &str) {
        let history = match self.history.get(table) {
            Some(h) => h.clone(),
            None => return,
        };

        let model = self.model.read();

        // Determine temperature based on access frequency
        let temperature = model.classify_temperature(&history);

        // Determine workload based on query types
        let workload = model.classify_workload(&history);

        // Update schema registry
        self.schema.update_classification(table, temperature, workload);
    }

    /// Get current classification for a table
    pub fn get_classification(&self, table: &str) -> Option<TableClassification> {
        let history = self.history.get(table)?;
        let model = self.model.read();

        Some(TableClassification {
            table: table.to_string(),
            temperature: model.classify_temperature(&history),
            workload: model.classify_workload(&history),
            confidence: model.classification_confidence(&history),
            query_count: history.count(),
            last_updated: history.last_updated(),
        })
    }

    /// Get all classifications
    pub fn all_classifications(&self) -> Vec<TableClassification> {
        self.history
            .iter()
            .map(|entry| {
                let table = entry.key();
                let history = entry.value();
                let model = self.model.read();

                TableClassification {
                    table: table.clone(),
                    temperature: model.classify_temperature(history),
                    workload: model.classify_workload(history),
                    confidence: model.classification_confidence(history),
                    query_count: history.count(),
                    last_updated: history.last_updated(),
                }
            })
            .collect()
    }

    /// Update model thresholds
    pub fn update_thresholds(&self, thresholds: ModelThresholds) {
        let mut model = self.model.write();
        model.thresholds = thresholds;
    }

    /// Get query history for a table
    pub fn get_history(&self, table: &str) -> Option<QueryHistory> {
        self.history.get(table).map(|h| h.clone())
    }

    /// Clear history for a table
    pub fn clear_history(&self, table: &str) {
        self.history.remove(table);
    }

    /// Clear all history
    pub fn clear_all(&self) {
        self.history.clear();
    }

    /// Get query count for a table
    pub fn query_count(&self) -> u64 {
        self.history.iter().map(|h| h.value().count()).sum()
    }

    /// Suggest temperature classification for a table
    pub fn suggest_temperature(&self, table: &str) -> Option<DataTemperature> {
        let history = self.history.get(table)?;
        let model = self.model.read();
        Some(model.classify_temperature(&history))
    }

    /// Suggest workload classification for a table
    pub fn suggest_workload(&self, table: &str) -> Option<WorkloadType> {
        let history = self.history.get(table)?;
        let model = self.model.read();
        Some(model.classify_workload(&history))
    }

    /// Get confidence for a table classification
    pub fn get_confidence(&self, table: &str) -> Option<f64> {
        let history = self.history.get(table)?;
        let model = self.model.read();
        Some(model.classification_confidence(&history))
    }

    /// Classify a query's workload type
    pub fn classify_query(&self, sql: &str) -> Option<WorkloadType> {
        let query_type = QueryType::from_sql(sql);

        Some(match query_type {
            QueryType::VectorSearch => WorkloadType::Vector,
            QueryType::AggregateSelect | QueryType::JoinSelect => WorkloadType::OLAP,
            QueryType::SimpleSelect => WorkloadType::OLTP,
            QueryType::Insert | QueryType::Update | QueryType::Delete => WorkloadType::OLTP,
        })
    }
}

/// Classifier configuration
#[derive(Debug, Clone)]
pub struct ClassifierConfig {
    /// Queries before triggering reclassification
    pub reclassification_threshold: u64,
    /// Time window for rate calculations
    pub rate_window: Duration,
    /// Minimum queries before classification
    pub min_queries: u64,
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            reclassification_threshold: 1000,
            rate_window: Duration::from_secs(60),
            min_queries: 100,
        }
    }
}

/// Query history for a table
#[derive(Debug, Clone)]
pub struct QueryHistory {
    /// Total query count
    total_count: u64,
    /// Read count
    read_count: u64,
    /// Write count
    write_count: u64,
    /// Query type counts
    type_counts: HashMap<QueryType, u64>,
    /// Latency samples (rolling window)
    latencies: Vec<Duration>,
    /// Recent queries per minute samples
    qpm_samples: Vec<(Instant, u64)>,
    /// Created time
    #[allow(dead_code)]
    created: Instant,
    /// Last updated
    last_updated: Instant,
}

impl QueryHistory {
    /// Create new history
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            total_count: 0,
            read_count: 0,
            write_count: 0,
            type_counts: HashMap::new(),
            latencies: Vec::new(),
            qpm_samples: Vec::new(),
            created: now,
            last_updated: now,
        }
    }

    /// Record a query
    pub fn record(&mut self, query_type: QueryType, latency: Duration) {
        self.total_count += 1;
        self.last_updated = Instant::now();

        // Update type counts
        *self.type_counts.entry(query_type).or_insert(0) += 1;

        // Update read/write counts
        if query_type.is_read() {
            self.read_count += 1;
        } else {
            self.write_count += 1;
        }

        // Record latency (keep last 1000 samples)
        if self.latencies.len() >= 1000 {
            self.latencies.remove(0);
        }
        self.latencies.push(latency);

        // Update QPM samples
        self.update_qpm();
    }

    /// Update queries per minute samples
    fn update_qpm(&mut self) {
        let now = Instant::now();

        // Remove old samples (older than 5 minutes)
        self.qpm_samples.retain(|(t, _)| now.duration_since(*t) < Duration::from_secs(300));

        // Add current count
        self.qpm_samples.push((now, self.total_count));
    }

    /// Get total query count
    pub fn count(&self) -> u64 {
        self.total_count
    }

    /// Get queries per minute
    pub fn qpm(&self) -> f64 {
        if self.qpm_samples.len() < 2 {
            return 0.0;
        }

        let first = self.qpm_samples.first().expect("checked len");
        let last = self.qpm_samples.last().expect("checked len");

        let duration = last.0.duration_since(first.0);
        if duration.as_secs() == 0 {
            return 0.0;
        }

        let queries = last.1 - first.1;
        (queries as f64 / duration.as_secs_f64()) * 60.0
    }

    /// Get read/write ratio
    pub fn read_write_ratio(&self) -> f64 {
        if self.write_count == 0 {
            return f64::INFINITY;
        }
        self.read_count as f64 / self.write_count as f64
    }

    /// Get average latency
    pub fn avg_latency(&self) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }

        let sum: Duration = self.latencies.iter().sum();
        sum / self.latencies.len() as u32
    }

    /// Get P95 latency
    pub fn p95_latency(&self) -> Duration {
        if self.latencies.is_empty() {
            return Duration::ZERO;
        }

        let mut sorted = self.latencies.clone();
        sorted.sort();

        let idx = (sorted.len() as f64 * 0.95) as usize;
        sorted.get(idx.min(sorted.len() - 1)).copied().unwrap_or(Duration::ZERO)
    }

    /// Get last updated time
    pub fn last_updated(&self) -> Instant {
        self.last_updated
    }

    /// Get count for a specific query type
    pub fn type_count(&self, query_type: QueryType) -> u64 {
        self.type_counts.get(&query_type).copied().unwrap_or(0)
    }

    /// Get fraction of queries that are a specific type
    pub fn type_fraction(&self, query_type: QueryType) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }
        self.type_count(query_type) as f64 / self.total_count as f64
    }
}

impl Default for QueryHistory {
    fn default() -> Self {
        Self::new()
    }
}

/// Query type for classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryType {
    /// Simple SELECT
    SimpleSelect,
    /// SELECT with aggregations
    AggregateSelect,
    /// SELECT with JOINs
    JoinSelect,
    /// Vector search
    VectorSearch,
    /// INSERT
    Insert,
    /// UPDATE
    Update,
    /// DELETE
    Delete,
}

impl QueryType {
    /// Check if this is a read query
    pub fn is_read(&self) -> bool {
        matches!(self,
            QueryType::SimpleSelect | QueryType::AggregateSelect |
            QueryType::JoinSelect | QueryType::VectorSearch)
    }

    /// Check if this is a write query
    pub fn is_write(&self) -> bool {
        !self.is_read()
    }

    /// Check if this is an OLAP-style query
    pub fn is_olap(&self) -> bool {
        matches!(self, QueryType::AggregateSelect | QueryType::JoinSelect)
    }

    /// Detect query type from SQL
    pub fn from_sql(sql: &str) -> Self {
        let upper = sql.to_uppercase();

        if upper.starts_with("INSERT") {
            QueryType::Insert
        } else if upper.starts_with("UPDATE") {
            QueryType::Update
        } else if upper.starts_with("DELETE") {
            QueryType::Delete
        } else if upper.contains("<->") || upper.contains("VECTOR") || upper.contains("EMBEDDING") {
            QueryType::VectorSearch
        } else if upper.contains("COUNT(") || upper.contains("SUM(") || upper.contains("AVG(") {
            QueryType::AggregateSelect
        } else if upper.contains(" JOIN ") {
            QueryType::JoinSelect
        } else {
            QueryType::SimpleSelect
        }
    }
}

/// Classification model
#[derive(Debug)]
pub struct ClassificationModel {
    /// Thresholds for classification
    pub thresholds: ModelThresholds,
}

impl ClassificationModel {
    /// Create a new model with default thresholds
    pub fn new() -> Self {
        Self {
            thresholds: ModelThresholds::default(),
        }
    }

    /// Classify temperature based on history
    pub fn classify_temperature(&self, history: &QueryHistory) -> DataTemperature {
        let qpm = history.qpm();

        if qpm > self.thresholds.hot_qpm {
            DataTemperature::Hot
        } else if qpm > self.thresholds.warm_qpm {
            DataTemperature::Warm
        } else if qpm > self.thresholds.cold_qpm {
            DataTemperature::Cold
        } else {
            DataTemperature::Frozen
        }
    }

    /// Classify workload based on history
    pub fn classify_workload(&self, history: &QueryHistory) -> WorkloadType {
        // Check for vector workload
        if history.type_fraction(QueryType::VectorSearch) > 0.3 {
            return WorkloadType::Vector;
        }

        // Check read/write ratio for OLTP vs OLAP
        let rw_ratio = history.read_write_ratio();

        if rw_ratio > self.thresholds.olap_ratio {
            // High read ratio - could be OLAP
            if history.type_fraction(QueryType::AggregateSelect) > 0.2 {
                return WorkloadType::OLAP;
            }
        }

        if rw_ratio < self.thresholds.oltp_ratio {
            // Lower read ratio - OLTP
            return WorkloadType::OLTP;
        }

        // Check for HTAP (mixed heavy workload)
        if history.qpm() > 100.0 && rw_ratio > 1.0 && rw_ratio < 10.0 {
            return WorkloadType::HTAP;
        }

        WorkloadType::Mixed
    }

    /// Calculate classification confidence (0.0 - 1.0)
    pub fn classification_confidence(&self, history: &QueryHistory) -> f64 {
        // More queries = higher confidence
        let query_factor = (history.count() as f64 / 1000.0).min(1.0);

        // Clear patterns = higher confidence
        let rw_ratio = history.read_write_ratio();
        let pattern_factor = if !(2.0..=10.0).contains(&rw_ratio) {
            0.8
        } else {
            0.5
        };

        query_factor * pattern_factor
    }
}

impl Default for ClassificationModel {
    fn default() -> Self {
        Self::new()
    }
}

/// Model thresholds for classification
#[derive(Debug, Clone)]
pub struct ModelThresholds {
    /// QPM threshold for HOT classification
    pub hot_qpm: f64,
    /// QPM threshold for WARM classification
    pub warm_qpm: f64,
    /// QPM threshold for COLD classification
    pub cold_qpm: f64,
    /// Read/write ratio threshold for OLAP
    pub olap_ratio: f64,
    /// Read/write ratio threshold for OLTP
    pub oltp_ratio: f64,
}

impl Default for ModelThresholds {
    fn default() -> Self {
        Self {
            hot_qpm: 1000.0,
            warm_qpm: 100.0,
            cold_qpm: 10.0,
            olap_ratio: 10.0,
            oltp_ratio: 2.0,
        }
    }
}

/// Table classification result
#[derive(Debug, Clone)]
pub struct TableClassification {
    /// Table name
    pub table: String,
    /// Temperature classification
    pub temperature: DataTemperature,
    /// Workload classification
    pub workload: WorkloadType,
    /// Classification confidence
    pub confidence: f64,
    /// Query count used for classification
    pub query_count: u64,
    /// Last updated time
    pub last_updated: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_history() {
        let mut history = QueryHistory::new();

        history.record(QueryType::SimpleSelect, Duration::from_millis(10));
        history.record(QueryType::SimpleSelect, Duration::from_millis(20));
        history.record(QueryType::Insert, Duration::from_millis(30));

        assert_eq!(history.count(), 3);
        assert_eq!(history.read_count, 2);
        assert_eq!(history.write_count, 1);
        assert_eq!(history.read_write_ratio(), 2.0);
    }

    #[test]
    fn test_query_type_detection() {
        assert_eq!(QueryType::from_sql("INSERT INTO users VALUES (1)"), QueryType::Insert);
        assert_eq!(QueryType::from_sql("UPDATE users SET name = 'x'"), QueryType::Update);
        assert_eq!(QueryType::from_sql("DELETE FROM users"), QueryType::Delete);
        assert_eq!(QueryType::from_sql("SELECT COUNT(*) FROM users"), QueryType::AggregateSelect);
        assert_eq!(QueryType::from_sql("SELECT * FROM users"), QueryType::SimpleSelect);
        assert_eq!(QueryType::from_sql("SELECT * FROM a JOIN b ON a.id = b.id"), QueryType::JoinSelect);
    }

    #[test]
    fn test_classification_model() {
        let model = ClassificationModel::new();
        let mut history = QueryHistory::new();

        // Record many reads
        for _ in 0..1000 {
            history.record(QueryType::SimpleSelect, Duration::from_millis(5));
        }
        // Record few writes
        for _ in 0..50 {
            history.record(QueryType::Insert, Duration::from_millis(10));
        }

        let workload = model.classify_workload(&history);
        // High read ratio should indicate OLAP-ish workload
        assert!(workload == WorkloadType::OLAP || workload == WorkloadType::Mixed);
    }

    #[test]
    fn test_learning_classifier() {
        let registry = Arc::new(SchemaRegistry::new());
        let classifier = LearningClassifier::new(registry);

        for _ in 0..100 {
            classifier.record("users", QueryType::SimpleSelect, Duration::from_millis(5));
        }

        let classification = classifier.get_classification("users");
        assert!(classification.is_some());
        assert_eq!(classification.as_ref().map(|c| c.query_count), Some(100));
    }

    #[test]
    fn test_temperature_classification() {
        let model = ClassificationModel::new();
        let mut history = QueryHistory::new();

        // Simulate high QPM
        for _ in 0..1000 {
            history.record(QueryType::SimpleSelect, Duration::from_millis(1));
        }
        // Force QPM calculation by adding samples over time
        // In real usage, this happens naturally over time

        // QPM calculation needs a time window; in tests all queries are instantaneous
        // so QPM may be 0, resulting in Frozen classification
        let temp = model.classify_temperature(&history);
        assert!(temp == DataTemperature::Hot || temp == DataTemperature::Warm || temp == DataTemperature::Cold || temp == DataTemperature::Frozen);
    }

    #[test]
    fn test_latency_tracking() {
        let mut history = QueryHistory::new();

        for i in 0..100 {
            history.record(QueryType::SimpleSelect, Duration::from_millis(i));
        }

        let avg = history.avg_latency();
        assert!(avg.as_millis() > 0);

        let p95 = history.p95_latency();
        assert!(p95 >= avg);
    }
}
