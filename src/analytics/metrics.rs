//! Analytics Metrics
//!
//! Track aggregated query metrics and provide snapshots.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;

use super::fingerprinter::{QueryFingerprint, OperationType};
use super::statistics::QueryExecution;
use super::intent::QueryIntent;

/// Per-operation metrics
struct OperationMetrics {
    /// Query count
    count: AtomicU64,
    /// Total time in microseconds
    total_time_us: AtomicU64,
    /// Error count
    errors: AtomicU64,
    /// Total rows
    rows: AtomicU64,
}

impl OperationMetrics {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_time_us: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            rows: AtomicU64::new(0),
        }
    }

    fn record(&self, execution: &QueryExecution) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_time_us
            .fetch_add(execution.duration.as_micros() as u64, Ordering::Relaxed);
        self.rows
            .fetch_add(execution.rows as u64, Ordering::Relaxed);

        if execution.error.is_some() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> OperationSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let total_time_us = self.total_time_us.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);
        let rows = self.rows.load(Ordering::Relaxed);

        let avg_time_us = if count > 0 {
            total_time_us / count
        } else {
            0
        };

        OperationSnapshot {
            count,
            total_time: Duration::from_micros(total_time_us),
            avg_time: Duration::from_micros(avg_time_us),
            errors,
            error_rate: if count > 0 {
                errors as f64 / count as f64
            } else {
                0.0
            },
            rows,
        }
    }

    fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.total_time_us.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.rows.store(0, Ordering::Relaxed);
    }
}

/// Snapshot of operation metrics
#[derive(Debug, Clone)]
pub struct OperationSnapshot {
    pub count: u64,
    pub total_time: Duration,
    pub avg_time: Duration,
    pub errors: u64,
    pub error_rate: f64,
    pub rows: u64,
}

/// Per-intent metrics
struct IntentMetrics {
    /// Query count
    count: AtomicU64,
    /// Total time in microseconds
    total_time_us: AtomicU64,
    /// Cache hits (for retrieval intent)
    cache_hits: AtomicU64,
}

impl IntentMetrics {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            total_time_us: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
        }
    }

    fn record(&self, duration: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_time_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> super::IntentStats {
        let count = self.count.load(Ordering::Relaxed);
        let total_us = self.total_time_us.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);

        super::IntentStats {
            calls: count,
            total_time_ms: total_us / 1000,
            avg_time_ms: if count > 0 {
                (total_us as f64 / count as f64) / 1000.0
            } else {
                0.0
            },
            cache_hit_ratio: if count > 0 {
                cache_hits as f64 / count as f64
            } else {
                0.0
            },
        }
    }

    fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.total_time_us.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
    }
}

/// Query metric entry (for recent queries tracking)
#[derive(Debug, Clone)]
pub struct QueryMetricEntry {
    pub fingerprint_hash: u64,
    pub normalized: String,
    pub duration: Duration,
    pub timestamp_nanos: u64,
    pub user: String,
    pub database: String,
    pub intent: QueryIntent,
}

/// Analytics metrics aggregator
pub struct AnalyticsMetrics {
    /// Total query count
    total_queries: AtomicU64,

    /// Total time in microseconds
    total_time_us: AtomicU64,

    /// Total errors
    total_errors: AtomicU64,

    /// Per-operation metrics
    operations: DashMap<OperationType, OperationMetrics>,

    /// Per-intent metrics
    intents: DashMap<QueryIntent, IntentMetrics>,

    /// Per-user metrics
    users: DashMap<String, OperationMetrics>,

    /// Per-database metrics
    databases: DashMap<String, OperationMetrics>,

    /// Per-node metrics
    nodes: DashMap<String, OperationMetrics>,

    /// Recent query entries (for debugging)
    recent: RwLock<Vec<QueryMetricEntry>>,

    /// Max recent entries
    max_recent: usize,
}

impl AnalyticsMetrics {
    /// Create new metrics aggregator
    pub fn new() -> Self {
        Self::with_max_recent(100)
    }

    /// Create with custom recent entries limit
    pub fn with_max_recent(max_recent: usize) -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            total_time_us: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            operations: DashMap::new(),
            intents: DashMap::new(),
            users: DashMap::new(),
            databases: DashMap::new(),
            nodes: DashMap::new(),
            recent: RwLock::new(Vec::new()),
            max_recent,
        }
    }

    /// Record query execution
    pub fn record(
        &self,
        fingerprint: &QueryFingerprint,
        execution: &QueryExecution,
        intent: QueryIntent,
    ) {
        // Global counters
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        self.total_time_us
            .fetch_add(execution.duration.as_micros() as u64, Ordering::Relaxed);

        if execution.error.is_some() {
            self.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        // Per-operation
        self.operations
            .entry(fingerprint.operation)
            .or_insert_with(OperationMetrics::new)
            .record(execution);

        // Per-intent
        self.intents
            .entry(intent)
            .or_insert_with(IntentMetrics::new)
            .record(execution.duration);

        // Per-user
        self.users
            .entry(execution.user.clone())
            .or_insert_with(OperationMetrics::new)
            .record(execution);

        // Per-database
        self.databases
            .entry(execution.database.clone())
            .or_insert_with(OperationMetrics::new)
            .record(execution);

        // Per-node
        self.nodes
            .entry(execution.node.clone())
            .or_insert_with(OperationMetrics::new)
            .record(execution);

        // Recent entries
        {
            let mut recent = self.recent.write();
            if recent.len() >= self.max_recent {
                recent.remove(0);
            }
            recent.push(QueryMetricEntry {
                fingerprint_hash: fingerprint.hash,
                normalized: fingerprint.normalized.clone(),
                duration: execution.duration,
                timestamp_nanos: now_nanos(),
                user: execution.user.clone(),
                database: execution.database.clone(),
                intent,
            });
        }
    }

    /// Record cache hit for an intent
    pub fn record_cache_hit(&self, intent: QueryIntent) {
        self.intents
            .entry(intent)
            .or_insert_with(IntentMetrics::new)
            .record_cache_hit();
    }

    /// Get snapshot of all metrics
    pub fn snapshot(&self) -> AnalyticsSnapshot {
        let total_queries = self.total_queries.load(Ordering::Relaxed);
        let total_time_us = self.total_time_us.load(Ordering::Relaxed);
        let total_errors = self.total_errors.load(Ordering::Relaxed);

        let operations: HashMap<_, _> = self
            .operations
            .iter()
            .map(|r| (*r.key(), r.value().snapshot()))
            .collect();

        let users: HashMap<_, _> = self
            .users
            .iter()
            .map(|r| (r.key().clone(), r.value().snapshot()))
            .collect();

        let databases: HashMap<_, _> = self
            .databases
            .iter()
            .map(|r| (r.key().clone(), r.value().snapshot()))
            .collect();

        let nodes: HashMap<_, _> = self
            .nodes
            .iter()
            .map(|r| (r.key().clone(), r.value().snapshot()))
            .collect();

        AnalyticsSnapshot {
            total_queries,
            total_time: Duration::from_micros(total_time_us),
            total_errors,
            error_rate: if total_queries > 0 {
                total_errors as f64 / total_queries as f64
            } else {
                0.0
            },
            qps: 0.0, // Would need time tracking for accurate QPS
            avg_time: if total_queries > 0 {
                Duration::from_micros(total_time_us / total_queries)
            } else {
                Duration::ZERO
            },
            by_operation: operations,
            by_user: users,
            by_database: databases,
            by_node: nodes,
        }
    }

    /// Get metrics by intent
    pub fn by_intent(&self) -> HashMap<QueryIntent, super::IntentStats> {
        self.intents
            .iter()
            .map(|r| (*r.key(), r.value().snapshot()))
            .collect()
    }

    /// Get recent queries
    pub fn recent_queries(&self, limit: usize) -> Vec<QueryMetricEntry> {
        let recent = self.recent.read();
        recent.iter().rev().take(limit).cloned().collect()
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_queries.store(0, Ordering::Relaxed);
        self.total_time_us.store(0, Ordering::Relaxed);
        self.total_errors.store(0, Ordering::Relaxed);

        for entry in self.operations.iter() {
            entry.value().reset();
        }
        for entry in self.intents.iter() {
            entry.value().reset();
        }
        for entry in self.users.iter() {
            entry.value().reset();
        }
        for entry in self.databases.iter() {
            entry.value().reset();
        }
        for entry in self.nodes.iter() {
            entry.value().reset();
        }

        self.recent.write().clear();
    }
}

impl Default for AnalyticsMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of analytics metrics
#[derive(Debug, Clone)]
pub struct AnalyticsSnapshot {
    /// Total queries executed
    pub total_queries: u64,

    /// Total execution time
    pub total_time: Duration,

    /// Total errors
    pub total_errors: u64,

    /// Error rate (0.0 - 1.0)
    pub error_rate: f64,

    /// Queries per second (approximate)
    pub qps: f64,

    /// Average query time
    pub avg_time: Duration,

    /// Metrics by operation type
    pub by_operation: HashMap<OperationType, OperationSnapshot>,

    /// Metrics by user
    pub by_user: HashMap<String, OperationSnapshot>,

    /// Metrics by database
    pub by_database: HashMap<String, OperationSnapshot>,

    /// Metrics by node
    pub by_node: HashMap<String, OperationSnapshot>,
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::fingerprinter::QueryFingerprinter;

    #[test]
    fn test_metrics_new() {
        let metrics = AnalyticsMetrics::new();
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_queries, 0);
        assert_eq!(snapshot.total_errors, 0);
    }

    #[test]
    fn test_metrics_record() {
        let metrics = AnalyticsMetrics::new();
        let fp = QueryFingerprinter::new();

        let fingerprint = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        let execution = QueryExecution::new("SELECT * FROM users WHERE id = 1", Duration::from_millis(10))
            .with_user("alice")
            .with_database("mydb")
            .with_node("primary")
            .with_rows(1);

        metrics.record(&fingerprint, &execution, QueryIntent::Retrieval);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_queries, 1);
        assert!(snapshot.by_operation.contains_key(&OperationType::Select));
        assert!(snapshot.by_user.contains_key("alice"));
        assert!(snapshot.by_database.contains_key("mydb"));
    }

    #[test]
    fn test_metrics_by_intent() {
        let metrics = AnalyticsMetrics::new();
        let fp = QueryFingerprinter::new();

        // Record retrieval query
        let fingerprint = fp.fingerprint("SELECT * FROM users");
        let execution = QueryExecution::new("SELECT * FROM users", Duration::from_millis(5));
        metrics.record(&fingerprint, &execution, QueryIntent::Retrieval);

        // Record storage query
        let fingerprint = fp.fingerprint("INSERT INTO users VALUES (1, 'Alice')");
        let execution = QueryExecution::new("INSERT INTO users VALUES (1, 'Alice')", Duration::from_millis(10));
        metrics.record(&fingerprint, &execution, QueryIntent::Storage);

        let by_intent = metrics.by_intent();
        assert!(by_intent.contains_key(&QueryIntent::Retrieval));
        assert!(by_intent.contains_key(&QueryIntent::Storage));
    }

    #[test]
    fn test_metrics_error_tracking() {
        let metrics = AnalyticsMetrics::new();
        let fp = QueryFingerprinter::new();

        // Record successful query
        let fingerprint = fp.fingerprint("SELECT 1");
        let execution = QueryExecution::new("SELECT 1", Duration::from_millis(1));
        metrics.record(&fingerprint, &execution, QueryIntent::Retrieval);

        // Record failed query
        let execution = QueryExecution::new("SELECT 1", Duration::from_millis(1))
            .with_error("Connection refused");
        metrics.record(&fingerprint, &execution, QueryIntent::Retrieval);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_queries, 2);
        assert_eq!(snapshot.total_errors, 1);
        assert!((snapshot.error_rate - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = AnalyticsMetrics::new();
        let fp = QueryFingerprinter::new();

        let fingerprint = fp.fingerprint("SELECT 1");
        let execution = QueryExecution::new("SELECT 1", Duration::from_millis(1));
        metrics.record(&fingerprint, &execution, QueryIntent::Retrieval);

        metrics.reset();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_queries, 0);
    }

    #[test]
    fn test_recent_queries() {
        let metrics = AnalyticsMetrics::with_max_recent(5);
        let fp = QueryFingerprinter::new();

        // Record 10 queries
        for i in 0..10 {
            let query = format!("SELECT {}", i);
            let fingerprint = fp.fingerprint(&query);
            let execution = QueryExecution::new(query, Duration::from_millis(1));
            metrics.record(&fingerprint, &execution, QueryIntent::Retrieval);
        }

        // Should only keep last 5
        let recent = metrics.recent_queries(10);
        assert_eq!(recent.len(), 5);
    }

    #[test]
    fn test_cache_hit_recording() {
        let metrics = AnalyticsMetrics::new();

        // Record cache hits
        for _ in 0..5 {
            metrics.record_cache_hit(QueryIntent::Retrieval);
        }

        let by_intent = metrics.by_intent();
        if let Some(stats) = by_intent.get(&QueryIntent::Retrieval) {
            assert_eq!(stats.calls, 0); // Only recorded cache hits, no queries
        }
    }
}
