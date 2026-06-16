//! Query Statistics
//!
//! Track execution statistics per query fingerprint.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;

use super::fingerprinter::{OperationType, QueryFingerprint};
use super::histogram::LatencyHistogram;
use super::OrderBy;

/// Query execution record
#[derive(Debug, Clone)]
pub struct QueryExecution {
    /// Query text
    pub query: String,

    /// Execution duration
    pub duration: Duration,

    /// Rows returned/affected
    pub rows: usize,

    /// Error message (if failed)
    pub error: Option<String>,

    /// User who executed the query
    pub user: String,

    /// Client IP address
    pub client_ip: String,

    /// Database name
    pub database: String,

    /// Node that executed the query
    pub node: String,

    /// Session ID (for pattern detection)
    pub session_id: Option<String>,

    /// Workflow ID (for tracing)
    pub workflow_id: Option<String>,

    /// Query parameters (if tracking enabled)
    pub parameters: Option<Vec<String>>,
}

impl QueryExecution {
    /// Create a new execution record
    pub fn new(query: impl Into<String>, duration: Duration) -> Self {
        Self {
            query: query.into(),
            duration,
            rows: 0,
            error: None,
            user: "unknown".to_string(),
            client_ip: "unknown".to_string(),
            database: "default".to_string(),
            node: "primary".to_string(),
            session_id: None,
            workflow_id: None,
            parameters: None,
        }
    }

    pub fn with_rows(mut self, rows: usize) -> Self {
        self.rows = rows;
        self
    }

    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(error.into());
        self
    }

    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    pub fn with_client_ip(mut self, ip: impl Into<String>) -> Self {
        self.client_ip = ip.into();
        self
    }

    pub fn with_database(mut self, db: impl Into<String>) -> Self {
        self.database = db.into();
        self
    }

    pub fn with_node(mut self, node: impl Into<String>) -> Self {
        self.node = node.into();
        self
    }

    pub fn with_session(mut self, session: impl Into<String>) -> Self {
        self.session_id = Some(session.into());
        self
    }

    pub fn with_workflow(mut self, workflow: impl Into<String>) -> Self {
        self.workflow_id = Some(workflow.into());
        self
    }

    /// Check if this execution failed
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

/// Statistics for a single query fingerprint
pub struct QueryStatistics {
    /// Query fingerprint
    fingerprint: QueryFingerprint,

    /// Call count
    calls: AtomicU64,

    /// Total execution time (microseconds)
    total_time_us: AtomicU64,

    /// Minimum execution time (microseconds)
    min_time_us: AtomicU64,

    /// Maximum execution time (microseconds)
    max_time_us: AtomicU64,

    /// Total rows returned
    rows: AtomicU64,

    /// Error count
    errors: AtomicU64,

    /// Latency histogram
    histogram: LatencyHistogram,

    /// First seen timestamp (nanos since epoch)
    first_seen: AtomicU64,

    /// Last seen timestamp (nanos since epoch)
    last_seen: AtomicU64,

    /// Per-user call counts
    users: DashMap<String, AtomicU64>,

    /// Per-client call counts
    clients: DashMap<String, AtomicU64>,

    /// Per-database call counts
    databases: DashMap<String, AtomicU64>,
}

impl QueryStatistics {
    /// Create new statistics for a fingerprint
    pub fn new(fingerprint: QueryFingerprint) -> Self {
        let now = now_nanos();
        Self {
            fingerprint,
            calls: AtomicU64::new(0),
            total_time_us: AtomicU64::new(0),
            min_time_us: AtomicU64::new(u64::MAX),
            max_time_us: AtomicU64::new(0),
            rows: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            histogram: LatencyHistogram::new(),
            first_seen: AtomicU64::new(now),
            last_seen: AtomicU64::new(now),
            users: DashMap::new(),
            clients: DashMap::new(),
            databases: DashMap::new(),
        }
    }

    /// Record an execution
    pub fn record(&self, execution: &QueryExecution) {
        self.calls.fetch_add(1, Ordering::Relaxed);

        let duration_us = execution.duration.as_micros() as u64;
        self.total_time_us.fetch_add(duration_us, Ordering::Relaxed);
        self.rows
            .fetch_add(execution.rows as u64, Ordering::Relaxed);

        if execution.error.is_some() {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }

        // Update min/max
        self.update_min(duration_us);
        self.update_max(duration_us);

        // Record in histogram
        self.histogram.record(execution.duration);

        // Update last seen
        self.last_seen.store(now_nanos(), Ordering::Relaxed);

        // User attribution
        self.users
            .entry(execution.user.clone())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);

        // Client attribution
        self.clients
            .entry(execution.client_ip.clone())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);

        // Database attribution
        self.databases
            .entry(execution.database.clone())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    fn update_min(&self, value: u64) {
        let mut current = self.min_time_us.load(Ordering::Relaxed);
        while value < current {
            match self.min_time_us.compare_exchange_weak(
                current,
                value,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    fn update_max(&self, value: u64) {
        let mut current = self.max_time_us.load(Ordering::Relaxed);
        while value > current {
            match self.max_time_us.compare_exchange_weak(
                current,
                value,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    /// Get fingerprint
    pub fn fingerprint(&self) -> &QueryFingerprint {
        &self.fingerprint
    }

    /// Get call count
    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }

    /// Get average execution time
    pub fn avg_time(&self) -> Duration {
        let total = self.total_time_us.load(Ordering::Relaxed);
        let calls = self.calls.load(Ordering::Relaxed);
        Duration::from_micros(total / calls.max(1))
    }

    /// Get total execution time
    pub fn total_time(&self) -> Duration {
        Duration::from_micros(self.total_time_us.load(Ordering::Relaxed))
    }

    /// Get min execution time
    pub fn min_time(&self) -> Duration {
        let min = self.min_time_us.load(Ordering::Relaxed);
        if min == u64::MAX {
            Duration::ZERO
        } else {
            Duration::from_micros(min)
        }
    }

    /// Get max execution time
    pub fn max_time(&self) -> Duration {
        Duration::from_micros(self.max_time_us.load(Ordering::Relaxed))
    }

    /// Get total rows
    pub fn rows(&self) -> u64 {
        self.rows.load(Ordering::Relaxed)
    }

    /// Get error count
    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    /// Get P50 latency
    pub fn p50(&self) -> Duration {
        self.histogram.percentile(0.50)
    }

    /// Get P90 latency
    pub fn p90(&self) -> Duration {
        self.histogram.percentile(0.90)
    }

    /// Get P99 latency
    pub fn p99(&self) -> Duration {
        self.histogram.percentile(0.99)
    }

    /// Get error rate
    pub fn error_rate(&self) -> f64 {
        let calls = self.calls() as f64;
        if calls == 0.0 {
            return 0.0;
        }
        self.errors() as f64 / calls
    }

    /// Convert to QueryStats
    pub fn to_stats(&self) -> QueryStats {
        QueryStats {
            fingerprint_hash: self.fingerprint.hash,
            normalized: self.fingerprint.normalized.clone(),
            tables: self.fingerprint.tables.clone(),
            operation: self.fingerprint.operation,
            calls: self.calls(),
            total_time: self.total_time(),
            avg_time: self.avg_time(),
            min_time: self.min_time(),
            max_time: self.max_time(),
            rows: self.rows(),
            errors: self.errors(),
            error_rate: self.error_rate(),
            p50: self.p50(),
            p90: self.p90(),
            p99: self.p99(),
            first_seen_nanos: self.first_seen.load(Ordering::Relaxed),
            last_seen_nanos: self.last_seen.load(Ordering::Relaxed),
        }
    }
}

/// Query stats (snapshot of statistics)
#[derive(Debug, Clone)]
pub struct QueryStats {
    pub fingerprint_hash: u64,
    pub normalized: String,
    pub tables: Vec<String>,
    pub operation: OperationType,
    pub calls: u64,
    pub total_time: Duration,
    pub avg_time: Duration,
    pub min_time: Duration,
    pub max_time: Duration,
    pub rows: u64,
    pub errors: u64,
    pub error_rate: f64,
    pub p50: Duration,
    pub p90: Duration,
    pub p99: Duration,
    pub first_seen_nanos: u64,
    pub last_seen_nanos: u64,
}

impl QueryStats {
    /// Get fingerprint short ID
    pub fn short_id(&self) -> String {
        format!("{:016x}", self.fingerprint_hash)
    }
}

/// Statistics store (all fingerprints)
pub struct StatisticsStore {
    /// Statistics by fingerprint hash
    stats: DashMap<u64, QueryStatistics>,

    /// Maximum fingerprints to track
    max_fingerprints: usize,
}

impl StatisticsStore {
    /// Create new statistics store
    pub fn new(max_fingerprints: usize) -> Self {
        Self {
            stats: DashMap::new(),
            max_fingerprints,
        }
    }

    /// Record execution for a fingerprint
    pub fn record(&self, fingerprint: &QueryFingerprint, execution: &QueryExecution) {
        // Enforce max fingerprints before entering the entry API
        // (reading len() inside or_insert_with would deadlock on DashMap)
        if !self.stats.contains_key(&fingerprint.hash) && self.stats.len() >= self.max_fingerprints
        {
            self.evict_oldest();
        }

        let stats = self
            .stats
            .entry(fingerprint.hash)
            .or_insert_with(|| QueryStatistics::new(fingerprint.clone()));

        stats.record(execution);
    }

    /// Get statistics for a fingerprint
    pub fn get(&self, fingerprint_hash: u64) -> Option<QueryStats> {
        self.stats.get(&fingerprint_hash).map(|s| s.to_stats())
    }

    /// Get top queries by metric
    pub fn top(&self, order_by: OrderBy, limit: usize) -> Vec<QueryStats> {
        let mut all: Vec<_> = self.stats.iter().map(|r| r.to_stats()).collect();

        match order_by {
            OrderBy::TotalTime => all.sort_by_key(|b| std::cmp::Reverse(b.total_time)),
            OrderBy::AvgTime => all.sort_by_key(|b| std::cmp::Reverse(b.avg_time)),
            OrderBy::Calls => all.sort_by_key(|b| std::cmp::Reverse(b.calls)),
            OrderBy::Errors => all.sort_by_key(|b| std::cmp::Reverse(b.errors)),
            OrderBy::P99Time => all.sort_by_key(|b| std::cmp::Reverse(b.p99)),
            OrderBy::Rows => all.sort_by_key(|b| std::cmp::Reverse(b.rows)),
        }

        all.truncate(limit);
        all
    }

    /// Get all statistics
    pub fn all(&self) -> Vec<QueryStats> {
        self.stats.iter().map(|r| r.to_stats()).collect()
    }

    /// Get count of tracked fingerprints
    pub fn count(&self) -> usize {
        self.stats.len()
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.stats.clear();
    }

    /// Evict oldest fingerprint
    fn evict_oldest(&self) {
        let oldest = self
            .stats
            .iter()
            .min_by_key(|r| r.last_seen.load(Ordering::Relaxed))
            .map(|r| *r.key());

        if let Some(hash) = oldest {
            self.stats.remove(&hash);
        }
    }
}

fn now_nanos() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_execution_builder() {
        let exec = QueryExecution::new("SELECT 1", Duration::from_millis(5))
            .with_rows(1)
            .with_user("alice")
            .with_database("test");

        assert_eq!(exec.rows, 1);
        assert_eq!(exec.user, "alice");
        assert_eq!(exec.database, "test");
    }

    #[test]
    fn test_query_statistics_record() {
        use crate::analytics::fingerprinter::QueryFingerprinter;

        let fp = QueryFingerprinter::new();
        let fingerprint = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        let stats = QueryStatistics::new(fingerprint);

        let exec =
            QueryExecution::new("SELECT * FROM users WHERE id = 1", Duration::from_millis(5))
                .with_rows(1);

        stats.record(&exec);
        stats.record(&exec);

        assert_eq!(stats.calls(), 2);
        assert_eq!(stats.rows(), 2);
    }

    #[test]
    fn test_statistics_store() {
        use crate::analytics::fingerprinter::QueryFingerprinter;

        let store = StatisticsStore::new(100);
        let fp = QueryFingerprinter::new();

        let fingerprint = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        let exec =
            QueryExecution::new("SELECT * FROM users WHERE id = 1", Duration::from_millis(5));

        store.record(&fingerprint, &exec);
        store.record(&fingerprint, &exec);

        let stats = store.get(fingerprint.hash).unwrap();
        assert_eq!(stats.calls, 2);
    }

    #[test]
    fn test_top_queries() {
        use crate::analytics::fingerprinter::QueryFingerprinter;

        let store = StatisticsStore::new(100);
        let fp = QueryFingerprinter::new();

        // Query 1: 10 calls
        let fp1 = fp.fingerprint("SELECT * FROM users");
        for _ in 0..10 {
            let exec = QueryExecution::new("SELECT * FROM users", Duration::from_millis(1));
            store.record(&fp1, &exec);
        }

        // Query 2: 5 calls
        let fp2 = fp.fingerprint("SELECT * FROM orders");
        for _ in 0..5 {
            let exec = QueryExecution::new("SELECT * FROM orders", Duration::from_millis(1));
            store.record(&fp2, &exec);
        }

        let top = store.top(OrderBy::Calls, 10);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].calls, 10);
        assert_eq!(top[1].calls, 5);
    }
}
