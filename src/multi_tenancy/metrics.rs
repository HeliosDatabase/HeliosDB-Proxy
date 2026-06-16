//! Per-Tenant Metrics Collection
//!
//! This module provides comprehensive metrics collection and reporting
//! for multi-tenant deployments.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use dashmap::DashMap;

use super::config::TenantId;

/// Per-tenant metrics tracker
pub struct TenantMetrics {
    /// Metrics per tenant
    tenants: DashMap<TenantId, Arc<TenantStats>>,

    /// Global start time
    start_time: Instant,

    /// Global query counter
    total_queries: AtomicU64,

    /// Global error counter
    total_errors: AtomicU64,
}

impl Default for TenantMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl TenantMetrics {
    /// Create a new metrics tracker
    pub fn new() -> Self {
        Self {
            tenants: DashMap::new(),
            start_time: Instant::now(),
            total_queries: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
        }
    }

    /// Get or create stats for a tenant
    pub fn get_or_create(&self, tenant: &TenantId) -> Arc<TenantStats> {
        self.tenants
            .entry(tenant.clone())
            .or_insert_with(|| Arc::new(TenantStats::new(tenant.clone())))
            .clone()
    }

    /// Get stats for a tenant (if exists)
    pub fn get(&self, tenant: &TenantId) -> Option<Arc<TenantStats>> {
        self.tenants.get(tenant).map(|s| s.clone())
    }

    /// Record a query execution
    pub fn record_query(
        &self,
        tenant: &TenantId,
        duration: Duration,
        rows: u64,
        success: bool,
    ) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        if !success {
            self.total_errors.fetch_add(1, Ordering::Relaxed);
        }

        let stats = self.get_or_create(tenant);
        stats.record_query(duration, rows, success);
    }

    /// Record bytes transferred
    pub fn record_bytes(&self, tenant: &TenantId, bytes_read: u64, bytes_written: u64) {
        let stats = self.get_or_create(tenant);
        stats.record_bytes(bytes_read, bytes_written);
    }

    /// Record a connection event
    pub fn record_connection(&self, tenant: &TenantId, connected: bool) {
        let stats = self.get_or_create(tenant);
        if connected {
            stats.record_connect();
        } else {
            stats.record_disconnect();
        }
    }

    /// Get all tenant IDs
    pub fn tenant_ids(&self) -> Vec<TenantId> {
        self.tenants.iter().map(|e| e.key().clone()).collect()
    }

    /// Get snapshot for all tenants
    pub fn snapshot_all(&self) -> Vec<TenantMetricsSnapshot> {
        self.tenants
            .iter()
            .map(|entry| entry.value().snapshot())
            .collect()
    }

    /// Get snapshot for a specific tenant
    pub fn snapshot(&self, tenant: &TenantId) -> Option<TenantMetricsSnapshot> {
        self.tenants.get(tenant).map(|s| s.snapshot())
    }

    /// Get aggregate snapshot
    pub fn aggregate_snapshot(&self) -> AggregateMetricsSnapshot {
        let mut total_queries = 0u64;
        let mut total_errors = 0u64;
        let mut total_time_us = 0u64;
        let mut total_rows = 0u64;
        let mut total_bytes_read = 0u64;
        let mut total_bytes_written = 0u64;
        let mut active_connections = 0u32;

        for entry in self.tenants.iter() {
            let stats = entry.value();
            total_queries += stats.queries.load(Ordering::Relaxed);
            total_errors += stats.errors.load(Ordering::Relaxed);
            total_time_us += stats.total_time_us.load(Ordering::Relaxed);
            total_rows += stats.rows_processed.load(Ordering::Relaxed);
            total_bytes_read += stats.bytes_read.load(Ordering::Relaxed);
            total_bytes_written += stats.bytes_written.load(Ordering::Relaxed);
            active_connections += stats.active_connections.load(Ordering::Relaxed) as u32;
        }

        let elapsed = self.start_time.elapsed();
        let qps = if elapsed.as_secs() > 0 {
            total_queries as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        AggregateMetricsSnapshot {
            tenant_count: self.tenants.len(),
            total_queries,
            total_errors,
            error_rate: if total_queries > 0 {
                total_errors as f64 / total_queries as f64
            } else {
                0.0
            },
            total_time: Duration::from_micros(total_time_us),
            total_rows,
            total_bytes_read,
            total_bytes_written,
            active_connections,
            qps,
            uptime: elapsed,
        }
    }

    /// Get top tenants by query count
    pub fn top_by_queries(&self, limit: usize) -> Vec<TenantMetricsSnapshot> {
        let mut snapshots: Vec<_> = self.snapshot_all();
        snapshots.sort_by_key(|b| std::cmp::Reverse(b.queries));
        snapshots.truncate(limit);
        snapshots
    }

    /// Get top tenants by total time
    pub fn top_by_time(&self, limit: usize) -> Vec<TenantMetricsSnapshot> {
        let mut snapshots: Vec<_> = self.snapshot_all();
        snapshots.sort_by_key(|b| std::cmp::Reverse(b.total_time));
        snapshots.truncate(limit);
        snapshots
    }

    /// Get top tenants by error count
    pub fn top_by_errors(&self, limit: usize) -> Vec<TenantMetricsSnapshot> {
        let mut snapshots: Vec<_> = self.snapshot_all();
        snapshots.sort_by_key(|b| std::cmp::Reverse(b.errors));
        snapshots.truncate(limit);
        snapshots
    }

    /// Reset metrics for a tenant
    pub fn reset_tenant(&self, tenant: &TenantId) {
        if let Some(stats) = self.tenants.get(tenant) {
            stats.reset();
        }
    }

    /// Reset all metrics
    pub fn reset_all(&self) {
        for entry in self.tenants.iter() {
            entry.value().reset();
        }
        self.total_queries.store(0, Ordering::Relaxed);
        self.total_errors.store(0, Ordering::Relaxed);
    }
}

/// Statistics for a single tenant
pub struct TenantStats {
    /// Tenant ID
    tenant_id: TenantId,

    /// Total queries executed
    queries: AtomicU64,

    /// Total errors
    errors: AtomicU64,

    /// Total execution time (microseconds)
    total_time_us: AtomicU64,

    /// Minimum query time (microseconds)
    min_time_us: AtomicU64,

    /// Maximum query time (microseconds)
    max_time_us: AtomicU64,

    /// Total rows processed
    rows_processed: AtomicU64,

    /// Total bytes read
    bytes_read: AtomicU64,

    /// Total bytes written
    bytes_written: AtomicU64,

    /// Active connections
    active_connections: AtomicU64,

    /// Total connections made
    total_connections: AtomicU64,

    /// Stats creation time
    created_at: Instant,

    /// Last activity time (as duration since creation)
    last_activity_us: AtomicU64,
}

impl TenantStats {
    /// Create new tenant stats
    pub fn new(tenant_id: TenantId) -> Self {
        Self {
            tenant_id,
            queries: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_time_us: AtomicU64::new(0),
            min_time_us: AtomicU64::new(u64::MAX),
            max_time_us: AtomicU64::new(0),
            rows_processed: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            created_at: Instant::now(),
            last_activity_us: AtomicU64::new(0),
        }
    }

    /// Record a query execution
    pub fn record_query(&self, duration: Duration, rows: u64, success: bool) {
        self.queries.fetch_add(1, Ordering::Relaxed);

        if !success {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }

        let duration_us = duration.as_micros() as u64;
        self.total_time_us.fetch_add(duration_us, Ordering::Relaxed);
        self.rows_processed.fetch_add(rows, Ordering::Relaxed);

        // Update min/max
        self.update_min(&self.min_time_us, duration_us);
        self.update_max(&self.max_time_us, duration_us);

        // Update last activity
        let now = self.created_at.elapsed().as_micros() as u64;
        self.last_activity_us.store(now, Ordering::Relaxed);
    }

    /// Record bytes transferred
    pub fn record_bytes(&self, read: u64, written: u64) {
        self.bytes_read.fetch_add(read, Ordering::Relaxed);
        self.bytes_written.fetch_add(written, Ordering::Relaxed);
    }

    /// Record a connection
    pub fn record_connect(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a disconnection
    pub fn record_disconnect(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get a snapshot of current stats
    pub fn snapshot(&self) -> TenantMetricsSnapshot {
        let queries = self.queries.load(Ordering::Relaxed);
        let total_time_us = self.total_time_us.load(Ordering::Relaxed);

        let min_time = {
            let min = self.min_time_us.load(Ordering::Relaxed);
            if min == u64::MAX {
                Duration::ZERO
            } else {
                Duration::from_micros(min)
            }
        };

        TenantMetricsSnapshot {
            tenant_id: self.tenant_id.clone(),
            queries,
            errors: self.errors.load(Ordering::Relaxed),
            total_time: Duration::from_micros(total_time_us),
            avg_time: Duration::from_micros(total_time_us.checked_div(queries).unwrap_or(0)),
            min_time,
            max_time: Duration::from_micros(self.max_time_us.load(Ordering::Relaxed)),
            rows_processed: self.rows_processed.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed) as u32,
            total_connections: self.total_connections.load(Ordering::Relaxed),
            uptime: self.created_at.elapsed(),
            last_activity: Duration::from_micros(self.last_activity_us.load(Ordering::Relaxed)),
        }
    }

    /// Reset all stats
    pub fn reset(&self) {
        self.queries.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.total_time_us.store(0, Ordering::Relaxed);
        self.min_time_us.store(u64::MAX, Ordering::Relaxed);
        self.max_time_us.store(0, Ordering::Relaxed);
        self.rows_processed.store(0, Ordering::Relaxed);
        self.bytes_read.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
    }

    /// Update minimum value atomically
    fn update_min(&self, atomic: &AtomicU64, value: u64) {
        let mut current = atomic.load(Ordering::Relaxed);
        while value < current {
            match atomic.compare_exchange_weak(
                current,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    /// Update maximum value atomically
    fn update_max(&self, atomic: &AtomicU64, value: u64) {
        let mut current = atomic.load(Ordering::Relaxed);
        while value > current {
            match atomic.compare_exchange_weak(
                current,
                value,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }
}

/// Snapshot of tenant metrics
#[derive(Debug, Clone)]
pub struct TenantMetricsSnapshot {
    /// Tenant ID
    pub tenant_id: TenantId,

    /// Total queries executed
    pub queries: u64,

    /// Total errors
    pub errors: u64,

    /// Total execution time
    pub total_time: Duration,

    /// Average execution time
    pub avg_time: Duration,

    /// Minimum execution time
    pub min_time: Duration,

    /// Maximum execution time
    pub max_time: Duration,

    /// Total rows processed
    pub rows_processed: u64,

    /// Total bytes read
    pub bytes_read: u64,

    /// Total bytes written
    pub bytes_written: u64,

    /// Current active connections
    pub active_connections: u32,

    /// Total connections made
    pub total_connections: u64,

    /// Time since stats collection started
    pub uptime: Duration,

    /// Time since last activity
    pub last_activity: Duration,
}

impl TenantMetricsSnapshot {
    /// Calculate queries per second
    pub fn qps(&self) -> f64 {
        if self.uptime.as_secs() > 0 {
            self.queries as f64 / self.uptime.as_secs_f64()
        } else {
            0.0
        }
    }

    /// Calculate error rate
    pub fn error_rate(&self) -> f64 {
        if self.queries > 0 {
            self.errors as f64 / self.queries as f64
        } else {
            0.0
        }
    }

    /// Calculate average rows per query
    pub fn avg_rows(&self) -> f64 {
        if self.queries > 0 {
            self.rows_processed as f64 / self.queries as f64
        } else {
            0.0
        }
    }

    /// Format as JSON-like string
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"tenant_id":"{}","queries":{},"errors":{},"error_rate":{:.4},"avg_time_ms":{:.2},"qps":{:.2},"active_connections":{}}}"#,
            self.tenant_id.0,
            self.queries,
            self.errors,
            self.error_rate(),
            self.avg_time.as_secs_f64() * 1000.0,
            self.qps(),
            self.active_connections
        )
    }
}

/// Aggregate metrics across all tenants
#[derive(Debug, Clone)]
pub struct AggregateMetricsSnapshot {
    /// Number of tenants
    pub tenant_count: usize,

    /// Total queries across all tenants
    pub total_queries: u64,

    /// Total errors across all tenants
    pub total_errors: u64,

    /// Overall error rate
    pub error_rate: f64,

    /// Total execution time
    pub total_time: Duration,

    /// Total rows processed
    pub total_rows: u64,

    /// Total bytes read
    pub total_bytes_read: u64,

    /// Total bytes written
    pub total_bytes_written: u64,

    /// Total active connections
    pub active_connections: u32,

    /// Overall queries per second
    pub qps: f64,

    /// System uptime
    pub uptime: Duration,
}

impl AggregateMetricsSnapshot {
    /// Format as JSON-like string
    pub fn to_json(&self) -> String {
        format!(
            r#"{{"tenant_count":{},"total_queries":{},"total_errors":{},"error_rate":{:.4},"qps":{:.2},"active_connections":{},"uptime_secs":{}}}"#,
            self.tenant_count,
            self.total_queries,
            self.total_errors,
            self.error_rate,
            self.qps,
            self.active_connections,
            self.uptime.as_secs()
        )
    }
}

/// Cost tracking for tenant billing
pub struct TenantCostTracker {
    /// Cost per query
    cost_per_query: f64,

    /// Cost per 1000 rows
    cost_per_1000_rows: f64,

    /// Cost per MB read
    cost_per_mb_read: f64,

    /// Cost per MB written
    cost_per_mb_written: f64,

    /// Cost per connection-second
    #[allow(dead_code)]
    cost_per_conn_second: f64,

    /// Per-tenant accumulated costs
    costs: DashMap<TenantId, TenantCost>,
}

impl TenantCostTracker {
    /// Create with default pricing
    pub fn new() -> Self {
        Self {
            cost_per_query: 0.000001,      // $0.001 per 1000 queries
            cost_per_1000_rows: 0.00001,   // $0.01 per million rows
            cost_per_mb_read: 0.00001,     // $0.01 per GB read
            cost_per_mb_written: 0.0001,   // $0.10 per GB written
            cost_per_conn_second: 0.0,     // Free connections by default
            costs: DashMap::new(),
        }
    }

    /// Set pricing
    pub fn with_pricing(
        mut self,
        per_query: f64,
        per_1000_rows: f64,
        per_mb_read: f64,
        per_mb_written: f64,
    ) -> Self {
        self.cost_per_query = per_query;
        self.cost_per_1000_rows = per_1000_rows;
        self.cost_per_mb_read = per_mb_read;
        self.cost_per_mb_written = per_mb_written;
        self
    }

    /// Calculate and record cost for a query
    pub fn record_query_cost(
        &self,
        tenant: &TenantId,
        rows: u64,
        bytes_read: u64,
        bytes_written: u64,
    ) {
        let cost = self.cost_per_query
            + (rows as f64 / 1000.0) * self.cost_per_1000_rows
            + (bytes_read as f64 / 1_048_576.0) * self.cost_per_mb_read
            + (bytes_written as f64 / 1_048_576.0) * self.cost_per_mb_written;

        self.costs
            .entry(tenant.clone())
            .or_insert_with(TenantCost::new)
            .add_cost(cost);
    }

    /// Get accumulated cost for a tenant
    pub fn get_cost(&self, tenant: &TenantId) -> Option<f64> {
        self.costs.get(tenant).map(|c| c.total_cost())
    }

    /// Get all tenant costs
    pub fn all_costs(&self) -> HashMap<TenantId, f64> {
        self.costs
            .iter()
            .map(|e| (e.key().clone(), e.value().total_cost()))
            .collect()
    }

    /// Reset costs for a tenant
    pub fn reset_tenant(&self, tenant: &TenantId) {
        if let Some(mut cost) = self.costs.get_mut(tenant) {
            cost.reset();
        }
    }

    /// Generate cost report
    pub fn cost_report(&self) -> TenantCostReport {
        let mut entries: Vec<_> = self
            .costs
            .iter()
            .map(|e| TenantCostEntry {
                tenant_id: e.key().clone(),
                total_cost: e.value().total_cost(),
                query_count: e.value().query_count(),
            })
            .collect();

        entries.sort_by(|a, b| b.total_cost.partial_cmp(&a.total_cost).unwrap());

        let total = entries.iter().map(|e| e.total_cost).sum();

        TenantCostReport {
            entries,
            total_cost: total,
            generated_at: SystemTime::now(),
        }
    }
}

impl Default for TenantCostTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Accumulated cost for a tenant
struct TenantCost {
    total: std::sync::atomic::AtomicU64,  // Stored as cost * 1_000_000 for precision
    queries: AtomicU64,
}

impl TenantCost {
    fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            queries: AtomicU64::new(0),
        }
    }

    fn add_cost(&self, cost: f64) {
        let scaled = (cost * 1_000_000.0) as u64;
        self.total.fetch_add(scaled, Ordering::Relaxed);
        self.queries.fetch_add(1, Ordering::Relaxed);
    }

    fn total_cost(&self) -> f64 {
        self.total.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    fn query_count(&self) -> u64 {
        self.queries.load(Ordering::Relaxed)
    }

    fn reset(&mut self) {
        self.total.store(0, Ordering::Relaxed);
        self.queries.store(0, Ordering::Relaxed);
    }
}

/// Cost entry for a tenant
#[derive(Debug, Clone)]
pub struct TenantCostEntry {
    /// Tenant ID
    pub tenant_id: TenantId,

    /// Total accumulated cost
    pub total_cost: f64,

    /// Number of queries
    pub query_count: u64,
}

/// Cost report for all tenants
#[derive(Debug, Clone)]
pub struct TenantCostReport {
    /// Per-tenant cost entries (sorted by cost descending)
    pub entries: Vec<TenantCostEntry>,

    /// Total cost across all tenants
    pub total_cost: f64,

    /// When report was generated
    pub generated_at: SystemTime,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_stats() {
        let tenant = TenantId::new("test");
        let stats = TenantStats::new(tenant.clone());

        stats.record_query(Duration::from_millis(10), 100, true);
        stats.record_query(Duration::from_millis(20), 200, true);
        stats.record_query(Duration::from_millis(5), 50, false);

        let snapshot = stats.snapshot();

        assert_eq!(snapshot.queries, 3);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.rows_processed, 350);
        assert_eq!(snapshot.min_time, Duration::from_millis(5));
        assert_eq!(snapshot.max_time, Duration::from_millis(20));
    }

    #[test]
    fn test_tenant_metrics() {
        let metrics = TenantMetrics::new();

        let tenant_a = TenantId::new("tenant_a");
        let tenant_b = TenantId::new("tenant_b");

        metrics.record_query(&tenant_a, Duration::from_millis(10), 100, true);
        metrics.record_query(&tenant_a, Duration::from_millis(15), 150, true);
        metrics.record_query(&tenant_b, Duration::from_millis(20), 200, false);

        let snapshot_a = metrics.snapshot(&tenant_a).unwrap();
        assert_eq!(snapshot_a.queries, 2);
        assert_eq!(snapshot_a.errors, 0);

        let snapshot_b = metrics.snapshot(&tenant_b).unwrap();
        assert_eq!(snapshot_b.queries, 1);
        assert_eq!(snapshot_b.errors, 1);

        let aggregate = metrics.aggregate_snapshot();
        assert_eq!(aggregate.tenant_count, 2);
        assert_eq!(aggregate.total_queries, 3);
        assert_eq!(aggregate.total_errors, 1);
    }

    #[test]
    fn test_top_tenants() {
        let metrics = TenantMetrics::new();

        for i in 0..5 {
            let tenant = TenantId::new(format!("tenant_{}", i));
            for _ in 0..(i + 1) {
                metrics.record_query(&tenant, Duration::from_millis(10), 10, true);
            }
        }

        let top = metrics.top_by_queries(3);
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].queries, 5);
        assert_eq!(top[1].queries, 4);
        assert_eq!(top[2].queries, 3);
    }

    #[test]
    fn test_connection_tracking() {
        let metrics = TenantMetrics::new();
        let tenant = TenantId::new("test");

        metrics.record_connection(&tenant, true);
        metrics.record_connection(&tenant, true);

        let snapshot = metrics.snapshot(&tenant).unwrap();
        assert_eq!(snapshot.active_connections, 2);
        assert_eq!(snapshot.total_connections, 2);

        metrics.record_connection(&tenant, false);
        let snapshot = metrics.snapshot(&tenant).unwrap();
        assert_eq!(snapshot.active_connections, 1);
        assert_eq!(snapshot.total_connections, 2);
    }

    #[test]
    fn test_bytes_tracking() {
        let metrics = TenantMetrics::new();
        let tenant = TenantId::new("test");

        metrics.record_bytes(&tenant, 1024, 512);
        metrics.record_bytes(&tenant, 2048, 1024);

        let snapshot = metrics.snapshot(&tenant).unwrap();
        assert_eq!(snapshot.bytes_read, 3072);
        assert_eq!(snapshot.bytes_written, 1536);
    }

    #[test]
    fn test_cost_tracker() {
        let tracker = TenantCostTracker::new();
        let tenant = TenantId::new("test");

        tracker.record_query_cost(&tenant, 1000, 1_048_576, 524_288);
        tracker.record_query_cost(&tenant, 500, 0, 0);

        let cost = tracker.get_cost(&tenant).unwrap();
        assert!(cost > 0.0);

        let report = tracker.cost_report();
        assert_eq!(report.entries.len(), 1);
        assert_eq!(report.entries[0].query_count, 2);
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = TenantMetrics::new();
        let tenant = TenantId::new("test");

        metrics.record_query(&tenant, Duration::from_millis(10), 100, true);

        let snapshot = metrics.snapshot(&tenant).unwrap();
        assert_eq!(snapshot.queries, 1);

        metrics.reset_tenant(&tenant);

        let snapshot = metrics.snapshot(&tenant).unwrap();
        assert_eq!(snapshot.queries, 0);
    }

    #[test]
    fn test_snapshot_json() {
        let tenant = TenantId::new("test");
        let stats = TenantStats::new(tenant);
        stats.record_query(Duration::from_millis(10), 100, true);

        let snapshot = stats.snapshot();
        let json = snapshot.to_json();

        assert!(json.contains("\"tenant_id\":\"test\""));
        assert!(json.contains("\"queries\":1"));
    }
}
