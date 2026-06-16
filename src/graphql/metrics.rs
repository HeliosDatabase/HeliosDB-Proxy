//! GraphQL Metrics
//!
//! Metrics collection for GraphQL queries.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::{ErrorCode, OperationType};

/// GraphQL metrics collector
#[derive(Debug)]
pub struct GraphQLMetrics {
    /// Query statistics
    query_stats: QueryStats,
    /// Operation metrics by name
    operations: Mutex<HashMap<String, OperationMetrics>>,
    /// Error counts by code
    error_counts: Mutex<HashMap<ErrorCode, u64>>,
    /// Created timestamp
    created_at: Instant,
}

impl GraphQLMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            query_stats: QueryStats::new(),
            operations: Mutex::new(HashMap::new()),
            error_counts: Mutex::new(HashMap::new()),
            created_at: Instant::now(),
        }
    }

    /// Record a query execution
    pub fn record_query(&self, duration: Duration, operation_type: OperationType) {
        self.query_stats.record(duration, operation_type);
    }

    /// Record a named operation
    pub fn record_operation(&self, name: &str, duration: Duration, operation_type: OperationType) {
        let mut operations = self.operations.lock().unwrap();
        let metrics = operations
            .entry(name.to_string())
            .or_insert_with(|| OperationMetrics::new(operation_type));
        metrics.record(duration);
    }

    /// Record an error
    pub fn record_error(&self, error: &super::GraphQLError) {
        let mut counts = self.error_counts.lock().unwrap();
        *counts.entry(error.code).or_insert(0) += 1;
    }

    /// Get query statistics
    pub fn query_stats(&self) -> &QueryStats {
        &self.query_stats
    }

    /// Get operation metrics
    pub fn operation_metrics(&self, name: &str) -> Option<OperationMetrics> {
        self.operations.lock().unwrap().get(name).cloned()
    }

    /// Get all operation metrics
    pub fn all_operations(&self) -> HashMap<String, OperationMetrics> {
        self.operations.lock().unwrap().clone()
    }

    /// Get error counts
    pub fn error_counts(&self) -> HashMap<ErrorCode, u64> {
        self.error_counts.lock().unwrap().clone()
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.query_stats.reset();
        self.operations.lock().unwrap().clear();
        self.error_counts.lock().unwrap().clear();
    }

    /// Export metrics in Prometheus format
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();

        // Query counts
        output.push_str("# HELP helios_graphql_queries_total Total GraphQL queries\n");
        output.push_str("# TYPE helios_graphql_queries_total counter\n");
        output.push_str(&format!(
            "helios_graphql_queries_total{{type=\"query\"}} {}\n",
            self.query_stats.query_count.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "helios_graphql_queries_total{{type=\"mutation\"}} {}\n",
            self.query_stats.mutation_count.load(Ordering::Relaxed)
        ));
        output.push_str(&format!(
            "helios_graphql_queries_total{{type=\"subscription\"}} {}\n",
            self.query_stats.subscription_count.load(Ordering::Relaxed)
        ));

        // Latency
        output.push_str("\n# HELP helios_graphql_latency_ms Query latency in milliseconds\n");
        output.push_str("# TYPE helios_graphql_latency_ms gauge\n");
        if let Some(avg) = self.query_stats.average_duration() {
            output.push_str(&format!(
                "helios_graphql_latency_ms{{quantile=\"avg\"}} {}\n",
                avg.as_millis()
            ));
        }
        if let Some(min) = self.query_stats.min_duration() {
            output.push_str(&format!(
                "helios_graphql_latency_ms{{quantile=\"min\"}} {}\n",
                min.as_millis()
            ));
        }
        if let Some(max) = self.query_stats.max_duration() {
            output.push_str(&format!(
                "helios_graphql_latency_ms{{quantile=\"max\"}} {}\n",
                max.as_millis()
            ));
        }

        // Errors
        output.push_str("\n# HELP helios_graphql_errors_total Total GraphQL errors\n");
        output.push_str("# TYPE helios_graphql_errors_total counter\n");
        for (code, count) in self.error_counts() {
            output.push_str(&format!(
                "helios_graphql_errors_total{{code=\"{:?}\"}} {}\n",
                code, count
            ));
        }

        // Operations
        output.push_str("\n# HELP helios_graphql_operation_calls Operation call counts\n");
        output.push_str("# TYPE helios_graphql_operation_calls counter\n");
        for (name, metrics) in self.all_operations() {
            output.push_str(&format!(
                "helios_graphql_operation_calls{{name=\"{}\"}} {}\n",
                name,
                metrics.call_count.load(Ordering::Relaxed)
            ));
        }

        output
    }
}

impl Default for GraphQLMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Query statistics
#[derive(Debug)]
pub struct QueryStats {
    /// Total query count
    pub query_count: AtomicU64,
    /// Total mutation count
    pub mutation_count: AtomicU64,
    /// Total subscription count
    pub subscription_count: AtomicU64,
    /// Total duration (in microseconds)
    total_duration_us: AtomicU64,
    /// Minimum duration (in microseconds)
    min_duration_us: AtomicU64,
    /// Maximum duration (in microseconds)
    max_duration_us: AtomicU64,
    /// Latency histogram buckets (in microseconds)
    latency_buckets: Mutex<LatencyHistogram>,
}

impl QueryStats {
    /// Create new query statistics
    pub fn new() -> Self {
        Self {
            query_count: AtomicU64::new(0),
            mutation_count: AtomicU64::new(0),
            subscription_count: AtomicU64::new(0),
            total_duration_us: AtomicU64::new(0),
            min_duration_us: AtomicU64::new(u64::MAX),
            max_duration_us: AtomicU64::new(0),
            latency_buckets: Mutex::new(LatencyHistogram::new()),
        }
    }

    /// Record a query execution
    pub fn record(&self, duration: Duration, operation_type: OperationType) {
        let duration_us = duration.as_micros() as u64;

        // Update operation count
        match operation_type {
            OperationType::Query => {
                self.query_count.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::Mutation => {
                self.mutation_count.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::Subscription => {
                self.subscription_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update duration stats
        self.total_duration_us
            .fetch_add(duration_us, Ordering::Relaxed);

        // Update min
        let mut current_min = self.min_duration_us.load(Ordering::Relaxed);
        while duration_us < current_min {
            match self.min_duration_us.compare_exchange_weak(
                current_min,
                duration_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max
        let mut current_max = self.max_duration_us.load(Ordering::Relaxed);
        while duration_us > current_max {
            match self.max_duration_us.compare_exchange_weak(
                current_max,
                duration_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }

        // Update histogram
        self.latency_buckets.lock().unwrap().record(duration_us);
    }

    /// Get total query count
    pub fn total_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
            + self.mutation_count.load(Ordering::Relaxed)
            + self.subscription_count.load(Ordering::Relaxed)
    }

    /// Get average duration
    pub fn average_duration(&self) -> Option<Duration> {
        let total = self.total_count();
        if total == 0 {
            return None;
        }

        let total_us = self.total_duration_us.load(Ordering::Relaxed);
        Some(Duration::from_micros(total_us / total))
    }

    /// Get minimum duration
    pub fn min_duration(&self) -> Option<Duration> {
        let min = self.min_duration_us.load(Ordering::Relaxed);
        if min == u64::MAX {
            None
        } else {
            Some(Duration::from_micros(min))
        }
    }

    /// Get maximum duration
    pub fn max_duration(&self) -> Option<Duration> {
        let max = self.max_duration_us.load(Ordering::Relaxed);
        if max == 0 {
            None
        } else {
            Some(Duration::from_micros(max))
        }
    }

    /// Get percentile duration
    pub fn percentile(&self, p: f64) -> Option<Duration> {
        self.latency_buckets.lock().unwrap().percentile(p)
    }

    /// Reset statistics
    pub fn reset(&self) {
        self.query_count.store(0, Ordering::Relaxed);
        self.mutation_count.store(0, Ordering::Relaxed);
        self.subscription_count.store(0, Ordering::Relaxed);
        self.total_duration_us.store(0, Ordering::Relaxed);
        self.min_duration_us.store(u64::MAX, Ordering::Relaxed);
        self.max_duration_us.store(0, Ordering::Relaxed);
        self.latency_buckets.lock().unwrap().reset();
    }
}

impl Default for QueryStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Operation-specific metrics
#[derive(Debug)]
pub struct OperationMetrics {
    /// Operation type
    pub operation_type: OperationType,
    /// Call count
    pub call_count: AtomicU64,
    /// Total duration (microseconds)
    pub total_duration_us: AtomicU64,
    /// Error count
    pub error_count: AtomicU64,
}

impl Clone for OperationMetrics {
    fn clone(&self) -> Self {
        Self {
            operation_type: self.operation_type,
            call_count: AtomicU64::new(self.call_count.load(Ordering::Relaxed)),
            total_duration_us: AtomicU64::new(self.total_duration_us.load(Ordering::Relaxed)),
            error_count: AtomicU64::new(self.error_count.load(Ordering::Relaxed)),
        }
    }
}

impl OperationMetrics {
    /// Create new operation metrics
    pub fn new(operation_type: OperationType) -> Self {
        Self {
            operation_type,
            call_count: AtomicU64::new(0),
            total_duration_us: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
        }
    }

    /// Record an execution
    pub fn record(&self, duration: Duration) {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        self.total_duration_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record an error
    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get average duration
    pub fn average_duration(&self) -> Option<Duration> {
        let count = self.call_count.load(Ordering::Relaxed);
        if count == 0 {
            return None;
        }

        let total_us = self.total_duration_us.load(Ordering::Relaxed);
        Some(Duration::from_micros(total_us / count))
    }

    /// Get error rate
    pub fn error_rate(&self) -> f64 {
        let count = self.call_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }

        self.error_count.load(Ordering::Relaxed) as f64 / count as f64
    }
}

// Implement Clone for AtomicU64 by loading values
impl Clone for QueryStats {
    fn clone(&self) -> Self {
        Self {
            query_count: AtomicU64::new(self.query_count.load(Ordering::Relaxed)),
            mutation_count: AtomicU64::new(self.mutation_count.load(Ordering::Relaxed)),
            subscription_count: AtomicU64::new(self.subscription_count.load(Ordering::Relaxed)),
            total_duration_us: AtomicU64::new(self.total_duration_us.load(Ordering::Relaxed)),
            min_duration_us: AtomicU64::new(self.min_duration_us.load(Ordering::Relaxed)),
            max_duration_us: AtomicU64::new(self.max_duration_us.load(Ordering::Relaxed)),
            latency_buckets: Mutex::new(self.latency_buckets.lock().unwrap().clone()),
        }
    }
}

/// Latency histogram for percentile calculations
#[derive(Debug, Clone)]
struct LatencyHistogram {
    /// Bucket boundaries (in microseconds)
    boundaries: Vec<u64>,
    /// Bucket counts
    counts: Vec<u64>,
    /// All values for percentile calculation (limited size)
    values: Vec<u64>,
    /// Maximum values to store
    max_values: usize,
}

impl LatencyHistogram {
    /// Create a new histogram
    fn new() -> Self {
        // Boundaries: 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s
        let boundaries = vec![
            100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
        ];
        let counts = vec![0u64; boundaries.len() + 1];

        Self {
            boundaries,
            counts,
            values: Vec::new(),
            max_values: 10000,
        }
    }

    /// Record a value
    fn record(&mut self, value_us: u64) {
        // Update bucket
        let bucket = self
            .boundaries
            .iter()
            .position(|&b| value_us <= b)
            .unwrap_or(self.boundaries.len());
        self.counts[bucket] += 1;

        // Store value for percentile calculation
        if self.values.len() < self.max_values {
            self.values.push(value_us);
        } else {
            // Reservoir sampling
            let idx = rand_index(self.values.len() + 1);
            if idx < self.values.len() {
                self.values[idx] = value_us;
            }
        }
    }

    /// Get percentile value
    fn percentile(&self, p: f64) -> Option<Duration> {
        if self.values.is_empty() {
            return None;
        }

        let mut sorted = self.values.clone();
        sorted.sort_unstable();

        let idx = ((p / 100.0) * (sorted.len() - 1) as f64) as usize;
        Some(Duration::from_micros(sorted[idx]))
    }

    /// Reset histogram
    fn reset(&mut self) {
        for count in &mut self.counts {
            *count = 0;
        }
        self.values.clear();
    }
}

/// Simple random index for reservoir sampling
fn rand_index(max: usize) -> usize {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as usize;
    seed % max
}

/// Metrics reporter trait
pub trait MetricsReporter: Send + Sync {
    /// Report metrics
    fn report(&self, metrics: &GraphQLMetrics);
}

/// Console metrics reporter
pub struct ConsoleReporter;

impl MetricsReporter for ConsoleReporter {
    fn report(&self, metrics: &GraphQLMetrics) {
        let stats = metrics.query_stats();

        println!("=== GraphQL Metrics ===");
        println!("Queries: {}", stats.query_count.load(Ordering::Relaxed));
        println!(
            "Mutations: {}",
            stats.mutation_count.load(Ordering::Relaxed)
        );
        println!(
            "Subscriptions: {}",
            stats.subscription_count.load(Ordering::Relaxed)
        );

        if let Some(avg) = stats.average_duration() {
            println!("Avg latency: {:?}", avg);
        }
        if let Some(min) = stats.min_duration() {
            println!("Min latency: {:?}", min);
        }
        if let Some(max) = stats.max_duration() {
            println!("Max latency: {:?}", max);
        }

        println!("Errors: {:?}", metrics.error_counts());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_stats_recording() {
        let stats = QueryStats::new();

        stats.record(Duration::from_millis(10), OperationType::Query);
        stats.record(Duration::from_millis(20), OperationType::Query);
        stats.record(Duration::from_millis(5), OperationType::Mutation);

        assert_eq!(stats.query_count.load(Ordering::Relaxed), 2);
        assert_eq!(stats.mutation_count.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_count(), 3);
    }

    #[test]
    fn test_query_stats_duration() {
        let stats = QueryStats::new();

        stats.record(Duration::from_millis(10), OperationType::Query);
        stats.record(Duration::from_millis(20), OperationType::Query);
        stats.record(Duration::from_millis(30), OperationType::Query);

        assert_eq!(stats.min_duration(), Some(Duration::from_millis(10)));
        assert_eq!(stats.max_duration(), Some(Duration::from_millis(30)));
        assert_eq!(stats.average_duration(), Some(Duration::from_millis(20)));
    }

    #[test]
    fn test_graphql_metrics() {
        let metrics = GraphQLMetrics::new();

        metrics.record_query(Duration::from_millis(10), OperationType::Query);
        metrics.record_operation("GetUsers", Duration::from_millis(10), OperationType::Query);

        assert_eq!(metrics.query_stats().total_count(), 1);
        assert!(metrics.operation_metrics("GetUsers").is_some());
    }

    #[test]
    fn test_operation_metrics() {
        let metrics = OperationMetrics::new(OperationType::Query);

        metrics.record(Duration::from_millis(10));
        metrics.record(Duration::from_millis(20));
        metrics.record_error();

        assert_eq!(metrics.call_count.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.error_count.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.average_duration(), Some(Duration::from_millis(15)));
        assert_eq!(metrics.error_rate(), 0.5);
    }

    #[test]
    fn test_error_recording() {
        let metrics = GraphQLMetrics::new();

        let error1 = super::super::GraphQLError::parse_error("error1");
        let error2 = super::super::GraphQLError::parse_error("error2");
        let error3 = super::super::GraphQLError::validation_error("error3");

        metrics.record_error(&error1);
        metrics.record_error(&error2);
        metrics.record_error(&error3);

        let counts = metrics.error_counts();
        assert_eq!(counts.get(&ErrorCode::ParseError), Some(&2));
        assert_eq!(counts.get(&ErrorCode::ValidationError), Some(&1));
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = GraphQLMetrics::new();

        metrics.record_query(Duration::from_millis(10), OperationType::Query);
        metrics.record_query(Duration::from_millis(5), OperationType::Mutation);

        let output = metrics.to_prometheus();

        assert!(output.contains("helios_graphql_queries_total"));
        assert!(output.contains("helios_graphql_latency_ms"));
    }

    #[test]
    fn test_metrics_reset() {
        let metrics = GraphQLMetrics::new();

        metrics.record_query(Duration::from_millis(10), OperationType::Query);
        metrics.record_operation("GetUsers", Duration::from_millis(10), OperationType::Query);

        assert_eq!(metrics.query_stats().total_count(), 1);

        metrics.reset();

        assert_eq!(metrics.query_stats().total_count(), 0);
        assert!(metrics.all_operations().is_empty());
    }

    #[test]
    fn test_latency_histogram_percentile() {
        let mut histogram = LatencyHistogram::new();

        for i in 1..=100 {
            histogram.record(i * 1000); // 1ms to 100ms
        }

        let p50 = histogram.percentile(50.0).unwrap();
        let p99 = histogram.percentile(99.0).unwrap();

        // p50 should be around 50ms
        assert!(p50.as_millis() >= 45 && p50.as_millis() <= 55);

        // p99 should be around 99ms
        assert!(p99.as_millis() >= 95);
    }

    #[test]
    fn test_query_stats_empty() {
        let stats = QueryStats::new();

        assert_eq!(stats.total_count(), 0);
        assert!(stats.average_duration().is_none());
        assert!(stats.min_duration().is_none());
        assert!(stats.max_duration().is_none());
    }

    #[test]
    fn test_metrics_uptime() {
        let metrics = GraphQLMetrics::new();

        std::thread::sleep(Duration::from_millis(10));

        let uptime = metrics.uptime();
        assert!(uptime >= Duration::from_millis(10));
    }

    #[test]
    fn test_operation_metrics_error_rate_zero() {
        let metrics = OperationMetrics::new(OperationType::Query);

        assert_eq!(metrics.error_rate(), 0.0);
    }
}
