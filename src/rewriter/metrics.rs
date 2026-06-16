//! Rewrite Metrics
//!
//! Metrics collection for query rewriting.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Rewrite metrics collector
pub struct RewriteMetrics {
    /// Total queries processed
    total_queries: AtomicU64,

    /// Queries that were rewritten
    rewritten_queries: AtomicU64,

    /// Queries with no matching rules
    no_match_queries: AtomicU64,

    /// Total rewrite time (nanoseconds)
    total_rewrite_time_ns: AtomicU64,

    /// Per-rule statistics
    rule_stats: RwLock<HashMap<String, RuleStats>>,

    /// Histogram buckets for latency
    latency_buckets: RwLock<LatencyHistogram>,
}

impl RewriteMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self {
            total_queries: AtomicU64::new(0),
            rewritten_queries: AtomicU64::new(0),
            no_match_queries: AtomicU64::new(0),
            total_rewrite_time_ns: AtomicU64::new(0),
            rule_stats: RwLock::new(HashMap::new()),
            latency_buckets: RwLock::new(LatencyHistogram::new()),
        }
    }

    /// Record a rewrite operation
    pub fn record_rewrite(&self, duration: Duration, was_rewritten: bool) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);

        if was_rewritten {
            self.rewritten_queries.fetch_add(1, Ordering::Relaxed);
        }

        let nanos = duration.as_nanos() as u64;
        self.total_rewrite_time_ns
            .fetch_add(nanos, Ordering::Relaxed);

        self.latency_buckets.write().record(duration);
    }

    /// Record a no-match query
    pub fn record_no_match(&self, duration: Duration) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        self.no_match_queries.fetch_add(1, Ordering::Relaxed);

        let nanos = duration.as_nanos() as u64;
        self.total_rewrite_time_ns
            .fetch_add(nanos, Ordering::Relaxed);

        self.latency_buckets.write().record(duration);
    }

    /// Record a rule match
    pub fn record_rule_match(&self, rule_id: &str) {
        let mut stats = self.rule_stats.write();
        let entry = stats.entry(rule_id.to_string()).or_default();
        entry.matches.fetch_add(1, Ordering::Relaxed);
    }

    /// Get statistics
    pub fn stats(&self) -> RewriteStats {
        let total = self.total_queries.load(Ordering::Relaxed);
        let rewritten = self.rewritten_queries.load(Ordering::Relaxed);
        let no_match = self.no_match_queries.load(Ordering::Relaxed);
        let total_time_ns = self.total_rewrite_time_ns.load(Ordering::Relaxed);

        let avg_time = Duration::from_nanos(total_time_ns.checked_div(total).unwrap_or(0));

        let rewrite_ratio = if total > 0 {
            rewritten as f64 / total as f64
        } else {
            0.0
        };

        let rule_stats: HashMap<String, RuleStatsSnapshot> = self
            .rule_stats
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect();

        let latency = self.latency_buckets.read().percentiles();

        RewriteStats {
            total_queries: total,
            rewritten_queries: rewritten,
            no_match_queries: no_match,
            rewrite_ratio,
            avg_rewrite_time: avg_time,
            total_rewrite_time: Duration::from_nanos(total_time_ns),
            rule_stats,
            latency,
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_queries.store(0, Ordering::Relaxed);
        self.rewritten_queries.store(0, Ordering::Relaxed);
        self.no_match_queries.store(0, Ordering::Relaxed);
        self.total_rewrite_time_ns.store(0, Ordering::Relaxed);
        self.rule_stats.write().clear();
        *self.latency_buckets.write() = LatencyHistogram::new();
    }
}

impl Default for RewriteMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-rule statistics
pub struct RuleStats {
    /// Number of matches
    pub matches: AtomicU64,

    /// Number of successful applications
    pub applied: AtomicU64,

    /// Number of failures
    pub failures: AtomicU64,

    /// Total time saved (estimated, nanoseconds)
    pub time_saved_ns: AtomicU64,
}

impl RuleStats {
    /// Create new stats
    pub fn new() -> Self {
        Self {
            matches: AtomicU64::new(0),
            applied: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            time_saved_ns: AtomicU64::new(0),
        }
    }

    /// Get a snapshot
    pub fn snapshot(&self) -> RuleStatsSnapshot {
        RuleStatsSnapshot {
            matches: self.matches.load(Ordering::Relaxed),
            applied: self.applied.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            time_saved: Duration::from_nanos(self.time_saved_ns.load(Ordering::Relaxed)),
        }
    }
}

impl Default for RuleStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of rule statistics
#[derive(Debug, Clone)]
pub struct RuleStatsSnapshot {
    /// Number of matches
    pub matches: u64,

    /// Number of successful applications
    pub applied: u64,

    /// Number of failures
    pub failures: u64,

    /// Total time saved
    pub time_saved: Duration,
}

/// Overall rewrite statistics
#[derive(Debug, Clone)]
pub struct RewriteStats {
    /// Total queries processed
    pub total_queries: u64,

    /// Queries that were rewritten
    pub rewritten_queries: u64,

    /// Queries with no matching rules
    pub no_match_queries: u64,

    /// Ratio of rewritten queries
    pub rewrite_ratio: f64,

    /// Average rewrite time
    pub avg_rewrite_time: Duration,

    /// Total rewrite time
    pub total_rewrite_time: Duration,

    /// Per-rule statistics
    pub rule_stats: HashMap<String, RuleStatsSnapshot>,

    /// Latency percentiles
    pub latency: LatencyPercentiles,
}

/// Latency histogram
struct LatencyHistogram {
    /// Bucket boundaries (microseconds)
    boundaries: Vec<u64>,

    /// Counts per bucket
    counts: Vec<AtomicU64>,

    /// Total count
    total: AtomicU64,
}

impl LatencyHistogram {
    fn new() -> Self {
        // Buckets: 1μs, 5μs, 10μs, 25μs, 50μs, 100μs, 250μs, 500μs, 1ms, 5ms, 10ms
        let boundaries = vec![1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000];
        let counts: Vec<AtomicU64> = (0..=boundaries.len()).map(|_| AtomicU64::new(0)).collect();

        Self {
            boundaries,
            counts,
            total: AtomicU64::new(0),
        }
    }

    fn record(&mut self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        let mut bucket = self.boundaries.len();

        for (i, &boundary) in self.boundaries.iter().enumerate() {
            if micros <= boundary {
                bucket = i;
                break;
            }
        }

        self.counts[bucket].fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    fn percentiles(&self) -> LatencyPercentiles {
        let total = self.total.load(Ordering::Relaxed) as f64;

        if total == 0.0 {
            return LatencyPercentiles::default();
        }

        let cumulative: Vec<u64> = self
            .counts
            .iter()
            .scan(0u64, |acc, c| {
                *acc += c.load(Ordering::Relaxed);
                Some(*acc)
            })
            .collect();

        let get_percentile = |p: f64| -> Duration {
            let target = (total * p) as u64;
            for (i, &count) in cumulative.iter().enumerate() {
                if count >= target {
                    if i < self.boundaries.len() {
                        return Duration::from_micros(self.boundaries[i]);
                    } else {
                        return Duration::from_micros(
                            self.boundaries.last().copied().unwrap_or(10000) * 2,
                        );
                    }
                }
            }
            Duration::from_micros(10000)
        };

        LatencyPercentiles {
            p50: get_percentile(0.50),
            p90: get_percentile(0.90),
            p95: get_percentile(0.95),
            p99: get_percentile(0.99),
        }
    }
}

/// Latency percentiles
#[derive(Debug, Clone, Default)]
pub struct LatencyPercentiles {
    /// 50th percentile
    pub p50: Duration,

    /// 90th percentile
    pub p90: Duration,

    /// 95th percentile
    pub p95: Duration,

    /// 99th percentile
    pub p99: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basic() {
        let metrics = RewriteMetrics::new();

        metrics.record_rewrite(Duration::from_micros(100), true);
        metrics.record_rewrite(Duration::from_micros(50), false);
        metrics.record_no_match(Duration::from_micros(10));

        let stats = metrics.stats();
        assert_eq!(stats.total_queries, 3);
        assert_eq!(stats.rewritten_queries, 1);
        assert_eq!(stats.no_match_queries, 1);
    }

    #[test]
    fn test_rule_stats() {
        let metrics = RewriteMetrics::new();

        metrics.record_rule_match("rule1");
        metrics.record_rule_match("rule1");
        metrics.record_rule_match("rule2");

        let stats = metrics.stats();
        assert_eq!(stats.rule_stats.get("rule1").unwrap().matches, 2);
        assert_eq!(stats.rule_stats.get("rule2").unwrap().matches, 1);
    }

    #[test]
    fn test_reset() {
        let metrics = RewriteMetrics::new();

        metrics.record_rewrite(Duration::from_micros(100), true);
        metrics.record_rule_match("rule1");

        metrics.reset();

        let stats = metrics.stats();
        assert_eq!(stats.total_queries, 0);
        assert!(stats.rule_stats.is_empty());
    }

    #[test]
    fn test_rewrite_ratio() {
        let metrics = RewriteMetrics::new();

        // 3 rewritten, 7 not = 30% ratio
        for _ in 0..3 {
            metrics.record_rewrite(Duration::from_micros(10), true);
        }
        for _ in 0..7 {
            metrics.record_rewrite(Duration::from_micros(10), false);
        }

        let stats = metrics.stats();
        assert!((stats.rewrite_ratio - 0.3).abs() < 0.01);
    }
}
