//! Latency Histogram
//!
//! Track query latency distributions with configurable bucket boundaries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Default bucket boundaries in microseconds
const DEFAULT_BUCKETS_US: &[u64] = &[
    100,        // 100µs
    500,        // 500µs
    1_000,      // 1ms
    5_000,      // 5ms
    10_000,     // 10ms
    25_000,     // 25ms
    50_000,     // 50ms
    100_000,    // 100ms
    250_000,    // 250ms
    500_000,    // 500ms
    1_000_000,  // 1s
    2_500_000,  // 2.5s
    5_000_000,  // 5s
    10_000_000, // 10s
];

/// Histogram bucket
#[derive(Debug)]
pub struct HistogramBucket {
    /// Upper bound in microseconds (exclusive)
    pub upper_bound_us: u64,
    /// Count of values in this bucket
    count: AtomicU64,
}

impl HistogramBucket {
    /// Create new bucket with upper bound
    pub fn new(upper_bound_us: u64) -> Self {
        Self {
            upper_bound_us,
            count: AtomicU64::new(0),
        }
    }

    /// Increment count
    pub fn increment(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get count
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

/// Latency histogram for tracking query execution times
pub struct LatencyHistogram {
    /// Histogram buckets
    buckets: Vec<HistogramBucket>,
    /// Overflow bucket (values exceeding max bucket)
    overflow: AtomicU64,
    /// Total count
    total_count: AtomicU64,
    /// Sum of all values (for mean calculation)
    total_sum_us: AtomicU64,
}

impl LatencyHistogram {
    /// Create histogram with default buckets
    pub fn new() -> Self {
        Self::with_buckets(DEFAULT_BUCKETS_US)
    }

    /// Create histogram with custom bucket boundaries
    pub fn with_buckets(boundaries_us: &[u64]) -> Self {
        let buckets = boundaries_us
            .iter()
            .map(|&bound| HistogramBucket::new(bound))
            .collect();

        Self {
            buckets,
            overflow: AtomicU64::new(0),
            total_count: AtomicU64::new(0),
            total_sum_us: AtomicU64::new(0),
        }
    }

    /// Record a duration
    pub fn record(&self, duration: Duration) {
        let value_us = duration.as_micros() as u64;

        self.total_count.fetch_add(1, Ordering::Relaxed);
        self.total_sum_us.fetch_add(value_us, Ordering::Relaxed);

        // Find the appropriate bucket
        let mut recorded = false;
        for bucket in &self.buckets {
            if value_us < bucket.upper_bound_us {
                bucket.increment();
                recorded = true;
                break;
            }
        }

        if !recorded {
            self.overflow.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a value in microseconds
    pub fn record_us(&self, value_us: u64) {
        self.record(Duration::from_micros(value_us));
    }

    /// Get total count
    pub fn count(&self) -> u64 {
        self.total_count.load(Ordering::Relaxed)
    }

    /// Get mean latency
    pub fn mean(&self) -> Duration {
        let count = self.total_count.load(Ordering::Relaxed);
        if count == 0 {
            return Duration::ZERO;
        }
        let sum = self.total_sum_us.load(Ordering::Relaxed);
        Duration::from_micros(sum / count)
    }

    /// Get percentile (0.0 - 1.0)
    pub fn percentile(&self, p: f64) -> Duration {
        let p = p.clamp(0.0, 1.0);
        let total = self.total_count.load(Ordering::Relaxed);

        if total == 0 {
            return Duration::ZERO;
        }

        let target = (total as f64 * p).ceil() as u64;
        let mut cumulative = 0u64;

        for bucket in &self.buckets {
            cumulative += bucket.count();
            if cumulative >= target {
                return Duration::from_micros(bucket.upper_bound_us);
            }
        }

        // Overflow bucket - return last bucket boundary
        if let Some(last) = self.buckets.last() {
            Duration::from_micros(last.upper_bound_us)
        } else {
            Duration::ZERO
        }
    }

    /// Get P50 (median)
    pub fn p50(&self) -> Duration {
        self.percentile(0.50)
    }

    /// Get P90
    pub fn p90(&self) -> Duration {
        self.percentile(0.90)
    }

    /// Get P95
    pub fn p95(&self) -> Duration {
        self.percentile(0.95)
    }

    /// Get P99
    pub fn p99(&self) -> Duration {
        self.percentile(0.99)
    }

    /// Get snapshot of histogram
    pub fn snapshot(&self) -> HistogramSnapshot {
        let buckets: Vec<_> = self
            .buckets
            .iter()
            .map(|b| BucketSnapshot {
                upper_bound_us: b.upper_bound_us,
                count: b.count(),
            })
            .collect();

        HistogramSnapshot {
            buckets,
            overflow: self.overflow.load(Ordering::Relaxed),
            total_count: self.total_count.load(Ordering::Relaxed),
            total_sum_us: self.total_sum_us.load(Ordering::Relaxed),
        }
    }

    /// Reset histogram
    pub fn reset(&self) {
        for bucket in &self.buckets {
            bucket.count.store(0, Ordering::Relaxed);
        }
        self.overflow.store(0, Ordering::Relaxed);
        self.total_count.store(0, Ordering::Relaxed);
        self.total_sum_us.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of a histogram bucket
#[derive(Debug, Clone)]
pub struct BucketSnapshot {
    /// Upper bound in microseconds
    pub upper_bound_us: u64,
    /// Count of values
    pub count: u64,
}

/// Snapshot of histogram state
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    /// Bucket snapshots
    pub buckets: Vec<BucketSnapshot>,
    /// Overflow count
    pub overflow: u64,
    /// Total count
    pub total_count: u64,
    /// Total sum in microseconds
    pub total_sum_us: u64,
}

impl HistogramSnapshot {
    /// Get mean latency
    pub fn mean(&self) -> Duration {
        if self.total_count == 0 {
            return Duration::ZERO;
        }
        Duration::from_micros(self.total_sum_us / self.total_count)
    }

    /// Get percentile from snapshot
    pub fn percentile(&self, p: f64) -> Duration {
        let p = p.clamp(0.0, 1.0);

        if self.total_count == 0 {
            return Duration::ZERO;
        }

        let target = (self.total_count as f64 * p).ceil() as u64;
        let mut cumulative = 0u64;

        for bucket in &self.buckets {
            cumulative += bucket.count;
            if cumulative >= target {
                return Duration::from_micros(bucket.upper_bound_us);
            }
        }

        if let Some(last) = self.buckets.last() {
            Duration::from_micros(last.upper_bound_us)
        } else {
            Duration::ZERO
        }
    }

    /// Format as ASCII histogram
    pub fn format_ascii(&self, width: usize) -> String {
        let max_count = self.buckets.iter().map(|b| b.count).max().unwrap_or(1);
        let mut output = String::new();

        for bucket in &self.buckets {
            let label = format_duration(bucket.upper_bound_us);
            let bar_len = if max_count > 0 {
                (bucket.count as f64 / max_count as f64 * width as f64) as usize
            } else {
                0
            };
            let bar: String = "#".repeat(bar_len);
            output.push_str(&format!("{:>8} | {:6} | {}\n", label, bucket.count, bar));
        }

        if self.overflow > 0 {
            output.push_str(&format!("{:>8} | {:6} | (overflow)\n", ">max", self.overflow));
        }

        output
    }
}

/// Format microseconds as human-readable duration
fn format_duration(us: u64) -> String {
    if us < 1_000 {
        format!("{}µs", us)
    } else if us < 1_000_000 {
        format!("{}ms", us / 1_000)
    } else {
        format!("{:.1}s", us as f64 / 1_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram_new() {
        let hist = LatencyHistogram::new();
        assert_eq!(hist.count(), 0);
        assert_eq!(hist.mean(), Duration::ZERO);
    }

    #[test]
    fn test_histogram_record() {
        let hist = LatencyHistogram::new();

        hist.record(Duration::from_micros(500));
        hist.record(Duration::from_millis(5));
        hist.record(Duration::from_millis(50));

        assert_eq!(hist.count(), 3);
    }

    #[test]
    fn test_histogram_mean() {
        let hist = LatencyHistogram::new();

        hist.record(Duration::from_millis(10));
        hist.record(Duration::from_millis(20));
        hist.record(Duration::from_millis(30));

        let mean = hist.mean();
        assert_eq!(mean, Duration::from_millis(20));
    }

    #[test]
    fn test_histogram_percentiles() {
        let hist = LatencyHistogram::new();

        // Record 100 values from 1ms to 100ms
        for i in 1..=100 {
            hist.record(Duration::from_millis(i));
        }

        // P50 should be around 50ms bucket
        let p50 = hist.p50();
        assert!(p50 >= Duration::from_millis(50));

        // P99 should be around 99ms bucket
        let p99 = hist.p99();
        assert!(p99 >= Duration::from_millis(100));
    }

    #[test]
    fn test_histogram_snapshot() {
        let hist = LatencyHistogram::new();

        hist.record(Duration::from_millis(1));
        hist.record(Duration::from_millis(10));

        let snapshot = hist.snapshot();
        assert_eq!(snapshot.total_count, 2);
    }

    #[test]
    fn test_histogram_reset() {
        let hist = LatencyHistogram::new();

        hist.record(Duration::from_millis(10));
        assert_eq!(hist.count(), 1);

        hist.reset();
        assert_eq!(hist.count(), 0);
    }

    #[test]
    fn test_custom_buckets() {
        let hist = LatencyHistogram::with_buckets(&[100, 1000, 10000]);

        hist.record(Duration::from_micros(50));   // bucket 0 (< 100)
        hist.record(Duration::from_micros(500));  // bucket 1 (< 1000)
        hist.record(Duration::from_micros(5000)); // bucket 2 (< 10000)
        hist.record(Duration::from_micros(50000)); // overflow

        let snapshot = hist.snapshot();
        assert_eq!(snapshot.buckets[0].count, 1);
        assert_eq!(snapshot.buckets[1].count, 1);
        assert_eq!(snapshot.buckets[2].count, 1);
        assert_eq!(snapshot.overflow, 1);
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(500), "500µs");
        assert_eq!(format_duration(5_000), "5ms");
        assert_eq!(format_duration(5_000_000), "5.0s");
    }

    #[test]
    fn test_snapshot_format_ascii() {
        let hist = LatencyHistogram::with_buckets(&[1000, 10000, 100000]);

        hist.record(Duration::from_micros(500));
        hist.record(Duration::from_micros(500));
        hist.record(Duration::from_micros(5000));

        let snapshot = hist.snapshot();
        let ascii = snapshot.format_ascii(20);

        assert!(ascii.contains("1ms"));
        assert!(ascii.contains("10ms"));
    }
}
