//! Adaptive Circuit Breaker Thresholds
//!
//! Automatically adjusts failure thresholds based on historical data
//! to distinguish between normal failure rates and anomalies.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Rolling statistics calculator
#[derive(Debug)]
pub struct RollingStats {
    /// Data points
    values: RwLock<VecDeque<(Instant, f64)>>,
    /// Window duration
    window: Duration,
    /// Maximum number of data points
    max_points: usize,
}

impl RollingStats {
    /// Create a new rolling statistics calculator
    pub fn new(window: Duration) -> Self {
        Self {
            values: RwLock::new(VecDeque::new()),
            window,
            max_points: 1000,
        }
    }

    /// Create with specific max points
    pub fn with_max_points(window: Duration, max_points: usize) -> Self {
        Self {
            values: RwLock::new(VecDeque::new()),
            window,
            max_points,
        }
    }

    /// Add a data point
    pub fn add(&self, value: f64) {
        let now = Instant::now();
        let mut values = self.values.write();

        // Remove expired entries
        let cutoff = now - self.window;
        while values.front().map(|(t, _)| *t < cutoff).unwrap_or(false) {
            values.pop_front();
        }

        // Enforce max points
        while values.len() >= self.max_points {
            values.pop_front();
        }

        values.push_back((now, value));
    }

    /// Get current average
    pub fn average(&self) -> Option<f64> {
        let now = Instant::now();
        let mut values = self.values.write();

        // Remove expired entries
        let cutoff = now - self.window;
        while values.front().map(|(t, _)| *t < cutoff).unwrap_or(false) {
            values.pop_front();
        }

        if values.is_empty() {
            return None;
        }

        let sum: f64 = values.iter().map(|(_, v)| v).sum();
        Some(sum / values.len() as f64)
    }

    /// Get current standard deviation
    pub fn std_dev(&self) -> Option<f64> {
        let avg = self.average()?;
        let values = self.values.read();

        if values.len() < 2 {
            return None;
        }

        let variance: f64 =
            values.iter().map(|(_, v)| (v - avg).powi(2)).sum::<f64>() / (values.len() - 1) as f64;

        Some(variance.sqrt())
    }

    /// Get count of data points
    pub fn count(&self) -> usize {
        let now = Instant::now();
        let mut values = self.values.write();

        // Remove expired entries
        let cutoff = now - self.window;
        while values.front().map(|(t, _)| *t < cutoff).unwrap_or(false) {
            values.pop_front();
        }

        values.len()
    }

    /// Get minimum value
    pub fn min(&self) -> Option<f64> {
        let values = self.values.read();
        values
            .iter()
            .map(|(_, v)| *v)
            .fold(None, |min, v| Some(min.map_or(v, |m: f64| m.min(v))))
    }

    /// Get maximum value
    pub fn max(&self) -> Option<f64> {
        let values = self.values.read();
        values
            .iter()
            .map(|(_, v)| *v)
            .fold(None, |max, v| Some(max.map_or(v, |m: f64| m.max(v))))
    }

    /// Get percentile value (0.0 - 1.0)
    pub fn percentile(&self, p: f64) -> Option<f64> {
        let values = self.values.read();
        if values.is_empty() {
            return None;
        }

        let mut sorted: Vec<f64> = values.iter().map(|(_, v)| *v).collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let idx = ((sorted.len() - 1) as f64 * p.clamp(0.0, 1.0)) as usize;
        Some(sorted[idx])
    }

    /// Reset statistics
    pub fn reset(&self) {
        self.values.write().clear();
    }
}

impl Clone for RollingStats {
    fn clone(&self) -> Self {
        Self {
            values: RwLock::new(self.values.read().clone()),
            window: self.window,
            max_points: self.max_points,
        }
    }
}

/// Adaptive threshold calculator
#[derive(Debug)]
pub struct AdaptiveThreshold {
    /// Historical failure rates
    failure_stats: RollingStats,

    /// Base threshold (starting point)
    base_threshold: AtomicU32,

    /// Minimum threshold (floor)
    min_threshold: u32,

    /// Maximum threshold (ceiling)
    max_threshold: u32,

    /// Number of standard deviations for anomaly detection
    sigma_multiplier: f64,

    /// Minimum samples needed before adapting
    min_samples: usize,

    /// Last computed threshold
    cached_threshold: RwLock<Option<u32>>,
}

impl AdaptiveThreshold {
    /// Create a new adaptive threshold calculator
    pub fn new(base_threshold: u32) -> Self {
        Self {
            failure_stats: RollingStats::new(Duration::from_secs(3600)), // 1 hour window
            base_threshold: AtomicU32::new(base_threshold),
            min_threshold: 2,
            max_threshold: 100,
            sigma_multiplier: 3.0, // 3 sigma rule
            min_samples: 10,
            cached_threshold: RwLock::new(None),
        }
    }

    /// Create with custom configuration
    pub fn with_config(
        base_threshold: u32,
        window: Duration,
        min_threshold: u32,
        max_threshold: u32,
        sigma_multiplier: f64,
    ) -> Self {
        Self {
            failure_stats: RollingStats::new(window),
            base_threshold: AtomicU32::new(base_threshold),
            min_threshold,
            max_threshold,
            sigma_multiplier,
            min_samples: 10,
            cached_threshold: RwLock::new(None),
        }
    }

    /// Record a failure count observation
    pub fn record_failures(&self, failure_count: u32) {
        self.failure_stats.add(failure_count as f64);
        // Invalidate cached threshold
        *self.cached_threshold.write() = None;
    }

    /// Compute adaptive threshold
    ///
    /// Uses the formula: threshold = avg + (sigma_multiplier * std_dev)
    /// This means the circuit opens when failures exceed normal by 3 sigma (by default)
    pub fn compute_threshold(&self) -> u32 {
        // Return cached value if available
        if let Some(cached) = *self.cached_threshold.read() {
            return cached;
        }

        let base = self.base_threshold.load(Ordering::SeqCst);

        // Need minimum samples for statistical validity
        if self.failure_stats.count() < self.min_samples {
            return base;
        }

        let threshold = match (self.failure_stats.average(), self.failure_stats.std_dev()) {
            (Some(avg), Some(std)) => {
                let computed = avg + (self.sigma_multiplier * std);
                let computed = computed.round() as u32;

                // Clamp to configured bounds
                computed.clamp(self.min_threshold, self.max_threshold)
            }
            (Some(avg), None) => {
                // Not enough variance, use average + 50%
                let computed = (avg * 1.5).round() as u32;
                computed.clamp(self.min_threshold, self.max_threshold)
            }
            _ => base,
        };

        // Cache the result
        *self.cached_threshold.write() = Some(threshold);

        threshold
    }

    /// Get the base threshold
    pub fn base_threshold(&self) -> u32 {
        self.base_threshold.load(Ordering::SeqCst)
    }

    /// Update base threshold
    pub fn set_base_threshold(&self, threshold: u32) {
        self.base_threshold.store(threshold, Ordering::SeqCst);
        *self.cached_threshold.write() = None;
    }

    /// Get current statistics summary
    pub fn get_stats(&self) -> AdaptiveStats {
        AdaptiveStats {
            base_threshold: self.base_threshold(),
            computed_threshold: self.compute_threshold(),
            sample_count: self.failure_stats.count(),
            average_failures: self.failure_stats.average(),
            std_deviation: self.failure_stats.std_dev(),
            min_observed: self.failure_stats.min(),
            max_observed: self.failure_stats.max(),
        }
    }

    /// Reset statistics
    pub fn reset(&self) {
        self.failure_stats.reset();
        *self.cached_threshold.write() = None;
    }

    /// Check if we have enough data for reliable adaptation
    pub fn is_calibrated(&self) -> bool {
        self.failure_stats.count() >= self.min_samples
    }
}

impl Clone for AdaptiveThreshold {
    fn clone(&self) -> Self {
        Self {
            failure_stats: self.failure_stats.clone(),
            base_threshold: AtomicU32::new(self.base_threshold.load(Ordering::SeqCst)),
            min_threshold: self.min_threshold,
            max_threshold: self.max_threshold,
            sigma_multiplier: self.sigma_multiplier,
            min_samples: self.min_samples,
            cached_threshold: RwLock::new(*self.cached_threshold.read()),
        }
    }
}

/// Statistics about adaptive threshold
#[derive(Debug, Clone)]
pub struct AdaptiveStats {
    pub base_threshold: u32,
    pub computed_threshold: u32,
    pub sample_count: usize,
    pub average_failures: Option<f64>,
    pub std_deviation: Option<f64>,
    pub min_observed: Option<f64>,
    pub max_observed: Option<f64>,
}

impl AdaptiveStats {
    /// Check if threshold was adjusted from base
    pub fn is_adjusted(&self) -> bool {
        self.computed_threshold != self.base_threshold
    }

    /// Get adjustment percentage
    pub fn adjustment_percentage(&self) -> f64 {
        if self.base_threshold == 0 {
            return 0.0;
        }

        ((self.computed_threshold as f64 - self.base_threshold as f64) / self.base_threshold as f64)
            * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rolling_stats_basic() {
        let stats = RollingStats::new(Duration::from_secs(60));

        stats.add(10.0);
        stats.add(20.0);
        stats.add(30.0);

        assert_eq!(stats.count(), 3);
        assert!((stats.average().unwrap() - 20.0).abs() < 0.01);
    }

    #[test]
    fn test_rolling_stats_std_dev() {
        let stats = RollingStats::new(Duration::from_secs(60));

        // Values: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean: 5, StdDev: ~2.14
        for v in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0] {
            stats.add(v);
        }

        assert!((stats.average().unwrap() - 5.0).abs() < 0.01);
        assert!((stats.std_dev().unwrap() - 2.14).abs() < 0.1);
    }

    #[test]
    fn test_rolling_stats_percentile() {
        let stats = RollingStats::new(Duration::from_secs(60));

        for i in 1..=100 {
            stats.add(i as f64);
        }

        let p50 = stats.percentile(0.5).unwrap();
        assert!((p50 - 50.0).abs() < 1.0);

        let p99 = stats.percentile(0.99).unwrap();
        assert!((p99 - 99.0).abs() < 1.0);
    }

    #[test]
    fn test_adaptive_threshold_basic() {
        let adaptive = AdaptiveThreshold::new(5);

        // Not enough samples, should return base
        assert_eq!(adaptive.compute_threshold(), 5);
        assert!(!adaptive.is_calibrated());
    }

    #[test]
    fn test_adaptive_threshold_with_data() {
        let adaptive = AdaptiveThreshold::with_config(5, Duration::from_secs(3600), 2, 100, 3.0);

        // Add consistent failure counts
        for _ in 0..20 {
            adaptive.record_failures(3);
        }

        assert!(adaptive.is_calibrated());

        // With low variance, threshold should be close to average
        let threshold = adaptive.compute_threshold();
        assert!(threshold >= 3);
        assert!(threshold <= 10);
    }

    #[test]
    fn test_adaptive_threshold_high_variance() {
        let adaptive = AdaptiveThreshold::with_config(5, Duration::from_secs(3600), 2, 100, 3.0);

        // Add highly variable failure counts
        for i in 0..20 {
            adaptive.record_failures(i % 10);
        }

        let threshold = adaptive.compute_threshold();
        let stats = adaptive.get_stats();

        // Higher variance should increase threshold
        assert!(threshold > stats.average_failures.unwrap_or(0.0) as u32);
    }

    #[test]
    fn test_adaptive_stats() {
        let adaptive = AdaptiveThreshold::new(10);

        for _ in 0..15 {
            adaptive.record_failures(5);
        }

        let stats = adaptive.get_stats();
        assert_eq!(stats.base_threshold, 10);
        assert_eq!(stats.sample_count, 15);
        assert!(stats.average_failures.is_some());
    }
}
