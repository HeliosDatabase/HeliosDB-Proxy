//! Rate Limit Metrics
//!
//! Metrics collection and statistics for rate limiting decisions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

use super::limiter::{LimiterKey, RateLimitResult};

/// Rate limit metrics collector
pub struct RateLimitMetrics {
    /// Total requests checked
    total_requests: AtomicU64,

    /// Requests allowed
    allowed: AtomicU64,

    /// Requests queued
    queued: AtomicU64,

    /// Requests throttled
    throttled: AtomicU64,

    /// Requests warned
    warned: AtomicU64,

    /// Requests denied
    denied: AtomicU64,

    /// Per-key statistics
    key_stats: DashMap<String, KeyStats>,

    /// Decision timing (microseconds)
    decision_times_us: RwLock<Vec<u64>>,

    /// Maximum timing samples
    max_timing_samples: usize,

    /// Start time
    started_at: Instant,
}

impl RateLimitMetrics {
    /// Create new metrics collector
    pub fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            allowed: AtomicU64::new(0),
            queued: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            warned: AtomicU64::new(0),
            denied: AtomicU64::new(0),
            key_stats: DashMap::new(),
            decision_times_us: RwLock::new(Vec::with_capacity(1000)),
            max_timing_samples: 1000,
            started_at: Instant::now(),
        }
    }

    /// Record a rate limit decision
    pub fn record_decision(&self, key: &LimiterKey, result: &RateLimitResult, elapsed: Duration) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);

        match result {
            RateLimitResult::Allowed => {
                self.allowed.fetch_add(1, Ordering::Relaxed);
            }
            RateLimitResult::Queued(_) => {
                self.queued.fetch_add(1, Ordering::Relaxed);
            }
            RateLimitResult::Throttled(_) => {
                self.throttled.fetch_add(1, Ordering::Relaxed);
            }
            RateLimitResult::Warned(_) => {
                self.warned.fetch_add(1, Ordering::Relaxed);
            }
            RateLimitResult::Denied(_) => {
                self.denied.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Update per-key stats
        let key_str = key.to_string();
        self.key_stats
            .entry(key_str)
            .and_modify(|stats| stats.record(result))
            .or_insert_with(|| {
                let stats = KeyStats::new();
                stats.record(result);
                stats
            });

        // Record timing
        self.record_timing(elapsed);
    }

    /// Record timing sample
    fn record_timing(&self, elapsed: Duration) {
        let us = elapsed.as_micros() as u64;
        let mut times = self.decision_times_us.write();

        if times.len() >= self.max_timing_samples {
            times.drain(0..self.max_timing_samples / 2);
        }
        times.push(us);
    }

    /// Reset stats for a key
    pub fn reset_key(&self, key: &LimiterKey) {
        let key_str = key.to_string();
        self.key_stats.remove(&key_str);
    }

    /// Get current statistics snapshot
    pub fn get_stats(&self) -> RateLimitStats {
        let total = self.total_requests.load(Ordering::Relaxed);
        let allowed = self.allowed.load(Ordering::Relaxed);
        let queued = self.queued.load(Ordering::Relaxed);
        let throttled = self.throttled.load(Ordering::Relaxed);
        let warned = self.warned.load(Ordering::Relaxed);
        let denied = self.denied.load(Ordering::Relaxed);

        // Calculate timing stats
        let times = self.decision_times_us.read();
        let (avg_time_us, p50_time_us, p99_time_us) = if times.is_empty() {
            (0, 0, 0)
        } else {
            let mut sorted = times.clone();
            sorted.sort_unstable();

            let avg = sorted.iter().sum::<u64>() / sorted.len() as u64;
            let p50 = sorted[sorted.len() / 2];
            let p99_idx = ((sorted.len() as f64) * 0.99) as usize;
            let p99 = sorted.get(p99_idx).copied().unwrap_or(sorted[sorted.len() - 1]);

            (avg, p50, p99)
        };

        // Collect per-key stats
        let key_stats: HashMap<_, _> = self
            .key_stats
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().snapshot()))
            .collect();

        RateLimitStats {
            total_requests: total,
            allowed,
            queued,
            throttled,
            warned,
            denied,
            avg_decision_time_us: avg_time_us,
            p50_decision_time_us: p50_time_us,
            p99_decision_time_us: p99_time_us,
            key_stats,
            uptime_secs: self.started_at.elapsed().as_secs(),
        }
    }

    /// Get total requests
    pub fn total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }

    /// Get allowed count
    pub fn allowed(&self) -> u64 {
        self.allowed.load(Ordering::Relaxed)
    }

    /// Get denied count
    pub fn denied(&self) -> u64 {
        self.denied.load(Ordering::Relaxed)
    }

    /// Get denial rate (0.0 - 1.0)
    pub fn denial_rate(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        let denied = self.denied.load(Ordering::Relaxed);

        if total == 0 {
            0.0
        } else {
            denied as f64 / total as f64
        }
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.total_requests.store(0, Ordering::Relaxed);
        self.allowed.store(0, Ordering::Relaxed);
        self.queued.store(0, Ordering::Relaxed);
        self.throttled.store(0, Ordering::Relaxed);
        self.warned.store(0, Ordering::Relaxed);
        self.denied.store(0, Ordering::Relaxed);
        self.key_stats.clear();
        self.decision_times_us.write().clear();
    }
}

impl Default for RateLimitMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for RateLimitMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimitMetrics")
            .field("total_requests", &self.total_requests.load(Ordering::Relaxed))
            .field("denied", &self.denied.load(Ordering::Relaxed))
            .field("key_count", &self.key_stats.len())
            .finish()
    }
}

/// Per-key statistics
pub struct KeyStats {
    /// Total requests for this key
    total: AtomicU64,

    /// Allowed requests
    allowed: AtomicU64,

    /// Denied requests
    denied: AtomicU64,

    /// Last request time (nanos since epoch)
    last_request_ns: AtomicU64,

    /// Epoch for time calculations
    epoch: Instant,
}

impl KeyStats {
    fn new() -> Self {
        Self {
            total: AtomicU64::new(0),
            allowed: AtomicU64::new(0),
            denied: AtomicU64::new(0),
            last_request_ns: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    fn record(&self, result: &RateLimitResult) {
        self.total.fetch_add(1, Ordering::Relaxed);

        match result {
            RateLimitResult::Allowed | RateLimitResult::Queued(_) |
            RateLimitResult::Throttled(_) | RateLimitResult::Warned(_) => {
                self.allowed.fetch_add(1, Ordering::Relaxed);
            }
            RateLimitResult::Denied(_) => {
                self.denied.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.last_request_ns.store(
            self.epoch.elapsed().as_nanos() as u64,
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> KeyStatsSnapshot {
        let last_ns = self.last_request_ns.load(Ordering::Relaxed);
        let last_request = if last_ns > 0 {
            Some(Duration::from_nanos(last_ns))
        } else {
            None
        };

        KeyStatsSnapshot {
            total: self.total.load(Ordering::Relaxed),
            allowed: self.allowed.load(Ordering::Relaxed),
            denied: self.denied.load(Ordering::Relaxed),
            last_request_age: last_request,
        }
    }
}

/// Snapshot of per-key statistics
#[derive(Debug, Clone)]
pub struct KeyStatsSnapshot {
    /// Total requests
    pub total: u64,

    /// Allowed requests
    pub allowed: u64,

    /// Denied requests
    pub denied: u64,

    /// Age of last request (time since)
    pub last_request_age: Option<Duration>,
}

impl KeyStatsSnapshot {
    /// Get denial rate for this key
    pub fn denial_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.denied as f64 / self.total as f64
        }
    }

    /// Get allow rate for this key
    pub fn allow_rate(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            self.allowed as f64 / self.total as f64
        }
    }
}

/// Overall rate limit statistics snapshot
#[derive(Debug, Clone)]
pub struct RateLimitStats {
    /// Total requests checked
    pub total_requests: u64,

    /// Requests allowed
    pub allowed: u64,

    /// Requests queued
    pub queued: u64,

    /// Requests throttled
    pub throttled: u64,

    /// Requests warned
    pub warned: u64,

    /// Requests denied
    pub denied: u64,

    /// Average decision time (microseconds)
    pub avg_decision_time_us: u64,

    /// P50 decision time (microseconds)
    pub p50_decision_time_us: u64,

    /// P99 decision time (microseconds)
    pub p99_decision_time_us: u64,

    /// Per-key statistics
    pub key_stats: HashMap<String, KeyStatsSnapshot>,

    /// Uptime in seconds
    pub uptime_secs: u64,
}

impl RateLimitStats {
    /// Get overall denial rate
    pub fn denial_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.denied as f64 / self.total_requests as f64
        }
    }

    /// Get overall allow rate
    pub fn allow_rate(&self) -> f64 {
        if self.total_requests == 0 {
            0.0
        } else {
            self.allowed as f64 / self.total_requests as f64
        }
    }

    /// Get requests per second
    pub fn requests_per_second(&self) -> f64 {
        if self.uptime_secs == 0 {
            0.0
        } else {
            self.total_requests as f64 / self.uptime_secs as f64
        }
    }

    /// Get keys with highest denial rate
    pub fn top_denied_keys(&self, n: usize) -> Vec<(&String, &KeyStatsSnapshot)> {
        let mut entries: Vec<_> = self.key_stats.iter().collect();
        entries.sort_by(|a, b| {
            b.1.denial_rate()
                .partial_cmp(&a.1.denial_rate())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        entries.truncate(n);
        entries
    }

    /// Get keys with most requests
    pub fn top_request_keys(&self, n: usize) -> Vec<(&String, &KeyStatsSnapshot)> {
        let mut entries: Vec<_> = self.key_stats.iter().collect();
        entries.sort_by(|a, b| b.1.total.cmp(&a.1.total));
        entries.truncate(n);
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = RateLimitMetrics::new();
        let stats = metrics.get_stats();

        assert_eq!(stats.total_requests, 0);
        assert_eq!(stats.denied, 0);
    }

    #[test]
    fn test_record_allowed() {
        let metrics = RateLimitMetrics::new();
        let key = LimiterKey::User("test".to_string());

        metrics.record_decision(&key, &RateLimitResult::Allowed, Duration::from_micros(10));

        let stats = metrics.get_stats();
        assert_eq!(stats.total_requests, 1);
        assert_eq!(stats.allowed, 1);
        assert_eq!(stats.denied, 0);
    }

    #[test]
    fn test_record_denied() {
        let metrics = RateLimitMetrics::new();
        let key = LimiterKey::User("test".to_string());

        let error = super::super::limiter::RateLimitExceeded {
            key: key.clone(),
            limit_type: super::super::limiter::LimitType::TokenBucket,
            current: 0,
            limit: 100,
            retry_after: Duration::from_secs(1),
            message: "test".to_string(),
        };

        metrics.record_decision(&key, &RateLimitResult::Denied(error), Duration::from_micros(10));

        let stats = metrics.get_stats();
        assert_eq!(stats.total_requests, 1);
        assert_eq!(stats.denied, 1);
    }

    #[test]
    fn test_record_queued_throttled_warned() {
        let metrics = RateLimitMetrics::new();
        let key = LimiterKey::User("test".to_string());

        metrics.record_decision(&key, &RateLimitResult::Queued(Duration::from_secs(1)), Duration::from_micros(10));
        metrics.record_decision(&key, &RateLimitResult::Throttled(Duration::from_secs(1)), Duration::from_micros(10));
        metrics.record_decision(&key, &RateLimitResult::Warned("test".to_string()), Duration::from_micros(10));

        let stats = metrics.get_stats();
        assert_eq!(stats.total_requests, 3);
        assert_eq!(stats.queued, 1);
        assert_eq!(stats.throttled, 1);
        assert_eq!(stats.warned, 1);
    }

    #[test]
    fn test_per_key_stats() {
        let metrics = RateLimitMetrics::new();
        let key1 = LimiterKey::User("user1".to_string());
        let key2 = LimiterKey::User("user2".to_string());

        metrics.record_decision(&key1, &RateLimitResult::Allowed, Duration::from_micros(10));
        metrics.record_decision(&key1, &RateLimitResult::Allowed, Duration::from_micros(10));
        metrics.record_decision(&key2, &RateLimitResult::Allowed, Duration::from_micros(10));

        let stats = metrics.get_stats();
        assert_eq!(stats.key_stats.len(), 2);

        let user1_stats = stats.key_stats.get("user:user1").unwrap();
        assert_eq!(user1_stats.total, 2);
    }

    #[test]
    fn test_denial_rate() {
        let metrics = RateLimitMetrics::new();
        let key = LimiterKey::User("test".to_string());

        // 3 allowed, 2 denied = 40% denial rate
        for _ in 0..3 {
            metrics.record_decision(&key, &RateLimitResult::Allowed, Duration::from_micros(10));
        }

        let error = super::super::limiter::RateLimitExceeded {
            key: key.clone(),
            limit_type: super::super::limiter::LimitType::TokenBucket,
            current: 0,
            limit: 100,
            retry_after: Duration::from_secs(1),
            message: "test".to_string(),
        };

        for _ in 0..2 {
            metrics.record_decision(&key, &RateLimitResult::Denied(error.clone()), Duration::from_micros(10));
        }

        let rate = metrics.denial_rate();
        assert!((rate - 0.4).abs() < 0.01);
    }

    #[test]
    fn test_timing_stats() {
        let metrics = RateLimitMetrics::new();
        let key = LimiterKey::User("test".to_string());

        for i in 1..=100 {
            metrics.record_decision(&key, &RateLimitResult::Allowed, Duration::from_micros(i * 10));
        }

        let stats = metrics.get_stats();
        assert!(stats.avg_decision_time_us > 0);
        assert!(stats.p50_decision_time_us > 0);
        assert!(stats.p99_decision_time_us >= stats.p50_decision_time_us);
    }

    #[test]
    fn test_reset() {
        let metrics = RateLimitMetrics::new();
        let key = LimiterKey::User("test".to_string());

        metrics.record_decision(&key, &RateLimitResult::Allowed, Duration::from_micros(10));

        assert!(metrics.total_requests() > 0);

        metrics.reset();

        assert_eq!(metrics.total_requests(), 0);
        assert_eq!(metrics.denied(), 0);
    }

    #[test]
    fn test_reset_key() {
        let metrics = RateLimitMetrics::new();
        let key1 = LimiterKey::User("user1".to_string());
        let key2 = LimiterKey::User("user2".to_string());

        metrics.record_decision(&key1, &RateLimitResult::Allowed, Duration::from_micros(10));
        metrics.record_decision(&key2, &RateLimitResult::Allowed, Duration::from_micros(10));

        assert_eq!(metrics.get_stats().key_stats.len(), 2);

        metrics.reset_key(&key1);

        let stats = metrics.get_stats();
        assert_eq!(stats.key_stats.len(), 1);
        assert!(!stats.key_stats.contains_key("user:user1"));
        assert!(stats.key_stats.contains_key("user:user2"));
    }

    #[test]
    fn test_stats_methods() {
        let stats = RateLimitStats {
            total_requests: 100,
            allowed: 80,
            queued: 5,
            throttled: 5,
            warned: 5,
            denied: 5,
            avg_decision_time_us: 50,
            p50_decision_time_us: 45,
            p99_decision_time_us: 100,
            key_stats: HashMap::new(),
            uptime_secs: 10,
        };

        assert!((stats.denial_rate() - 0.05).abs() < 0.01);
        assert!((stats.allow_rate() - 0.80).abs() < 0.01);
        assert!((stats.requests_per_second() - 10.0).abs() < 0.1);
    }

    #[test]
    fn test_top_keys() {
        let mut key_stats = HashMap::new();

        key_stats.insert("user:high".to_string(), KeyStatsSnapshot {
            total: 100,
            allowed: 50,
            denied: 50,
            last_request_age: None,
        });

        key_stats.insert("user:low".to_string(), KeyStatsSnapshot {
            total: 100,
            allowed: 90,
            denied: 10,
            last_request_age: None,
        });

        key_stats.insert("user:most".to_string(), KeyStatsSnapshot {
            total: 1000,
            allowed: 900,
            denied: 100,
            last_request_age: None,
        });

        let stats = RateLimitStats {
            total_requests: 1200,
            allowed: 1040,
            queued: 0,
            throttled: 0,
            warned: 0,
            denied: 160,
            avg_decision_time_us: 50,
            p50_decision_time_us: 45,
            p99_decision_time_us: 100,
            key_stats,
            uptime_secs: 60,
        };

        // Top denied should be "high" (50% denial rate)
        let top_denied = stats.top_denied_keys(1);
        assert_eq!(top_denied[0].0, "user:high");

        // Top requests should be "most" (1000 requests)
        let top_requests = stats.top_request_keys(1);
        assert_eq!(top_requests[0].0, "user:most");
    }

    #[test]
    fn test_key_stats_snapshot_rates() {
        let snapshot = KeyStatsSnapshot {
            total: 100,
            allowed: 80,
            denied: 20,
            last_request_age: Some(Duration::from_secs(5)),
        };

        assert!((snapshot.denial_rate() - 0.2).abs() < 0.01);
        assert!((snapshot.allow_rate() - 0.8).abs() < 0.01);
    }
}
