//! Cache Metrics
//!
//! Tracks cache performance statistics including hit rates,
//! latencies, memory usage, and invalidation counts.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::CacheLevel;

/// Cache metrics collector
#[derive(Debug)]
pub struct CacheMetrics {
    /// L1 cache statistics
    l1: CacheStats,

    /// L2 cache statistics
    l2: CacheStats,

    /// L3 cache statistics
    l3: CacheStats,

    /// Total misses
    misses: AtomicU64,

    /// Total skips (due to hints)
    skips: AtomicU64,

    /// Total puts
    puts: AtomicU64,

    /// Total invalidations
    invalidations: AtomicU64,

    /// Tables invalidated
    tables_invalidated: AtomicU64,

    /// Cache clears
    clears: AtomicU64,

    /// Size exceeded rejections
    size_exceeded: AtomicU64,

    /// Creation time
    created_at: Instant,
}

/// Statistics for a single cache level
#[derive(Debug, Default)]
pub struct CacheStats {
    /// Cache hits
    hits: AtomicU64,

    /// Total latency in microseconds (for average calculation)
    total_latency_us: AtomicU64,

    /// Minimum latency in microseconds
    min_latency_us: AtomicU64,

    /// Maximum latency in microseconds
    max_latency_us: AtomicU64,

    /// Current entry count
    entry_count: AtomicU64,

    /// Current memory usage in bytes
    memory_bytes: AtomicU64,

    /// Evictions
    evictions: AtomicU64,
}

impl CacheStats {
    fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            total_latency_us: AtomicU64::new(0),
            min_latency_us: AtomicU64::new(u64::MAX),
            max_latency_us: AtomicU64::new(0),
            entry_count: AtomicU64::new(0),
            memory_bytes: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    fn record_hit(&self, latency: Duration) {
        self.hits.fetch_add(1, Ordering::Relaxed);

        let latency_us = latency.as_micros() as u64;
        self.total_latency_us.fetch_add(latency_us, Ordering::Relaxed);

        // Update min
        let mut current = self.min_latency_us.load(Ordering::Relaxed);
        while latency_us < current {
            match self.min_latency_us.compare_exchange_weak(
                current,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }

        // Update max
        let mut current = self.max_latency_us.load(Ordering::Relaxed);
        while latency_us > current {
            match self.max_latency_us.compare_exchange_weak(
                current,
                latency_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    fn snapshot(&self) -> CacheStatsLevelSnapshot {
        let hits = self.hits.load(Ordering::Relaxed);
        let total_latency = self.total_latency_us.load(Ordering::Relaxed);
        let min_latency = self.min_latency_us.load(Ordering::Relaxed);
        let max_latency = self.max_latency_us.load(Ordering::Relaxed);

        CacheStatsLevelSnapshot {
            hits,
            avg_latency_us: if hits > 0 { total_latency / hits } else { 0 },
            min_latency_us: if min_latency == u64::MAX { 0 } else { min_latency },
            max_latency_us: max_latency,
            entry_count: self.entry_count.load(Ordering::Relaxed),
            memory_bytes: self.memory_bytes.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}

impl CacheMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            l1: CacheStats::new(),
            l2: CacheStats::new(),
            l3: CacheStats::new(),
            misses: AtomicU64::new(0),
            skips: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
            tables_invalidated: AtomicU64::new(0),
            clears: AtomicU64::new(0),
            size_exceeded: AtomicU64::new(0),
            created_at: Instant::now(),
        }
    }

    /// Record a cache hit
    pub fn record_hit(&self, level: CacheLevel, latency: Duration) {
        match level {
            CacheLevel::L1Hot => self.l1.record_hit(latency),
            CacheLevel::L2Warm => self.l2.record_hit(latency),
            CacheLevel::L3Semantic => self.l3.record_hit(latency),
        }
    }

    /// Record a cache miss
    pub fn record_miss(&self, _latency: Duration) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache skip (due to hint)
    pub fn record_skip(&self) {
        self.skips.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache put
    pub fn record_put(&self) {
        self.puts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record cache invalidation
    pub fn record_invalidation(&self, table_count: usize) {
        self.invalidations.fetch_add(1, Ordering::Relaxed);
        self.tables_invalidated.fetch_add(table_count as u64, Ordering::Relaxed);
    }

    /// Record cache clear
    pub fn record_clear(&self) {
        self.clears.fetch_add(1, Ordering::Relaxed);
    }

    /// Record size exceeded rejection
    pub fn record_size_exceeded(&self) {
        self.size_exceeded.fetch_add(1, Ordering::Relaxed);
    }

    /// Record eviction for a cache level
    pub fn record_eviction(&self, level: CacheLevel) {
        match level {
            CacheLevel::L1Hot => self.l1.evictions.fetch_add(1, Ordering::Relaxed),
            CacheLevel::L2Warm => self.l2.evictions.fetch_add(1, Ordering::Relaxed),
            CacheLevel::L3Semantic => self.l3.evictions.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// Update entry count for a cache level
    pub fn set_entry_count(&self, level: CacheLevel, count: u64) {
        match level {
            CacheLevel::L1Hot => self.l1.entry_count.store(count, Ordering::Relaxed),
            CacheLevel::L2Warm => self.l2.entry_count.store(count, Ordering::Relaxed),
            CacheLevel::L3Semantic => self.l3.entry_count.store(count, Ordering::Relaxed),
        }
    }

    /// Update memory usage for a cache level
    pub fn set_memory_bytes(&self, level: CacheLevel, bytes: u64) {
        match level {
            CacheLevel::L1Hot => self.l1.memory_bytes.store(bytes, Ordering::Relaxed),
            CacheLevel::L2Warm => self.l2.memory_bytes.store(bytes, Ordering::Relaxed),
            CacheLevel::L3Semantic => self.l3.memory_bytes.store(bytes, Ordering::Relaxed),
        }
    }

    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        let l1 = self.l1.snapshot();
        let l2 = self.l2.snapshot();
        let l3 = self.l3.snapshot();
        let misses = self.misses.load(Ordering::Relaxed);
        let skips = self.skips.load(Ordering::Relaxed);

        let total_hits = l1.hits + l2.hits + l3.hits;
        let total_requests = total_hits + misses;

        CacheStatsSnapshot {
            l1,
            l2,
            l3,
            total_hits,
            total_misses: misses,
            total_skips: skips,
            hit_rate: if total_requests > 0 {
                (total_hits as f64 / total_requests as f64) * 100.0
            } else {
                0.0
            },
            puts: self.puts.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            tables_invalidated: self.tables_invalidated.load(Ordering::Relaxed),
            clears: self.clears.load(Ordering::Relaxed),
            size_exceeded: self.size_exceeded.load(Ordering::Relaxed),
            uptime_secs: self.created_at.elapsed().as_secs(),
        }
    }

    /// Get total hit count
    pub fn total_hits(&self) -> u64 {
        self.l1.hits.load(Ordering::Relaxed)
            + self.l2.hits.load(Ordering::Relaxed)
            + self.l3.hits.load(Ordering::Relaxed)
    }

    /// Get total miss count
    pub fn total_misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Calculate hit rate percentage
    pub fn hit_rate(&self) -> f64 {
        let hits = self.total_hits();
        let misses = self.total_misses();
        let total = hits + misses;

        if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }
}

impl Default for CacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of cache statistics for a single level
#[derive(Debug, Clone)]
pub struct CacheStatsLevelSnapshot {
    /// Number of cache hits
    pub hits: u64,

    /// Average latency in microseconds
    pub avg_latency_us: u64,

    /// Minimum latency in microseconds
    pub min_latency_us: u64,

    /// Maximum latency in microseconds
    pub max_latency_us: u64,

    /// Current entry count
    pub entry_count: u64,

    /// Current memory usage in bytes
    pub memory_bytes: u64,

    /// Number of evictions
    pub evictions: u64,
}

/// Snapshot of all cache statistics
#[derive(Debug, Clone)]
pub struct CacheStatsSnapshot {
    /// L1 cache statistics
    pub l1: CacheStatsLevelSnapshot,

    /// L2 cache statistics
    pub l2: CacheStatsLevelSnapshot,

    /// L3 cache statistics
    pub l3: CacheStatsLevelSnapshot,

    /// Total hits across all levels
    pub total_hits: u64,

    /// Total misses
    pub total_misses: u64,

    /// Total skips (due to hints)
    pub total_skips: u64,

    /// Overall hit rate percentage
    pub hit_rate: f64,

    /// Total cache puts
    pub puts: u64,

    /// Total invalidation operations
    pub invalidations: u64,

    /// Total tables invalidated
    pub tables_invalidated: u64,

    /// Total cache clears
    pub clears: u64,

    /// Requests rejected due to size limits
    pub size_exceeded: u64,

    /// Uptime in seconds
    pub uptime_secs: u64,
}

impl CacheStatsSnapshot {
    /// Calculate total memory usage across all levels
    pub fn total_memory_bytes(&self) -> u64 {
        self.l1.memory_bytes + self.l2.memory_bytes + self.l3.memory_bytes
    }

    /// Calculate total entry count across all levels
    pub fn total_entries(&self) -> u64 {
        self.l1.entry_count + self.l2.entry_count + self.l3.entry_count
    }

    /// Format as human-readable string
    pub fn format(&self) -> String {
        format!(
            "Cache Stats:\n\
             ├─ Hit Rate: {:.2}%\n\
             ├─ Total Hits: {} (L1: {}, L2: {}, L3: {})\n\
             ├─ Total Misses: {}\n\
             ├─ Total Entries: {} ({} bytes)\n\
             ├─ L1 Avg Latency: {}μs\n\
             ├─ L2 Avg Latency: {}μs\n\
             ├─ L3 Avg Latency: {}μs\n\
             ├─ Invalidations: {} ({} tables)\n\
             └─ Uptime: {}s",
            self.hit_rate,
            self.total_hits,
            self.l1.hits,
            self.l2.hits,
            self.l3.hits,
            self.total_misses,
            self.total_entries(),
            self.total_memory_bytes(),
            self.l1.avg_latency_us,
            self.l2.avg_latency_us,
            self.l3.avg_latency_us,
            self.invalidations,
            self.tables_invalidated,
            self.uptime_secs
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = CacheMetrics::new();
        assert_eq!(metrics.total_hits(), 0);
        assert_eq!(metrics.total_misses(), 0);
    }

    #[test]
    fn test_record_hit() {
        let metrics = CacheMetrics::new();

        metrics.record_hit(CacheLevel::L1Hot, Duration::from_micros(100));
        metrics.record_hit(CacheLevel::L1Hot, Duration::from_micros(200));
        metrics.record_hit(CacheLevel::L2Warm, Duration::from_micros(500));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.l1.hits, 2);
        assert_eq!(snapshot.l2.hits, 1);
        assert_eq!(snapshot.total_hits, 3);
    }

    #[test]
    fn test_record_miss() {
        let metrics = CacheMetrics::new();

        metrics.record_miss(Duration::from_micros(100));
        metrics.record_miss(Duration::from_micros(100));

        assert_eq!(metrics.total_misses(), 2);
    }

    #[test]
    fn test_hit_rate() {
        let metrics = CacheMetrics::new();

        // 3 hits, 1 miss = 75% hit rate
        metrics.record_hit(CacheLevel::L1Hot, Duration::from_micros(100));
        metrics.record_hit(CacheLevel::L1Hot, Duration::from_micros(100));
        metrics.record_hit(CacheLevel::L2Warm, Duration::from_micros(100));
        metrics.record_miss(Duration::from_micros(100));

        let rate = metrics.hit_rate();
        assert!((rate - 75.0).abs() < 0.01);
    }

    #[test]
    fn test_latency_tracking() {
        let metrics = CacheMetrics::new();

        metrics.record_hit(CacheLevel::L1Hot, Duration::from_micros(100));
        metrics.record_hit(CacheLevel::L1Hot, Duration::from_micros(300));
        metrics.record_hit(CacheLevel::L1Hot, Duration::from_micros(200));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.l1.min_latency_us, 100);
        assert_eq!(snapshot.l1.max_latency_us, 300);
        assert_eq!(snapshot.l1.avg_latency_us, 200); // (100+300+200)/3 = 200
    }

    #[test]
    fn test_invalidation_tracking() {
        let metrics = CacheMetrics::new();

        metrics.record_invalidation(3);
        metrics.record_invalidation(2);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.invalidations, 2);
        assert_eq!(snapshot.tables_invalidated, 5);
    }

    #[test]
    fn test_entry_count_tracking() {
        let metrics = CacheMetrics::new();

        metrics.set_entry_count(CacheLevel::L1Hot, 100);
        metrics.set_entry_count(CacheLevel::L2Warm, 500);
        metrics.set_entry_count(CacheLevel::L3Semantic, 50);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.l1.entry_count, 100);
        assert_eq!(snapshot.l2.entry_count, 500);
        assert_eq!(snapshot.l3.entry_count, 50);
        assert_eq!(snapshot.total_entries(), 650);
    }

    #[test]
    fn test_memory_tracking() {
        let metrics = CacheMetrics::new();

        metrics.set_memory_bytes(CacheLevel::L1Hot, 1024);
        metrics.set_memory_bytes(CacheLevel::L2Warm, 1024 * 1024);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.l1.memory_bytes, 1024);
        assert_eq!(snapshot.l2.memory_bytes, 1024 * 1024);
    }

    #[test]
    fn test_snapshot_format() {
        let metrics = CacheMetrics::new();
        metrics.record_hit(CacheLevel::L1Hot, Duration::from_micros(100));
        metrics.record_miss(Duration::from_micros(100));

        let snapshot = metrics.snapshot();
        let formatted = snapshot.format();

        assert!(formatted.contains("Hit Rate:"));
        assert!(formatted.contains("Total Hits:"));
    }

    #[test]
    fn test_eviction_tracking() {
        let metrics = CacheMetrics::new();

        metrics.record_eviction(CacheLevel::L1Hot);
        metrics.record_eviction(CacheLevel::L1Hot);
        metrics.record_eviction(CacheLevel::L2Warm);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.l1.evictions, 2);
        assert_eq!(snapshot.l2.evictions, 1);
    }
}
