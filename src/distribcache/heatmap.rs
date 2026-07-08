//! Cache heatmap analytics
//!
//! Provides visual cache utilization metrics and optimization recommendations.

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Duration;

use super::QueryFingerprint;

/// Access statistics for a table or query
#[derive(Debug)]
pub struct AccessStats {
    /// Total hits
    pub hits: AtomicU64,
    /// Total misses
    pub misses: AtomicU64,
    /// Total time saved in microseconds
    pub total_time_saved_us: AtomicU64,
    /// Last access timestamp (Unix nanos)
    pub last_access: AtomicU64,
}

impl Default for AccessStats {
    fn default() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            total_time_saved_us: AtomicU64::new(0),
            last_access: AtomicU64::new(0),
        }
    }
}

impl AccessStats {
    fn record_hit(&self, time_saved: Duration) {
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.total_time_saved_us
            .fetch_add(time_saved.as_micros() as u64, Ordering::Relaxed);
        self.update_last_access();
    }

    fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.update_last_access();
    }

    fn update_last_access(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        self.last_access.store(now, Ordering::Relaxed);
    }

    fn hit_ratio(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        }
    }

    fn total_accesses(&self) -> u64 {
        self.hits.load(Ordering::Relaxed) + self.misses.load(Ordering::Relaxed)
    }
}

/// Time bucket for time-series data
#[derive(Debug, Clone)]
pub struct TimeBucket {
    /// Bucket start time (Unix timestamp)
    pub start: u64,
    /// Bucket end time (Unix timestamp)
    pub end: u64,
    /// Accesses per table
    pub accesses: HashMap<String, u64>,
    /// Hit ratio for this bucket
    pub hit_ratio: f64,
}

/// Table heat information
#[derive(Debug, Clone)]
pub struct TableHeat {
    /// Table name
    pub name: String,
    /// Total accesses
    pub total_accesses: u64,
    /// Hit ratio
    pub hit_ratio: f64,
    /// Time saved in milliseconds
    pub time_saved_ms: u64,
    /// Temperature classification
    pub temperature: Temperature,
}

/// Temperature classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Temperature {
    /// Very frequently accessed
    Hot,
    /// Moderately accessed
    Warm,
    /// Infrequently accessed
    Cold,
    /// Rarely accessed
    Frozen,
}

/// Priority level for recommendations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    High,
    Medium,
    Low,
}

/// Cache optimization recommendation
#[derive(Debug, Clone)]
pub struct Recommendation {
    /// Target table
    pub table: String,
    /// Issue description
    pub issue: String,
    /// Suggestion for improvement
    pub suggestion: String,
    /// Priority level
    pub priority: Priority,
}

/// Heatmap visualization data
#[derive(Debug, Clone)]
pub struct HeatmapData {
    /// Per-table heat information
    pub tables: Vec<TableHeat>,
    /// Time series data
    pub time_series: Vec<TimeBucket>,
    /// Optimization recommendations
    pub recommendations: Vec<Recommendation>,
}

/// Cache heatmap analytics
pub struct CacheHeatmap {
    /// Access stats per table
    table_accesses: DashMap<String, AccessStats>,

    /// Access stats per query fingerprint
    query_accesses: DashMap<QueryFingerprint, AccessStats>,

    /// Time-bucketed data
    time_buckets: RwLock<Vec<TimeBucket>>,

    /// Current bucket
    current_bucket: RwLock<TimeBucket>,

    /// Bucket size in seconds
    bucket_size_secs: u64,

    /// Maximum buckets to retain
    max_buckets: usize,
}

impl CacheHeatmap {
    /// Create a new heatmap
    pub fn new() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            table_accesses: DashMap::new(),
            query_accesses: DashMap::new(),
            time_buckets: RwLock::new(Vec::new()),
            current_bucket: RwLock::new(TimeBucket {
                start: now,
                end: now + 300, // 5 minute default
                accesses: HashMap::new(),
                hit_ratio: 0.0,
            }),
            bucket_size_secs: 300,
            max_buckets: 2016, // 7 days at 5-minute buckets
        }
    }

    /// Record a cache access
    pub fn record_access(&self, fingerprint: &QueryFingerprint, hit: bool, time_saved: Duration) {
        // Update table stats
        for table in &fingerprint.tables {
            let stats = self.table_accesses.entry(table.clone()).or_default();

            if hit {
                stats.record_hit(time_saved);
            } else {
                stats.record_miss();
            }
        }

        // Update query stats
        let query_stats = self.query_accesses.entry(fingerprint.clone()).or_default();

        if hit {
            query_stats.record_hit(time_saved);
        } else {
            query_stats.record_miss();
        }

        // Update time bucket
        self.update_time_bucket(&fingerprint.tables, hit);
    }

    /// Update time bucket
    fn update_time_bucket(&self, tables: &[String], _hit: bool) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut current = self.current_bucket.write().unwrap();

        // Check if we need to roll to a new bucket
        if now >= current.end {
            // Finalize current bucket
            let mut buckets = self.time_buckets.write().unwrap();

            // Calculate hit ratio for completed bucket
            let total_hits: u64 = self
                .table_accesses
                .iter()
                .map(|e| e.value().hits.load(Ordering::Relaxed))
                .sum();
            let total_misses: u64 = self
                .table_accesses
                .iter()
                .map(|e| e.value().misses.load(Ordering::Relaxed))
                .sum();

            let total = total_hits + total_misses;
            current.hit_ratio = if total > 0 {
                total_hits as f64 / total as f64
            } else {
                0.0
            };

            buckets.push(current.clone());

            // Trim old buckets
            while buckets.len() > self.max_buckets {
                buckets.remove(0);
            }

            // Create new bucket
            *current = TimeBucket {
                start: now,
                end: now + self.bucket_size_secs,
                accesses: HashMap::new(),
                hit_ratio: 0.0,
            };
        }

        // Update current bucket
        for table in tables {
            *current.accesses.entry(table.clone()).or_default() += 1;
        }
    }

    /// Calculate temperature from access count
    fn calculate_temperature(&self, accesses: u64) -> Temperature {
        // Get percentiles from all tables
        let mut all_accesses: Vec<u64> = self
            .table_accesses
            .iter()
            .map(|e| e.value().total_accesses())
            .collect();
        all_accesses.sort();

        if all_accesses.is_empty() {
            return Temperature::Cold;
        }

        let p75 = all_accesses
            .get(all_accesses.len() * 3 / 4)
            .copied()
            .unwrap_or(0);
        let p50 = all_accesses
            .get(all_accesses.len() / 2)
            .copied()
            .unwrap_or(0);
        let p25 = all_accesses
            .get(all_accesses.len() / 4)
            .copied()
            .unwrap_or(0);

        if accesses >= p75 {
            Temperature::Hot
        } else if accesses >= p50 {
            Temperature::Warm
        } else if accesses >= p25 {
            Temperature::Cold
        } else {
            Temperature::Frozen
        }
    }

    /// Generate heatmap visualization data
    pub fn generate_heatmap(&self) -> HeatmapData {
        let mut tables: Vec<TableHeat> = self
            .table_accesses
            .iter()
            .map(|entry| {
                let stats = entry.value();
                let hits = stats.hits.load(Ordering::Relaxed);
                let misses = stats.misses.load(Ordering::Relaxed);
                let total = hits + misses;

                TableHeat {
                    name: entry.key().clone(),
                    total_accesses: total,
                    hit_ratio: stats.hit_ratio(),
                    time_saved_ms: stats.total_time_saved_us.load(Ordering::Relaxed) / 1000,
                    temperature: self.calculate_temperature(total),
                }
            })
            .collect();

        // Sort by total accesses (descending)
        tables.sort_by_key(|b| std::cmp::Reverse(b.total_accesses));

        let time_series = self.get_time_series();
        let recommendations = self.generate_recommendations();

        HeatmapData {
            tables,
            time_series,
            recommendations,
        }
    }

    /// Get time series data
    fn get_time_series(&self) -> Vec<TimeBucket> {
        let buckets = self.time_buckets.read().unwrap();
        buckets.clone()
    }

    /// Generate optimization recommendations
    fn generate_recommendations(&self) -> Vec<Recommendation> {
        let mut recs = Vec::new();

        for entry in self.table_accesses.iter() {
            let table = entry.key();
            let stats = entry.value();
            let hits = stats.hits.load(Ordering::Relaxed);
            let misses = stats.misses.load(Ordering::Relaxed);
            let total = hits + misses;

            if total < 100 {
                continue; // Not enough data
            }

            let hit_ratio = stats.hit_ratio();

            // Low hit ratio recommendation
            if hit_ratio < 0.5 {
                recs.push(Recommendation {
                    table: table.clone(),
                    issue: "Low cache hit ratio".to_string(),
                    suggestion: format!(
                        "Consider increasing TTL or cache size for '{}' (current hit ratio: {:.1}%)",
                        table,
                        hit_ratio * 100.0
                    ),
                    priority: Priority::High,
                });
            }

            // Cold data in cache recommendation
            let last_access = stats.last_access.load(Ordering::Relaxed);
            if last_access > 0 {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;

                // saturating_sub: a wall-clock regression (NTP step-back) can
                // leave last_access > now; an unguarded u64 subtraction would
                // wrap to a huge age and wrongly flag live data as cold.
                let age_secs = now.saturating_sub(last_access) / 1_000_000_000;

                if age_secs > 3600 && total < 1000 {
                    recs.push(Recommendation {
                        table: table.clone(),
                        issue: "Cold data in cache".to_string(),
                        suggestion: format!(
                            "'{}' hasn't been accessed in {} minutes, consider reducing TTL",
                            table,
                            age_secs / 60
                        ),
                        priority: Priority::Medium,
                    });
                }
            }
        }

        recs
    }

    /// Clear all heatmap data
    pub fn clear(&self) {
        self.table_accesses.clear();
        self.query_accesses.clear();
        self.time_buckets.write().unwrap().clear();
    }
}

impl Default for CacheHeatmap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_stats() {
        let stats = AccessStats::default();

        stats.record_hit(Duration::from_millis(10));
        stats.record_hit(Duration::from_millis(20));
        stats.record_miss();

        assert_eq!(stats.hits.load(Ordering::Relaxed), 2);
        assert_eq!(stats.misses.load(Ordering::Relaxed), 1);
        assert!((stats.hit_ratio() - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_record_access() {
        let heatmap = CacheHeatmap::new();
        let fp = QueryFingerprint::from_query("SELECT * FROM users");

        heatmap.record_access(&fp, true, Duration::from_millis(10));
        heatmap.record_access(&fp, true, Duration::from_millis(15));
        heatmap.record_access(&fp, false, Duration::ZERO);

        let data = heatmap.generate_heatmap();
        assert!(!data.tables.is_empty());

        let users_heat = data.tables.iter().find(|t| t.name == "USERS").unwrap();

        assert_eq!(users_heat.total_accesses, 3);
        assert!((users_heat.hit_ratio - 0.666).abs() < 0.01);
    }

    #[test]
    fn test_temperature_classification() {
        let heatmap = CacheHeatmap::new();

        // Add varying access patterns
        for i in 0..100 {
            let fp = QueryFingerprint::from_query(&format!("SELECT * FROM table_{}", i % 10));
            for _ in 0..(i * 10) {
                heatmap.record_access(&fp, true, Duration::from_millis(1));
            }
        }

        let data = heatmap.generate_heatmap();

        // Should have hot, warm, cold, and frozen tables
        let temps: Vec<_> = data.tables.iter().map(|t| t.temperature).collect();
        assert!(temps.contains(&Temperature::Hot));
    }

    #[test]
    fn test_recommendations() {
        let heatmap = CacheHeatmap::new();

        // Create low hit ratio scenario
        let fp = QueryFingerprint::from_query("SELECT * FROM slow_table");
        for _ in 0..50 {
            heatmap.record_access(&fp, true, Duration::from_millis(1));
        }
        for _ in 0..150 {
            heatmap.record_access(&fp, false, Duration::ZERO);
        }

        let data = heatmap.generate_heatmap();

        // Should have a recommendation for low hit ratio
        assert!(!data.recommendations.is_empty());
        let rec = data
            .recommendations
            .iter()
            .find(|r| r.issue.contains("hit ratio"));
        assert!(rec.is_some());
    }
}
