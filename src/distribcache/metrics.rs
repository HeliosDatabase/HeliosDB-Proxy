//! DistribCache metrics
//!
//! Prometheus-compatible metrics for cache observability.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::classifier::WorkloadType;
use super::tiers::CacheTier;

/// Comprehensive metrics for DistribCache
#[derive(Debug)]
pub struct DistribCacheMetrics {
    /// Start time for uptime calculation
    start_time: Instant,

    // Cache operations
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_puts: AtomicU64,
    pub cache_evictions: AtomicU64,
    pub cache_invalidations: AtomicU64,

    // Per-tier metrics
    pub l1_hits: AtomicU64,
    pub l1_misses: AtomicU64,
    pub l1_size_bytes: AtomicU64,
    pub l1_entries: AtomicU64,

    pub l2_hits: AtomicU64,
    pub l2_misses: AtomicU64,
    pub l2_size_bytes: AtomicU64,
    pub l2_entries: AtomicU64,

    pub l3_hits: AtomicU64,
    pub l3_misses: AtomicU64,
    pub l3_size_bytes: AtomicU64,
    pub l3_entries: AtomicU64,

    // Latency buckets (count of operations in each bucket)
    pub latency_under_100us: AtomicU64,
    pub latency_100us_1ms: AtomicU64,
    pub latency_1ms_10ms: AtomicU64,
    pub latency_10ms_100ms: AtomicU64,
    pub latency_over_100ms: AtomicU64,
    pub latency_total_us: AtomicU64,
    pub latency_count: AtomicU64,

    // Workload metrics
    pub oltp_queries: AtomicU64,
    pub olap_queries: AtomicU64,
    pub vector_queries: AtomicU64,
    pub ai_agent_queries: AtomicU64,
    pub rag_queries: AtomicU64,
    pub mixed_queries: AtomicU64,

    // AI cache metrics
    pub conversation_cache_hits: AtomicU64,
    pub conversation_cache_misses: AtomicU64,
    pub rag_cache_hits: AtomicU64,
    pub rag_cache_misses: AtomicU64,
    pub tool_cache_hits: AtomicU64,
    pub tool_cache_misses: AtomicU64,
    pub semantic_cache_hits: AtomicU64,
    pub semantic_cache_misses: AtomicU64,

    // Prefetch metrics
    pub prefetch_hits: AtomicU64,
    pub prefetch_misses: AtomicU64,
    pub prefetch_predictions: AtomicU64,

    // Invalidation metrics
    pub wal_invalidations: AtomicU64,
    pub ttl_invalidations: AtomicU64,
    pub manual_invalidations: AtomicU64,

    // Scheduler metrics
    pub scheduled_queries: AtomicU64,
    pub queued_queries: AtomicU64,
    pub rejected_queries: AtomicU64,

    // Error metrics
    pub cache_errors: AtomicU64,
    pub timeout_errors: AtomicU64,
    pub serialization_errors: AtomicU64,
}

impl Default for DistribCacheMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl DistribCacheMetrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_puts: AtomicU64::new(0),
            cache_evictions: AtomicU64::new(0),
            cache_invalidations: AtomicU64::new(0),
            l1_hits: AtomicU64::new(0),
            l1_misses: AtomicU64::new(0),
            l1_size_bytes: AtomicU64::new(0),
            l1_entries: AtomicU64::new(0),
            l2_hits: AtomicU64::new(0),
            l2_misses: AtomicU64::new(0),
            l2_size_bytes: AtomicU64::new(0),
            l2_entries: AtomicU64::new(0),
            l3_hits: AtomicU64::new(0),
            l3_misses: AtomicU64::new(0),
            l3_size_bytes: AtomicU64::new(0),
            l3_entries: AtomicU64::new(0),
            latency_under_100us: AtomicU64::new(0),
            latency_100us_1ms: AtomicU64::new(0),
            latency_1ms_10ms: AtomicU64::new(0),
            latency_10ms_100ms: AtomicU64::new(0),
            latency_over_100ms: AtomicU64::new(0),
            latency_total_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            oltp_queries: AtomicU64::new(0),
            olap_queries: AtomicU64::new(0),
            vector_queries: AtomicU64::new(0),
            ai_agent_queries: AtomicU64::new(0),
            rag_queries: AtomicU64::new(0),
            mixed_queries: AtomicU64::new(0),
            conversation_cache_hits: AtomicU64::new(0),
            conversation_cache_misses: AtomicU64::new(0),
            rag_cache_hits: AtomicU64::new(0),
            rag_cache_misses: AtomicU64::new(0),
            tool_cache_hits: AtomicU64::new(0),
            tool_cache_misses: AtomicU64::new(0),
            semantic_cache_hits: AtomicU64::new(0),
            semantic_cache_misses: AtomicU64::new(0),
            prefetch_hits: AtomicU64::new(0),
            prefetch_misses: AtomicU64::new(0),
            prefetch_predictions: AtomicU64::new(0),
            wal_invalidations: AtomicU64::new(0),
            ttl_invalidations: AtomicU64::new(0),
            manual_invalidations: AtomicU64::new(0),
            scheduled_queries: AtomicU64::new(0),
            queued_queries: AtomicU64::new(0),
            rejected_queries: AtomicU64::new(0),
            cache_errors: AtomicU64::new(0),
            timeout_errors: AtomicU64::new(0),
            serialization_errors: AtomicU64::new(0),
        }
    }

    /// Record cache hit
    pub fn record_hit(&self, tier: CacheTier) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        match tier {
            CacheTier::L1 => { self.l1_hits.fetch_add(1, Ordering::Relaxed); }
            CacheTier::L2 => { self.l2_hits.fetch_add(1, Ordering::Relaxed); }
            CacheTier::L3 => { self.l3_hits.fetch_add(1, Ordering::Relaxed); }
        }
    }

    /// Record cache miss
    pub fn record_miss(&self, tier: CacheTier) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        match tier {
            CacheTier::L1 => { self.l1_misses.fetch_add(1, Ordering::Relaxed); }
            CacheTier::L2 => { self.l2_misses.fetch_add(1, Ordering::Relaxed); }
            CacheTier::L3 => { self.l3_misses.fetch_add(1, Ordering::Relaxed); }
        }
    }

    /// Record cache put
    pub fn record_put(&self) {
        self.cache_puts.fetch_add(1, Ordering::Relaxed);
    }

    /// Record eviction
    pub fn record_eviction(&self) {
        self.cache_evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record invalidation
    pub fn record_invalidation(&self, source: InvalidationSource) {
        self.cache_invalidations.fetch_add(1, Ordering::Relaxed);
        match source {
            InvalidationSource::WAL => { self.wal_invalidations.fetch_add(1, Ordering::Relaxed); }
            InvalidationSource::TTL => { self.ttl_invalidations.fetch_add(1, Ordering::Relaxed); }
            InvalidationSource::Manual => { self.manual_invalidations.fetch_add(1, Ordering::Relaxed); }
        }
    }

    /// Record latency
    pub fn record_latency(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.latency_total_us.fetch_add(us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);

        if us < 100 {
            self.latency_under_100us.fetch_add(1, Ordering::Relaxed);
        } else if us < 1000 {
            self.latency_100us_1ms.fetch_add(1, Ordering::Relaxed);
        } else if us < 10_000 {
            self.latency_1ms_10ms.fetch_add(1, Ordering::Relaxed);
        } else if us < 100_000 {
            self.latency_10ms_100ms.fetch_add(1, Ordering::Relaxed);
        } else {
            self.latency_over_100ms.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record workload type
    pub fn record_workload(&self, workload: WorkloadType) {
        match workload {
            WorkloadType::OLTP => { self.oltp_queries.fetch_add(1, Ordering::Relaxed); }
            WorkloadType::OLAP => { self.olap_queries.fetch_add(1, Ordering::Relaxed); }
            WorkloadType::Vector => { self.vector_queries.fetch_add(1, Ordering::Relaxed); }
            WorkloadType::AIAgent => { self.ai_agent_queries.fetch_add(1, Ordering::Relaxed); }
            WorkloadType::RAG => { self.rag_queries.fetch_add(1, Ordering::Relaxed); }
            WorkloadType::Mixed => { self.mixed_queries.fetch_add(1, Ordering::Relaxed); }
        }
    }

    /// Update tier size
    pub fn update_tier_size(&self, tier: CacheTier, size_bytes: u64, entries: u64) {
        match tier {
            CacheTier::L1 => {
                self.l1_size_bytes.store(size_bytes, Ordering::Relaxed);
                self.l1_entries.store(entries, Ordering::Relaxed);
            }
            CacheTier::L2 => {
                self.l2_size_bytes.store(size_bytes, Ordering::Relaxed);
                self.l2_entries.store(entries, Ordering::Relaxed);
            }
            CacheTier::L3 => {
                self.l3_size_bytes.store(size_bytes, Ordering::Relaxed);
                self.l3_entries.store(entries, Ordering::Relaxed);
            }
        }
    }

    /// Record error
    pub fn record_error(&self, error_type: ErrorType) {
        self.cache_errors.fetch_add(1, Ordering::Relaxed);
        match error_type {
            ErrorType::Timeout => { self.timeout_errors.fetch_add(1, Ordering::Relaxed); }
            ErrorType::Serialization => { self.serialization_errors.fetch_add(1, Ordering::Relaxed); }
            ErrorType::Other => {}
        }
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get overall hit rate
    pub fn hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Get average latency in microseconds
    pub fn avg_latency_us(&self) -> f64 {
        let total = self.latency_total_us.load(Ordering::Relaxed);
        let count = self.latency_count.load(Ordering::Relaxed);
        if count > 0 {
            total as f64 / count as f64
        } else {
            0.0
        }
    }

    /// Export as Prometheus text format
    pub fn to_prometheus(&self) -> String {
        let mut output = String::with_capacity(4096);

        // Uptime
        output.push_str(&format!(
            "# HELP distribcache_uptime_seconds Cache uptime in seconds\n\
             # TYPE distribcache_uptime_seconds gauge\n\
             distribcache_uptime_seconds {}\n\n",
            self.uptime().as_secs()
        ));

        // Cache operations
        output.push_str(&format!(
            "# HELP distribcache_operations_total Total cache operations\n\
             # TYPE distribcache_operations_total counter\n\
             distribcache_operations_total{{operation=\"hit\"}} {}\n\
             distribcache_operations_total{{operation=\"miss\"}} {}\n\
             distribcache_operations_total{{operation=\"put\"}} {}\n\
             distribcache_operations_total{{operation=\"eviction\"}} {}\n\
             distribcache_operations_total{{operation=\"invalidation\"}} {}\n\n",
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
            self.cache_puts.load(Ordering::Relaxed),
            self.cache_evictions.load(Ordering::Relaxed),
            self.cache_invalidations.load(Ordering::Relaxed),
        ));

        // Hit rate
        output.push_str(&format!(
            "# HELP distribcache_hit_rate Cache hit rate\n\
             # TYPE distribcache_hit_rate gauge\n\
             distribcache_hit_rate {:.4}\n\n",
            self.hit_rate()
        ));

        // Per-tier metrics
        output.push_str(&format!(
            "# HELP distribcache_tier_hits_total Hits per tier\n\
             # TYPE distribcache_tier_hits_total counter\n\
             distribcache_tier_hits_total{{tier=\"l1\"}} {}\n\
             distribcache_tier_hits_total{{tier=\"l2\"}} {}\n\
             distribcache_tier_hits_total{{tier=\"l3\"}} {}\n\n",
            self.l1_hits.load(Ordering::Relaxed),
            self.l2_hits.load(Ordering::Relaxed),
            self.l3_hits.load(Ordering::Relaxed),
        ));

        output.push_str(&format!(
            "# HELP distribcache_tier_size_bytes Size per tier in bytes\n\
             # TYPE distribcache_tier_size_bytes gauge\n\
             distribcache_tier_size_bytes{{tier=\"l1\"}} {}\n\
             distribcache_tier_size_bytes{{tier=\"l2\"}} {}\n\
             distribcache_tier_size_bytes{{tier=\"l3\"}} {}\n\n",
            self.l1_size_bytes.load(Ordering::Relaxed),
            self.l2_size_bytes.load(Ordering::Relaxed),
            self.l3_size_bytes.load(Ordering::Relaxed),
        ));

        output.push_str(&format!(
            "# HELP distribcache_tier_entries Entries per tier\n\
             # TYPE distribcache_tier_entries gauge\n\
             distribcache_tier_entries{{tier=\"l1\"}} {}\n\
             distribcache_tier_entries{{tier=\"l2\"}} {}\n\
             distribcache_tier_entries{{tier=\"l3\"}} {}\n\n",
            self.l1_entries.load(Ordering::Relaxed),
            self.l2_entries.load(Ordering::Relaxed),
            self.l3_entries.load(Ordering::Relaxed),
        ));

        // Latency histogram
        output.push_str(&format!(
            "# HELP distribcache_latency_bucket Latency distribution\n\
             # TYPE distribcache_latency_bucket histogram\n\
             distribcache_latency_bucket{{le=\"0.0001\"}} {}\n\
             distribcache_latency_bucket{{le=\"0.001\"}} {}\n\
             distribcache_latency_bucket{{le=\"0.01\"}} {}\n\
             distribcache_latency_bucket{{le=\"0.1\"}} {}\n\
             distribcache_latency_bucket{{le=\"+Inf\"}} {}\n\n",
            self.latency_under_100us.load(Ordering::Relaxed),
            self.latency_under_100us.load(Ordering::Relaxed) +
                self.latency_100us_1ms.load(Ordering::Relaxed),
            self.latency_under_100us.load(Ordering::Relaxed) +
                self.latency_100us_1ms.load(Ordering::Relaxed) +
                self.latency_1ms_10ms.load(Ordering::Relaxed),
            self.latency_under_100us.load(Ordering::Relaxed) +
                self.latency_100us_1ms.load(Ordering::Relaxed) +
                self.latency_1ms_10ms.load(Ordering::Relaxed) +
                self.latency_10ms_100ms.load(Ordering::Relaxed),
            self.latency_count.load(Ordering::Relaxed),
        ));

        output.push_str(&format!(
            "# HELP distribcache_latency_avg_us Average latency in microseconds\n\
             # TYPE distribcache_latency_avg_us gauge\n\
             distribcache_latency_avg_us {:.2}\n\n",
            self.avg_latency_us()
        ));

        // Workload distribution
        output.push_str(&format!(
            "# HELP distribcache_workload_total Queries by workload type\n\
             # TYPE distribcache_workload_total counter\n\
             distribcache_workload_total{{type=\"oltp\"}} {}\n\
             distribcache_workload_total{{type=\"olap\"}} {}\n\
             distribcache_workload_total{{type=\"vector\"}} {}\n\
             distribcache_workload_total{{type=\"ai_agent\"}} {}\n\
             distribcache_workload_total{{type=\"rag\"}} {}\n\
             distribcache_workload_total{{type=\"mixed\"}} {}\n\n",
            self.oltp_queries.load(Ordering::Relaxed),
            self.olap_queries.load(Ordering::Relaxed),
            self.vector_queries.load(Ordering::Relaxed),
            self.ai_agent_queries.load(Ordering::Relaxed),
            self.rag_queries.load(Ordering::Relaxed),
            self.mixed_queries.load(Ordering::Relaxed),
        ));

        // AI cache metrics
        output.push_str(&format!(
            "# HELP distribcache_ai_cache_hits AI cache hits\n\
             # TYPE distribcache_ai_cache_hits counter\n\
             distribcache_ai_cache_hits{{cache=\"conversation\"}} {}\n\
             distribcache_ai_cache_hits{{cache=\"rag\"}} {}\n\
             distribcache_ai_cache_hits{{cache=\"tool\"}} {}\n\
             distribcache_ai_cache_hits{{cache=\"semantic\"}} {}\n\n",
            self.conversation_cache_hits.load(Ordering::Relaxed),
            self.rag_cache_hits.load(Ordering::Relaxed),
            self.tool_cache_hits.load(Ordering::Relaxed),
            self.semantic_cache_hits.load(Ordering::Relaxed),
        ));

        // Invalidation by source
        output.push_str(&format!(
            "# HELP distribcache_invalidations_total Invalidations by source\n\
             # TYPE distribcache_invalidations_total counter\n\
             distribcache_invalidations_total{{source=\"wal\"}} {}\n\
             distribcache_invalidations_total{{source=\"ttl\"}} {}\n\
             distribcache_invalidations_total{{source=\"manual\"}} {}\n\n",
            self.wal_invalidations.load(Ordering::Relaxed),
            self.ttl_invalidations.load(Ordering::Relaxed),
            self.manual_invalidations.load(Ordering::Relaxed),
        ));

        // Errors
        output.push_str(&format!(
            "# HELP distribcache_errors_total Cache errors\n\
             # TYPE distribcache_errors_total counter\n\
             distribcache_errors_total{{type=\"timeout\"}} {}\n\
             distribcache_errors_total{{type=\"serialization\"}} {}\n\
             distribcache_errors_total{{type=\"total\"}} {}\n",
            self.timeout_errors.load(Ordering::Relaxed),
            self.serialization_errors.load(Ordering::Relaxed),
            self.cache_errors.load(Ordering::Relaxed),
        ));

        output
    }

    /// Export as JSON
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "uptime_secs": self.uptime().as_secs(),
            "operations": {
                "hits": self.cache_hits.load(Ordering::Relaxed),
                "misses": self.cache_misses.load(Ordering::Relaxed),
                "puts": self.cache_puts.load(Ordering::Relaxed),
                "evictions": self.cache_evictions.load(Ordering::Relaxed),
                "invalidations": self.cache_invalidations.load(Ordering::Relaxed),
            },
            "hit_rate": self.hit_rate(),
            "tiers": {
                "l1": {
                    "hits": self.l1_hits.load(Ordering::Relaxed),
                    "misses": self.l1_misses.load(Ordering::Relaxed),
                    "size_bytes": self.l1_size_bytes.load(Ordering::Relaxed),
                    "entries": self.l1_entries.load(Ordering::Relaxed),
                },
                "l2": {
                    "hits": self.l2_hits.load(Ordering::Relaxed),
                    "misses": self.l2_misses.load(Ordering::Relaxed),
                    "size_bytes": self.l2_size_bytes.load(Ordering::Relaxed),
                    "entries": self.l2_entries.load(Ordering::Relaxed),
                },
                "l3": {
                    "hits": self.l3_hits.load(Ordering::Relaxed),
                    "misses": self.l3_misses.load(Ordering::Relaxed),
                    "size_bytes": self.l3_size_bytes.load(Ordering::Relaxed),
                    "entries": self.l3_entries.load(Ordering::Relaxed),
                },
            },
            "latency": {
                "avg_us": self.avg_latency_us(),
                "buckets": {
                    "under_100us": self.latency_under_100us.load(Ordering::Relaxed),
                    "100us_1ms": self.latency_100us_1ms.load(Ordering::Relaxed),
                    "1ms_10ms": self.latency_1ms_10ms.load(Ordering::Relaxed),
                    "10ms_100ms": self.latency_10ms_100ms.load(Ordering::Relaxed),
                    "over_100ms": self.latency_over_100ms.load(Ordering::Relaxed),
                },
            },
            "workloads": {
                "oltp": self.oltp_queries.load(Ordering::Relaxed),
                "olap": self.olap_queries.load(Ordering::Relaxed),
                "vector": self.vector_queries.load(Ordering::Relaxed),
                "ai_agent": self.ai_agent_queries.load(Ordering::Relaxed),
                "rag": self.rag_queries.load(Ordering::Relaxed),
                "mixed": self.mixed_queries.load(Ordering::Relaxed),
            },
            "ai_caches": {
                "conversation": {
                    "hits": self.conversation_cache_hits.load(Ordering::Relaxed),
                    "misses": self.conversation_cache_misses.load(Ordering::Relaxed),
                },
                "rag": {
                    "hits": self.rag_cache_hits.load(Ordering::Relaxed),
                    "misses": self.rag_cache_misses.load(Ordering::Relaxed),
                },
                "tool": {
                    "hits": self.tool_cache_hits.load(Ordering::Relaxed),
                    "misses": self.tool_cache_misses.load(Ordering::Relaxed),
                },
                "semantic": {
                    "hits": self.semantic_cache_hits.load(Ordering::Relaxed),
                    "misses": self.semantic_cache_misses.load(Ordering::Relaxed),
                },
            },
            "errors": {
                "total": self.cache_errors.load(Ordering::Relaxed),
                "timeout": self.timeout_errors.load(Ordering::Relaxed),
                "serialization": self.serialization_errors.load(Ordering::Relaxed),
            },
        })
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.cache_puts.store(0, Ordering::Relaxed);
        self.cache_evictions.store(0, Ordering::Relaxed);
        self.cache_invalidations.store(0, Ordering::Relaxed);

        self.l1_hits.store(0, Ordering::Relaxed);
        self.l1_misses.store(0, Ordering::Relaxed);
        self.l2_hits.store(0, Ordering::Relaxed);
        self.l2_misses.store(0, Ordering::Relaxed);
        self.l3_hits.store(0, Ordering::Relaxed);
        self.l3_misses.store(0, Ordering::Relaxed);

        self.latency_under_100us.store(0, Ordering::Relaxed);
        self.latency_100us_1ms.store(0, Ordering::Relaxed);
        self.latency_1ms_10ms.store(0, Ordering::Relaxed);
        self.latency_10ms_100ms.store(0, Ordering::Relaxed);
        self.latency_over_100ms.store(0, Ordering::Relaxed);
        self.latency_total_us.store(0, Ordering::Relaxed);
        self.latency_count.store(0, Ordering::Relaxed);

        self.oltp_queries.store(0, Ordering::Relaxed);
        self.olap_queries.store(0, Ordering::Relaxed);
        self.vector_queries.store(0, Ordering::Relaxed);
        self.ai_agent_queries.store(0, Ordering::Relaxed);
        self.rag_queries.store(0, Ordering::Relaxed);
        self.mixed_queries.store(0, Ordering::Relaxed);

        self.cache_errors.store(0, Ordering::Relaxed);
        self.timeout_errors.store(0, Ordering::Relaxed);
        self.serialization_errors.store(0, Ordering::Relaxed);
    }
}

// CacheTier is imported from super::tiers

/// Invalidation source
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationSource {
    WAL,
    TTL,
    Manual,
}

// WorkloadType is imported from super::classifier

/// Error type for metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    Timeout,
    Serialization,
    Other,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = DistribCacheMetrics::new();
        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.cache_misses.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_record_hit() {
        let metrics = DistribCacheMetrics::new();

        metrics.record_hit(CacheTier::L1);
        metrics.record_hit(CacheTier::L2);
        metrics.record_hit(CacheTier::L1);

        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.l1_hits.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.l2_hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_hit_rate() {
        let metrics = DistribCacheMetrics::new();

        metrics.record_hit(CacheTier::L1);
        metrics.record_hit(CacheTier::L1);
        metrics.record_miss(CacheTier::L1);
        metrics.record_miss(CacheTier::L1);

        assert!((metrics.hit_rate() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_record_latency() {
        let metrics = DistribCacheMetrics::new();

        metrics.record_latency(Duration::from_micros(50));   // under 100us
        metrics.record_latency(Duration::from_micros(500));  // 100us-1ms
        metrics.record_latency(Duration::from_millis(5));    // 1ms-10ms

        assert_eq!(metrics.latency_under_100us.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.latency_100us_1ms.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.latency_1ms_10ms.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.latency_count.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn test_prometheus_export() {
        let metrics = DistribCacheMetrics::new();

        metrics.record_hit(CacheTier::L1);
        metrics.record_miss(CacheTier::L2);
        metrics.record_workload(WorkloadType::OLTP);

        let prometheus = metrics.to_prometheus();

        assert!(prometheus.contains("distribcache_operations_total"));
        assert!(prometheus.contains("distribcache_hit_rate"));
        assert!(prometheus.contains("distribcache_tier_hits_total"));
    }

    #[test]
    fn test_json_export() {
        let metrics = DistribCacheMetrics::new();

        metrics.record_hit(CacheTier::L1);
        metrics.record_put();

        let json = metrics.to_json();

        assert_eq!(json["operations"]["hits"], 1);
        assert_eq!(json["operations"]["puts"], 1);
    }

    #[test]
    fn test_reset() {
        let metrics = DistribCacheMetrics::new();

        metrics.record_hit(CacheTier::L1);
        metrics.record_miss(CacheTier::L2);
        metrics.record_put();

        metrics.reset();

        assert_eq!(metrics.cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.cache_misses.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.cache_puts.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_workload_tracking() {
        let metrics = DistribCacheMetrics::new();

        metrics.record_workload(WorkloadType::OLTP);
        metrics.record_workload(WorkloadType::OLTP);
        metrics.record_workload(WorkloadType::OLAP);
        metrics.record_workload(WorkloadType::AIAgent);

        assert_eq!(metrics.oltp_queries.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.olap_queries.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.ai_agent_queries.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_error_tracking() {
        let metrics = DistribCacheMetrics::new();

        metrics.record_error(ErrorType::Timeout);
        metrics.record_error(ErrorType::Timeout);
        metrics.record_error(ErrorType::Serialization);

        assert_eq!(metrics.cache_errors.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.timeout_errors.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.serialization_errors.load(Ordering::Relaxed), 1);
    }
}
