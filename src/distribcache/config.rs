//! DistribCache configuration types
//!
//! Configuration for multi-tier distributed caching with workload-specific settings.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use super::tiers::{CompressionType, EvictionPolicy};

/// Main configuration for Helios-DistribCache
#[derive(Debug, Clone)]
pub struct DistribCacheConfig {
    /// Whether caching is enabled
    pub enabled: bool,

    // L1 (Hot Cache) configuration
    /// L1 cache size in megabytes
    pub l1_size_mb: usize,
    /// Maximum entry size in L1
    pub l1_max_entry_size: usize,
    /// L1 eviction policy
    pub l1_eviction_policy: EvictionPolicy,

    // L2 (Warm Cache) configuration
    /// Whether L2 is enabled
    pub l2_enabled: bool,
    /// L2 cache size in gigabytes
    pub l2_size_gb: u64,
    /// L2 storage path
    pub l2_path: PathBuf,
    /// L2 compression type
    pub l2_compression: CompressionType,

    // L3 (Distributed Cache) configuration
    /// Whether L3 is enabled
    pub l3_enabled: bool,
    /// Replication factor for distributed cache
    pub l3_replication_factor: u32,
    /// Peer addresses for distributed cache mesh
    pub l3_peers: Vec<SocketAddr>,

    // Workload-specific TTLs
    /// OLTP query cache TTL
    pub oltp_cache_ttl: Duration,
    /// OLAP query cache TTL
    pub olap_cache_ttl: Duration,
    /// Vector search cache TTL
    pub vector_cache_ttl: Duration,
    /// AI agent cache TTL
    pub ai_agent_cache_ttl: Duration,
    /// RAG pipeline cache TTL
    pub rag_cache_ttl: Duration,
    /// Default cache TTL
    pub default_cache_ttl: Duration,

    // Prefetching
    /// Whether prefetching is enabled
    pub prefetch_enabled: bool,
    /// Number of queries to look ahead for prefetching
    pub prefetch_lookahead: u32,
    /// Confidence threshold for prefetch predictions
    pub prefetch_confidence_threshold: f32,
    /// Maximum prefetch queue size
    pub max_prefetch_queue: usize,

    // Invalidation
    /// Invalidation mode
    pub invalidation_mode: InvalidationMode,
    /// WAL endpoint for streaming invalidation
    pub wal_endpoint: Option<String>,
    /// WAL lag tolerance
    pub wal_lag_tolerance: Duration,

    // Scheduling
    /// Workload scheduling policy
    pub scheduling_policy: SchedulingPolicy,
    /// OLTP priority weight
    pub oltp_priority: f64,
    /// OLAP priority weight
    pub olap_priority: f64,
    /// Vector priority weight
    pub vector_priority: f64,
    /// AI agent priority weight
    pub ai_agent_priority: f64,
    /// Maximum concurrent queries per workload type
    pub max_concurrent_oltp: u32,
    pub max_concurrent_olap: u32,
    pub max_concurrent_vector: u32,
    pub max_concurrent_ai: u32,

    // Heatmap
    /// Whether heatmap analytics is enabled
    pub heatmap_enabled: bool,
    /// Heatmap time bucket size
    pub heatmap_bucket_size: Duration,
    /// Heatmap data retention
    pub heatmap_retention: Duration,

    // AI-specific settings
    /// Maximum conversation turns to cache
    pub max_conversation_turns: usize,
    /// Semantic similarity threshold
    pub semantic_similarity_threshold: f32,
}

impl Default for DistribCacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,

            // L1 defaults
            l1_size_mb: 256,
            l1_max_entry_size: 1024 * 1024, // 1MB
            l1_eviction_policy: EvictionPolicy::LFU,

            // L2 defaults
            l2_enabled: true,
            l2_size_gb: 5,
            l2_path: PathBuf::from("/var/lib/heliosproxy/cache"),
            l2_compression: CompressionType::Lz4,

            // L3 defaults
            l3_enabled: false,
            l3_replication_factor: 2,
            l3_peers: Vec::new(),

            // TTL defaults
            oltp_cache_ttl: Duration::from_secs(60),
            olap_cache_ttl: Duration::from_secs(30 * 60),
            vector_cache_ttl: Duration::from_secs(5 * 60),
            ai_agent_cache_ttl: Duration::from_secs(10 * 60),
            rag_cache_ttl: Duration::from_secs(15 * 60),
            default_cache_ttl: Duration::from_secs(5 * 60),

            // Prefetch defaults
            prefetch_enabled: true,
            prefetch_lookahead: 3,
            prefetch_confidence_threshold: 0.3,
            max_prefetch_queue: 100,

            // Invalidation defaults
            invalidation_mode: InvalidationMode::Hybrid,
            wal_endpoint: None,
            wal_lag_tolerance: Duration::from_millis(100),

            // Scheduling defaults
            scheduling_policy: SchedulingPolicy::WeightedFair,
            oltp_priority: 1.0,
            olap_priority: 0.3,
            vector_priority: 0.5,
            ai_agent_priority: 0.7,
            max_concurrent_oltp: 500,
            max_concurrent_olap: 50,
            max_concurrent_vector: 100,
            max_concurrent_ai: 200,

            // Heatmap defaults
            heatmap_enabled: true,
            heatmap_bucket_size: Duration::from_secs(5 * 60),
            heatmap_retention: Duration::from_secs(7 * 24 * 60 * 60),

            // AI defaults
            max_conversation_turns: 50,
            semantic_similarity_threshold: 0.85,
        }
    }
}

impl DistribCacheConfig {
    /// Create a new configuration builder
    pub fn builder() -> DistribCacheConfigBuilder {
        DistribCacheConfigBuilder::default()
    }
}

/// Invalidation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationMode {
    /// TTL-based expiration only
    TTL,
    /// WAL-based invalidation only
    WAL,
    /// Both TTL and WAL
    Hybrid,
}

/// Workload scheduling policy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulingPolicy {
    /// Strict priority (OLTP always first)
    StrictPriority,
    /// Weighted fair queuing
    WeightedFair,
    /// Time-based (OLAP during off-hours)
    TimeBased,
    /// Adaptive (learn optimal distribution)
    Adaptive,
}

/// Builder for DistribCacheConfig
#[derive(Debug, Default)]
pub struct DistribCacheConfigBuilder {
    config: DistribCacheConfig,
}

impl DistribCacheConfigBuilder {
    /// Set whether caching is enabled
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Set L1 cache size in MB
    pub fn l1_size_mb(mut self, size: usize) -> Self {
        self.config.l1_size_mb = size;
        self
    }

    /// Set L1 maximum entry size
    pub fn l1_max_entry_size(mut self, size: usize) -> Self {
        self.config.l1_max_entry_size = size;
        self
    }

    /// Set L1 eviction policy
    pub fn l1_eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.config.l1_eviction_policy = policy;
        self
    }

    /// Enable L2 cache
    pub fn l2_enabled(mut self, enabled: bool) -> Self {
        self.config.l2_enabled = enabled;
        self
    }

    /// Set L2 cache size in GB
    pub fn l2_size_gb(mut self, size: u64) -> Self {
        self.config.l2_size_gb = size;
        self
    }

    /// Set L2 storage path
    pub fn l2_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.l2_path = path.into();
        self
    }

    /// Set L2 compression type
    pub fn l2_compression(mut self, compression: CompressionType) -> Self {
        self.config.l2_compression = compression;
        self
    }

    /// Enable L3 distributed cache
    pub fn l3_enabled(mut self, enabled: bool) -> Self {
        self.config.l3_enabled = enabled;
        self
    }

    /// Set L3 replication factor
    pub fn l3_replication_factor(mut self, factor: u32) -> Self {
        self.config.l3_replication_factor = factor;
        self
    }

    /// Set L3 peer addresses
    pub fn l3_peers(mut self, peers: Vec<SocketAddr>) -> Self {
        self.config.l3_peers = peers;
        self
    }

    /// Set OLTP cache TTL
    pub fn oltp_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.oltp_cache_ttl = ttl;
        self
    }

    /// Set OLAP cache TTL
    pub fn olap_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.olap_cache_ttl = ttl;
        self
    }

    /// Set vector cache TTL
    pub fn vector_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.vector_cache_ttl = ttl;
        self
    }

    /// Set AI agent cache TTL
    pub fn ai_agent_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.ai_agent_cache_ttl = ttl;
        self
    }

    /// Set RAG cache TTL
    pub fn rag_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.rag_cache_ttl = ttl;
        self
    }

    /// Enable prefetching
    pub fn prefetch_enabled(mut self, enabled: bool) -> Self {
        self.config.prefetch_enabled = enabled;
        self
    }

    /// Set prefetch lookahead
    pub fn prefetch_lookahead(mut self, lookahead: u32) -> Self {
        self.config.prefetch_lookahead = lookahead;
        self
    }

    /// Set prefetch confidence threshold
    pub fn prefetch_confidence_threshold(mut self, threshold: f32) -> Self {
        self.config.prefetch_confidence_threshold = threshold;
        self
    }

    /// Set invalidation mode
    pub fn invalidation_mode(mut self, mode: InvalidationMode) -> Self {
        self.config.invalidation_mode = mode;
        self
    }

    /// Set WAL endpoint
    pub fn wal_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.wal_endpoint = Some(endpoint.into());
        self
    }

    /// Set scheduling policy
    pub fn scheduling_policy(mut self, policy: SchedulingPolicy) -> Self {
        self.config.scheduling_policy = policy;
        self
    }

    /// Enable heatmap analytics
    pub fn heatmap_enabled(mut self, enabled: bool) -> Self {
        self.config.heatmap_enabled = enabled;
        self
    }

    /// Set maximum conversation turns to cache
    pub fn max_conversation_turns(mut self, turns: usize) -> Self {
        self.config.max_conversation_turns = turns;
        self
    }

    /// Set semantic similarity threshold
    pub fn semantic_similarity_threshold(mut self, threshold: f32) -> Self {
        self.config.semantic_similarity_threshold = threshold;
        self
    }

    /// Build the configuration
    pub fn build(self) -> DistribCacheConfig {
        self.config
    }
}

/// Per-workload resource limits
#[derive(Debug, Clone)]
pub struct WorkloadLimits {
    /// Maximum concurrent queries
    pub max_concurrent: u32,
    /// Maximum cache memory allocation (MB)
    pub max_cache_mb: usize,
    /// Priority weight (0.0 - 1.0)
    pub priority_weight: f64,
    /// Cache TTL
    pub cache_ttl: Duration,
}

impl Default for WorkloadLimits {
    fn default() -> Self {
        Self {
            max_concurrent: 100,
            max_cache_mb: 64,
            priority_weight: 0.5,
            cache_ttl: Duration::from_secs(300),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = DistribCacheConfig::default();
        assert!(config.enabled);
        assert_eq!(config.l1_size_mb, 256);
        assert!(config.l2_enabled);
        assert!(!config.l3_enabled);
        assert!(config.prefetch_enabled);
        assert!(config.heatmap_enabled);
    }

    #[test]
    fn test_config_builder() {
        let config = DistribCacheConfig::builder()
            .l1_size_mb(512)
            .l2_enabled(false)
            .l3_enabled(true)
            .l3_replication_factor(3)
            .prefetch_enabled(false)
            .build();

        assert_eq!(config.l1_size_mb, 512);
        assert!(!config.l2_enabled);
        assert!(config.l3_enabled);
        assert_eq!(config.l3_replication_factor, 3);
        assert!(!config.prefetch_enabled);
    }

    #[test]
    fn test_workload_limits_default() {
        let limits = WorkloadLimits::default();
        assert_eq!(limits.max_concurrent, 100);
        assert_eq!(limits.priority_weight, 0.5);
    }
}
