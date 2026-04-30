//! Multi-tier cache implementations
//!
//! L1: Hot cache (in-memory, <100μs)
//! L2: Warm cache (SSD, <1ms)
//! L3: Distributed cache (mesh, <10ms)

mod l1_hot;
mod l2_warm;
mod l3_distributed;

pub use l1_hot::HotCache;
pub use l2_warm::WarmCache;
pub use l3_distributed::DistributedCache;

use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU64;
use std::time::{Duration, SystemTime};

/// Cache tier identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CacheTier {
    /// L1: In-memory hot cache
    L1,
    /// L2: SSD warm cache
    L2,
    /// L3: Distributed mesh cache
    L3,
}

/// Eviction policy for cache tiers
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    /// Least Recently Used
    LRU,
    /// Least Frequently Used
    LFU,
    /// Adaptive (switches between LRU and LFU based on access patterns)
    Adaptive,
    /// Time-based (oldest entries first)
    FIFO,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::LFU
    }
}

/// Compression type for L2 cache
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression
    None,
    /// LZ4 (fast, moderate compression)
    Lz4,
    /// Zstd (slower, better compression)
    Zstd,
}

impl Default for CompressionType {
    fn default() -> Self {
        Self::Lz4
    }
}

/// Cache key for entry lookup
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CacheKey {
    /// Query fingerprint hash
    pub fingerprint_hash: u64,
    /// Optional parameter hash
    pub param_hash: Option<u64>,
    /// Branch name (for branch-aware caching)
    pub branch: Option<String>,
    /// Time travel timestamp (for historical queries)
    pub as_of: Option<u64>,
}

impl CacheKey {
    /// Create a new cache key from a query fingerprint
    pub fn from_fingerprint(fingerprint: &super::QueryFingerprint) -> Self {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        fingerprint.template.hash(&mut hasher);
        let fingerprint_hash = hasher.finish();

        Self {
            fingerprint_hash,
            param_hash: fingerprint.param_hash,
            branch: None,
            as_of: None,
        }
    }

    /// Create a cache key for a chunk
    pub fn chunk(id: u64) -> Self {
        Self {
            fingerprint_hash: id,
            param_hash: None,
            branch: None,
            as_of: None,
        }
    }

    /// Set branch for branch-aware caching
    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    /// Set timestamp for time-travel caching
    pub fn with_as_of(mut self, timestamp: u64) -> Self {
        self.as_of = Some(timestamp);
        self
    }

    /// Get table name from the key (if available)
    pub fn table(&self) -> &str {
        "unknown"
    }

    /// Get the fingerprint for metrics
    pub fn fingerprint(&self) -> super::QueryFingerprint {
        super::QueryFingerprint {
            template: format!("{:x}", self.fingerprint_hash),
            tables: Vec::new(),
            param_hash: self.param_hash,
        }
    }

    /// Check if key matches a pattern
    pub fn matches_pattern(&self, pattern: &str) -> bool {
        // Simple pattern matching for invalidation
        pattern.contains(&format!("{:x}", self.fingerprint_hash))
    }

    /// Convert to bytes for storage
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(24);
        bytes.extend_from_slice(&self.fingerprint_hash.to_le_bytes());
        if let Some(param) = self.param_hash {
            bytes.extend_from_slice(&param.to_le_bytes());
        }
        if let Some(ref branch) = self.branch {
            bytes.extend_from_slice(branch.as_bytes());
        }
        if let Some(ts) = self.as_of {
            bytes.extend_from_slice(&ts.to_le_bytes());
        }
        bytes
    }
}

/// Cache entry containing query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheEntry {
    /// Serialized result data
    pub data: Vec<u8>,
    /// Entry creation time (Unix timestamp)
    pub created_at: u64,
    /// Time-to-live duration in seconds
    pub ttl_secs: u64,
    /// Number of rows in result
    pub row_count: usize,
    /// Tables involved (for invalidation)
    pub tables: Vec<String>,
    /// Access count (for LFU)
    #[serde(skip)]
    pub access_count: u64,
}

impl CacheEntry {
    /// Create a new cache entry
    pub fn new(data: Vec<u8>, tables: Vec<String>, row_count: usize) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            data,
            created_at: now,
            ttl_secs: 300, // Default 5 minutes
            row_count,
            tables,
            access_count: 0,
        }
    }

    /// Create entry with specific TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl_secs = ttl.as_secs();
        self
    }

    /// Check if entry is expired
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now > self.created_at + self.ttl_secs
    }

    /// Get entry size in bytes
    pub fn size(&self) -> usize {
        self.data.len() + self.tables.iter().map(|t| t.len()).sum::<usize>() + 32
    }

    /// Create entry from a RAG chunk
    pub fn from_chunk(chunk: &super::ai::Chunk) -> Self {
        Self::new(
            chunk.content.as_bytes().to_vec(),
            vec!["chunks".to_string()],
            1,
        )
    }

    /// Get remaining TTL
    pub fn remaining_ttl(&self) -> Duration {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let elapsed = now.saturating_sub(self.created_at);
        let remaining = self.ttl_secs.saturating_sub(elapsed);
        Duration::from_secs(remaining)
    }
}

/// Statistics for a cache tier
#[derive(Debug, Clone, Default)]
pub struct TierStats {
    /// Current size in bytes
    pub size_bytes: u64,
    /// Maximum size in bytes
    pub max_size_bytes: u64,
    /// Number of entries
    pub entry_count: u64,
    /// Hit count
    pub hits: u64,
    /// Miss count
    pub misses: u64,
    /// Eviction count
    pub evictions: u64,
    /// Compression ratio (for L2)
    pub compression_ratio: Option<f64>,
    /// Peer count (for L3)
    pub peer_count: Option<u32>,
    /// Healthy peer count (for L3)
    pub healthy_peers: Option<u32>,
}

impl TierStats {
    /// Calculate hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        }
    }

    /// Calculate utilization percentage
    pub fn utilization(&self) -> f64 {
        if self.max_size_bytes > 0 {
            self.size_bytes as f64 / self.max_size_bytes as f64 * 100.0
        } else {
            0.0
        }
    }
}

/// LFU eviction tracker
pub struct LFUEviction {
    /// Access counts per key
    counts: dashmap::DashMap<u64, u64>,
    /// Minimum frequency for fast eviction
    min_freq: AtomicU64,
}

impl LFUEviction {
    pub fn new() -> Self {
        Self {
            counts: dashmap::DashMap::new(),
            min_freq: AtomicU64::new(1),
        }
    }

    pub fn touch(&self, key_hash: u64) {
        self.counts
            .entry(key_hash)
            .and_modify(|c| *c += 1)
            .or_insert(1);
    }

    pub fn insert(&self, key_hash: u64) {
        self.counts.insert(key_hash, 1);
    }

    pub fn remove(&self, key_hash: u64) {
        self.counts.remove(&key_hash);
    }

    pub fn evict_one(&self) -> Option<u64> {
        // Find entry with minimum frequency
        let mut min_key = None;
        let mut min_count = u64::MAX;

        for entry in self.counts.iter() {
            if *entry.value() < min_count {
                min_count = *entry.value();
                min_key = Some(*entry.key());
            }
        }

        if let Some(key) = min_key {
            self.counts.remove(&key);
        }

        min_key
    }
}

impl Default for LFUEviction {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_from_fingerprint() {
        let fp = super::super::QueryFingerprint::from_query("SELECT * FROM users");
        let key = CacheKey::from_fingerprint(&fp);
        assert!(key.fingerprint_hash > 0);
        assert!(key.branch.is_none());
    }

    #[test]
    fn test_cache_entry_expiration() {
        let entry = CacheEntry::new(vec![1, 2, 3], vec!["users".to_string()], 1)
            .with_ttl(Duration::from_secs(1));

        assert!(!entry.is_expired());

        // For actual expiration test we'd need to wait or mock time
    }

    #[test]
    fn test_cache_entry_size() {
        let entry = CacheEntry::new(
            vec![0; 1000],
            vec!["users".to_string(), "orders".to_string()],
            10,
        );
        assert!(entry.size() > 1000);
    }

    #[test]
    fn test_tier_stats_hit_ratio() {
        let mut stats = TierStats::default();
        stats.hits = 80;
        stats.misses = 20;
        assert!((stats.hit_ratio() - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_lfu_eviction() {
        let lfu = LFUEviction::new();

        lfu.insert(1);
        lfu.insert(2);
        lfu.insert(3);

        // Touch some keys
        lfu.touch(1);
        lfu.touch(1);
        lfu.touch(2);

        // Key 3 has lowest frequency, should be evicted
        let evicted = lfu.evict_one();
        assert_eq!(evicted, Some(3));
    }
}
