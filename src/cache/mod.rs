//! Query Caching Module
//!
//! Provides multi-tier query caching for HeliosProxy:
//!
//! - **L1 Hot Cache**: Per-connection, exact match, LRU eviction
//! - **L2 Warm Cache**: Shared, normalized queries, configurable storage
//! - **L3 Semantic Cache**: Vector similarity for AI workloads
//!
//! # Architecture
//!
//! ```text
//!                     ┌─────────────────────────────────────────────────┐
//!                     │                QUERY CACHE LAYER                 │
//!                     │                                                  │
//!   Query ───────────►│ ┌──────────────────────────────────────────────┐│
//!                     ││ L1: Hot Cache (in-memory, <1ms)               ││
//!                     │└──────────────────────────────────────────────┘│
//!                     │         │ miss                                  │
//!                     │         ▼                                       │
//!                     │ ┌──────────────────────────────────────────────┐│
//!                     ││ L2: Warm Cache (shared memory, <5ms)          ││
//!                     │└──────────────────────────────────────────────┘│
//!                     │         │ miss                                  │
//!                     │         ▼                                       │
//!                     │ ┌──────────────────────────────────────────────┐│
//!                     ││ L3: Semantic Cache (vector similarity, <20ms) ││
//!                     │└──────────────────────────────────────────────┘│
//!                     │         │ miss                                  │
//!                     │         ▼                                       │
//!                     │       BACKEND                                   │
//!                     └─────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use heliosdb_lite::proxy::cache::{QueryCache, CacheConfig};
//!
//! let config = CacheConfig::default();
//! let cache = QueryCache::new(config);
//!
//! // Check cache before executing query
//! if let Some(result) = cache.get(&query, &context).await {
//!     return result;
//! }
//!
//! // Execute query and cache result
//! let result = execute_query(&query).await?;
//! cache.put(&query, &context, result.clone()).await;
//! ```

pub mod config;
pub mod l1_hot;
pub mod l2_warm;
pub mod l3_semantic;
pub mod normalizer;
pub mod invalidation;
pub mod metrics;
pub mod hints;
pub mod result;

// Re-exports
pub use config::{CacheConfig, L1Config, L2Config, L3Config, StorageBackend};
pub use l1_hot::L1HotCache;
pub use l2_warm::L2WarmCache;
pub use l3_semantic::L3SemanticCache;
pub use normalizer::{QueryNormalizer, NormalizedQuery};
pub use invalidation::{InvalidationManager, InvalidationMode};
pub use metrics::{CacheMetrics, CacheStatsSnapshot, CacheStatsLevelSnapshot};
pub use hints::{CacheHint, parse_cache_hints};
pub use result::{CachedResult, CacheKey};

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Query cache context (for cache key generation)
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct CacheContext {
    /// Database name
    pub database: String,
    /// Username (for RLS)
    pub user: Option<String>,
    /// Branch name (for HeliosDB branching)
    pub branch: Option<String>,
    /// Connection ID (for L1 cache)
    pub connection_id: Option<u64>,
}

impl Default for CacheContext {
    fn default() -> Self {
        Self {
            database: "default".to_string(),
            user: None,
            branch: None,
            connection_id: None,
        }
    }
}

/// Cache lookup result
#[derive(Debug)]
pub enum CacheLookup {
    /// Cache hit with result
    Hit {
        result: CachedResult,
        level: CacheLevel,
    },
    /// Cache miss
    Miss,
}

/// Cache level indicator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheLevel {
    L1Hot,
    L2Warm,
    L3Semantic,
}

impl std::fmt::Display for CacheLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheLevel::L1Hot => write!(f, "L1"),
            CacheLevel::L2Warm => write!(f, "L2"),
            CacheLevel::L3Semantic => write!(f, "L3"),
        }
    }
}

/// Main query cache implementation
pub struct QueryCache {
    /// Configuration
    config: CacheConfig,

    /// L1: Per-connection hot cache (exact match)
    l1_caches: DashMap<u64, Arc<L1HotCache>>,

    /// L2: Shared normalized cache
    l2_cache: Option<Arc<L2WarmCache>>,

    /// L3: Semantic similarity cache
    l3_cache: Option<Arc<L3SemanticCache>>,

    /// Query normalizer
    normalizer: Arc<QueryNormalizer>,

    /// Cache invalidation manager
    invalidator: Arc<InvalidationManager>,

    /// Metrics collector
    metrics: Arc<CacheMetrics>,

    /// Request coalescing for cache stampede prevention
    pending_requests: DashMap<CacheKey, Arc<tokio::sync::Notify>>,
}

impl QueryCache {
    /// Create a new query cache with the given configuration
    pub fn new(config: CacheConfig) -> Self {
        let l2_cache = if config.l2.enabled {
            Some(Arc::new(L2WarmCache::new(config.l2.clone())))
        } else {
            None
        };

        let l3_cache = if config.l3.enabled {
            Some(Arc::new(L3SemanticCache::new(config.l3.clone())))
        } else {
            None
        };

        let invalidator = Arc::new(InvalidationManager::new(config.invalidation.clone()));

        Self {
            config: config.clone(),
            l1_caches: DashMap::new(),
            l2_cache,
            l3_cache,
            normalizer: Arc::new(QueryNormalizer::new()),
            invalidator,
            metrics: Arc::new(CacheMetrics::new()),
            pending_requests: DashMap::new(),
        }
    }

    /// Get or create L1 cache for a connection
    pub fn get_l1_cache(&self, connection_id: u64) -> Arc<L1HotCache> {
        self.l1_caches
            .entry(connection_id)
            .or_insert_with(|| Arc::new(L1HotCache::new(self.config.l1.clone())))
            .clone()
    }

    /// Remove L1 cache for a connection (on disconnect)
    pub fn remove_l1_cache(&self, connection_id: u64) {
        self.l1_caches.remove(&connection_id);
    }

    /// Look up a query in the cache hierarchy
    pub async fn get(&self, query: &str, context: &CacheContext) -> CacheLookup {
        // Parse cache hints
        let hints = parse_cache_hints(query);

        // Skip cache if hint says so
        if hints.skip {
            self.metrics.record_skip();
            return CacheLookup::Miss;
        }

        let start = Instant::now();

        // L1: Check hot cache (exact match)
        if self.config.l1.enabled {
            if let Some(conn_id) = context.connection_id {
                let l1 = self.get_l1_cache(conn_id);
                if let Some(result) = l1.get(query) {
                    self.metrics.record_hit(CacheLevel::L1Hot, start.elapsed());
                    return CacheLookup::Hit {
                        result,
                        level: CacheLevel::L1Hot,
                    };
                }
            }
        }

        // Normalize query for L2/L3 lookup
        let normalized = self.normalizer.normalize(query);
        let cache_key = CacheKey::new(&normalized, context);

        // L2: Check warm cache (normalized match)
        if let Some(ref l2) = self.l2_cache {
            if let Some(result) = l2.get(&cache_key).await {
                self.metrics.record_hit(CacheLevel::L2Warm, start.elapsed());

                // Promote to L1
                if self.config.l1.enabled {
                    if let Some(conn_id) = context.connection_id {
                        let l1 = self.get_l1_cache(conn_id);
                        l1.put(query.to_string(), result.clone());
                    }
                }

                return CacheLookup::Hit {
                    result,
                    level: CacheLevel::L2Warm,
                };
            }
        }

        // L3: Check semantic cache (similarity match)
        if hints.semantic_cache {
            if let Some(ref l3) = self.l3_cache {
                if let Some(result) = l3.get(query, context).await {
                    self.metrics.record_hit(CacheLevel::L3Semantic, start.elapsed());
                    return CacheLookup::Hit {
                        result,
                        level: CacheLevel::L3Semantic,
                    };
                }
            }
        }

        self.metrics.record_miss(start.elapsed());
        CacheLookup::Miss
    }

    /// Store a query result in the cache
    pub async fn put(
        &self,
        query: &str,
        context: &CacheContext,
        data: Bytes,
        row_count: usize,
        execution_time: Duration,
    ) {
        // Parse cache hints
        let hints = parse_cache_hints(query);

        // Skip if hint says so
        if hints.skip {
            return;
        }

        // Normalize query
        let normalized = self.normalizer.normalize(query);

        // Determine TTL
        let ttl = hints.ttl.unwrap_or_else(|| {
            self.get_table_ttl(&normalized.tables)
        });

        // Check size limit
        if data.len() > self.config.max_result_size {
            self.metrics.record_size_exceeded();
            return;
        }

        // Create cached result
        let result = CachedResult {
            data,
            row_count,
            cached_at: Instant::now(),
            ttl,
            tables: normalized.tables.clone(),
            execution_time,
        };

        // Store in L1 (exact match)
        if self.config.l1.enabled {
            if let Some(conn_id) = context.connection_id {
                let l1 = self.get_l1_cache(conn_id);
                l1.put(query.to_string(), result.clone());
            }
        }

        // Store in L2 (normalized)
        if let Some(ref l2) = self.l2_cache {
            let cache_key = CacheKey::new(&normalized, context);
            l2.put(cache_key.clone(), result.clone()).await;

            // Register for invalidation
            for table in &normalized.tables {
                self.invalidator.register(&cache_key, table);
            }
        }

        // Store in L3 (semantic) if hint enabled
        if hints.semantic_cache {
            if let Some(ref l3) = self.l3_cache {
                l3.put(query, context, result).await;
            }
        }

        self.metrics.record_put();
    }

    /// Invalidate cache entries for specific tables
    pub async fn invalidate_tables(&self, tables: &[String]) {
        for table in tables {
            let keys = self.invalidator.get_keys_for_table(table);

            // Invalidate L2
            if let Some(ref l2) = self.l2_cache {
                for key in &keys {
                    l2.remove(key).await;
                }
            }

            self.invalidator.invalidate_table(table);
        }

        // L1 caches are invalidated on next access (TTL-based)
        // L3 semantic cache has its own TTL handling

        self.metrics.record_invalidation(tables.len());
    }

    /// Clear all caches
    pub async fn clear(&self, levels: &[CacheLevel]) {
        for level in levels {
            match level {
                CacheLevel::L1Hot => {
                    self.l1_caches.clear();
                }
                CacheLevel::L2Warm => {
                    if let Some(ref l2) = self.l2_cache {
                        l2.clear().await;
                    }
                }
                CacheLevel::L3Semantic => {
                    if let Some(ref l3) = self.l3_cache {
                        l3.clear().await;
                    }
                }
            }
        }

        self.metrics.record_clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStatsSnapshot {
        self.metrics.snapshot()
    }

    /// Get configuration
    pub fn config(&self) -> &CacheConfig {
        &self.config
    }

    /// Get the invalidation manager (for WAL subscription)
    pub fn invalidator(&self) -> Arc<InvalidationManager> {
        self.invalidator.clone()
    }

    /// Get table-specific TTL or default
    fn get_table_ttl(&self, tables: &[String]) -> Duration {
        // Find shortest TTL among tables
        let mut min_ttl = self.config.default_ttl;

        for table in tables {
            if let Some(table_config) = self.config.table_configs.get(table) {
                if table_config.ttl < min_ttl {
                    min_ttl = table_config.ttl;
                }
            }
        }

        min_ttl
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_context_default() {
        let ctx = CacheContext::default();
        assert_eq!(ctx.database, "default");
        assert!(ctx.user.is_none());
        assert!(ctx.branch.is_none());
        assert!(ctx.connection_id.is_none());
    }

    #[test]
    fn test_cache_level_display() {
        assert_eq!(format!("{}", CacheLevel::L1Hot), "L1");
        assert_eq!(format!("{}", CacheLevel::L2Warm), "L2");
        assert_eq!(format!("{}", CacheLevel::L3Semantic), "L3");
    }

    #[tokio::test]
    async fn test_query_cache_creation() {
        let config = CacheConfig::default();
        let cache = QueryCache::new(config);

        assert!(cache.config.l1.enabled);
        assert!(cache.config.l2.enabled);
    }

    #[tokio::test]
    async fn test_l1_cache_per_connection() {
        let config = CacheConfig::default();
        let cache = QueryCache::new(config);

        let l1_a = cache.get_l1_cache(1);
        let l1_b = cache.get_l1_cache(2);
        let l1_a2 = cache.get_l1_cache(1);

        // Same connection should get same cache
        assert!(Arc::ptr_eq(&l1_a, &l1_a2));
        // Different connections should get different caches
        assert!(!Arc::ptr_eq(&l1_a, &l1_b));
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let config = CacheConfig::default();
        let cache = QueryCache::new(config);
        let context = CacheContext::default();

        let result = cache.get("SELECT * FROM users", &context).await;
        assert!(matches!(result, CacheLookup::Miss));
    }
}
