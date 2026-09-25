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
pub mod hints;
pub mod invalidation;
pub mod l1_hot;
pub mod l2_warm;
pub mod l3_semantic;
pub mod metrics;
pub mod normalizer;
pub mod result;

// Re-exports
pub use config::{CacheConfig, L1Config, L2Config, L3Config, StorageBackend};
pub use hints::{parse_cache_hints, CacheHint};
pub use invalidation::{InvalidationManager, InvalidationMode};
pub use l1_hot::L1HotCache;
pub use l2_warm::L2WarmCache;
pub use l3_semantic::L3SemanticCache;
pub use metrics::{CacheMetrics, CacheStatsLevelSnapshot, CacheStatsSnapshot};
pub use normalizer::{NormalizedQuery, QueryNormalizer};
pub use result::{CacheKey, CachedResult};

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

/// The per-query lexical work a cache lookup performs before it can consult
/// the cache levels: the parsed cache hints and — when the lookup got as far
/// as L2/L3 — the normalized query (fingerprint + extracted tables).
///
/// [`QueryCache::get_with_prep`] hands it back so a miss followed by the
/// crate-internal `QueryCache::put_prepared` reuses that work instead of
/// re-parsing the hints and re-running the normalizer's regex pass over the
/// same SQL a second time (one normalization per miss+store, not two).
#[derive(Debug, Clone)]
pub struct QueryPrep {
    /// Hints parsed from the query's `helios:` comments.
    hints: CacheHint,
    /// Normalized form — present only when the lookup reached the levels that
    /// need it. Absent on a hint-skip or an L1 exact hit (neither normalizes);
    /// `put_prepared` then normalizes lazily, exactly as `put` always did.
    normalized: Option<NormalizedQuery>,
    /// Cache generation of the query's tables when the lookup missed, i.e.
    /// before the backend fetch (C-02). `put_prepared` refuses to store a
    /// result whose tables were written while it was being fetched.
    generation: Option<u64>,
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
    #[allow(dead_code)]
    pending_requests: DashMap<CacheKey, Arc<tokio::sync::Notify>>,

    /// Per-table write generations (C-02). Bumped by every invalidation of the
    /// table; a cached result stores the sum observed before its fetch and is
    /// served only while the sum is unchanged, on every tier (L1/L2/L3).
    table_generations: DashMap<String, u64>,

    /// Generation of invalidations whose table set is unknown (DDL,
    /// `EXECUTE`/`CALL`/`DO`, `COPY`, unparsable writes): part of every sum.
    global_generation: std::sync::atomic::AtomicU64,

    /// Work for the `cache-reclaim` thread, so bulk deallocation (10^5-10^6
    /// entries after a read-heavy phase) never runs on a connection: key
    /// sets a purge took out of the invalidation index, and L2 entries an
    /// eviction pass retired. `None` if that thread could not be started
    /// (callers then do the work themselves).
    reclaim_tx: Option<std::sync::mpsc::Sender<Reclaim>>,
}

/// A job for the `cache-reclaim` thread.
enum Reclaim {
    /// A purged table's key set: dropped.
    Keys(std::collections::HashSet<CacheKey>),
    /// Hashes of L2 entries an eviction pass retired: shed, then the pass
    /// ends.
    Shed(Vec<u64>),
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

        // Purges run on the connection that acknowledged a write; dropping a
        // table's key set there (10^5-10^6 keys after a read-heavy phase)
        // would hold that client for hundreds of milliseconds. The set is
        // dropped on this thread instead; it ends when the cache is dropped.
        let reclaim_tx = {
            let (tx, rx) = std::sync::mpsc::channel::<Reclaim>();
            let l2 = l2_cache.clone();
            std::thread::Builder::new()
                .name("cache-reclaim".into())
                .spawn(move || {
                    while let Ok(job) = rx.recv() {
                        match job {
                            Reclaim::Keys(keys) => drop(keys),
                            Reclaim::Shed(hashes) => {
                                if let Some(l2) = &l2 {
                                    l2.shed(&hashes);
                                    l2.end_eviction();
                                }
                            }
                        }
                    }
                })
                .ok()
                .map(|_| tx)
        };

        Self {
            config: config.clone(),
            l1_caches: DashMap::new(),
            l2_cache,
            l3_cache,
            normalizer: Arc::new(QueryNormalizer::new()),
            invalidator,
            metrics: Arc::new(CacheMetrics::new()),
            pending_requests: DashMap::new(),
            table_generations: DashMap::new(),
            global_generation: std::sync::atomic::AtomicU64::new(0),
            reclaim_tx,
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

    /// Number of per-connection L1 caches currently retained. Grows by one per
    /// connection that runs a cacheable read; must return to ~0 as connections
    /// close (see `remove_l1_cache`). Exposed for leak observability/testing.
    pub fn l1_cache_count(&self) -> usize {
        self.l1_caches.len()
    }

    /// Look up a query in the cache hierarchy
    pub async fn get(&self, query: &str, context: &CacheContext) -> CacheLookup {
        self.get_with_prep(query, context).await.0
    }

    /// Look up a query in the cache hierarchy, also returning the [`QueryPrep`]
    /// the lookup computed. On a miss, pass it to the crate-internal
    /// `put_prepared` so the hint parse and query normalization are not
    /// repeated for the store.
    /// Behaviour is identical to [`Self::get`] in every other respect.
    pub async fn get_with_prep(
        &self,
        query: &str,
        context: &CacheContext,
    ) -> (CacheLookup, QueryPrep) {
        // Parse cache hints
        let hints = parse_cache_hints(query);

        // Skip cache if hint says so
        if hints.skip {
            self.metrics.record_skip();
            return (
                CacheLookup::Miss,
                QueryPrep {
                    hints,
                    normalized: None,
                    generation: None,
                },
            );
        }

        let start = Instant::now();

        // L1: Check hot cache (exact match)
        if self.config.l1.enabled {
            if let Some(conn_id) = context.connection_id {
                let l1 = self.get_l1_cache(conn_id);
                let hit = match l1.get(query) {
                    Some(result) if self.is_current(&result) => Some(result),
                    Some(_) => {
                        l1.remove(query);
                        self.metrics.record_stale_rejected();
                        None
                    }
                    None => None,
                };
                if let Some(result) = hit {
                    self.metrics.record_hit(CacheLevel::L1Hot, start.elapsed());
                    return (
                        CacheLookup::Hit {
                            result,
                            level: CacheLevel::L1Hot,
                        },
                        QueryPrep {
                            hints,
                            normalized: None,
                            generation: None,
                        },
                    );
                }
            }
        }

        // Normalize query for L2/L3 lookup
        let normalized = self.normalizer.normalize(query);
        let cache_key = CacheKey::new(&normalized, context);

        // L2: Check warm cache (normalized match)
        if let Some(ref l2) = self.l2_cache {
            let hit = match l2.get(&cache_key).await {
                Some(result) if self.is_current(&result) => Some(result),
                Some(_) => {
                    l2.remove(&cache_key).await;
                    self.metrics.record_stale_rejected();
                    None
                }
                None => None,
            };
            if let Some(result) = hit {
                self.metrics.record_hit(CacheLevel::L2Warm, start.elapsed());

                // Promote to L1
                if self.config.l1.enabled {
                    if let Some(conn_id) = context.connection_id {
                        let l1 = self.get_l1_cache(conn_id);
                        l1.put(query.to_string(), result.clone());
                    }
                }

                return (
                    CacheLookup::Hit {
                        result,
                        level: CacheLevel::L2Warm,
                    },
                    QueryPrep {
                        hints,
                        normalized: Some(normalized),
                        generation: None,
                    },
                );
            }
        }

        // L3: Check semantic cache (similarity match)
        if hints.semantic_cache {
            if let Some(ref l3) = self.l3_cache {
                let hit = match l3.get(query, context).await {
                    Some(result) if self.is_current(&result) => Some(result),
                    Some(_) => {
                        self.metrics.record_stale_rejected();
                        None
                    }
                    None => None,
                };
                if let Some(result) = hit {
                    self.metrics
                        .record_hit(CacheLevel::L3Semantic, start.elapsed());
                    return (
                        CacheLookup::Hit {
                            result,
                            level: CacheLevel::L3Semantic,
                        },
                        QueryPrep {
                            hints,
                            normalized: Some(normalized),
                            generation: None,
                        },
                    );
                }
            }
        }

        self.metrics.record_miss(start.elapsed());
        let generation = Some(self.generation_of(&normalized.tables));
        (
            CacheLookup::Miss,
            QueryPrep {
                hints,
                normalized: Some(normalized),
                generation,
            },
        )
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
        let prep = QueryPrep {
            hints: parse_cache_hints(query),
            normalized: None,
            generation: None,
        };
        self.put_prepared(query, context, &prep, data, row_count, execution_time)
            .await
    }

    /// Store a query result in the cache, reusing the hint parse (and, when
    /// present, the normalization) a preceding [`Self::get_with_prep`] already
    /// performed for the same SQL. When `prep` carries no normalization the
    /// query is normalized here — so this is behaviourally identical to
    /// [`Self::put`], only cheaper on the miss+store path.
    ///
    /// The `prep` MUST be the one [`Self::get_with_prep`] returned for this
    /// exact `query` — it carries that query's hints and normalization, so a
    /// foreign prep would key, TTL, or skip the entry differently from
    /// [`Self::put`]. Hence `pub(crate)`, and hence `QueryPrep` has no
    /// `Default`: the only way to obtain one is from a lookup.
    pub(crate) async fn put_prepared(
        &self,
        query: &str,
        context: &CacheContext,
        prep: &QueryPrep,
        data: Bytes,
        row_count: usize,
        execution_time: Duration,
    ) {
        let hints = &prep.hints;

        // Skip if hint says so
        if hints.skip {
            return;
        }

        // Normalize query (reusing the lookup's normalization when it has one)
        let owned_normalized;
        let normalized = match prep.normalized.as_ref() {
            Some(n) => n,
            None => {
                owned_normalized = self.normalizer.normalize(query);
                &owned_normalized
            }
        };

        // Determine TTL
        let ttl = hints
            .ttl
            .unwrap_or_else(|| self.get_table_ttl(&normalized.tables));

        // Check size limit
        if data.len() > self.config.max_result_size {
            self.metrics.record_size_exceeded();
            return;
        }

        // A write to one of these tables since the lookup missed (i.e. while
        // the result was being fetched) means the rows may predate it: do not
        // store them (C-02). Without a lookup snapshot (plain `put`) the
        // generation is taken now, which is as strong as the old behaviour.
        let current = self.generation_of(&normalized.tables);
        let generation = prep.generation.unwrap_or(current);
        if generation != current {
            self.metrics.record_fill_raced();
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
            generation,
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
            let cache_key = CacheKey::new(normalized, context);
            // When L2 must make room, one pass sheds every write-retired
            // entry, on the reclaim thread.
            l2.put_evicting(
                cache_key.clone(),
                result.clone(),
                |r| !self.is_current(r),
                |hashes| self.hand_off_shed(hashes),
            )
            .await;

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

    /// Invalidate any cached results that reference a table written by `sql`.
    /// Normalizes the (write) query to extract its tables, then drops their
    /// cached entries.
    pub async fn invalidate_query(&self, sql: &str) {
        let normalized = self.normalizer.normalize(sql);
        if !normalized.tables.is_empty() {
            self.invalidate_tables(&normalized.tables).await;
        }
    }

    /// The tables a (write) query references, without invalidating anything.
    ///
    /// Used by the commit-aware invalidation path (C-02): the tables touched
    /// inside an explicit transaction are remembered per session and
    /// re-invalidated when that transaction COMMITs, closing the window where
    /// a concurrent reader could refill an entry between the write statement
    /// and its commit.
    pub fn query_tables(&self, sql: &str) -> Vec<String> {
        self.normalizer.normalize(sql).tables
    }

    /// Invalidate cache entries for specific tables: [`Self::mark_written`]
    /// then [`Self::purge_tables`].
    pub async fn invalidate_tables(&self, tables: &[String]) {
        self.mark_written(tables);
        self.purge_tables(tables).await;
    }

    /// Move the write generations of `tables` (C-02). Synchronous and cheap:
    /// from here on no tier (L1/L2/L3) serves, and no in-flight fetch stores,
    /// a result that read these tables before this point.
    pub fn mark_written(&self, tables: &[String]) {
        for table in tables {
            // Runs before the write's ReadyForQuery: a known table (the common
            // case) is bumped in place without allocating its name again.
            if let Some(mut g) = self.table_generations.get_mut(table.as_str()) {
                *g += 1;
            } else {
                *self.table_generations.entry(table.clone()).or_insert(0) += 1;
            }
        }
    }

    /// Release the invalidation index of `tables` — the memory half of
    /// [`Self::invalidate_tables`]. Serving their entries is already refused
    /// on every tier once [`Self::mark_written`] moved their generations, so
    /// the entries themselves are not walked: a stale L2 entry is replaced
    /// by the next fill of its key or evicted by the L2 size bound and TTL,
    /// like L1 and L3 entries.
    ///
    /// O(1) per table: the key set leaves the index in one step (so of the
    /// writers purging a table at once only one gets it) and is dropped by
    /// the `cache-reclaim` thread. Walking it per key instead (reverse index
    /// and L2 removal of ~600k keys after a read-heavy phase) cost the
    /// write path a lasting ~25 % of committed TPS on the user-path gate.
    pub async fn purge_tables(&self, tables: &[String]) {
        for table in tables {
            let keys = self.invalidator.take_table(table);
            if keys.is_empty() {
                continue;
            }
            match &self.reclaim_tx {
                // The thread only ends when the cache is dropped; if it died
                // anyway, send hands the set back and it is dropped here.
                Some(tx) => drop(tx.send(Reclaim::Keys(keys))),
                None => drop(keys),
            }
        }

        self.metrics.record_invalidation(tables.len());
    }

    /// Give an L2 eviction pass's retired hashes to the reclaim thread
    /// (which sheds them and ends the pass); hands them back if it cannot.
    fn hand_off_shed(&self, hashes: Vec<u64>) -> Result<(), Vec<u64>> {
        let Some(tx) = &self.reclaim_tx else {
            return Err(hashes);
        };
        tx.send(Reclaim::Shed(hashes)).map_err(|e| match e.0 {
            Reclaim::Shed(hashes) => hashes,
            Reclaim::Keys(_) => Vec::new(),
        })
    }

    /// Invalidate everything, for a write whose table set is unknown (DDL,
    /// `EXECUTE`/`CALL`/`DO`, `COPY`, a statement no table could be read
    /// from). Every cached result on every tier stops being served.
    pub fn invalidate_all(&self) {
        self.global_generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.metrics.record_invalidation(0);
    }

    /// The tables a write statement touches, or `None` when they cannot be
    /// determined from its text (DDL, `EXECUTE`/`CALL`/`DO`, `COPY`, no
    /// table found) — the caller then invalidates everything.
    pub fn write_tables(&self, sql: &str) -> Option<Vec<String>> {
        use crate::protocol::starts_with_ci;
        const OPAQUE: &[&str] = &[
            "CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT", "GRANT", "REVOKE", "EXECUTE", "CALL",
            "DO", "COPY", "IMPORT", "SECURITY", "REFRESH", "REINDEX", "CLUSTER", "VACUUM", "LOCK",
            "SELECT",
        ];
        // Only the table names are needed, and this runs before the client
        // sees the write's ReadyForQuery: strip comments (so a leading one
        // cannot hide a DDL keyword, nor an inner one a table name) and match
        // tables, without the literal rewriting and hashing of `normalize`.
        let stripped = self.normalizer.strip_comments(sql);
        let t = stripped.trim_start();
        if OPAQUE.iter().any(|kw| starts_with_ci(t, kw)) {
            return None;
        }
        let tables = self.normalizer.extract_tables(&stripped);
        if tables.is_empty() {
            None
        } else {
            Some(tables)
        }
    }

    /// Sum of the global generation and the generations of `tables`.
    /// Generations only grow, so the sum changes whenever any of them does.
    fn generation_of(&self, tables: &[String]) -> u64 {
        let mut sum = self
            .global_generation
            .load(std::sync::atomic::Ordering::Relaxed);
        for table in tables {
            if let Some(g) = self.table_generations.get(table) {
                sum = sum.wrapping_add(*g);
            }
        }
        sum
    }

    /// Whether `result` may still be served: nothing it read has been
    /// written (or invalidated wholesale) since it was fetched.
    fn is_current(&self, result: &CachedResult) -> bool {
        result.generation == self.generation_of(&result.tables)
    }

    /// Hits refused because a table they read was written since the fetch.
    pub fn stale_rejected(&self) -> u64 {
        self.metrics.stale_rejected()
    }

    /// Fills not stored because a write raced their backend fetch.
    pub fn fills_raced(&self) -> u64 {
        self.metrics.fills_raced()
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

    /// Per-connection L1 caches must be reclaimable so they don't leak per
    /// session. `get_l1_cache` creates one; `remove_l1_cache` reclaims it.
    fn ctx(conn: u64) -> CacheContext {
        CacheContext {
            database: "db".into(),
            connection_id: Some(conn),
            ..Default::default()
        }
    }

    async fn fill(cache: &QueryCache, sql: &str, conn: u64, payload: &'static [u8]) {
        let (lookup, prep) = cache.get_with_prep(sql, &ctx(conn)).await;
        assert!(
            matches!(lookup, CacheLookup::Miss),
            "{sql} should miss first"
        );
        cache
            .put_prepared(
                sql,
                &ctx(conn),
                &prep,
                Bytes::from_static(payload),
                1,
                Duration::from_millis(1),
            )
            .await;
    }

    async fn is_hit(cache: &QueryCache, sql: &str, conn: u64) -> bool {
        matches!(cache.get(sql, &ctx(conn)).await, CacheLookup::Hit { .. })
    }

    /// A write to a table stops every tier from serving results that read it
    /// (C-02): the same connection (L1) and another connection (L2).
    #[tokio::test]
    async fn a_write_refuses_l1_and_l2_hits_of_its_tables_only() {
        let cache = QueryCache::new(CacheConfig::default());
        let q = "SELECT v FROM accounts WHERE id = 1";
        let other = "SELECT name FROM users WHERE id = 1";
        fill(&cache, q, 1, b"v=1").await;
        fill(&cache, other, 1, b"n=a").await;
        assert!(is_hit(&cache, q, 1).await, "L1 hit");
        assert!(is_hit(&cache, q, 2).await, "L2 hit from another connection");

        cache.mark_written(&["accounts".to_string()]);
        assert!(
            !is_hit(&cache, q, 1).await,
            "L1 entry refused after the write"
        );
        assert!(
            !is_hit(&cache, q, 2).await,
            "L2 entry refused after the write"
        );
        assert!(
            is_hit(&cache, other, 1).await,
            "unrelated table still served"
        );
        assert!(cache.stale_rejected() >= 2);
    }

    /// A purge only releases the table's index (the set goes to the
    /// `cache-reclaim` thread): stale L2 entries stay until a refill
    /// replaces them, are never served meanwhile, and other tables are
    /// untouched.
    #[tokio::test]
    async fn purge_releases_the_index_and_a_refill_replaces_the_stale_entry() {
        let cache = QueryCache::new(CacheConfig::default());
        assert!(cache.reclaim_tx.is_some(), "reclaimer thread started");
        let l2 = cache.l2_cache.clone().expect("L2 on by default");
        for id in 0..50 {
            fill(
                &cache,
                &format!("SELECT v FROM accounts WHERE id = {id}"),
                1,
                b"v",
            )
            .await;
        }
        let users = "SELECT name FROM users WHERE id = 1";
        fill(&cache, users, 1, b"n").await;
        assert_eq!(l2.stats().entry_count, 51);
        let usage = l2.memory_usage();

        cache.invalidate_tables(&["accounts".to_string()]).await;
        assert!(cache.invalidator.get_keys_for_table("accounts").is_empty());
        assert_eq!(cache.invalidator.get_keys_for_table("users").len(), 1);
        assert_eq!(l2.stats().entry_count, 51, "entries are not walked");
        let q = "SELECT v FROM accounts WHERE id = 7";
        assert!(!is_hit(&cache, q, 2).await, "stale L2 entry refused");
        assert!(is_hit(&cache, users, 2).await, "other table still served");

        // The refill replaces the stale entry in place: same count, and the
        // byte account does not grow (it used to count both).
        fill(&cache, q, 2, b"v").await;
        assert!(is_hit(&cache, q, 3).await);
        assert_eq!(l2.stats().entry_count, 51);
        assert_eq!(l2.memory_usage(), usage);
    }

    /// A result whose table was written while it was being fetched is not
    /// stored: it may predate the write (the reader-refill race).
    #[tokio::test]
    async fn a_fill_raced_by_a_write_is_not_stored() {
        let cache = QueryCache::new(CacheConfig::default());
        let q = "SELECT v FROM accounts WHERE id = 1";
        let (lookup, prep) = cache.get_with_prep(q, &ctx(1)).await;
        assert!(matches!(lookup, CacheLookup::Miss));
        cache.mark_written(&["accounts".to_string()]);
        cache
            .put_prepared(
                q,
                &ctx(1),
                &prep,
                Bytes::from_static(b"old"),
                1,
                Duration::ZERO,
            )
            .await;
        assert_eq!(cache.fills_raced(), 1);
        assert!(!is_hit(&cache, q, 1).await);
        assert!(!is_hit(&cache, q, 2).await);
    }

    /// An invalidation of unknown scope refuses every cached result.
    #[tokio::test]
    async fn invalidate_all_refuses_every_entry() {
        let cache = QueryCache::new(CacheConfig::default());
        let a = "SELECT v FROM accounts";
        let b = "SELECT name FROM users";
        fill(&cache, a, 1, b"a").await;
        fill(&cache, b, 1, b"b").await;
        cache.invalidate_all();
        assert!(!is_hit(&cache, a, 1).await);
        assert!(!is_hit(&cache, b, 2).await);
        // New fills after the invalidation are served again.
        fill(&cache, a, 1, b"a2").await;
        assert!(is_hit(&cache, a, 1).await);
    }

    #[test]
    fn write_tables_is_none_when_the_scope_is_unknown() {
        let cache = QueryCache::new(CacheConfig::default());
        for sql in [
            "ALTER TABLE t ADD COLUMN c int",
            "create index i on t (c)",
            "DROP TABLE t",
            "TRUNCATE t",
            "EXECUTE upd(1)",
            "CALL do_things()",
            "DO $$ BEGIN END $$",
            "COPY t FROM STDIN",
            "SELECT pg_advisory_lock(1)",
            "",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql}");
        }
        assert_eq!(
            cache.write_tables("UPDATE accounts SET v = 2 WHERE id = 1"),
            Some(vec!["accounts".to_string()])
        );
        assert_eq!(
            cache.write_tables("INSERT INTO audit SELECT * FROM public.accounts"),
            Some(vec!["audit".to_string(), "accounts".to_string()])
        );
        // Comments neither hide a DDL keyword nor a written table.
        assert_eq!(
            cache.write_tables("/* migration */ ALTER TABLE t ADD c int"),
            None
        );
        assert_eq!(cache.write_tables("-- note\nDROP TABLE t"), None);
        assert_eq!(
            cache.write_tables("UPDATE /*helios:route=primary*/ accounts SET v = 1"),
            Some(vec!["accounts".to_string()])
        );
    }

    #[test]
    fn l1_cache_is_reclaimed_on_remove() {
        let cache = QueryCache::new(CacheConfig::default());
        assert_eq!(cache.l1_cache_count(), 0);
        let _ = cache.get_l1_cache(42);
        let _ = cache.get_l1_cache(43);
        assert_eq!(cache.l1_cache_count(), 2);
        cache.remove_l1_cache(42);
        cache.remove_l1_cache(43);
        assert_eq!(cache.l1_cache_count(), 0, "L1 caches must be reclaimed");
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

    /// A miss returns the hint parse + normalization it performed, so the
    /// store can reuse them. The prep must describe the query it was taken
    /// from — otherwise `put_prepared` would key or TTL the entry differently
    /// from `put`.
    #[tokio::test]
    async fn get_with_prep_returns_the_lookups_normalization() {
        let cache = QueryCache::new(CacheConfig::default());
        let ctx = CacheContext::default();
        let sql = "SELECT id FROM users WHERE id = 7";

        let (lookup, prep) = cache.get_with_prep(sql, &ctx).await;
        assert!(matches!(lookup, CacheLookup::Miss));
        assert!(!prep.hints.skip);
        let normalized = prep.normalized.as_ref().expect("miss normalizes once");
        let fresh = cache.normalizer.normalize(sql);
        assert_eq!(normalized.fingerprint, fresh.fingerprint);
        assert_eq!(normalized.hash, fresh.hash);
        assert_eq!(normalized.tables, fresh.tables);
    }

    /// `put_prepared` with a reused prep must store exactly what `put` would
    /// have stored (same L1 entry, same TTL, same table dependencies) — the
    /// single-normalization path is an optimisation, not a behaviour change.
    #[tokio::test]
    async fn put_prepared_matches_put() {
        let sql = "SELECT id FROM users WHERE id = 7";
        let ctx = CacheContext {
            connection_id: Some(1),
            ..Default::default()
        };
        let body = Bytes::from_static(b"rows");

        let via_put = QueryCache::new(CacheConfig::default());
        via_put
            .put(sql, &ctx, body.clone(), 3, Duration::from_millis(5))
            .await;

        let via_prep = QueryCache::new(CacheConfig::default());
        let (lookup, prep) = via_prep.get_with_prep(sql, &ctx).await;
        assert!(matches!(lookup, CacheLookup::Miss));
        via_prep
            .put_prepared(sql, &ctx, &prep, body.clone(), 3, Duration::from_millis(5))
            .await;

        let a = via_put.get_l1_cache(1).get(sql).expect("put stored to L1");
        let b = via_prep
            .get_l1_cache(1)
            .get(sql)
            .expect("put_prepared stored to L1");
        assert_eq!(a.data, b.data);
        assert_eq!(a.row_count, b.row_count);
        assert_eq!(a.ttl, b.ttl);
        assert_eq!(a.tables, b.tables);
    }

    /// A `cache=skip` hint short-circuits the lookup before normalizing; the
    /// prep it hands back must still make `put_prepared` skip the store, just
    /// as `put` does.
    #[tokio::test]
    async fn skip_hint_prep_still_skips_the_store() {
        let cache = QueryCache::new(CacheConfig::default());
        let ctx = CacheContext {
            connection_id: Some(1),
            ..Default::default()
        };
        let sql = "/* helios:cache=skip */ SELECT 1";

        let (lookup, prep) = cache.get_with_prep(sql, &ctx).await;
        assert!(matches!(lookup, CacheLookup::Miss));
        assert!(prep.hints.skip);
        assert!(prep.normalized.is_none(), "skip must not normalize");

        cache
            .put_prepared(
                sql,
                &ctx,
                &prep,
                Bytes::from_static(b"rows"),
                1,
                Duration::ZERO,
            )
            .await;
        assert!(cache.get_l1_cache(1).get(sql).is_none());
    }
}
