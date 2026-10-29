//! Query Caching Module
//!
//! Provides multi-tier query caching for HeliosProxy:
//!
//! - **L1 Hot Cache**: Per-connection, exact match, LRU eviction
//! - **L2 Warm Cache**: Shared, normalized queries, configurable storage
//! - **L3 Semantic Cache**: opt-in per statement (`cache=semantic` hint),
//!   single-relation statements of its grammar: verbatim-text match, or
//!   (`semantic_match = canonical`, PostgreSQL only) canonical-form match
//!   (the relation's alias, which alias a qualifier uses, keyword case,
//!   whitespace, hint comments and `AS` do not matter; names keep their
//!   case) plus optional column substitution by name similarity
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
//!                     ││ L3: Semantic Cache (canonical form, hinted)   ││
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
//! use heliosdb_proxy::cache::{QueryCache, CacheConfig};
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

pub(crate) mod canonical;
pub mod config;
pub mod hints;
pub mod invalidation;
pub mod l1_hot;
pub mod l2_warm;
pub mod l3_semantic;
pub mod logical_source;
pub mod metrics;
pub mod normalizer;
pub mod result;

// Re-exports
pub use config::{CacheConfig, L1Config, L2Config, L3Config, StorageBackend};
pub use hints::{parse_cache_hints, CacheHint};
pub use invalidation::{InvalidationManager, InvalidationMode};
pub use l1_hot::L1HotCache;
pub use l2_warm::L2WarmCache;
pub use l3_semantic::{EndpointState, L3SemanticCache, SemanticHealth};
pub use metrics::{CacheMetrics, CacheStatsLevelSnapshot, CacheStatsSnapshot};
pub use normalizer::{NormalizedQuery, QueryNormalizer};
pub use result::{CacheKey, CachedResult};

use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::sql_text::ShorterTtls;

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
    /// Whether `normalized` keeps whitespace and comments (a HeliosDB
    /// backend had been seen when it was made, [`QueryCache::heliosdb_backend`]):
    /// `put_prepared` stores nothing under a key made in the other mode.
    keyed_verbatim: bool,
    /// Cache generation of the query's tables when the lookup missed, i.e.
    /// before the backend fetch (C-02). `put_prepared` refuses to store a
    /// result whose tables were written while it was being fetched.
    generation: Option<u64>,
    /// What the semantic tier made of the query, so it is parsed once.
    semantic: SemanticPrep,
}

impl QueryPrep {
    /// Whether the read may be stored: false when its hints say
    /// `cache=skip` (a zero `cache_ttl` included), so the caller need not
    /// capture the response at all.
    pub(crate) fn storable(&self) -> bool {
        !self.hints.skip
    }
}

/// The semantic tier's view of a query, computed at most once per read.
#[derive(Debug, Clone)]
enum SemanticPrep {
    /// Not looked at (no `cache=semantic` hint, no tier, or the lookup ended
    /// before L3).
    NotConsulted,
    /// Outside the tier's grammar (or longer than its statement bound), or
    /// its relation was not extracted: the tier neither serves nor stores
    /// it, and its relations are not complete.
    Ineligible,
    /// Its form (canonical or verbatim, `semantic_match`), which says whether
    /// its relations are complete.
    Canonical(canonical::Canonical),
}

impl SemanticPrep {
    /// The form `l3` keys `query` by (with the extracted `tables`, at most
    /// `max_bytes` long, under `semantic_match`), or ineligible.
    fn of(query: &str, tables: &[String], l3: &L3Config) -> Self {
        match canonical::semantic_form(query, tables, l3.max_statement_bytes, l3.semantic_match) {
            Some(c) => SemanticPrep::Canonical(c),
            None => SemanticPrep::Ineligible,
        }
    }

    /// Whether the query's relations are complete (syntactically: the text
    /// names no relation beyond the extracted tables), when the parse already
    /// says: never without a canonical form (the grammar refused it or its
    /// relation was not extracted), and with one only when its relation is
    /// spelled in lower case and no column is qualified: exactly what
    /// `canonical::relations_complete` answers for an unhinted read, so the
    /// TTL does not depend on the hint.
    fn relations_complete(&self) -> Option<bool> {
        match self {
            SemanticPrep::NotConsulted => None,
            SemanticPrep::Ineligible => Some(false),
            SemanticPrep::Canonical(c) => Some(c.relations_complete),
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

/// Bytes a [`QueryCache`] retains in each tier.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct RetainedBytes {
    pub l1: u64,
    pub l2: u64,
    pub l3: u64,
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

    /// Counters of the logical-decoding invalidation source (C-02 slice 2),
    /// when `[cache] invalidation_source = "logical"`; read by `/metrics`.
    logical: std::sync::OnceLock<Arc<logical_source::LogicalStats>>,

    /// The `[[cache.tables]]` TTLs shorter than `default_ttl`, applied
    /// fail-closed to a read whose relations were not all recognised
    /// (`canonical::relations_complete`).
    shorter_ttls: Option<ShorterTtls>,

    /// Sticky: a backend reported a HeliosDB `server_version`
    /// ([`Self::note_heliosdb_backend`]). From then on L2 keys keep
    /// whitespace and comments, and a canonical semantic tier neither
    /// stores nor serves.
    heliosdb_backend: std::sync::atomic::AtomicBool,
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
                                    l2.shed_paced(&hashes);
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
            shorter_ttls: ShorterTtls::new(
                config.table_configs.iter().map(|(name, c)| (name, c.ttl)),
                config.default_ttl,
            ),
            config: config.clone(),
            l1_caches: DashMap::new(),
            l2_cache,
            l3_cache,
            normalizer: Arc::new(QueryNormalizer::new()),
            invalidator,
            metrics: Arc::new(CacheMetrics::new()),
            table_generations: DashMap::new(),
            global_generation: std::sync::atomic::AtomicU64::new(0),
            reclaim_tx,
            logical: std::sync::OnceLock::new(),
            heliosdb_backend: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Note that a backend reported a HeliosDB `server_version` (HeliosDB-
    /// Nano, HeliosDB-Lite), whose engines read the raw statement text
    /// (comments, whitespace, case) before parsing. Sticky for the cache's
    /// life: from now on the L2 key of a read keeps its whitespace and
    /// comments (`sql_text::fingerprint_verbatim`), and a semantic tier with
    /// `semantic_match = canonical` neither stores nor serves (its hinted
    /// reads count as ineligible). Every entry stored before is retired on
    /// every tier ([`Self::invalidate_all`]): it was keyed the other way, and
    /// a statement whose verbatim key equals another's collapsed one must not
    /// be served from it. Returns whether this call set it.
    pub fn note_heliosdb_backend(&self) -> bool {
        let first = !self
            .heliosdb_backend
            .swap(true, std::sync::atomic::Ordering::AcqRel);
        if first {
            self.invalidate_all();
        }
        first
    }

    /// Whether [`Self::note_heliosdb_backend`] ran.
    pub fn heliosdb_backend(&self) -> bool {
        self.heliosdb_backend
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Whether the semantic tier is canonical and so off for a HeliosDB
    /// backend (neither stores nor serves).
    fn canonical_tier_off(&self) -> bool {
        self.config.l3.semantic_match == crate::config::SemanticMatch::Canonical
            && self.heliosdb_backend()
    }

    /// Attach the logical-decoding source's counters (set once at startup).
    pub fn set_logical_stats(&self, stats: Arc<logical_source::LogicalStats>) {
        let _ = self.logical.set(stats);
    }

    /// The logical-decoding source's counters, when that source runs.
    pub fn logical_stats(&self) -> Option<&Arc<logical_source::LogicalStats>> {
        self.logical.get()
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

    /// Bytes retained per tier, for `/metrics` (P-03): every connection's L1
    /// (with per-entry overhead, C-01), L2 in memory, and L3.
    pub fn retained_bytes(&self) -> RetainedBytes {
        RetainedBytes {
            l1: self
                .l1_caches
                .iter()
                .map(|c| c.retained_bytes() as u64)
                .sum(),
            l2: self
                .l2_cache
                .as_ref()
                .map_or(0, |c| c.memory_usage() as u64),
            l3: self
                .l3_cache
                .as_ref()
                .map_or(0, |c| c.retained_bytes() as u64),
        }
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
        self.get_with_prep_hinted(query, None, context).await
    }

    /// [`Self::get_with_prep`] for a `query` that is not the text the client
    /// sent: routing hints (`[routing_hints] strip_hints`), a rewrite rule or
    /// the tenant filter may have removed the client's cache hints from it.
    /// `client_sql`, when given, is the client's own text; its hints are
    /// merged into the query's ([`CacheHint::merge`]).
    pub async fn get_with_prep_hinted(
        &self,
        query: &str,
        client_sql: Option<&str>,
        context: &CacheContext,
    ) -> (CacheLookup, QueryPrep) {
        // Parse cache hints
        let mut hints = parse_cache_hints(query);
        if let Some(client_sql) = client_sql {
            hints.merge(parse_cache_hints(client_sql));
        }

        // Skip cache if hint says so
        if hints.skip {
            self.metrics.record_skip();
            return (
                CacheLookup::Miss,
                QueryPrep {
                    hints,
                    normalized: None,
                    keyed_verbatim: false,
                    generation: None,
                    semantic: SemanticPrep::NotConsulted,
                },
            );
        }

        let start = Instant::now();

        // A TTL hint bounds what the read is served too: an entry older than
        // it (stored by an unhinted read, or under a longer hint) is a miss
        // on every tier, and the refill stores the result with that TTL.
        let max_age = hints.ttl;
        let young_enough = |r: &CachedResult| max_age.is_none_or(|age| r.age() <= age);

        // L1: Check hot cache (exact match)
        if self.config.l1.enabled {
            if let Some(conn_id) = context.connection_id {
                let l1 = self.get_l1_cache(conn_id);
                let hit = match l1.get(query) {
                    Some(result) if self.is_current(&result) => {
                        young_enough(&result).then_some(result)
                    }
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
                            keyed_verbatim: false,
                            generation: None,
                            semantic: SemanticPrep::NotConsulted,
                        },
                    );
                }
            }
        }

        // Normalize query for L2/L3 lookup: whitespace and comments kept once
        // a backend reported a HeliosDB server_version (its engines read
        // them before parsing). The mode is carried to the store, which
        // refuses a key made in the other one.
        let keyed_verbatim = self.heliosdb_backend();
        let normalized = self.normalizer.normalize_with(query, keyed_verbatim);
        let cache_key = CacheKey::new(&normalized, context);

        // L2: Check warm cache (normalized match)
        if let Some(ref l2) = self.l2_cache {
            let hit = match l2.get(&cache_key).await {
                Some(result) if self.is_current(&result) => young_enough(&result).then_some(result),
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
                        keyed_verbatim,
                        generation: None,
                        semantic: SemanticPrep::NotConsulted,
                    },
                );
            }
        }

        // L3: the semantic tier, for hinted reads its grammar accepts. The
        // statement is parsed here once; the store reuses the outcome.
        // Synchronous: it never waits on its embedding endpoint. A canonical
        // tier is off once a HeliosDB backend was seen (the parse still
        // decides whether the relations are complete, for the TTL).
        let mut semantic = SemanticPrep::NotConsulted;
        if hints.semantic_cache {
            if let Some(ref l3) = self.l3_cache {
                semantic = SemanticPrep::of(query, &normalized.tables, &self.config.l3);
                match &semantic {
                    SemanticPrep::Canonical(_) if self.canonical_tier_off() => {
                        l3.record_ineligible()
                    }
                    SemanticPrep::Canonical(c) => {
                        match l3.lookup_within(c, context, &|r| self.is_current(r), max_age) {
                            l3_semantic::SemanticLookup::Hit {
                                result,
                                approximate,
                            } => {
                                tracing::debug!(
                                    target: "helios::cache",
                                    approximate,
                                    "semantic tier hit"
                                );
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
                                        keyed_verbatim,
                                        generation: None,
                                        semantic,
                                    },
                                );
                            }
                            l3_semantic::SemanticLookup::Stale => {
                                self.metrics.record_stale_rejected()
                            }
                            l3_semantic::SemanticLookup::Miss => {}
                        }
                    }
                    _ => l3.record_ineligible(),
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
                keyed_verbatim,
                generation,
                semantic,
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
            keyed_verbatim: false,
            generation: None,
            semantic: SemanticPrep::NotConsulted,
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
        if !prep.storable() {
            return;
        }

        // Normalize query (reusing the lookup's normalization when it has
        // one), noting the key mode it was made in: a HeliosDB backend seen
        // since (another session's first dial, between this lookup's key and
        // its generation snapshot, or this session's own, dialed for the
        // read) switches L2 to the verbatim key, and the store is refused
        // below whether or not the snapshot already included the
        // `invalidate_all` that came with the switch.
        let owned_normalized;
        let (normalized, keyed_verbatim) = match prep.normalized.as_ref() {
            Some(n) => (n, prep.keyed_verbatim),
            None => {
                let verbatim = self.heliosdb_backend();
                owned_normalized = self.normalizer.normalize_with(query, verbatim);
                (&owned_normalized, verbatim)
            }
        };

        // The semantic tier's parse: the lookup's, or (a plain `put` of a
        // hinted read) made here, once, for both the TTL and the L3 store.
        let computed;
        let semantic = match &prep.semantic {
            SemanticPrep::NotConsulted if hints.semantic_cache && self.l3_cache.is_some() => {
                computed = SemanticPrep::of(query, &normalized.tables, &self.config.l3);
                &computed
            }
            semantic => semantic,
        };

        // Determine TTL: the configured one for the read's tables, which a
        // client hint can only shorten (L2 does not clamp to its own TTL).
        // Whether the relations are complete comes from that parse when there
        // was one.
        let table_ttl = self.get_table_ttl(query, &normalized.tables, || {
            semantic.relations_complete().unwrap_or_else(|| {
                canonical::relations_complete(
                    query,
                    &normalized.tables,
                    self.config.l3.max_statement_bytes,
                )
            })
        });
        let ttl = hints.ttl.map_or(table_ttl, |hint| hint.min(table_ttl));

        // Check size limit
        if data.len() > self.config.max_result_size {
            self.metrics.record_size_exceeded();
            return;
        }

        // A write to one of these tables since the lookup missed (i.e. while
        // the result was being fetched) means the rows may predate it: do not
        // store them (C-02). Without a lookup snapshot (plain `put`) the
        // generation is taken now, which is as strong as the old behaviour.
        // Nor is a result stored under a key made before the HeliosDB flag
        // was set: the flag is checked after the snapshot, so a switch either
        // shows here or retires the entry with its `invalidate_all`.
        let current = self.generation_of(&normalized.tables);
        let generation = prep.generation.unwrap_or(current);
        if generation != current || self.heliosdb_backend() != keyed_verbatim {
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

        // Store in L3 (semantic) if hint enabled, by the form above; never a
        // canonical form once a HeliosDB backend was seen (checked now: the
        // backend may have been dialed for this very read).
        if hints.semantic_cache && !self.canonical_tier_off() {
            if let (Some(l3), SemanticPrep::Canonical(c)) = (&self.l3_cache, semantic) {
                l3.store(c, context, result, &|r| self.is_current(r));
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
    /// determined from its text (DDL, `EXECUTE`/`CALL`/`DO`, `COPY`, a
    /// `MERGE` that is not `MERGE INTO`, an `INSERT` without `INTO`, an
    /// `UPDATE OR …` / `UPDATE REPLACE` or `INSERT INTO LOCAL` that
    /// HeliosDB-Nano and HeliosDB-Lite run on the table named next, no table
    /// found, backend-ambiguous text or more than one statement:
    /// `sql_text::single_statement`) — the caller then invalidates
    /// everything.
    pub fn write_tables(&self, sql: &str) -> Option<Vec<String>> {
        use crate::protocol::starts_with_ci;
        const OPAQUE: &[&str] = &[
            "CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT", "GRANT", "REVOKE", "EXECUTE", "CALL",
            "DO", "COPY", "IMPORT", "SECURITY", "REFRESH", "REINDEX", "CLUSTER", "VACUUM", "LOCK",
            "SELECT",
        ];
        // Only the table names are needed, and this runs before the client
        // sees the write's ReadyForQuery: one pass of the lexer gives the
        // first word past the leading comments (so one cannot hide a DDL
        // keyword) and the comment-free text the tables are matched on (so
        // an inner comment cannot hide a table name), without the literal
        // rewriting and hashing of `normalize`. A journaled `SELECT`
        // (`SELECT … INTO`, a function call) has an unknown scope.
        let (t, text) = crate::sql_text::single_statement(sql)?;
        if OPAQUE.iter().any(|kw| starts_with_ci(t, kw))
            || crate::sql_text::merge_without_into(t)
            || crate::sql_text::insert_without_into(t)
        {
            return None;
        }
        let tables = self.normalizer.write_statement_tables(&text)?;
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

    /// [`Self::invalidate_all`] for a simple-query string a backend may
    /// split into a statement the journal capture does not see (the relay's
    /// `cache_unseen_write`), counted in
    /// [`Self::unseen_write_invalidations`].
    pub fn invalidate_all_for_unseen_write(&self) {
        self.metrics.record_unseen_write_invalidation();
        self.invalidate_all();
    }

    /// Whole-cache invalidations for statements the journal capture could
    /// not see ([`Self::invalidate_all_for_unseen_write`]).
    pub fn unseen_write_invalidations(&self) -> u64 {
        self.metrics.unseen_write_invalidations()
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

    /// The semantic tier, when `l3.enabled`.
    pub fn semantic_cache(&self) -> Option<&Arc<L3SemanticCache>> {
        self.l3_cache.as_ref()
    }

    /// The semantic tier's health and counters, when it is built.
    pub fn semantic_health(&self) -> Option<SemanticHealth> {
        self.l3_cache.as_ref().map(|l3| l3.health())
    }

    /// Get the invalidation manager (for WAL subscription)
    pub fn invalidator(&self) -> Arc<InvalidationManager> {
        self.invalidator.clone()
    }

    /// The TTL of `sql`, a read of the extracted `tables`: the shortest over
    /// its tables of the table's own TTL (`table_configs`), else
    /// `default_ttl`, when `complete()` says the extracted `tables` name every
    /// relation the read's text names (a longer table TTL then applies; the
    /// check is syntactic, so a view's or a row-level security policy's own
    /// tables are not seen, and the documentation says never to give such a
    /// relation a longer TTL). Otherwise a
    /// relation the extraction missed could carry another TTL: the result is
    /// capped at `default_ttl`, and every shorter configured TTL whose table
    /// the text names anywhere applies (fail-closed, [`ShorterTtls`]).
    /// `complete()` (a parse) is evaluated only when it can change the
    /// result: when one of `tables` has a TTL longer than the default, or
    /// when the text scan (cheap) finds a TTL shorter than theirs.
    fn get_table_ttl(
        &self,
        sql: &str,
        tables: &[String],
        complete: impl FnOnce() -> bool,
    ) -> Duration {
        let default = self.config.default_ttl;
        if self.config.table_configs.is_empty() {
            return default;
        }
        let min = tables
            .iter()
            .map(|t| self.config.table_configs.get(t).map_or(default, |c| c.ttl))
            .min()
            .unwrap_or(default);
        let scan = || {
            self.shorter_ttls
                .as_ref()
                .and_then(|s| s.min_in(sql))
                .unwrap_or(default)
        };
        if min > default {
            // A longer table TTL applies only on complete relations.
            return if complete() { min } else { default.min(scan()) };
        }
        // Complete or not, the result is `min` unless the scan finds a
        // shorter configured TTL in the text.
        let scanned = scan();
        if scanned >= min || complete() {
            min
        } else {
            scanned
        }
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
        // `ONLY` is skipped, never read as a table named `only`.
        for sql in [
            "UPDATE ONLY kv SET val = 'new' WHERE id = 1",
            "DELETE FROM ONLY kv WHERE id = 1",
            "DELETE FROM ONLY (kv) WHERE id = 1",
            "MERGE INTO ONLY kv USING src ON kv.id = src.id WHEN MATCHED THEN DELETE",
        ] {
            assert_eq!(
                cache.write_tables(sql),
                Some(vec!["kv".to_string()]),
                "{sql}"
            );
        }
        // A three-part name is its last part; a parenthesized join keeps its
        // first table.
        assert_eq!(
            cache.write_tables("UPDATE benchdb.public.kv SET v = 2 WHERE id = 1"),
            Some(vec!["kv".to_string()])
        );
        assert_eq!(
            cache.write_tables(
                "UPDATE orders SET v = 1 FROM (items JOIN lots ON true) WHERE orders.id = items.id"
            ),
            Some(vec![
                "orders".to_string(),
                "items".to_string(),
                "lots".to_string()
            ])
        );
    }

    /// A leading comment the backends end in different places hides what the
    /// statement is: PostgreSQL ends a `--` comment at a lone `\r` and runs
    /// the `CREATE OR REPLACE VIEW` after it (HeliosDB-Nano reads one
    /// comment). Its table text only named the view's base table, so the
    /// view's cached reads stayed current; the scope is unknown instead.
    #[test]
    fn write_tables_is_none_after_an_ambiguous_leading_comment() {
        let cache = QueryCache::new(CacheConfig::default());
        for sql in [
            "-- v2\rCREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE active",
            "-- x\rREVOKE SELECT ON kv FROM r",
            "/* a */ -- x\rCREATE POLICY p ON kv USING (EXISTS (SELECT 1 FROM acl))",
            "/* /* */ CREATE VIEW v AS SELECT * FROM t */ UPDATE t SET v = 1",
            "-- x\rUPDATE t SET v = 1",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql:?}");
        }
        // Comments both backends end alike still leave the first word known.
        assert_eq!(
            cache.write_tables("-- x\r\nUPDATE t SET v = 1"),
            Some(vec!["t".to_string()])
        );
        // HeliosDB-Nano and HeliosDB-Lite read a Unicode space as whitespace:
        // the DDL after one is DDL there (its table text only named the
        // view's base table).
        for sql in [
            "\u{a0}CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE active",
            "\u{3000}/* c */\u{a0}ALTER TABLE t RENAME TO u",
            "\u{a0}MERGE\u{a0}BRANCH fix INTO main",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql:?}");
        }
        assert_eq!(
            cache.write_tables("\u{a0}UPDATE\u{a0}t SET v = 1"),
            Some(vec!["t".to_string()])
        );
    }

    /// R6-1: a write whose text PostgreSQL and HeliosDB-Nano/Lite (or the two
    /// `standard_conforming_strings` settings) may read as different
    /// statements has an unknown scope: everything is invalidated. Its tables
    /// used to be read every server's way and unioned.
    #[test]
    fn write_tables_is_none_for_backend_ambiguous_text() {
        let cache = QueryCache::new(CacheConfig::default());
        for sql in [
            "UPDATE a SET p = 'C:\\'; UPDATE -- c\n b SET v = 1",
            "UPDATE a SET p = E'x\\'y' WHERE id = 1",
            "UPDATE a SET v = 1 /* /* */ ; UPDATE b SET v = 2 -- */",
            "UPDATE a SET v = 1 -- c\r WHERE id = 2",
            "UPDATE a SET p = $$it's$$",
            "UPDATE a SET p = 'open",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql:?}");
        }
        // A backslash, a `$` or a comment marker alone is no ambiguity.
        assert_eq!(
            cache.write_tables("UPDATE a SET p = 'C:\\tmp', q = $$x$$ -- c\r\n"),
            Some(vec!["a".to_string()])
        );
    }

    /// A quoted column, conflict target or alias names no table, and a
    /// query-tag comment whose quotes all close is read alike by every
    /// backend (HeliosDB-Lite's comment-blind splitter included), a `;` in a
    /// later literal or not: such writes keep their target. Each used to
    /// invalidate everything (`global_generation`).
    #[test]
    fn write_tables_keep_the_target_of_ordinary_orm_text() {
        let cache = QueryCache::new(CacheConfig::default());
        for (sql, table) in [
            ("UPDATE t SET \"x\" = 1", "t"),
            (
                "UPDATE accounts SET \"lastLogin\" = now() WHERE id = $1",
                "accounts",
            ),
            (
                "INSERT INTO orders (\"orderId\", \"createdAt\") VALUES ($1, $2) \
                 ON CONFLICT (\"orderId\") DO NOTHING",
                "orders",
            ),
            (
                "/*application='app',action='save'*/ UPDATE t SET note = 'a;b' WHERE id = 1",
                "t",
            ),
            (
                "UPDATE t SET note = 'Tom; Jerry' /*controller='users'*/ WHERE id = 1",
                "t",
            ),
        ] {
            assert_eq!(
                cache.write_tables(sql),
                Some(vec![table.to_string()]),
                "{sql:?}"
            );
        }
        // Where the extraction reads a table, a quoted name is still unknown.
        for sql in [
            "UPDATE \"t\" SET v = 1 FROM u",
            "UPDATE U&\"t\" SET v = 1 FROM u",
            "WITH d AS (DELETE FROM \"t\" RETURNING id) INSERT INTO u SELECT id FROM d",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql:?}");
        }
    }

    /// R6-2: a string of more than one statement has an unknown scope
    /// (`UPDATE a …; TRUNCATE b` used to move only `a`). Statement
    /// boundaries come only from the lexer: a `;` inside a literal or a
    /// comment ends nothing, and its words are never read as SQL.
    #[test]
    fn write_tables_is_none_for_more_than_one_statement() {
        let cache = QueryCache::new(CacheConfig::default());
        for sql in [
            "UPDATE a SET x = 1; TRUNCATE b",
            "UPDATE a SET x = 1; UPDATE b SET v = 2;",
            "INSERT INTO a VALUES (1); CREATE OR REPLACE VIEW v AS SELECT * FROM t",
            "UPDATE a SET note = 'x;y'; DROP TABLE b",
            "UPDATE a SET x = 1;; ",
            // HeliosDB-Lite ends a statement at a `;` inside a comment.
            "UPDATE a SET x = 1 -- ; TRUNCATE b",
            "UPDATE a SET v = 1 -- old: x; DELETE FROM b",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql:?}");
        }
        for (sql, table) in [
            ("UPDATE a SET s = 'x;y' WHERE id = 1;", "a"),
            (
                "UPDATE posts SET body = 'Hi; do you know?' WHERE id = 1",
                "posts",
            ),
            (
                "INSERT INTO comments (post_id, body) VALUES (7, 'Thanks; do you ship to Canada?')",
                "comments",
            ),
            (
                "UPDATE posts SET body = 'Costs $5; call us' WHERE id = 1",
                "posts",
            ),
            ("UPDATE a SET x = 1; -- done", "a"),
            ("UPDATE a SET x = 1 -- done;", "a"),
        ] {
            assert_eq!(
                cache.write_tables(sql),
                Some(vec![table.to_string()]),
                "{sql:?}"
            );
        }
    }

    /// A write naming something the table pattern cannot read whole where it
    /// reads a table name (a subquery's `FROM` included): a quoted
    /// identifier, a quoted literal, or a name with a `$` or a non-ASCII
    /// character. The pattern stepped back over `INSERT`'s optional `INTO`
    /// and took the keyword for the table (`INSERT INTO "app_book" …` moved
    /// only a table `into`, `MERGE INTO "t" … THEN INSERT VALUES …` a table
    /// `values`), read a qualifier or another table instead (`UPDATE s."t"`
    /// moved `s`, `UPDATE "t" … FROM u` moved `u`) or cut the name (`t$x`
    /// moved `t`): the write's scope is unknown, so everything is
    /// invalidated.
    #[test]
    fn write_tables_is_none_for_a_name_the_extraction_cannot_read() {
        let cache = QueryCache::new(CacheConfig::default());
        for sql in [
            "INSERT INTO \"t\" VALUES (1)",
            "INSERT INTO \"app_book\" (\"title\") VALUES ('x') RETURNING \"app_book\".\"id\"",
            "INSERT INTO \"s\".t VALUES (1)",
            "MERGE INTO \"t\" USING s ON \"t\".id = s.id WHEN NOT MATCHED THEN INSERT VALUES (s.id)",
            "UPDATE s.\"t\" SET v = 1",
            "UPDATE \"t\" SET v = 1 FROM u WHERE \"t\".id = u.id",
            "DELETE FROM kv WHERE id IN (SELECT id FROM \"Gone\")",
            "INSERT INTO caf\u{e9} VALUES (1)",
            "MERGE INTO caf\u{e9} USING s ON true WHEN NOT MATCHED THEN INSERT DEFAULT VALUES",
            "UPDATE t$x SET v = 1",
            // HeliosDB-Nano and HeliosDB-Lite read a quoted literal there as
            // the table (`public.'kv'` is `kv`): these moved `public`, `t2`
            // and a table `set`.
            "UPDATE public.'kv' SET v = 'new' WHERE id = 1",
            "INSERT INTO s.'t' VALUES (1)",
            "DELETE FROM 'kv' WHERE id IN (SELECT id FROM t2)",
            "MERGE INTO 'kv' USING s ON kv.id = s.id WHEN MATCHED THEN UPDATE SET v = 1",
            // sqlparser's `DELETE x FROM 'kv'` deletes from `kv`: this moved `u`.
            "DELETE x FROM 'kv' WHERE id IN (SELECT id FROM u)",
            // HeliosDB-Lite upper-cases `ı` to `I`: this CTE writes `kv` there.
            "WITH d AS (\u{131}nsert \u{131}nto kv VALUES (1) RETURNING *) SELECT * FROM d",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql:?}");
        }
        // Literals, `$n` parameters, Unicode spaces and non-ASCII columns
        // are read as before.
        for (sql, table) in [
            ("UPDATE t SET v = 'caf\u{e9}', w = $1 WHERE id = $2", "t"),
            ("INSERT INTO t VALUES ('a$$b', $$c$$)", "t"),
            ("UPDATE\u{a0}t SET v = 1", "t"),
            (
                "UPDATE customers SET \u{540d}\u{524d} = 'x' WHERE id = 1",
                "customers",
            ),
            (
                "UPDATE kunden SET gr\u{f6}\u{df}e = 3 WHERE id = 7",
                "kunden",
            ),
            (
                "INSERT INTO produits (pr\u{e9}nom) VALUES ('a')",
                "produits",
            ),
            // Upper-cased, these name no keyword (`STRASSE`, `ADI`).
            (
                "UPDATE kunden SET stra\u{df}e = 'Hauptstr. 1' WHERE id = 7",
                "kunden",
            ),
            ("UPDATE users SET ad\u{131} = 'x' WHERE id = 1", "users"),
            // A literal after the `FROM` of `IS DISTINCT FROM` (the usual
            // no-op-update guard) or of a function is a value.
            (
                "UPDATE jobs SET status = 'done' WHERE id = 5 AND status IS DISTINCT FROM 'done'",
                "jobs",
            ),
            (
                "UPDATE t SET y = EXTRACT(year FROM '2024-01-01'::date) WHERE id = 1",
                "t",
            ),
        ] {
            assert_eq!(
                cache.write_tables(sql),
                Some(vec![table.to_string()]),
                "{sql:?}"
            );
        }
    }

    /// HeliosDB-Nano and HeliosDB-Lite take `INTO` as optional in an
    /// `INSERT`: `INSERT orders SELECT … FROM staging` writes `orders`, and
    /// only the generation of `staging` used to move.
    #[test]
    fn write_tables_is_none_for_an_insert_without_into() {
        let cache = QueryCache::new(CacheConfig::default());
        for sql in [
            "INSERT orders SELECT id, v FROM staging",
            "INSERT orders (c) VALUES ((SELECT max(c) FROM s))",
            "INSERT OR REPLACE orders SELECT * FROM staging",
            "/* load */ INSERT OVERWRITE TABLE orders SELECT * FROM staging",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql:?}");
        }
        assert_eq!(
            cache.write_tables("INSERT OR REPLACE INTO orders SELECT * FROM staging"),
            Some(vec!["orders".to_string(), "staging".to_string()])
        );
        // Nor in a later statement: the string's scope is unknown anyway.
        assert_eq!(
            cache.write_tables("UPDATE a SET x = 1; INSERT orders SELECT * FROM staging"),
            None
        );
    }

    /// HeliosDB-Nano and HeliosDB-Lite (sqlparser) read an `UPDATE`'s
    /// conflict clause and an `INSERT`'s `INTO LOCAL` before the target and
    /// write the table named next: only the generation of a table `or`,
    /// `replace` or `local` used to move, and `kv`'s reads stayed current.
    #[test]
    fn write_tables_is_none_for_a_word_sqlparser_reads_before_the_target() {
        let cache = QueryCache::new(CacheConfig::default());
        for sql in [
            "UPDATE OR IGNORE kv SET v = 2 WHERE id = 1",
            "UPDATE REPLACE kv SET v = 2",
            "INSERT INTO LOCAL kv VALUES (1)",
            "INSERT OR REPLACE INTO LOCAL kv VALUES (1)",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql:?}");
        }
        assert_eq!(
            cache.write_tables("UPDATE orders SET v = 2"),
            Some(vec!["orders".to_string()])
        );
    }

    /// HeliosDB-Nano's `MERGE [DATABASE] BRANCH src INTO dst` changes every
    /// table of the branch it merges into; the word after its `INTO` is a
    /// branch. Only the generation of a table `main` used to move.
    #[test]
    fn write_tables_is_none_for_a_branch_merge() {
        let cache = QueryCache::new(CacheConfig::default());
        for sql in [
            "MERGE BRANCH fix INTO main",
            "MERGE DATABASE BRANCH fix INTO main WITH (strategy = 'theirs')",
            "/* deploy */ merge branch fix into main",
        ] {
            assert_eq!(cache.write_tables(sql), None, "{sql:?}");
        }
        assert_eq!(
            cache.write_tables(
                "MERGE INTO accounts a USING fixes f ON a.id = f.id WHEN MATCHED THEN DELETE"
            ),
            Some(vec!["accounts".to_string()])
        );
    }

    /// A write through `ONLY` retires the table's cached reads, the
    /// semantic tier's included: it used to move the generation of a table
    /// `only`, and the old value was served until the TTL ended.
    #[tokio::test]
    async fn a_write_through_only_retires_the_tables_reads() {
        let cache = semantic_cache(1.0, None);
        let read = "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/";
        let paraphrase = "select val from kv x where id = 1 /*helios:cache=semantic*/";
        fill(&cache, read, 1, b"old").await;
        assert_eq!(level_of(&cache, read, 2).await, Some(CacheLevel::L2Warm));
        assert_eq!(
            level_of(&cache, paraphrase, 3).await,
            Some(CacheLevel::L3Semantic)
        );
        let tables = cache
            .write_tables("UPDATE ONLY kv SET val = 'new' WHERE id = 1")
            .expect("kv");
        cache.mark_written(&tables);
        assert_eq!(level_of(&cache, paraphrase, 4).await, None);
        assert_eq!(level_of(&cache, read, 1).await, None);
        assert_eq!(level_of(&cache, read, 2).await, None);
    }

    /// A write retires a read whose last select-list column ends in a
    /// keyword (`valid_from`): its tables used to be `[from]` (the `from` of
    /// `valid_from` matched and took the real `FROM` as the table), which no
    /// write to `prices` moves, so L1 and L2 served the old row until the TTL
    /// ended (and L3 refused the read).
    #[tokio::test]
    async fn a_write_retires_a_read_whose_column_ends_in_a_keyword() {
        let cache = semantic_cache(1.0, None);
        let read = "SELECT id, valid_from FROM prices WHERE id = 1 /*helios:cache=semantic*/";
        let paraphrase =
            "select id, valid_from from prices p where id = 1 /*helios:cache=semantic*/";
        fill(&cache, read, 1, b"old").await;
        assert_eq!(level_of(&cache, read, 2).await, Some(CacheLevel::L2Warm));
        assert_eq!(
            level_of(&cache, paraphrase, 3).await,
            Some(CacheLevel::L3Semantic)
        );
        let tables = cache
            .write_tables("UPDATE prices SET valid_from = now() WHERE id = 1")
            .expect("prices");
        assert_eq!(tables, ["prices"]);
        cache.mark_written(&tables);
        assert_eq!(level_of(&cache, read, 1).await, None);
        assert_eq!(level_of(&cache, read, 2).await, None);
        assert_eq!(level_of(&cache, paraphrase, 4).await, None);
    }

    /// A write that spells its table's schema qualifier with a comment or
    /// whitespace around the dot (`UPDATE app./**/kv`) retires `app.kv`'s
    /// reads in every tier, and `UPDATE app.kv` retires a read spelled that
    /// way: the table text's `app. kv` used to be read as the table `app`,
    /// so L1, L2 and L3 served the old row until the TTL ended.
    #[tokio::test]
    async fn a_write_with_a_spaced_qualifier_dot_retires_the_tables_reads() {
        let cache = semantic_cache(1.0, None);
        let read = "SELECT v FROM app.kv WHERE id = 1 /*helios:cache=semantic*/";
        let paraphrase = "select v from app . kv x where id = 1 /*helios:cache=semantic*/";
        fill(&cache, read, 1, b"old").await;
        assert_eq!(level_of(&cache, read, 2).await, Some(CacheLevel::L2Warm));
        assert_eq!(
            level_of(&cache, paraphrase, 3).await,
            Some(CacheLevel::L3Semantic)
        );
        let tables = cache
            .write_tables("UPDATE app./**/kv SET v = 2 WHERE id = 1")
            .expect("kv");
        assert_eq!(tables, ["kv"]);
        cache.mark_written(&tables);
        assert_eq!(level_of(&cache, read, 1).await, None);
        assert_eq!(level_of(&cache, read, 2).await, None);
        assert_eq!(level_of(&cache, paraphrase, 4).await, None);

        // The other way round: the read spells the spaced dot.
        let spaced = "SELECT v FROM app/**/.kv WHERE id = 2";
        fill(&cache, spaced, 5, b"old").await;
        assert!(is_hit(&cache, spaced, 6).await);
        let tables = cache
            .write_tables("INSERT INTO app.kv VALUES (2, 3)")
            .expect("kv");
        assert_eq!(tables, ["kv"]);
        cache.mark_written(&tables);
        assert!(!is_hit(&cache, spaced, 5).await);
        assert!(!is_hit(&cache, spaced, 6).await);
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

    /// The TTL an L2 entry for `sql` was stored with.
    async fn l2_ttl(cache: &QueryCache, sql: &str, ctx: &CacheContext) -> Option<Duration> {
        let key = CacheKey::new(&cache.normalizer.normalize(sql), ctx);
        cache.l2_cache.as_ref()?.get(&key).await.map(|r| r.ttl)
    }

    /// A zero `cache_ttl` hint is a skip: it used to miss every tier and then
    /// replace the entry unhinted reads of the statement share with one never
    /// served, so the next unhinted read missed too.
    #[tokio::test]
    async fn a_zero_ttl_hint_leaves_the_shared_entry_alone() {
        let cache = QueryCache::new(CacheConfig::default());
        let sql = "SELECT v FROM t WHERE id = 1";
        fill(&cache, sql, 1, b"v").await;
        let hinted = "SELECT v FROM t WHERE id = 1 /*helios:cache_ttl=0*/";
        let (lookup, prep) = cache.get_with_prep(hinted, &ctx(2)).await;
        assert!(matches!(lookup, CacheLookup::Miss));
        assert!(!prep.storable(), "no capture armed");
        cache
            .put_prepared(
                hinted,
                &ctx(2),
                &prep,
                Bytes::from_static(b"fresh"),
                1,
                Duration::ZERO,
            )
            .await;
        match cache.get(sql, &ctx(3)).await {
            CacheLookup::Hit { result, level } => {
                assert_eq!(level, CacheLevel::L2Warm);
                assert_eq!(&result.data[..], b"v");
            }
            CacheLookup::Miss => panic!("the shared entry was replaced"),
        }
    }

    /// A client `cache_ttl` hint can shorten the configured TTL, never
    /// lengthen it (L2 does not clamp; it used to keep 86400 s here).
    #[tokio::test]
    async fn hint_ttl_can_only_shorten() {
        let cache = QueryCache::new(CacheConfig::default());
        let ctx = CacheContext::default();
        for (sql, want) in [
            (
                "SELECT v FROM t WHERE id = 1 /*helios:cache_ttl=86400*/",
                300,
            ),
            ("SELECT v FROM t WHERE id = 2 /*helios:cache_ttl=60*/", 60),
            ("SELECT v FROM t WHERE id = 3", 300),
        ] {
            cache
                .put(sql, &ctx, Bytes::from_static(b"r"), 1, Duration::ZERO)
                .await;
            assert_eq!(
                l2_ttl(&cache, sql, &ctx).await,
                Some(Duration::from_secs(want)),
                "{sql}"
            );
        }
    }

    /// A `[[cache.tables]]` TTL longer than `ttl_secs` applies only when
    /// the extracted tables name every relation of the read; a multi-table
    /// read takes the shortest.
    #[tokio::test]
    async fn per_table_ttl_lengthens_only_on_complete_relations() {
        let mut config = CacheConfig::default();
        config.l1.ttl = Duration::from_secs(3600);
        config.table_configs.insert(
            "a".to_string(),
            config::TableCacheConfig {
                ttl: Duration::from_secs(3600),
                ..Default::default()
            },
        );
        config.table_configs.insert(
            "s".to_string(),
            config::TableCacheConfig {
                ttl: Duration::from_secs(10),
                ..Default::default()
            },
        );
        let cache = QueryCache::new(config);
        let ctx = CacheContext::default();
        for (sql, want) in [
            ("SELECT v FROM a WHERE id = 1", 3600),
            ("SELECT v FROM a JOIN b ON a.id = b.id", 300),
            ("SELECT v FROM b", 300),
            // The extraction misses `c` (comma join): not lengthened.
            ("SELECT v FROM a, c", 300),
            ("SELECT v FROM a JOIN s ON true", 10),
            ("SELECT v FROM a /*helios:cache_ttl=20*/", 20),
            // A shorter table TTL holds even where the extraction misses the
            // table: the text is scanned for it (fail-closed).
            ("SELECT v FROM a, s", 10),
            ("SELECT v FROM \"S\" WHERE id = 1", 10),
            (
                "SELECT v FROM b WHERE id IN (SELECT id FROM public.\"s\")",
                10,
            ),
            ("SELECT v FROM U&\"\\0073\"", 10),
            // A recognised relation is not shortened by a column named `s`.
            ("SELECT s FROM a WHERE id = 2", 3600),
            // A qualified column may call a function `v(x)` that reads other
            // tables: not lengthened.
            ("SELECT x.v FROM a x WHERE id = 3", 300),
            ("SELECT v FROM a x WHERE x.v > 3", 300),
        ] {
            cache
                .put(sql, &ctx, Bytes::from_static(b"r"), 1, Duration::ZERO)
                .await;
            assert_eq!(
                l2_ttl(&cache, sql, &ctx).await,
                Some(Duration::from_secs(want)),
                "{sql}"
            );
        }
        // A statement over the parse bound is not parsed: not lengthened, and
        // a shorter TTL still holds through the text scan.
        let bound = CacheConfig::default().l3.max_statement_bytes;
        let list = ",1".repeat(bound / 2);
        for (sql, want) in [
            (format!("SELECT v FROM a WHERE id IN (1{list})"), 300),
            (format!("SELECT v FROM s WHERE id IN (1{list})"), 10),
        ] {
            assert!(sql.len() > bound);
            cache
                .put(&sql, &ctx, Bytes::from_static(b"r"), 1, Duration::ZERO)
                .await;
            assert_eq!(
                l2_ttl(&cache, &sql, &ctx).await,
                Some(Duration::from_secs(want)),
                "{}",
                &sql[..40]
            );
        }
    }

    /// Hints in the client's own text count when the forwarded text lost
    /// them (`[routing_hints] strip_hints`): a skip neither looks up nor
    /// lets the read be stored.
    #[tokio::test]
    async fn client_text_hints_are_merged() {
        let cache = QueryCache::new(CacheConfig::default());
        let sql = "SELECT v FROM t";
        fill(&cache, sql, 1, b"v").await;
        let (lookup, prep) = cache
            .get_with_prep_hinted(sql, Some("/*helios:cache=skip*/ SELECT v FROM t"), &ctx(1))
            .await;
        assert!(matches!(lookup, CacheLookup::Miss), "a skip never looks up");
        assert!(!prep.storable());
        assert_eq!(cache.stats().total_skips, 1);
        let (lookup, prep) = cache.get_with_prep_hinted(sql, Some(sql), &ctx(1)).await;
        assert!(matches!(lookup, CacheLookup::Hit { .. }));
        assert!(prep.storable());
    }

    /// A configuration with the semantic tier on (endpoint on a closed port
    /// unless given).
    fn semantic_config(threshold: f32, endpoint: Option<&str>) -> CacheConfig {
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let fallback = format!("http://{}", closed.local_addr().unwrap());
        CacheConfig {
            l3: L3Config {
                enabled: true,
                similarity_threshold: threshold,
                embedding_endpoint: endpoint.map_or(fallback, str::to_string),
                embedding_dim: 64,
                embedding_timeout: Duration::from_millis(200),
                embedding_retry_after: Duration::from_secs(30),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// A cache with the semantic tier on (endpoint on a closed port unless
    /// given).
    fn semantic_cache(threshold: f32, endpoint: Option<&str>) -> QueryCache {
        QueryCache::new(semantic_config(threshold, endpoint))
    }

    async fn level_of(cache: &QueryCache, sql: &str, conn: u64) -> Option<CacheLevel> {
        match cache.get(sql, &ctx(conn)).await {
            CacheLookup::Hit { level, .. } => Some(level),
            CacheLookup::Miss => None,
        }
    }

    /// A paraphrase another connection sends is served by L3 (L1 is per
    /// connection, L2 keys the literal text's fingerprint); a write to its
    /// table retires it (C-02) and a refill serves again.
    #[tokio::test]
    async fn l3_hit_honours_c02_and_recovers_after_refill() {
        let cache = semantic_cache(1.0, None);
        let fill_sql = "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/";
        let paraphrase = "select val from kv as t where id = 1 /*helios:cache=semantic*/";
        fill(&cache, fill_sql, 1, b"v1").await;
        assert_eq!(
            level_of(&cache, paraphrase, 2).await,
            Some(CacheLevel::L3Semantic)
        );
        // Unhinted, the paraphrase never reaches L3.
        assert_eq!(
            level_of(&cache, "select val from kv as t where id = 1", 3).await,
            None
        );
        // A qualified column may be a function of the row: never served from
        // the bare one's entry.
        assert_eq!(
            level_of(
                &cache,
                "SELECT t.val FROM kv AS t WHERE t.id = 1 /*helios:cache=semantic*/",
                3
            )
            .await,
            None
        );

        let stale = cache.stale_rejected();
        cache.mark_written(&["kv".to_string()]);
        assert_eq!(level_of(&cache, paraphrase, 4).await, None);
        assert_eq!(cache.stale_rejected(), stale + 1);

        fill(&cache, fill_sql, 5, b"v2").await;
        assert_eq!(
            level_of(&cache, paraphrase, 6).await,
            Some(CacheLevel::L3Semantic)
        );
        let l3 = cache.semantic_cache().unwrap();
        assert_eq!(l3.len(), 1, "the refill replaced the retired entry");
        assert_eq!(cache.stats().l3.hits, 2);
    }

    /// An endpoint that is down never delays a read: hinted lookups and
    /// stores return at once and never error, exact canonical L3 hits keep
    /// working, and the probe reports the state.
    #[tokio::test]
    async fn down_endpoint_never_delays_reads() {
        let cache = semantic_cache(0.9, None);
        let l3 = cache.semantic_cache().unwrap().clone();
        l3.probe().await;
        assert_eq!(
            cache.semantic_health().unwrap().endpoint,
            EndpointState::Down
        );
        let started = Instant::now();
        for i in 0..20 {
            let sql = format!("SELECT c{i} FROM kv WHERE id = 1 /*helios:cache=semantic*/");
            fill(&cache, &sql, i, b"v").await;
            let paraphrase =
                format!("SELECT c{i} FROM kv AS k WHERE id = 1 /*helios:cache=semantic*/");
            assert_eq!(
                level_of(&cache, &paraphrase, 1000 + i).await,
                Some(CacheLevel::L3Semantic)
            );
        }
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "{:?}",
            started.elapsed()
        );
        let health = cache.semantic_health().unwrap();
        assert_eq!(
            health.embed_requests, 1,
            "only the probe called: {health:?}"
        );
        assert!(health.embed_skipped > 0);
    }

    #[tokio::test]
    async fn semantic_ineligible_shapes_are_counted() {
        let cache = semantic_cache(1.0, None);
        for sql in [
            "SELECT 1 /*helios:cache=semantic*/",
            "SELECT v FROM \"Kv\" /*helios:cache=semantic*/",
            "SELECT v FROM kv WHERE s = $$x$$ /*helios:cache=semantic*/",
        ] {
            let (lookup, prep) = cache.get_with_prep(sql, &ctx(1)).await;
            assert!(matches!(lookup, CacheLookup::Miss));
            assert!(matches!(prep.semantic, SemanticPrep::Ineligible), "{sql}");
        }
        // Unhinted reads never count.
        let _ = cache.get("SELECT 1", &ctx(1)).await;
        let health = cache.semantic_health().unwrap();
        assert_eq!(health.ineligible, 3);
        assert_eq!(health.entries, 0);
        assert!(QueryCache::new(CacheConfig::default())
            .semantic_health()
            .is_none());
    }

    /// A hinted read longer than `max_statement_bytes` is not parsed: it is
    /// ineligible (counted) and uses L1/L2 only.
    #[tokio::test]
    async fn semantic_statement_bound_is_configurable() {
        let mut config = semantic_config(1.0, None);
        let short = "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/";
        let long = "SELECT val FROM kv WHERE id IN (1, 2, 3, 4, 5) /*helios:cache=semantic*/";
        config.l3.max_statement_bytes = short.len();
        let cache = QueryCache::new(config);
        let (_, prep) = cache.get_with_prep(short, &ctx(1)).await;
        assert!(matches!(prep.semantic, SemanticPrep::Canonical(_)));
        let (_, prep) = cache.get_with_prep(long, &ctx(1)).await;
        assert!(matches!(prep.semantic, SemanticPrep::Ineligible));
        fill(&cache, long, 2, b"v").await;
        let health = cache.semantic_health().unwrap();
        assert_eq!((health.ineligible, health.entries), (2, 0), "{health:?}");
    }

    /// A result whose semantic entry alone exceeds `semantic_max_bytes` (yet
    /// fits `max_result_size`) is stored in L1/L2 only, and counted: it no
    /// longer empties the tier (storing it evicted every entry, then
    /// itself).
    #[tokio::test]
    async fn semantic_result_over_the_byte_budget_stays_out_of_l3() {
        static BIG: [u8; 8192] = [b'x'; 8192];
        let mut config = semantic_config(1.0, None);
        config.l3.max_bytes = 4096;
        let cache = QueryCache::new(config);
        let small = "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/";
        let large = "SELECT doc FROM kv WHERE id = 7 /*helios:cache=semantic*/";
        fill(&cache, small, 1, b"small").await;
        fill(&cache, large, 2, &BIG).await;
        let health = cache.semantic_health().unwrap();
        assert_eq!((health.entries, health.too_large), (1, 1), "{health:?}");
        assert_eq!(
            level_of(
                &cache,
                "select val from kv t where id = 1 /*helios:cache=semantic*/",
                3
            )
            .await,
            Some(CacheLevel::L3Semantic)
        );
        assert_eq!(level_of(&cache, large, 4).await, Some(CacheLevel::L2Warm));
        assert_eq!(
            level_of(
                &cache,
                "select doc from kv t where id = 7 /*helios:cache=semantic*/",
                5
            )
            .await,
            None
        );
    }

    /// A semantic hint in the client's own text (stripped from the forwarded
    /// one by routing hints) reaches L3.
    #[tokio::test]
    async fn client_text_semantic_hint_reaches_l3() {
        let cache = semantic_cache(1.0, None);
        let forwarded = "SELECT val FROM kv WHERE id = 1";
        let client = "/*helios:cache=semantic*/ SELECT val FROM kv WHERE id = 1";
        let (lookup, prep) = cache
            .get_with_prep_hinted(forwarded, Some(client), &ctx(1))
            .await;
        assert!(matches!(lookup, CacheLookup::Miss));
        assert!(matches!(prep.semantic, SemanticPrep::Canonical(_)));
        cache
            .put_prepared(
                forwarded,
                &ctx(1),
                &prep,
                Bytes::from_static(b"v"),
                1,
                Duration::ZERO,
            )
            .await;
        let (lookup, _) = cache
            .get_with_prep_hinted(
                "SELECT val FROM kv k WHERE id = 1",
                Some("/*helios:cache=semantic*/ SELECT val FROM kv k WHERE id = 1"),
                &ctx(2),
            )
            .await;
        assert!(matches!(
            lookup,
            CacheLookup::Hit {
                level: CacheLevel::L3Semantic,
                ..
            }
        ));
    }

    /// A `cache_ttl` hint bounds the age of what the read is served, not only
    /// the TTL of what it stores: an entry an unhinted read stored long ago
    /// used to answer a read hinted `cache_ttl=5`. Every tier: L1 (the
    /// forwarded text lost the hint to `[routing_hints]`), L2 and L3.
    #[tokio::test]
    async fn a_ttl_hint_bounds_the_age_of_what_is_served() {
        let cache = semantic_cache(1.0, None);
        let sql = "SELECT val FROM kv WHERE id = 1";
        fill(&cache, sql, 1, b"old").await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let short = "SELECT val FROM kv WHERE id = 1 /*helios:cache_ttl=200ms*/";
        let long = "SELECT val FROM kv WHERE id = 1 /*helios:cache_ttl=60s*/";
        // L1: same connection, the forwarded text without the hint.
        let (lookup, _) = cache.get_with_prep_hinted(sql, Some(short), &ctx(1)).await;
        assert!(
            matches!(lookup, CacheLookup::Miss),
            "L1 served an entry older than the hint"
        );
        let (lookup, _) = cache.get_with_prep_hinted(sql, Some(long), &ctx(1)).await;
        assert!(matches!(
            lookup,
            CacheLookup::Hit {
                level: CacheLevel::L1Hot,
                ..
            }
        ));
        // L2: another connection.
        assert_eq!(level_of(&cache, short, 2).await, None, "L2");
        assert_eq!(level_of(&cache, long, 3).await, Some(CacheLevel::L2Warm));
        // Unhinted reads still get the entry.
        assert_eq!(level_of(&cache, sql, 4).await, Some(CacheLevel::L2Warm));
        // The refused lookup refills: the result is stored with the hint's
        // TTL and served to the hinted read while young enough.
        let (lookup, prep) = cache.get_with_prep(short, &ctx(5)).await;
        assert!(matches!(lookup, CacheLookup::Miss));
        cache
            .put_prepared(
                short,
                &ctx(5),
                &prep,
                Bytes::from_static(b"new"),
                1,
                Duration::ZERO,
            )
            .await;
        match cache.get(short, &ctx(6)).await {
            CacheLookup::Hit { result, level } => {
                assert_eq!(
                    (level, result.data.as_ref()),
                    (CacheLevel::L2Warm, &b"new"[..])
                );
            }
            CacheLookup::Miss => panic!("the refill is served"),
        }

        // L3: a paraphrase with the same hints.
        let cache = semantic_cache(1.0, None);
        let read = "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/";
        fill(&cache, read, 1, b"old").await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        let paraphrase = |ttl: &str| {
            format!("select val from kv x where id = 1 /*helios:cache=semantic,cache_ttl={ttl}*/")
        };
        assert_eq!(level_of(&cache, &paraphrase("200ms"), 2).await, None, "L3");
        assert_eq!(
            level_of(&cache, &paraphrase("60s"), 3).await,
            Some(CacheLevel::L3Semantic)
        );
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

    /// The semantic tier's parse runs at most once per read: the lookup's
    /// outcome (a canonical form, or ineligible) also decides the per-table
    /// TTL and the L3 store. A read the lookup did not hand to L3 is parsed at
    /// the store, once, and only when a table TTL longer than the default
    /// needs to know its relations are complete, or the text names a table
    /// with a shorter one.
    #[tokio::test]
    async fn a_read_is_parsed_at_most_once() {
        let parses = || canonical::PARSES.with(|n| n.get());
        let mut config = semantic_config(1.0, None);
        config.l1.ttl = Duration::from_secs(3600);
        config.table_configs.insert(
            "kv".to_string(),
            config::TableCacheConfig {
                ttl: Duration::from_secs(3600),
                ..Default::default()
            },
        );
        let cache = QueryCache::new(config);
        let ctx = CacheContext::default();
        let l3 = cache.semantic_cache().unwrap().clone();
        for (sql, ttl, stored) in [
            (
                "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/",
                3600,
                1,
            ),
            (
                "SELECT val FROM kv, other /*helios:cache=semantic*/",
                300,
                1,
            ),
            ("SELECT val FROM kv WHERE id = 2", 3600, 1),
        ] {
            let before = parses();
            let (lookup, prep) = cache.get_with_prep(sql, &ctx).await;
            assert!(matches!(lookup, CacheLookup::Miss), "{sql}");
            cache
                .put_prepared(
                    sql,
                    &ctx,
                    &prep,
                    Bytes::from_static(b"r"),
                    1,
                    Duration::ZERO,
                )
                .await;
            assert_eq!(parses() - before, 1, "{sql}");
            assert_eq!(
                l2_ttl(&cache, sql, &ctx).await,
                Some(Duration::from_secs(ttl)),
                "{sql}"
            );
            assert_eq!(l3.len(), stored, "{sql}");
        }
        // A plain `put` of a hinted read parses once for the TTL and the store.
        let before = parses();
        let sql = "SELECT val FROM kv WHERE id = 3 /*helios:cache=semantic*/";
        cache
            .put(sql, &ctx, Bytes::from_static(b"r"), 1, Duration::ZERO)
            .await;
        assert_eq!(parses() - before, 1);
        assert_eq!(l3.len(), 2);
        assert_eq!(
            l2_ttl(&cache, sql, &ctx).await,
            Some(Duration::from_secs(3600))
        );
        // Without a longer table TTL an unhinted read is never parsed.
        let plain = QueryCache::new(CacheConfig::default());
        let before = parses();
        let sql = "SELECT val FROM kv WHERE id = 4";
        let (_, prep) = plain.get_with_prep(sql, &ctx).await;
        plain
            .put_prepared(
                sql,
                &ctx,
                &prep,
                Bytes::from_static(b"r"),
                1,
                Duration::ZERO,
            )
            .await;
        assert_eq!(parses(), before);
        // With only a shorter table TTL, an unhinted read is parsed only
        // when its text names that table (the parse then decides whether
        // the mention is the relation it reads).
        let mut config = CacheConfig::default();
        config.table_configs.insert(
            "audit".to_string(),
            config::TableCacheConfig {
                ttl: Duration::from_secs(10),
                ..Default::default()
            },
        );
        let shorter = QueryCache::new(config);
        for (sql, parsed, ttl) in [
            ("SELECT * FROM orders WHERE id = 1", 0, 300),
            ("SELECT v FROM orders WHERE note = 'audit'", 1, 300),
            ("SELECT v FROM orders, audit", 1, 10),
            ("SELECT v FROM audit WHERE id = 1", 0, 10),
        ] {
            let before = parses();
            let (lookup, prep) = shorter.get_with_prep(sql, &ctx).await;
            assert!(matches!(lookup, CacheLookup::Miss), "{sql}");
            shorter
                .put_prepared(
                    sql,
                    &ctx,
                    &prep,
                    Bytes::from_static(b"r"),
                    1,
                    Duration::ZERO,
                )
                .await;
            assert_eq!(parses() - before, parsed, "{sql}");
            assert_eq!(
                l2_ttl(&shorter, sql, &ctx).await,
                Some(Duration::from_secs(ttl)),
                "{sql}"
            );
        }
    }

    /// The per-table TTL is a function of the statement's parse alone: the
    /// same read gets the same TTL with and without the semantic hint, with
    /// the tier on or off. A lone `ORDER BY` name equal to an output alias
    /// used to be refused only by the hinted path (300 s) while the unhinted
    /// one lengthened it (3600 s).
    #[tokio::test]
    async fn the_per_table_ttl_does_not_depend_on_the_hint() {
        let with_table = |mut config: CacheConfig| {
            config.l1.ttl = Duration::from_secs(3600);
            config.table_configs.insert(
                "items".to_string(),
                config::TableCacheConfig {
                    ttl: Duration::from_secs(3600),
                    ..Default::default()
                },
            );
            QueryCache::new(config)
        };
        let ctx = CacheContext::default();
        for (sql, want) in [
            ("SELECT price_eur AS price FROM items ORDER BY price", 300),
            ("SELECT price_eur AS price FROM items GROUP BY price", 300),
            (
                "SELECT price_eur AS price FROM items ORDER BY price_eur",
                3600,
            ),
            ("SELECT price_eur FROM items ORDER BY price_eur", 3600),
            ("SELECT x.price_eur FROM items x ORDER BY price", 300),
        ] {
            for text in [sql.to_string(), format!("{sql} /*helios:cache=semantic*/")] {
                for cache in [
                    with_table(semantic_config(1.0, None)),
                    with_table(CacheConfig::default()),
                ] {
                    let (lookup, prep) = cache.get_with_prep(&text, &ctx).await;
                    assert!(matches!(lookup, CacheLookup::Miss), "{text}");
                    cache
                        .put_prepared(
                            &text,
                            &ctx,
                            &prep,
                            Bytes::from_static(b"r"),
                            1,
                            Duration::ZERO,
                        )
                        .await;
                    assert_eq!(
                        l2_ttl(&cache, &text, &ctx).await,
                        Some(Duration::from_secs(want)),
                        "{text}"
                    );
                }
            }
        }
    }

    /// PostgreSQL truncates an identifier to 63 bytes: a longer table name in
    /// the text reads the configured 63-byte table, whose shorter TTL holds
    /// (the statement is outside the grammar, so its text is scanned).
    #[tokio::test]
    async fn shorter_table_ttl_follows_identifier_truncation() {
        let name = "n".repeat(63);
        let mut config = CacheConfig::default();
        config.table_configs.insert(
            name.clone(),
            config::TableCacheConfig {
                ttl: Duration::from_secs(10),
                ..Default::default()
            },
        );
        let cache = QueryCache::new(config);
        let ctx = CacheContext::default();
        for (sql, want) in [
            (format!("SELECT v FROM {name}_and_more WHERE id = 1"), 10),
            (format!("SELECT v FROM {name} WHERE id = 1"), 10),
            (
                format!("SELECT v FROM {} WHERE id = 1", "n".repeat(62)),
                300,
            ),
        ] {
            cache
                .put(&sql, &ctx, Bytes::from_static(b"r"), 1, Duration::ZERO)
                .await;
            assert_eq!(
                l2_ttl(&cache, &sql, &ctx).await,
                Some(Duration::from_secs(want)),
                "{sql}"
            );
        }
    }

    /// A canonical semantic tier is off once a backend reported a HeliosDB
    /// `server_version` (R5-1): nothing is stored or served there (hinted
    /// reads count as ineligible), an entry stored before the flag is no
    /// longer served, and L1/L2 keep serving the exact text. Without the flag
    /// the same paraphrase is a canonical hit.
    #[tokio::test]
    async fn a_heliosdb_backend_turns_the_canonical_tier_off() {
        let fill_sql = "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/";
        let paraphrase = "select val from kv as t where id = 1 /*helios:cache=semantic*/";
        // Without a comment, this statement's verbatim L2 key is the
        // collapsed key of `fill_sql`.
        let plain = "SELECT val FROM kv WHERE id = 1";
        let pg = semantic_cache(1.0, None);
        assert!(!pg.heliosdb_backend());
        fill(&pg, fill_sql, 1, b"v1").await;
        assert_eq!(
            level_of(&pg, paraphrase, 2).await,
            Some(CacheLevel::L3Semantic)
        );
        assert_eq!(level_of(&pg, plain, 2).await, Some(CacheLevel::L2Warm));
        assert!(pg.note_heliosdb_backend());
        assert!(!pg.note_heliosdb_backend(), "sticky: set once");
        assert!(pg.heliosdb_backend());
        let ineligible = pg.semantic_health().unwrap().ineligible;
        assert_eq!(level_of(&pg, paraphrase, 3).await, None);
        assert_eq!(pg.semantic_health().unwrap().ineligible, ineligible + 1);
        // Entries keyed before the flag are retired on every tier.
        assert_eq!(level_of(&pg, plain, 4).await, None);
        assert_eq!(level_of(&pg, fill_sql, 1).await, None);

        let nano = semantic_cache(1.0, None);
        nano.note_heliosdb_backend();
        fill(&nano, fill_sql, 1, b"v1").await;
        assert_eq!(nano.semantic_cache().unwrap().len(), 0, "nothing stored");
        assert_eq!(level_of(&nano, paraphrase, 2).await, None);
        assert_eq!(level_of(&nano, fill_sql, 3).await, Some(CacheLevel::L2Warm));
        assert_eq!(level_of(&nano, fill_sql, 1).await, Some(CacheLevel::L1Hot));
    }

    /// The verbatim tier (`semantic_match = "verbatim"`, the daemon's
    /// default) serves the same text, on any backend, and nothing else.
    #[tokio::test]
    async fn the_verbatim_tier_serves_only_the_same_text() {
        let mut config = semantic_config(1.0, None);
        config.l3.semantic_match = crate::config::SemanticMatch::Verbatim;
        let sql = "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic*/";
        for heliosdb in [false, true] {
            let cache = QueryCache::new(config.clone());
            if heliosdb {
                cache.note_heliosdb_backend();
            }
            fill(&cache, sql, 1, b"v1").await;
            assert_eq!(cache.semantic_cache().unwrap().len(), 1, "{heliosdb}");
            cache.clear(&[CacheLevel::L2Warm]).await;
            for other in [
                "select val from kv as t where id = 1 /*helios:cache=semantic*/",
                "SELECT val FROM kv  WHERE id = 1 /*helios:cache=semantic*/",
                "SELECT val FROM kv WHERE id = 1 /*helios:cache=semantic,cache_ttl=60*/",
            ] {
                assert_eq!(
                    level_of(&cache, other, 2).await,
                    None,
                    "{heliosdb}: {other}"
                );
            }
            assert_eq!(
                level_of(&cache, sql, 3).await,
                Some(CacheLevel::L3Semantic),
                "{heliosdb}"
            );
        }
    }

    /// Once a HeliosDB backend was seen, L2 keys keep comments and whitespace:
    /// HeliosDB-Lite's `SIMILAR TO` rewrite reads the comment's apostrophe as
    /// the start of a literal and rewrites the real one after it, so the two
    /// texts below return different rows there. On PostgreSQL they are one
    /// entry. A read whose lookup ran before the flag (the first connection's
    /// backend dialed for it) is not stored: its key was the collapsed one,
    /// and the flag retired the generation its lookup took.
    #[tokio::test]
    async fn a_heliosdb_backend_keys_l2_by_the_text_as_written() {
        let a = "SELECT id FROM notes /* don't */ WHERE body = 'SIMILAR TO'";
        let b = "SELECT id FROM notes WHERE body = 'SIMILAR TO'";
        let pg = QueryCache::new(CacheConfig::default());
        fill(&pg, a, 1, b"a").await;
        assert!(is_hit(&pg, b, 2).await, "PostgreSQL: one statement");

        let lite = QueryCache::new(CacheConfig::default());
        lite.note_heliosdb_backend();
        fill(&lite, a, 1, b"a").await;
        assert!(!is_hit(&lite, b, 2).await);
        assert!(is_hit(&lite, a, 3).await);

        let raced = QueryCache::new(CacheConfig::default());
        let (lookup, prep) = raced.get_with_prep(a, &ctx(1)).await;
        assert!(matches!(lookup, CacheLookup::Miss));
        raced.note_heliosdb_backend();
        let raced_fills = raced.fills_raced();
        raced
            .put_prepared(
                a,
                &ctx(1),
                &prep,
                Bytes::from_static(b"a"),
                1,
                Duration::ZERO,
            )
            .await;
        assert_eq!(raced.fills_raced(), raced_fills + 1, "not stored");
        assert!(!is_hit(&raced, b, 2).await);
        assert!(!is_hit(&raced, a, 3).await);
        fill(&raced, a, 4, b"a").await;
        assert!(!is_hit(&raced, b, 5).await);
        assert!(is_hit(&raced, a, 6).await);
    }

    /// Another session's first HeliosDB dial can set the flag (and retire
    /// everything) after a lookup made its key but before it took its
    /// generation snapshot, which then includes that `invalidate_all`. The
    /// store used to go through under the collapsed key, which is the
    /// verbatim key of the comment-free text: HeliosDB-Nano answers the two
    /// differently (its point-lookup fast path declines a commented
    /// statement).
    #[tokio::test]
    async fn a_key_made_before_the_heliosdb_flag_is_not_stored() {
        let commented = "SELECT v FROM Kv WHERE id = 1 /* c */";
        let plain = "SELECT v FROM Kv WHERE id = 1";
        let cache = QueryCache::new(CacheConfig::default());
        let (lookup, mut prep) = cache.get_with_prep(commented, &ctx(1)).await;
        assert!(matches!(lookup, CacheLookup::Miss));
        assert!(cache.note_heliosdb_backend());
        // The snapshot taken after the switch.
        let tables = prep.normalized.as_ref().expect("normalized").tables.clone();
        prep.generation = Some(cache.generation_of(&tables));
        assert_eq!(
            prep.normalized.as_ref().unwrap().fingerprint,
            cache.normalizer.normalize_with(plain, true).fingerprint,
            "the collapsed key of one is the verbatim key of the other"
        );
        let raced = cache.fills_raced();
        cache
            .put_prepared(
                commented,
                &ctx(1),
                &prep,
                Bytes::from_static(b"commented"),
                1,
                Duration::ZERO,
            )
            .await;
        assert_eq!(cache.fills_raced(), raced + 1, "not stored");
        assert!(!is_hit(&cache, plain, 2).await);
        assert!(!is_hit(&cache, commented, 1).await);
    }
}
