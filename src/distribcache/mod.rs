//! Helios-DistribCache - Intelligent Distributed Caching Layer
//!
//! A multi-tier distributed caching system with workload-aware strategies,
//! intelligent prefetching, and AI/Agent optimizations.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     WORKLOAD CLASSIFIER                          │
//! │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐   │
//! │  │  OLTP   │ │  OLAP   │ │ Vector  │ │AIAgent  │ │   RAG   │   │
//! │  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//!                               │
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     MULTI-TIER CACHE                             │
//! │  ┌───────────────────────────────────────────────────────────┐  │
//! │  │ L1: Hot Cache (in-memory, uncompressed)                   │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! │                          │ miss                                  │
//! │  ┌───────────────────────────────────────────────────────────┐  │
//! │  │ L2: Warm Cache (in-memory, compressed; no disk)           │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! │                          │ miss                                  │
//! │  ┌───────────────────────────────────────────────────────────┐  │
//! │  │ L3: Distributed Cache (peer mesh over TCP)                │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Multi-Tier Caching**: L1 hot (memory), L2 warm (compressed memory), L3 distributed (mesh)
//! - **Workload Classification**: OLTP, OLAP, Vector, AI Agent, RAG pipelines
//! - **Prefetch Prediction**: Pattern-based and temporal prediction into a
//!   bounded queue the caller executes ([`HeliosDistribCache::prefetch_queue`])
//! - **WAL-Based Invalidation**: Real-time cache coherency via WAL streaming
//! - **Heatmap Analytics**: Visual cache utilization and recommendations
//! - **AI/Agent Caches**: Conversation context, RAG chunks, tool results, semantic queries

pub mod ai;
pub mod classifier;
pub mod config;
pub mod heatmap;
pub mod invalidator;
pub mod metrics;
pub mod prefetcher;
pub mod scheduler;
pub mod tiers;

pub use ai::{
    cosine_similarity,
    AIIntegrationConfig,
    // Cross-feature AI integration
    AIIntegrationCoordinator,
    AIIntegrationStatsSnapshot,
    AIWorkloadContext,
    AIWorkloadDetection,
    // Branch-aware and session-aware types (SessionId is defined locally as newtype)
    BranchContext,
    BranchId,
    CachePriority,
    CacheRecommendation,
    Chunk,
    ChunkId,
    ConversationCacheStats,
    ConversationContext,
    ConversationContextCache,
    Embedding,
    RagCacheStatsSnapshot,
    RagChunkCache,
    RecommendedTier,
    SemanticCacheStatsSnapshot,
    SemanticEntry,
    SemanticIndex,
    SemanticQueryCache,
    SessionTrackingInfo,
    SimilarityResult,
    ToolCacheStatsSnapshot,
    ToolCallKey,
    ToolResult,
    ToolResultCache,
    Turn,
    VectorId,
};
pub use classifier::*;
pub use config::*;
pub use heatmap::*;
pub use invalidator::*;
pub use metrics::{DistribCacheMetrics, ErrorType, InvalidationSource};
pub use prefetcher::*;
pub use scheduler::*;
pub use tiers::{
    CacheEntry, CacheKey, CacheTier, CompressionType, DistributedCache, EvictionPolicy, HotCache,
    PeerOpReport, ReplicationReport, TierStats, WarmCache,
};

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;

/// Cache errors
#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Cache miss")]
    Miss,

    #[error("Entry expired")]
    Expired,

    #[error("Entry too large: {0} bytes (max: {1})")]
    TooLarge(usize, usize),

    #[error("Tier unavailable: {0}")]
    TierUnavailable(String),

    #[error("Peer not found: {0}")]
    PeerNotFound(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Invalidation error: {0}")]
    Invalidation(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),
}

pub type CacheResult<T> = std::result::Result<T, CacheError>;

/// Query fingerprint for cache key generation
#[derive(Debug, Clone, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueryFingerprint {
    /// Normalized query template
    pub template: String,
    /// Table names referenced
    pub tables: Vec<String>,
    /// Parameter hash (for parameterized queries)
    pub param_hash: Option<u64>,
}

impl QueryFingerprint {
    /// Create a new fingerprint from a query
    pub fn from_query(query: &str) -> Self {
        let template = Self::normalize_query(query);
        let tables = Self::extract_tables(&template);
        let param_hash = None; // Set separately if parameterized

        Self {
            template,
            tables,
            param_hash,
        }
    }

    /// Create fingerprint with parameter binding
    pub fn with_params(mut self, params: &[&str]) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        for param in params {
            param.hash(&mut hasher);
        }
        self.param_hash = Some(hasher.finish());
        self
    }

    /// Normalize query by removing literals and whitespace
    fn normalize_query(query: &str) -> String {
        let upper = query.to_uppercase();
        // Simple normalization - replace string literals and numbers
        let mut result = String::new();
        let mut in_string = false;
        let mut quote_char = ' ';

        for ch in upper.chars() {
            if in_string {
                if ch == quote_char {
                    in_string = false;
                    result.push('?');
                }
            } else if ch == '\'' || ch == '"' {
                in_string = true;
                quote_char = ch;
            } else if ch.is_numeric() {
                if !result.ends_with('?') {
                    result.push('?');
                }
            } else if ch.is_whitespace() {
                if !result.ends_with(' ') {
                    result.push(' ');
                }
            } else {
                result.push(ch);
            }
        }

        result.trim().to_string()
    }

    /// Extract table names from query
    fn extract_tables(query: &str) -> Vec<String> {
        let mut tables = Vec::new();
        let words: Vec<&str> = query.split_whitespace().collect();

        for (i, word) in words.iter().enumerate() {
            if *word == "FROM" || *word == "JOIN" || *word == "INTO" || *word == "UPDATE" {
                if let Some(table) = words.get(i + 1) {
                    let table_name = table.trim_matches(|c| c == '(' || c == ')' || c == ',');
                    if !table_name.is_empty() && !tables.contains(&table_name.to_string()) {
                        tables.push(table_name.to_string());
                    }
                }
            }
        }

        tables
    }

    /// Convert to bytes for storage
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(self.template.as_bytes());
        if let Some(hash) = self.param_hash {
            bytes.extend_from_slice(&hash.to_le_bytes());
        }
        bytes
    }
}

/// Session identifier for session affinity
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct SessionId(pub String);

impl SessionId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

/// Query context for cache decisions
#[derive(Debug, Clone)]
pub struct QueryContext {
    /// Session identifier
    pub session_id: SessionId,
    /// Workload hint (if explicitly specified)
    pub workload_hint: Option<WorkloadType>,
    /// Branch name (for branch-aware caching)
    pub branch: Option<String>,
    /// Time travel timestamp (for historical queries)
    pub as_of: Option<u64>,
    /// Whether this is a prepared statement
    pub is_prepared: bool,
    /// Request timestamp
    pub timestamp: Instant,
}

impl QueryContext {
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: SessionId::new(session_id),
            workload_hint: None,
            branch: None,
            as_of: None,
            is_prepared: false,
            timestamp: Instant::now(),
        }
    }

    pub fn with_workload_hint(mut self, hint: WorkloadType) -> Self {
        self.workload_hint = Some(hint);
        self
    }

    pub fn with_branch(mut self, branch: impl Into<String>) -> Self {
        self.branch = Some(branch.into());
        self
    }

    pub fn with_as_of(mut self, timestamp: u64) -> Self {
        self.as_of = Some(timestamp);
        self
    }
}

/// Helios-DistribCache - Main distributed cache instance
pub struct HeliosDistribCache {
    /// Workload classifier
    classifier: WorkloadClassifier,

    /// L1 hot cache (in-memory)
    l1_hot: Arc<HotCache>,

    /// L2 warm cache (compressed, in memory)
    l2_warm: Arc<WarmCache>,

    /// L3 distributed cache (mesh)
    l3_distributed: Arc<DistributedCache>,

    /// Predictive prefetcher
    prefetcher: Arc<PredictivePrefetcher>,

    /// WAL-based invalidator
    invalidator: Arc<WalInvalidator>,

    /// WAL processing task started by `start`
    wal_task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,

    /// Cache heatmap analytics
    heatmap: Arc<CacheHeatmap>,

    /// Workload scheduler
    scheduler: Arc<WorkloadScheduler>,

    /// AI/Agent caches
    conversation_cache: Arc<ConversationContextCache>,
    rag_cache: Arc<RagChunkCache>,
    tool_cache: Arc<ToolResultCache>,
    semantic_cache: Arc<SemanticQueryCache>,

    /// Configuration
    config: DistribCacheConfig,

    /// Statistics
    stats: CacheStatistics,
}

/// Cache statistics
#[derive(Debug, Default)]
struct CacheStatistics {
    total_lookups: AtomicU64,
    l1_hits: AtomicU64,
    l2_hits: AtomicU64,
    l3_hits: AtomicU64,
    total_misses: AtomicU64,
    time_saved_us: AtomicU64,
    queries_avoided: AtomicU64,
}

impl HeliosDistribCache {
    /// Create a new distributed cache instance
    pub fn new(config: DistribCacheConfig) -> Self {
        let l1_hot = Arc::new(HotCache::new(
            config.l1_size_mb * 1024 * 1024,
            config.l1_max_entry_size,
            config.l1_eviction_policy,
        ));

        let l2_warm = Arc::new(WarmCache::new(
            config.l2_size_gb * 1024 * 1024 * 1024,
            config.l2_compression,
        ));

        let l3_distributed = Arc::new(DistributedCache::new(
            config.l3_replication_factor,
            config.l3_peers.clone(),
        ));

        let classifier = WorkloadClassifier::new(config.clone());
        let prefetcher = Arc::new(PredictivePrefetcher::new(config.clone()));
        let invalidator = Arc::new(WalInvalidator::new(config.clone()));
        let heatmap = Arc::new(CacheHeatmap::with_window(
            config.heatmap_bucket_size,
            config.heatmap_retention,
        ));
        let scheduler = Arc::new(WorkloadScheduler::new(config.clone()));

        // WAL-streamed changes invalidate every enabled tier.
        {
            let l1 = l1_hot.clone();
            let l2 = config.l2_enabled.then(|| l2_warm.clone());
            let l3 = config.l3_enabled.then(|| l3_distributed.clone());
            invalidator.add_callback_sync(Arc::new(move |target: &InvalidationTarget| {
                l1.invalidate_by_table(&target.table);
                if let Some(l2) = &l2 {
                    l2.invalidate_by_table(&target.table);
                }
                if let Some(l3) = &l3 {
                    // Peer acknowledgements are awaited off the WAL loop;
                    // failures are counted in `l3_invalidation_failures`.
                    let l3 = l3.clone();
                    let table = target.table.clone();
                    tokio::spawn(async move {
                        let _report = l3.invalidate_table(&table).await;
                    });
                }
            }));
        }

        // AI caches
        let conversation_cache = Arc::new(ConversationContextCache::new(
            1000,
            config.max_conversation_turns,
        ));
        let rag_cache = Arc::new(RagChunkCache::new(config.l1_size_mb / 4));
        let tool_cache = Arc::new(ToolResultCache::new());
        let semantic_cache = Arc::new(SemanticQueryCache::new(
            config.semantic_similarity_threshold,
        ));

        Self {
            classifier,
            l1_hot,
            l2_warm,
            l3_distributed,
            prefetcher,
            invalidator,
            wal_task: parking_lot::Mutex::new(None),
            heatmap,
            scheduler,
            conversation_cache,
            rag_cache,
            tool_cache,
            semantic_cache,
            config,
            stats: CacheStatistics::default(),
        }
    }

    /// Get an entry from the cache (checking all tiers)
    pub async fn get(
        &self,
        fingerprint: &QueryFingerprint,
        context: &QueryContext,
    ) -> CacheResult<CacheEntry> {
        self.ensure_enabled()?;
        self.stats.total_lookups.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();

        // Classify workload for scheduling
        let _workload = self
            .classifier
            .classify_query(&fingerprint.template, context);

        // Check L1 hot cache first
        if let Some(entry) = self.l1_hot.get(fingerprint) {
            self.stats.l1_hits.fetch_add(1, Ordering::Relaxed);
            self.record_hit(fingerprint, CacheTier::L1, start.elapsed());
            return Ok(entry);
        }

        // Check L2 warm cache
        if self.config.l2_enabled {
            if let Some(entry) = self.l2_warm.get(fingerprint) {
                self.stats.l2_hits.fetch_add(1, Ordering::Relaxed);
                // Promote to L1 (an entry too large for L1 stays in L2 only)
                Self::promote(self.l1_hot.insert(fingerprint.clone(), entry.clone()), "L1");
                self.record_hit(fingerprint, CacheTier::L2, start.elapsed());
                return Ok(entry);
            }
        }

        // Check L3 distributed cache
        if self.config.l3_enabled {
            if let Some(entry) = self.l3_distributed.get(fingerprint).await {
                self.stats.l3_hits.fetch_add(1, Ordering::Relaxed);
                // Promote to L1 and L2
                Self::promote(self.l1_hot.insert(fingerprint.clone(), entry.clone()), "L1");
                if self.config.l2_enabled {
                    Self::promote(
                        self.l2_warm.insert(fingerprint.clone(), entry.clone()),
                        "L2",
                    );
                }
                self.record_hit(fingerprint, CacheTier::L3, start.elapsed());
                return Ok(entry);
            }
        }

        // Cache miss
        self.stats.total_misses.fetch_add(1, Ordering::Relaxed);
        if self.config.heatmap_enabled {
            self.heatmap
                .record_access(fingerprint, false, Duration::ZERO);
        }

        // Trigger prefetching for related queries
        if self.config.prefetch_enabled {
            self.prefetcher
                .predict_and_prefetch(fingerprint, &context.session_id);
        }

        Err(CacheError::Miss)
    }

    /// Insert an entry into the cache.
    ///
    /// Stores it in L1, in L2 when it is larger than 1 KiB with a TTL above
    /// 60 s, and in L3 for non-OLTP workloads. Fails when no tier accepted it
    /// (e.g. [`CacheError::TooLarge`] for L1 with L2 not applicable). L3
    /// replication is awaited per owner; owners that did not acknowledge are
    /// logged and counted in [`DistribCacheStats::l3_replication_failures`]
    /// (the local copy is still stored).
    pub async fn insert(
        &self,
        fingerprint: QueryFingerprint,
        entry: CacheEntry,
        context: &QueryContext,
    ) -> CacheResult<()> {
        self.ensure_enabled()?;
        let workload = self
            .classifier
            .classify_query(&fingerprint.template, context);
        let ttl = self.get_ttl_for_workload(workload);

        let entry = entry.with_ttl(ttl);

        // Insert into L1
        let l1_result = self.l1_hot.insert(fingerprint.clone(), entry.clone());
        let mut stored = l1_result.is_ok();

        // Insert into L2 if entry is large enough and TTL warrants it
        if self.config.l2_enabled && entry.size() > 1024 && ttl > Duration::from_secs(60) {
            match self.l2_warm.insert(fingerprint.clone(), entry.clone()) {
                Ok(()) => stored = true,
                Err(e) => tracing::debug!(error = %e, "entry not stored in L2"),
            }
        }

        // Insert into L3 for shared caching
        if self.config.l3_enabled && !matches!(workload, WorkloadType::OLTP) {
            let report = self
                .l3_distributed
                .insert(fingerprint.clone(), entry.clone())
                .await;
            stored |= report.stored_anywhere();
        }

        if !stored {
            return l1_result;
        }

        // Record for prefetcher learning
        if self.config.prefetch_enabled {
            self.prefetcher.record(&context.session_id, fingerprint);
        }

        Ok(())
    }

    /// Invalidate entries for a table in every enabled tier.
    ///
    /// L1 and L2 are invalidated locally; with L3 enabled the invalidation is
    /// also sent to every mesh peer and each acknowledgement awaited. Fails
    /// with [`CacheError::Invalidation`] naming the peers that did not
    /// acknowledge (they may serve stale entries until their TTL expires).
    pub async fn invalidate_table(&self, table: &str) -> CacheResult<()> {
        self.l1_hot.invalidate_by_table(table);
        if self.config.l2_enabled {
            self.l2_warm.invalidate_by_table(table);
        }
        if self.config.l3_enabled {
            Self::invalidation_result(self.l3_distributed.invalidate_table(table).await)?;
        }
        Ok(())
    }

    /// Invalidate a specific entry in every enabled tier. L3 behaves as in
    /// [`Self::invalidate_table`].
    pub async fn invalidate(&self, fingerprint: &QueryFingerprint) -> CacheResult<()> {
        self.l1_hot.invalidate(fingerprint);
        if self.config.l2_enabled {
            self.l2_warm.invalidate(fingerprint);
        }
        if self.config.l3_enabled {
            Self::invalidation_result(self.l3_distributed.invalidate(fingerprint).await)?;
        }
        Ok(())
    }

    fn invalidation_result(report: PeerOpReport) -> CacheResult<()> {
        if report.is_complete() {
            return Ok(());
        }
        let failed: Vec<String> = report
            .failed
            .iter()
            .map(|(addr, reason)| format!("{}: {}", addr, reason))
            .collect();
        Err(CacheError::Invalidation(format!(
            "L3 peers did not acknowledge the invalidation ({})",
            failed.join("; ")
        )))
    }

    fn ensure_enabled(&self) -> CacheResult<()> {
        if self.config.enabled {
            Ok(())
        } else {
            Err(CacheError::TierUnavailable(
                "distribcache is disabled (DistribCacheConfig::enabled = false)".to_string(),
            ))
        }
    }

    /// Log a tier promotion that did not happen (e.g. entry too large).
    fn promote(result: CacheResult<()>, tier: &str) {
        if let Err(e) = result {
            tracing::debug!(tier, error = %e, "cache promotion skipped");
        }
    }

    /// Get TTL based on workload type
    fn get_ttl_for_workload(&self, workload: WorkloadType) -> Duration {
        match workload {
            WorkloadType::OLTP => self.config.oltp_cache_ttl,
            WorkloadType::OLAP => self.config.olap_cache_ttl,
            WorkloadType::Vector => self.config.vector_cache_ttl,
            WorkloadType::AIAgent => self.config.ai_agent_cache_ttl,
            WorkloadType::RAG => self.config.rag_cache_ttl,
            WorkloadType::Mixed => self.config.default_cache_ttl,
        }
    }

    /// Record cache hit for metrics and heatmap. The time saved is an
    /// estimate: `estimated_backend_latency` minus the measured lookup time.
    fn record_hit(&self, fingerprint: &QueryFingerprint, _tier: CacheTier, latency: Duration) {
        let time_saved = self
            .config
            .estimated_backend_latency
            .saturating_sub(latency);

        self.stats
            .time_saved_us
            .fetch_add(time_saved.as_micros() as u64, Ordering::Relaxed);
        self.stats.queries_avoided.fetch_add(1, Ordering::Relaxed);
        if self.config.heatmap_enabled {
            self.heatmap.record_access(fingerprint, true, time_saved);
        }
    }

    /// The prefetch queue: predicted queries for the caller to execute and
    /// insert. Nothing in the cache runs them.
    pub fn prefetch_queue(&self) -> Arc<PrefetchQueue> {
        self.prefetcher.queue()
    }

    /// Get conversation context cache
    pub fn conversation_cache(&self) -> &ConversationContextCache {
        &self.conversation_cache
    }

    /// Get RAG chunk cache
    pub fn rag_cache(&self) -> &RagChunkCache {
        &self.rag_cache
    }

    /// Get tool result cache
    pub fn tool_cache(&self) -> &ToolResultCache {
        &self.tool_cache
    }

    /// Get semantic query cache
    pub fn semantic_cache(&self) -> &SemanticQueryCache {
        &self.semantic_cache
    }

    /// Get cache statistics
    pub fn stats(&self) -> DistribCacheStats {
        let total = self.stats.total_lookups.load(Ordering::Relaxed);
        let l1_hits = self.stats.l1_hits.load(Ordering::Relaxed);
        let l2_hits = self.stats.l2_hits.load(Ordering::Relaxed);
        let l3_hits = self.stats.l3_hits.load(Ordering::Relaxed);
        let _misses = self.stats.total_misses.load(Ordering::Relaxed);

        DistribCacheStats {
            l1: self.l1_hot.stats(),
            l2: self.l2_warm.stats(),
            l3: self.l3_distributed.stats(),
            overall_hit_ratio: if total > 0 {
                (l1_hits + l2_hits + l3_hits) as f64 / total as f64
            } else {
                0.0
            },
            estimated_time_saved_seconds: self.stats.time_saved_us.load(Ordering::Relaxed) as f64
                / 1_000_000.0,
            queries_avoided: self.stats.queries_avoided.load(Ordering::Relaxed),
            l3_replication_failures: self.l3_distributed.replication_failures(),
            l3_invalidation_failures: self.l3_distributed.invalidation_failures(),
        }
    }

    /// Generate heatmap data
    pub fn heatmap(&self) -> HeatmapData {
        self.heatmap.generate_heatmap()
    }

    /// Get workload distribution
    pub fn workload_distribution(&self) -> WorkloadDistribution {
        self.scheduler.get_distribution()
    }

    /// Start background services: in `InvalidationMode::Hybrid`, connect the
    /// WAL invalidator and spawn its processing task. Fails when Hybrid has
    /// no `wal_endpoint`, when TTL mode has one (it would be ignored), or
    /// when the WAL endpoint cannot be subscribed to.
    pub async fn start(&self) -> CacheResult<()> {
        match (self.config.invalidation_mode, &self.config.wal_endpoint) {
            (InvalidationMode::TTL, None) => Ok(()),
            (InvalidationMode::TTL, Some(endpoint)) => Err(CacheError::Configuration(format!(
                "wal_endpoint {} is set but invalidation_mode is TTL; use \
                 InvalidationMode::Hybrid to stream WAL invalidations",
                endpoint
            ))),
            (InvalidationMode::Hybrid, None) => Err(CacheError::Configuration(
                "invalidation_mode Hybrid requires a wal_endpoint".to_string(),
            )),
            (InvalidationMode::Hybrid, Some(endpoint)) => {
                self.invalidator.start(endpoint).await?;
                let mut task = self.wal_task.lock();
                if task.as_ref().is_none_or(|t| t.is_finished()) {
                    *task = Some(self.invalidator.start_processing());
                }
                Ok(())
            }
        }
    }

    /// Stop background services
    pub async fn stop(&self) -> CacheResult<()> {
        self.invalidator.stop().await;
        if let Some(task) = self.wal_task.lock().take() {
            task.abort();
        }
        Ok(())
    }

    /// Whether WAL-streamed invalidation is running (`false` in TTL mode and
    /// after the WAL stream broke).
    pub fn wal_invalidation_running(&self) -> bool {
        self.invalidator.is_running()
    }
}

/// Cache statistics snapshot
#[derive(Debug, Clone)]
pub struct DistribCacheStats {
    /// L1 tier stats
    pub l1: TierStats,
    /// L2 tier stats
    pub l2: TierStats,
    /// L3 tier stats
    pub l3: TierStats,
    /// Overall hit ratio
    pub overall_hit_ratio: f64,
    /// Estimated time saved in seconds: per hit,
    /// `DistribCacheConfig::estimated_backend_latency` minus the measured
    /// lookup time. An estimate, not a measurement of backend time.
    pub estimated_time_saved_seconds: f64,
    /// Total queries avoided
    pub queries_avoided: u64,
    /// L3 owner replications that were not acknowledged
    pub l3_replication_failures: u64,
    /// L3 peer invalidations that were not acknowledged
    pub l3_invalidation_failures: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_fingerprint() {
        let fp = QueryFingerprint::from_query("SELECT * FROM users WHERE id = 42");
        assert!(fp.template.contains("SELECT"));
        assert!(fp.template.contains("USERS"));
        assert!(fp.tables.contains(&"USERS".to_string()));
    }

    #[test]
    fn test_query_fingerprint_normalization() {
        let fp1 = QueryFingerprint::from_query("SELECT * FROM users WHERE id = 1");
        let fp2 = QueryFingerprint::from_query("SELECT * FROM users WHERE id = 2");
        // Both should have same template after normalization
        assert_eq!(fp1.template, fp2.template);
    }

    #[test]
    fn test_session_id() {
        let sid = SessionId::new("test-session");
        assert_eq!(sid.0, "test-session");
    }

    #[test]
    fn test_query_context() {
        let ctx = QueryContext::new("session-1")
            .with_workload_hint(WorkloadType::OLTP)
            .with_branch("feature-x");

        assert_eq!(ctx.session_id.0, "session-1");
        assert_eq!(ctx.workload_hint, Some(WorkloadType::OLTP));
        assert_eq!(ctx.branch, Some("feature-x".to_string()));
    }

    fn entry(table: &str) -> CacheEntry {
        CacheEntry::new(vec![1, 2, 3], vec![table.to_string()], 1)
    }

    /// A disabled cache refuses lookups and inserts explicitly.
    #[tokio::test]
    async fn test_disabled_cache_fails_closed() {
        let cache = HeliosDistribCache::new(DistribCacheConfig::builder().enabled(false).build());
        let ctx = QueryContext::new("s");
        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        assert!(matches!(
            cache.insert(fp.clone(), entry("users"), &ctx).await,
            Err(CacheError::TierUnavailable(_))
        ));
        assert!(matches!(
            cache.get(&fp, &ctx).await,
            Err(CacheError::TierUnavailable(_))
        ));
    }

    /// `start` rejects invalidation settings it cannot honour instead of
    /// silently running without WAL invalidation.
    #[tokio::test]
    async fn test_start_rejects_inconsistent_invalidation_config() {
        let hybrid_without_endpoint = HeliosDistribCache::new(DistribCacheConfig {
            invalidation_mode: InvalidationMode::Hybrid,
            ..Default::default()
        });
        assert!(matches!(
            hybrid_without_endpoint.start().await,
            Err(CacheError::Configuration(_))
        ));

        let ttl_with_endpoint = HeliosDistribCache::new(DistribCacheConfig {
            invalidation_mode: InvalidationMode::TTL,
            wal_endpoint: Some("127.0.0.1:1".to_string()),
            ..Default::default()
        });
        assert!(matches!(
            ttl_with_endpoint.start().await,
            Err(CacheError::Configuration(_))
        ));

        // TTL mode without an endpoint has nothing to start.
        let ttl = HeliosDistribCache::new(DistribCacheConfig::default());
        ttl.start().await.unwrap();
        assert!(!ttl.wal_invalidation_running());
    }

    /// An unreachable WAL endpoint is a start error, not a "degraded" run.
    #[tokio::test]
    async fn test_start_with_unreachable_wal_endpoint_fails() {
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = dead.local_addr().unwrap();
        drop(dead);
        let cache = HeliosDistribCache::new(
            DistribCacheConfig::builder()
                .wal_endpoint(addr.to_string())
                .build(),
        );
        assert!(matches!(
            cache.start().await,
            Err(CacheError::ConnectionError(_))
        ));
        assert!(!cache.wal_invalidation_running());
    }

    /// Table invalidation reaches L3 and reports a peer that missed it.
    #[tokio::test]
    async fn test_invalidate_table_reports_unacknowledged_l3_peer() {
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = dead.local_addr().unwrap();
        drop(dead);
        let cache = HeliosDistribCache::new(
            DistribCacheConfig::builder()
                .l3_enabled(true)
                .l3_replication_factor(1)
                .l3_peers(vec![addr])
                .build(),
        );
        let ctx = QueryContext::new("s");
        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        cache
            .insert(fp.clone(), entry("users"), &ctx)
            .await
            .unwrap();

        let err = cache.invalidate_table("users").await.unwrap_err();
        assert!(matches!(err, CacheError::Invalidation(_)), "got {err:?}");
        // Local tiers were still invalidated.
        assert!(matches!(cache.get(&fp, &ctx).await, Err(CacheError::Miss)));
        assert_eq!(cache.stats().l3_invalidation_failures, 1);
    }

    /// The time-saved figure is the configured backend latency minus the
    /// measured lookup time, never more than the configured latency.
    #[tokio::test]
    async fn test_estimated_time_saved_uses_configured_latency() {
        let cache = HeliosDistribCache::new(
            DistribCacheConfig::builder()
                .estimated_backend_latency(Duration::from_millis(2))
                .build(),
        );
        let ctx = QueryContext::new("s");
        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        cache
            .insert(fp.clone(), entry("users"), &ctx)
            .await
            .unwrap();
        cache.get(&fp, &ctx).await.unwrap();

        let stats = cache.stats();
        assert_eq!(stats.queries_avoided, 1);
        assert!(stats.estimated_time_saved_seconds <= 0.002);
    }

    /// WAL-streamed changes reach the tiers through the processing task.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_wal_stream_invalidates_tiers() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let (mut sock, _) = listener.accept().unwrap();
            let mut req = [0u8; 13];
            sock.read_exact(&mut req).unwrap();
            sock.write_all(&[0x04, 0, 0, 0, 0]).unwrap(); // SubscribeAck
            go_rx.recv().unwrap();
            // Schema change for "users": [len: u16]["users"]
            let mut payload = Vec::new();
            payload.extend_from_slice(&1u64.to_be_bytes());
            payload.extend_from_slice(&0u64.to_be_bytes());
            payload.push(0x04);
            payload.extend_from_slice(&5u16.to_be_bytes());
            payload.extend_from_slice(b"users");
            let mut frame = vec![0x01];
            frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            frame.extend_from_slice(&payload);
            sock.write_all(&frame).unwrap();
            std::thread::sleep(Duration::from_secs(2));
        });

        let cache = HeliosDistribCache::new(
            DistribCacheConfig::builder()
                .wal_endpoint(addr.to_string())
                .build(),
        );
        let ctx = QueryContext::new("s");
        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        cache
            .insert(fp.clone(), entry("users"), &ctx)
            .await
            .unwrap();
        cache.start().await.unwrap();
        assert!(cache.wal_invalidation_running());
        go_tx.send(()).unwrap();

        let mut invalidated = false;
        for _ in 0..200 {
            if !cache.l1_hot.contains(&fp) {
                invalidated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(invalidated, "WAL schema change did not invalidate L1");
        cache.stop().await.unwrap();
    }
}
