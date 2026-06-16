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
//! │  │ L1: Hot Cache (In-Memory, <100μs)                         │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! │                          │ miss                                  │
//! │  ┌───────────────────────────────────────────────────────────┐  │
//! │  │ L2: Warm Cache (Local SSD, <1ms)                          │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! │                          │ miss                                  │
//! │  ┌───────────────────────────────────────────────────────────┐  │
//! │  │ L3: Distributed Cache (Mesh, <10ms)                       │  │
//! │  └───────────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Features
//!
//! - **Multi-Tier Caching**: L1 hot (memory), L2 warm (SSD), L3 distributed (mesh)
//! - **Workload Classification**: OLTP, OLAP, Vector, AI Agent, RAG pipelines
//! - **Intelligent Prefetching**: Pattern-based and temporal prediction
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
    SemanticIndexConfig,
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
    TierStats, WarmCache,
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

    /// L2 warm cache (SSD)
    l2_warm: Arc<WarmCache>,

    /// L3 distributed cache (mesh)
    l3_distributed: Arc<DistributedCache>,

    /// Predictive prefetcher
    prefetcher: Arc<PredictivePrefetcher>,

    /// WAL-based invalidator
    invalidator: Arc<WalInvalidator>,

    /// Cache heatmap analytics
    heatmap: Arc<CacheHeatmap>,

    /// Workload scheduler
    scheduler: Arc<WorkloadScheduler>,

    /// AI/Agent caches
    conversation_cache: Arc<ConversationContextCache>,
    rag_cache: Arc<RagChunkCache>,
    tool_cache: Arc<ToolResultCache>,
    semantic_cache: Arc<SemanticQueryCache>,

    /// Metrics
    #[allow(dead_code)]
    metrics: Arc<DistribCacheMetrics>,

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
            config.l2_path.clone(),
            config.l2_compression,
        ));

        let l3_distributed = Arc::new(DistributedCache::new(
            config.l3_replication_factor,
            config.l3_peers.clone(),
        ));

        let classifier = WorkloadClassifier::new(config.clone());
        let prefetcher = Arc::new(PredictivePrefetcher::new(config.clone()));
        let invalidator = Arc::new(WalInvalidator::new(config.clone()));
        let heatmap = Arc::new(CacheHeatmap::new());
        let scheduler = Arc::new(WorkloadScheduler::new(config.clone()));
        let metrics = Arc::new(DistribCacheMetrics::new());

        // AI caches
        let conversation_cache = Arc::new(ConversationContextCache::new(1000, 50));
        let rag_cache = Arc::new(RagChunkCache::new(config.l1_size_mb / 4));
        let tool_cache = Arc::new(ToolResultCache::new());
        let semantic_cache = Arc::new(SemanticQueryCache::new(0.85));

        Self {
            classifier,
            l1_hot,
            l2_warm,
            l3_distributed,
            prefetcher,
            invalidator,
            heatmap,
            scheduler,
            conversation_cache,
            rag_cache,
            tool_cache,
            semantic_cache,
            metrics,
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
        self.stats.total_lookups.fetch_add(1, Ordering::Relaxed);
        let start = Instant::now();

        // Classify workload for scheduling
        let _workload = self
            .classifier
            .classify_query(&fingerprint.template, context);

        // Check L1 hot cache first
        if let Some(entry) = self.l1_hot.get(fingerprint, context.session_id.clone()) {
            self.stats.l1_hits.fetch_add(1, Ordering::Relaxed);
            self.record_hit(fingerprint, CacheTier::L1, start.elapsed());
            return Ok(entry);
        }

        // Check L2 warm cache
        if self.config.l2_enabled {
            if let Some(entry) = self.l2_warm.get(fingerprint) {
                self.stats.l2_hits.fetch_add(1, Ordering::Relaxed);
                // Promote to L1
                self.l1_hot.insert(
                    fingerprint.clone(),
                    entry.clone(),
                    Some(context.session_id.clone()),
                );
                self.record_hit(fingerprint, CacheTier::L2, start.elapsed());
                return Ok(entry);
            }
        }

        // Check L3 distributed cache
        if self.config.l3_enabled {
            if let Some(entry) = self.l3_distributed.get(fingerprint).await {
                self.stats.l3_hits.fetch_add(1, Ordering::Relaxed);
                // Promote to L1 and L2
                self.l1_hot.insert(
                    fingerprint.clone(),
                    entry.clone(),
                    Some(context.session_id.clone()),
                );
                if self.config.l2_enabled {
                    self.l2_warm.insert(fingerprint.clone(), entry.clone());
                }
                self.record_hit(fingerprint, CacheTier::L3, start.elapsed());
                return Ok(entry);
            }
        }

        // Cache miss
        self.stats.total_misses.fetch_add(1, Ordering::Relaxed);
        self.heatmap
            .record_access(fingerprint, false, Duration::ZERO);

        // Trigger prefetching for related queries
        if self.config.prefetch_enabled {
            self.prefetcher
                .predict_and_prefetch(fingerprint, &context.session_id);
        }

        Err(CacheError::Miss)
    }

    /// Insert an entry into the cache
    pub async fn insert(
        &self,
        fingerprint: QueryFingerprint,
        entry: CacheEntry,
        context: &QueryContext,
    ) -> CacheResult<()> {
        let workload = self
            .classifier
            .classify_query(&fingerprint.template, context);
        let ttl = self.get_ttl_for_workload(workload);

        let entry = entry.with_ttl(ttl);

        // Insert into L1
        self.l1_hot.insert(
            fingerprint.clone(),
            entry.clone(),
            Some(context.session_id.clone()),
        );

        // Insert into L2 if entry is large enough and TTL warrants it
        if self.config.l2_enabled && entry.size() > 1024 && ttl > Duration::from_secs(60) {
            self.l2_warm.insert(fingerprint.clone(), entry.clone());
        }

        // Insert into L3 for shared caching
        if self.config.l3_enabled && !matches!(workload, WorkloadType::OLTP) {
            self.l3_distributed
                .insert(fingerprint.clone(), entry.clone())
                .await;
        }

        // Record for prefetcher learning
        if self.config.prefetch_enabled {
            self.prefetcher.record(&context.session_id, fingerprint);
        }

        Ok(())
    }

    /// Invalidate entries for a table
    pub fn invalidate_table(&self, table: &str) {
        self.l1_hot.invalidate_by_table(table);
        if self.config.l2_enabled {
            self.l2_warm.invalidate_by_table(table);
        }
        // L3 invalidation is handled by gossip protocol
    }

    /// Invalidate a specific entry
    pub fn invalidate(&self, fingerprint: &QueryFingerprint) {
        self.l1_hot.invalidate(fingerprint);
        if self.config.l2_enabled {
            self.l2_warm.invalidate(fingerprint);
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

    /// Record cache hit for metrics and heatmap
    fn record_hit(&self, fingerprint: &QueryFingerprint, tier: CacheTier, _latency: Duration) {
        let time_saved = match tier {
            CacheTier::L1 => Duration::from_millis(10), // Assume 10ms DB query
            CacheTier::L2 => Duration::from_millis(9),
            CacheTier::L3 => Duration::from_millis(5),
        };

        self.stats
            .time_saved_us
            .fetch_add(time_saved.as_micros() as u64, Ordering::Relaxed);
        self.stats.queries_avoided.fetch_add(1, Ordering::Relaxed);
        self.heatmap.record_access(fingerprint, true, time_saved);
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
            time_saved_seconds: self.stats.time_saved_us.load(Ordering::Relaxed) as f64
                / 1_000_000.0,
            queries_avoided: self.stats.queries_avoided.load(Ordering::Relaxed),
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

    /// Start background services (prefetcher, invalidator)
    pub async fn start(&self) -> CacheResult<()> {
        // Start WAL invalidator if configured
        if let Some(wal_endpoint) = &self.config.wal_endpoint {
            self.invalidator.start(wal_endpoint).await?;
        }

        // Start prefetcher background worker
        if self.config.prefetch_enabled {
            self.prefetcher.start().await;
        }

        Ok(())
    }

    /// Stop background services
    pub async fn stop(&self) -> CacheResult<()> {
        self.invalidator.stop().await;
        self.prefetcher.stop().await;
        Ok(())
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
    /// Total time saved in seconds
    pub time_saved_seconds: f64,
    /// Total queries avoided
    pub queries_avoided: u64,
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
}
