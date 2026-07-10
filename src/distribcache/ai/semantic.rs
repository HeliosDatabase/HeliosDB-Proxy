//! Semantic query cache
//!
//! Caches query results based on semantic similarity of queries.
//! Uses embedding-based lookup to find similar queries and return cached results.
//!
//! # Branch-Aware Caching
//!
//! The cache supports branch-aware lookups for time-travel queries:
//! - Entries can be scoped to specific branches
//! - Lookups can filter by branch context
//! - Cross-branch semantic search is supported
//!
//! # AI/Agent Optimizations
//!
//! - Session affinity for agent conversations
//! - Workload-aware TTL adjustments
//! - RAG-specific caching strategies

use dashmap::DashMap;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Vector ID for semantic indexing
pub type VectorId = u64;

/// Branch identifier for branch-aware caching
pub type BranchId = String;

/// Session identifier for agent sessions
pub type SessionId = String;

/// Branch context for cache entries
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BranchContext {
    /// Branch name (e.g., "main", "feature-x")
    pub branch: BranchId,
    /// Optional snapshot timestamp for time-travel queries
    pub snapshot_at: Option<u64>,
}

impl BranchContext {
    /// Create a new branch context
    pub fn new(branch: impl Into<String>) -> Self {
        Self {
            branch: branch.into(),
            snapshot_at: None,
        }
    }

    /// Create branch context with snapshot time
    pub fn with_snapshot(branch: impl Into<String>, snapshot: u64) -> Self {
        Self {
            branch: branch.into(),
            snapshot_at: Some(snapshot),
        }
    }

    /// Main branch context
    pub fn main() -> Self {
        Self::new("main")
    }

    /// Check if this context is compatible with another (for cache hits)
    pub fn is_compatible(&self, other: &BranchContext) -> bool {
        if self.branch != other.branch {
            return false;
        }
        // Snapshot compatibility: either both None, or entry snapshot <= query snapshot
        match (self.snapshot_at, other.snapshot_at) {
            (None, None) => true,
            (Some(entry_snap), Some(query_snap)) => entry_snap <= query_snap,
            (None, Some(_)) => true,  // Current data valid for any snapshot
            (Some(_), None) => false, // Historical entry not valid for current query
        }
    }
}

impl Default for BranchContext {
    fn default() -> Self {
        Self::main()
    }
}

/// AI workload context for cache optimization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AIWorkloadContext {
    /// RAG retrieval phase - fast, high-throughput
    RAGRetrieval,
    /// RAG generation phase - slower, lower frequency
    RAGGeneration,
    /// Agent conversation - session-aware
    AgentConversation,
    /// Tool call caching - deterministic
    ToolResult,
    /// General semantic query
    #[default]
    General,
}

/// Query embedding vector
pub type Embedding = Vec<f32>;

/// Semantic query entry with branch and session awareness
#[derive(Debug, Clone)]
pub struct SemanticEntry {
    /// Entry ID
    pub id: VectorId,
    /// Original query
    pub query: String,
    /// Query embedding
    pub embedding: Embedding,
    /// Cached result
    pub result: serde_json::Value,
    /// Creation time
    pub created_at: Instant,
    /// TTL
    pub ttl: Duration,
    /// Access count
    pub access_count: u64,
    /// Branch context (for branch-aware caching)
    pub branch_context: Option<BranchContext>,
    /// Session ID (for agent conversation affinity)
    pub session_id: Option<SessionId>,
    /// AI workload type
    pub workload: AIWorkloadContext,
    /// Tables referenced by this query (for invalidation)
    pub tables: Vec<String>,
}

impl SemanticEntry {
    /// Create a new semantic entry
    pub fn new(
        id: VectorId,
        query: impl Into<String>,
        embedding: Embedding,
        result: serde_json::Value,
    ) -> Self {
        Self {
            id,
            query: query.into(),
            embedding,
            result,
            created_at: Instant::now(),
            ttl: Duration::from_secs(3600), // Default 1 hour
            access_count: 0,
            branch_context: None,
            session_id: None,
            workload: AIWorkloadContext::default(),
            tables: Vec::new(),
        }
    }

    /// Set TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Set branch context
    pub fn with_branch(mut self, branch: BranchContext) -> Self {
        self.branch_context = Some(branch);
        self
    }

    /// Set session ID for agent affinity
    pub fn with_session(mut self, session: impl Into<String>) -> Self {
        self.session_id = Some(session.into());
        self
    }

    /// Set AI workload context
    pub fn with_workload(mut self, workload: AIWorkloadContext) -> Self {
        self.workload = workload;
        self
    }

    /// Set referenced tables for invalidation tracking
    pub fn with_tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    /// Get workload-adjusted TTL
    pub fn workload_ttl(&self) -> Duration {
        match self.workload {
            AIWorkloadContext::RAGRetrieval => Duration::from_secs(300), // 5 min - fast refresh
            AIWorkloadContext::RAGGeneration => Duration::from_secs(1800), // 30 min - slower refresh
            AIWorkloadContext::AgentConversation => Duration::from_secs(3600), // 1 hour - session lifetime
            AIWorkloadContext::ToolResult => Duration::from_secs(86400), // 24 hours - deterministic
            AIWorkloadContext::General => self.ttl,
        }
    }

    /// Check if expired (considering workload-adjusted TTL)
    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > self.workload_ttl()
    }

    /// Check if entry matches branch context
    pub fn matches_branch(&self, query_branch: &BranchContext) -> bool {
        match &self.branch_context {
            None => true, // No branch restriction
            Some(entry_branch) => entry_branch.is_compatible(query_branch),
        }
    }

    /// Check if entry belongs to session
    pub fn matches_session(&self, session: &SessionId) -> bool {
        match &self.session_id {
            None => true,
            Some(entry_session) => entry_session == session,
        }
    }

    /// Approximate size in bytes
    pub fn size(&self) -> usize {
        self.query.len()
            + self.embedding.len() * 4
            + self.result.to_string().len()
            + self.tables.iter().map(|t| t.len()).sum::<usize>()
            + self.session_id.as_ref().map(|s| s.len()).unwrap_or(0)
            + self
                .branch_context
                .as_ref()
                .map(|b| b.branch.len() + 8)
                .unwrap_or(0)
            + 96
    }
}

/// Similarity search result
#[derive(Debug, Clone)]
pub struct SimilarityResult {
    /// Entry ID
    pub id: VectorId,
    /// Similarity score (0.0 - 1.0)
    pub similarity: f32,
    /// The entry
    pub entry: SemanticEntry,
}

impl PartialEq for SimilarityResult {
    fn eq(&self, other: &Self) -> bool {
        self.similarity == other.similarity
    }
}

impl Eq for SimilarityResult {}

impl PartialOrd for SimilarityResult {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SimilarityResult {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap (we want highest similarity)
        other
            .similarity
            .partial_cmp(&self.similarity)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Simple HNSW-like index for semantic search
/// (Simplified implementation - production would use a proper HNSW library)
pub struct SemanticIndex {
    /// All vectors with their IDs
    vectors: DashMap<VectorId, Embedding>,

    /// Index configuration
    #[allow(dead_code)]
    config: SemanticIndexConfig,

    /// Next ID
    next_id: AtomicU64,
}

/// Semantic index configuration
#[derive(Debug, Clone)]
pub struct SemanticIndexConfig {
    /// Maximum connections per node (M parameter)
    pub max_connections: usize,
    /// Search expansion factor (ef parameter)
    pub ef_search: usize,
    /// Embedding dimension
    pub dimension: usize,
}

impl Default for SemanticIndexConfig {
    fn default() -> Self {
        Self {
            max_connections: 16,
            ef_search: 100,
            dimension: 384, // Common embedding size
        }
    }
}

impl SemanticIndex {
    /// Create a new semantic index
    pub fn new(config: SemanticIndexConfig) -> Self {
        Self {
            vectors: DashMap::new(),
            config,
            next_id: AtomicU64::new(1),
        }
    }

    /// Insert a vector and return its ID
    pub fn insert(&self, embedding: Embedding) -> VectorId {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.vectors.insert(id, embedding);
        id
    }

    /// Remove a vector
    pub fn remove(&self, id: VectorId) {
        self.vectors.remove(&id);
    }

    /// Search for k nearest neighbors
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(VectorId, f32)> {
        // Brute force search (production would use HNSW)
        let mut heap: BinaryHeap<(std::cmp::Reverse<i64>, VectorId)> = BinaryHeap::new();

        for entry in self.vectors.iter() {
            let similarity = cosine_similarity(query, entry.value());
            // Convert to integer for ordering (multiply by 1M for precision)
            let sim_int = (similarity * 1_000_000.0) as i64;
            heap.push((std::cmp::Reverse(sim_int), *entry.key()));

            if heap.len() > k {
                heap.pop();
            }
        }

        // Extract results in descending similarity order
        let mut results: Vec<_> = heap
            .into_iter()
            .map(|(std::cmp::Reverse(sim), id)| (id, sim as f32 / 1_000_000.0))
            .collect();

        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Get vector count
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Clear all vectors
    pub fn clear(&self) {
        self.vectors.clear();
    }
}

/// Compute cosine similarity between two vectors
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;

    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    let denominator = (norm_a * norm_b).sqrt();
    if denominator == 0.0 {
        0.0
    } else {
        dot / denominator
    }
}

/// Semantic query cache
pub struct SemanticQueryCache {
    /// Query index
    index: SemanticIndex,

    /// Cached entries
    entries: DashMap<VectorId, SemanticEntry>,

    /// Similarity threshold for cache hit
    threshold: f32,

    /// Maximum entries
    max_entries: usize,

    /// Statistics
    stats: SemanticCacheStats,
}

/// Semantic cache statistics
#[derive(Debug, Default)]
struct SemanticCacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    semantic_hits: AtomicU64,
    exact_hits: AtomicU64,
    insertions: AtomicU64,
    evictions: AtomicU64,
}

impl SemanticQueryCache {
    /// Create a new semantic query cache with default max entries
    pub fn new(threshold: f32) -> Self {
        Self::with_capacity(threshold, 10000)
    }

    /// Create a new semantic query cache with specified capacity
    pub fn with_capacity(threshold: f32, max_entries: usize) -> Self {
        Self {
            index: SemanticIndex::new(SemanticIndexConfig::default()),
            entries: DashMap::new(),
            threshold,
            max_entries,
            stats: SemanticCacheStats::default(),
        }
    }

    /// Create with custom index config
    pub fn with_config(
        threshold: f32,
        max_entries: usize,
        index_config: SemanticIndexConfig,
    ) -> Self {
        Self {
            index: SemanticIndex::new(index_config),
            entries: DashMap::new(),
            threshold,
            max_entries,
            stats: SemanticCacheStats::default(),
        }
    }

    /// Lookup by semantic similarity
    pub fn lookup(&self, embedding: &[f32]) -> Option<SimilarityResult> {
        // Search for nearest neighbor
        let results = self.index.search(embedding, 1);

        if let Some((id, similarity)) = results.first() {
            if *similarity >= self.threshold {
                if let Some(entry) = self.entries.get(id) {
                    if !entry.is_expired() {
                        self.stats.hits.fetch_add(1, Ordering::Relaxed);

                        if *similarity > 0.999 {
                            self.stats.exact_hits.fetch_add(1, Ordering::Relaxed);
                        } else {
                            self.stats.semantic_hits.fetch_add(1, Ordering::Relaxed);
                        }

                        return Some(SimilarityResult {
                            id: *id,
                            similarity: *similarity,
                            entry: entry.clone(),
                        });
                    } else {
                        // Remove expired entry
                        drop(entry);
                        self.remove(*id);
                    }
                }
            }
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Lookup with custom threshold
    pub fn lookup_with_threshold(
        &self,
        embedding: &[f32],
        threshold: f32,
    ) -> Option<SimilarityResult> {
        let results = self.index.search(embedding, 1);

        if let Some((id, similarity)) = results.first() {
            if *similarity >= threshold {
                if let Some(entry) = self.entries.get(id) {
                    if !entry.is_expired() {
                        return Some(SimilarityResult {
                            id: *id,
                            similarity: *similarity,
                            entry: entry.clone(),
                        });
                    }
                }
            }
        }

        None
    }

    /// Find k most similar queries
    pub fn find_similar(&self, embedding: &[f32], k: usize) -> Vec<SimilarityResult> {
        let results = self.index.search(embedding, k);

        results
            .into_iter()
            .filter_map(|(id, similarity)| {
                self.entries.get(&id).and_then(|entry| {
                    if !entry.is_expired() {
                        Some(SimilarityResult {
                            id,
                            similarity,
                            entry: entry.clone(),
                        })
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    /// Lookup with branch context filtering
    ///
    /// Returns cached entry only if it's compatible with the given branch context.
    /// This enables branch-aware caching for time-travel queries.
    pub fn lookup_with_branch(
        &self,
        embedding: &[f32],
        branch: &BranchContext,
    ) -> Option<SimilarityResult> {
        // Search for multiple candidates to filter by branch
        let results = self.index.search(embedding, 10);

        for (id, similarity) in results {
            if similarity < self.threshold {
                break; // Results are sorted by similarity
            }

            if let Some(entry) = self.entries.get(&id) {
                if !entry.is_expired() && entry.matches_branch(branch) {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    if similarity > 0.999 {
                        self.stats.exact_hits.fetch_add(1, Ordering::Relaxed);
                    } else {
                        self.stats.semantic_hits.fetch_add(1, Ordering::Relaxed);
                    }

                    return Some(SimilarityResult {
                        id,
                        similarity,
                        entry: entry.clone(),
                    });
                }
            }
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Lookup with session affinity for agent conversations
    ///
    /// Prioritizes entries from the same session for better conversation context.
    pub fn lookup_with_session(
        &self,
        embedding: &[f32],
        session: &SessionId,
    ) -> Option<SimilarityResult> {
        let results = self.index.search(embedding, 20);

        // First pass: look for same-session matches
        for (id, similarity) in &results {
            if *similarity < self.threshold {
                break;
            }

            if let Some(entry) = self.entries.get(id) {
                if !entry.is_expired() && entry.matches_session(session) {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    self.stats.semantic_hits.fetch_add(1, Ordering::Relaxed);

                    return Some(SimilarityResult {
                        id: *id,
                        similarity: *similarity,
                        entry: entry.clone(),
                    });
                }
            }
        }

        // Second pass: any matching entry (cross-session)
        for (id, similarity) in &results {
            if *similarity < self.threshold {
                break;
            }

            if let Some(entry) = self.entries.get(id) {
                if !entry.is_expired() {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    self.stats.semantic_hits.fetch_add(1, Ordering::Relaxed);

                    return Some(SimilarityResult {
                        id: *id,
                        similarity: *similarity,
                        entry: entry.clone(),
                    });
                }
            }
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Lookup with full AI context (branch + session + workload)
    ///
    /// Most comprehensive lookup that considers:
    /// - Branch context for time-travel
    /// - Session affinity for agent conversations
    /// - Workload type for TTL and priority
    pub fn lookup_with_context(
        &self,
        embedding: &[f32],
        branch: Option<&BranchContext>,
        session: Option<&SessionId>,
        workload: AIWorkloadContext,
    ) -> Option<SimilarityResult> {
        let results = self.index.search(embedding, 20);

        // Priority 1: Same session + same branch + same workload
        for (id, similarity) in &results {
            if *similarity < self.threshold {
                break;
            }

            if let Some(entry) = self.entries.get(id) {
                let branch_match = branch.map(|b| entry.matches_branch(b)).unwrap_or(true);
                let session_match = session.map(|s| entry.matches_session(s)).unwrap_or(false);
                let workload_match = entry.workload == workload;

                if !entry.is_expired() && branch_match && session_match && workload_match {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    self.stats.semantic_hits.fetch_add(1, Ordering::Relaxed);
                    return Some(SimilarityResult {
                        id: *id,
                        similarity: *similarity,
                        entry: entry.clone(),
                    });
                }
            }
        }

        // Priority 2: Same branch + same workload
        for (id, similarity) in &results {
            if *similarity < self.threshold {
                break;
            }

            if let Some(entry) = self.entries.get(id) {
                let branch_match = branch.map(|b| entry.matches_branch(b)).unwrap_or(true);
                let workload_match = entry.workload == workload;

                if !entry.is_expired() && branch_match && workload_match {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    self.stats.semantic_hits.fetch_add(1, Ordering::Relaxed);
                    return Some(SimilarityResult {
                        id: *id,
                        similarity: *similarity,
                        entry: entry.clone(),
                    });
                }
            }
        }

        // Priority 3: Same branch only
        for (id, similarity) in &results {
            if *similarity < self.threshold {
                break;
            }

            if let Some(entry) = self.entries.get(id) {
                let branch_match = branch.map(|b| entry.matches_branch(b)).unwrap_or(true);

                if !entry.is_expired() && branch_match {
                    self.stats.hits.fetch_add(1, Ordering::Relaxed);
                    self.stats.semantic_hits.fetch_add(1, Ordering::Relaxed);
                    return Some(SimilarityResult {
                        id: *id,
                        similarity: *similarity,
                        entry: entry.clone(),
                    });
                }
            }
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Find similar entries within a branch
    pub fn find_similar_in_branch(
        &self,
        embedding: &[f32],
        branch: &BranchContext,
        k: usize,
    ) -> Vec<SimilarityResult> {
        // Search for more candidates since we'll filter
        let results = self.index.search(embedding, k * 3);

        results
            .into_iter()
            .filter_map(|(id, similarity)| {
                self.entries.get(&id).and_then(|entry| {
                    if !entry.is_expired() && entry.matches_branch(branch) {
                        Some(SimilarityResult {
                            id,
                            similarity,
                            entry: entry.clone(),
                        })
                    } else {
                        None
                    }
                })
            })
            .take(k)
            .collect()
    }

    /// Invalidate entries by table name
    ///
    /// Used when WAL invalidation detects changes to a table.
    pub fn invalidate_by_table(&self, table: &str) -> usize {
        let to_remove: Vec<_> = self
            .entries
            .iter()
            .filter(|e| e.tables.iter().any(|t| t == table))
            .map(|e| *e.key())
            .collect();

        let count = to_remove.len();
        for id in to_remove {
            self.remove(id);
        }
        count
    }

    /// Invalidate entries by branch
    pub fn invalidate_branch(&self, branch: &BranchId) -> usize {
        let to_remove: Vec<_> = self
            .entries
            .iter()
            .filter(|e| {
                e.branch_context
                    .as_ref()
                    .map(|b| &b.branch == branch)
                    .unwrap_or(false)
            })
            .map(|e| *e.key())
            .collect();

        let count = to_remove.len();
        for id in to_remove {
            self.remove(id);
        }
        count
    }

    /// Insert a new entry
    pub fn insert(
        &self,
        query: impl Into<String>,
        embedding: Embedding,
        result: serde_json::Value,
    ) -> VectorId {
        // Evict if at capacity
        while self.entries.len() >= self.max_entries {
            self.evict_one();
        }

        // Insert into index
        let id = self.index.insert(embedding.clone());

        // Create and store entry
        let entry = SemanticEntry::new(id, query, embedding, result);
        self.entries.insert(id, entry);

        self.stats.insertions.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Insert with TTL
    pub fn insert_with_ttl(
        &self,
        query: impl Into<String>,
        embedding: Embedding,
        result: serde_json::Value,
        ttl: Duration,
    ) -> VectorId {
        while self.entries.len() >= self.max_entries {
            self.evict_one();
        }

        let id = self.index.insert(embedding.clone());
        let entry = SemanticEntry::new(id, query, embedding, result).with_ttl(ttl);
        self.entries.insert(id, entry);

        self.stats.insertions.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Insert with full AI context (branch, session, workload, tables)
    ///
    /// This is the recommended insertion method for AI/Agent workloads
    /// as it enables branch-aware caching, session affinity, and
    /// workload-specific TTL management.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_with_context(
        &self,
        query: impl Into<String>,
        embedding: Embedding,
        result: serde_json::Value,
        branch: Option<BranchContext>,
        session: Option<SessionId>,
        workload: AIWorkloadContext,
        tables: Vec<String>,
    ) -> VectorId {
        while self.entries.len() >= self.max_entries {
            self.evict_one();
        }

        let id = self.index.insert(embedding.clone());
        let mut entry = SemanticEntry::new(id, query, embedding, result)
            .with_workload(workload)
            .with_tables(tables);

        if let Some(b) = branch {
            entry = entry.with_branch(b);
        }
        if let Some(s) = session {
            entry = entry.with_session(s);
        }

        self.entries.insert(id, entry);
        self.stats.insertions.fetch_add(1, Ordering::Relaxed);
        id
    }

    /// Insert for RAG retrieval workload
    ///
    /// Optimized TTL for fast-refresh retrieval phase.
    pub fn insert_rag_retrieval(
        &self,
        query: impl Into<String>,
        embedding: Embedding,
        result: serde_json::Value,
        tables: Vec<String>,
    ) -> VectorId {
        self.insert_with_context(
            query,
            embedding,
            result,
            None,
            None,
            AIWorkloadContext::RAGRetrieval,
            tables,
        )
    }

    /// Insert for agent conversation
    ///
    /// Session-aware with longer TTL for conversation context.
    pub fn insert_agent_response(
        &self,
        query: impl Into<String>,
        embedding: Embedding,
        result: serde_json::Value,
        session: SessionId,
        branch: Option<BranchContext>,
    ) -> VectorId {
        self.insert_with_context(
            query,
            embedding,
            result,
            branch,
            Some(session),
            AIWorkloadContext::AgentConversation,
            Vec::new(),
        )
    }

    /// Insert deterministic tool result
    ///
    /// Long TTL for deterministic tool calls (e.g., math, date formatting).
    pub fn insert_tool_result(
        &self,
        query: impl Into<String>,
        embedding: Embedding,
        result: serde_json::Value,
    ) -> VectorId {
        self.insert_with_context(
            query,
            embedding,
            result,
            None,
            None,
            AIWorkloadContext::ToolResult,
            Vec::new(),
        )
    }

    /// Remove an entry
    pub fn remove(&self, id: VectorId) {
        self.index.remove(id);
        self.entries.remove(&id);
    }

    /// Evict one entry (oldest by creation time)
    fn evict_one(&self) {
        let mut oldest_id = None;
        let mut oldest_time = Instant::now();

        for entry in self.entries.iter() {
            if entry.created_at < oldest_time {
                oldest_time = entry.created_at;
                oldest_id = Some(*entry.key());
            }
        }

        if let Some(id) = oldest_id {
            self.remove(id);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Remove expired entries
    pub fn cleanup_expired(&self) {
        let expired: Vec<_> = self
            .entries
            .iter()
            .filter(|e| e.is_expired())
            .map(|e| *e.key())
            .collect();

        for id in expired {
            self.remove(id);
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.index.clear();
        self.entries.clear();
    }

    /// Get entry count
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get statistics
    pub fn stats(&self) -> SemanticCacheStatsSnapshot {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        SemanticCacheStatsSnapshot {
            entries: self.entries.len(),
            threshold: self.threshold,
            hits,
            misses,
            hit_rate: if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            },
            semantic_hits: self.stats.semantic_hits.load(Ordering::Relaxed),
            exact_hits: self.stats.exact_hits.load(Ordering::Relaxed),
            insertions: self.stats.insertions.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
        }
    }
}

/// Semantic cache statistics snapshot
#[derive(Debug, Clone)]
pub struct SemanticCacheStatsSnapshot {
    pub entries: usize,
    pub threshold: f32,
    pub hits: u64,
    pub misses: u64,
    pub hit_rate: f64,
    pub semantic_hits: u64,
    pub exact_hits: u64,
    pub insertions: u64,
    pub evictions: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 0.001);

        let c = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &c).abs() < 0.001);

        let d = vec![0.707, 0.707, 0.0];
        let sim = cosine_similarity(&a, &d);
        assert!((sim - 0.707).abs() < 0.01);
    }

    #[test]
    fn test_semantic_index() {
        let index = SemanticIndex::new(SemanticIndexConfig::default());

        let id1 = index.insert(vec![1.0, 0.0, 0.0]);
        let id2 = index.insert(vec![0.9, 0.1, 0.0]);
        let _id3 = index.insert(vec![0.0, 1.0, 0.0]);

        // Search for vector similar to [1.0, 0.0, 0.0]
        let results = index.search(&[1.0, 0.0, 0.0], 2);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, id1); // Most similar
        assert_eq!(results[1].0, id2); // Second most similar
    }

    #[test]
    fn test_semantic_cache_insert_lookup() {
        let cache = SemanticQueryCache::with_capacity(0.9, 100);

        let embedding = vec![1.0, 0.0, 0.0];
        let id = cache.insert(
            "SELECT * FROM users WHERE name = 'test'",
            embedding.clone(),
            json!({"count": 5}),
        );

        // Exact lookup
        let result = cache.lookup(&embedding);
        assert!(result.is_some());
        let res = result.unwrap();
        assert_eq!(res.id, id);
        assert!(res.similarity > 0.999);
    }

    #[test]
    fn test_semantic_similarity_lookup() {
        let cache = SemanticQueryCache::with_capacity(0.9, 100);

        // Insert original query
        cache.insert(
            "SELECT * FROM users WHERE id = 1",
            vec![1.0, 0.0, 0.0],
            json!({"user": "alice"}),
        );

        // Lookup with similar query embedding
        let similar_embedding = vec![0.95, 0.05, 0.0];
        let result = cache.lookup(&similar_embedding);

        assert!(result.is_some());
        let res = result.unwrap();
        assert!(res.similarity >= 0.9);
    }

    #[test]
    fn test_threshold_rejection() {
        let cache = SemanticQueryCache::with_capacity(0.95, 100);

        cache.insert(
            "SELECT * FROM orders",
            vec![1.0, 0.0, 0.0],
            json!({"total": 100}),
        );

        // Query too different (below threshold)
        let different_embedding = vec![0.7, 0.7, 0.0];
        let result = cache.lookup(&different_embedding);
        assert!(result.is_none());
    }

    #[test]
    fn test_find_similar() {
        let cache = SemanticQueryCache::with_capacity(0.5, 100);

        cache.insert("query1", vec![1.0, 0.0, 0.0], json!(1));
        cache.insert("query2", vec![0.9, 0.1, 0.0], json!(2));
        cache.insert("query3", vec![0.8, 0.2, 0.0], json!(3));
        cache.insert("query4", vec![0.0, 1.0, 0.0], json!(4));

        let similar = cache.find_similar(&[1.0, 0.0, 0.0], 3);

        assert_eq!(similar.len(), 3);
        // First should be most similar
        assert!(similar[0].similarity > similar[1].similarity);
        assert!(similar[1].similarity > similar[2].similarity);
    }

    #[test]
    fn test_expiration() {
        let cache = SemanticQueryCache::with_capacity(0.9, 100);

        let embedding = vec![1.0, 0.0, 0.0];
        cache.insert_with_ttl(
            "expiring query",
            embedding.clone(),
            json!({"expires": true}),
            Duration::from_millis(1),
        );

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(10));

        let result = cache.lookup(&embedding);
        assert!(result.is_none());
    }

    #[test]
    fn test_eviction() {
        let cache = SemanticQueryCache::with_capacity(0.9, 3);

        // Fill cache
        for i in 0..3 {
            cache.insert(format!("query{}", i), vec![i as f32, 0.0, 0.0], json!(i));
        }

        assert_eq!(cache.len(), 3);

        // Insert one more (should evict)
        cache.insert("query3", vec![3.0, 0.0, 0.0], json!(3));

        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_stats() {
        let cache = SemanticQueryCache::with_capacity(0.9, 100);

        let embedding = vec![1.0, 0.0, 0.0];
        cache.insert("test query", embedding.clone(), json!(1));

        // Hits
        cache.lookup(&embedding);
        cache.lookup(&embedding);

        // Miss
        cache.lookup(&[0.0, 1.0, 0.0]);

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.exact_hits, 2);
        assert_eq!(stats.insertions, 1);
    }

    #[test]
    fn test_branch_context_compatibility() {
        let main = BranchContext::main();
        let feature = BranchContext::new("feature-x");
        let snapshot = BranchContext::with_snapshot("main", 1000);
        let later_snapshot = BranchContext::with_snapshot("main", 2000);

        // Same branch
        assert!(main.is_compatible(&main));
        assert!(!main.is_compatible(&feature));

        // Snapshot compatibility
        assert!(snapshot.is_compatible(&later_snapshot)); // Entry @ 1000 valid for query @ 2000
        assert!(!later_snapshot.is_compatible(&snapshot)); // Entry @ 2000 not valid for query @ 1000
    }

    #[test]
    fn test_lookup_with_branch() {
        let cache = SemanticQueryCache::with_capacity(0.9, 100);

        // Insert entry for main branch
        let embedding = vec![1.0, 0.0, 0.0];
        cache.insert_with_context(
            "SELECT * FROM users",
            embedding.clone(),
            json!({"users": []}),
            Some(BranchContext::main()),
            None,
            AIWorkloadContext::General,
            vec!["users".to_string()],
        );

        // Insert entry for feature branch
        let embedding2 = vec![0.95, 0.05, 0.0];
        cache.insert_with_context(
            "SELECT * FROM users",
            embedding2.clone(),
            json!({"users": ["new_user"]}),
            Some(BranchContext::new("feature-x")),
            None,
            AIWorkloadContext::General,
            vec!["users".to_string()],
        );

        // Lookup for main should find main entry
        let main_result = cache.lookup_with_branch(&embedding, &BranchContext::main());
        assert!(main_result.is_some());
        assert_eq!(
            main_result
                .unwrap()
                .entry
                .branch_context
                .as_ref()
                .unwrap()
                .branch,
            "main"
        );

        // Lookup for feature should find feature entry
        let feature_result =
            cache.lookup_with_branch(&embedding2, &BranchContext::new("feature-x"));
        assert!(feature_result.is_some());
        assert_eq!(
            feature_result
                .unwrap()
                .entry
                .branch_context
                .as_ref()
                .unwrap()
                .branch,
            "feature-x"
        );
    }

    #[test]
    fn test_lookup_with_session() {
        let cache = SemanticQueryCache::with_capacity(0.9, 100);
        let session1 = "session-001".to_string();
        let session2 = "session-002".to_string();

        // Insert for session 1
        let embedding = vec![1.0, 0.0, 0.0];
        cache.insert_agent_response(
            "What is the weather?",
            embedding.clone(),
            json!({"weather": "sunny"}),
            session1.clone(),
            None,
        );

        // Insert similar query for session 2
        let embedding2 = vec![0.98, 0.02, 0.0];
        cache.insert_agent_response(
            "How's the weather?",
            embedding2,
            json!({"weather": "cloudy"}),
            session2.clone(),
            None,
        );

        // Lookup with session 1 should prefer session 1 entry
        let result = cache.lookup_with_session(&embedding, &session1);
        assert!(result.is_some());
        assert_eq!(
            result.unwrap().entry.session_id.as_ref().unwrap(),
            &session1
        );
    }

    #[test]
    fn test_lookup_with_context() {
        let cache = SemanticQueryCache::with_capacity(0.8, 100);
        let session = "agent-session".to_string();
        let branch = BranchContext::main();

        // Insert with full context
        let embedding = vec![1.0, 0.0, 0.0];
        cache.insert_with_context(
            "Find users with orders",
            embedding.clone(),
            json!({"users": 42}),
            Some(branch.clone()),
            Some(session.clone()),
            AIWorkloadContext::RAGRetrieval,
            vec!["users".to_string(), "orders".to_string()],
        );

        // Full context match
        let result = cache.lookup_with_context(
            &embedding,
            Some(&branch),
            Some(&session),
            AIWorkloadContext::RAGRetrieval,
        );
        assert!(result.is_some());

        // Different workload still matches (lower priority)
        let result2 =
            cache.lookup_with_context(&embedding, Some(&branch), None, AIWorkloadContext::General);
        assert!(result2.is_some());
    }

    #[test]
    fn test_invalidate_by_table() {
        let cache = SemanticQueryCache::with_capacity(0.9, 100);

        // Insert entries referencing different tables
        cache.insert_with_context(
            "SELECT * FROM users",
            vec![1.0, 0.0, 0.0],
            json!(1),
            None,
            None,
            AIWorkloadContext::General,
            vec!["users".to_string()],
        );
        cache.insert_with_context(
            "SELECT * FROM orders",
            vec![0.0, 1.0, 0.0],
            json!(2),
            None,
            None,
            AIWorkloadContext::General,
            vec!["orders".to_string()],
        );
        cache.insert_with_context(
            "SELECT * FROM users JOIN orders",
            vec![0.5, 0.5, 0.0],
            json!(3),
            None,
            None,
            AIWorkloadContext::General,
            vec!["users".to_string(), "orders".to_string()],
        );

        assert_eq!(cache.len(), 3);

        // Invalidate users table
        let removed = cache.invalidate_by_table("users");
        assert_eq!(removed, 2); // users and users+orders entries
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_invalidate_branch() {
        let cache = SemanticQueryCache::with_capacity(0.9, 100);

        // Insert entries for different branches
        cache.insert_with_context(
            "query1",
            vec![1.0, 0.0, 0.0],
            json!(1),
            Some(BranchContext::main()),
            None,
            AIWorkloadContext::General,
            Vec::new(),
        );
        cache.insert_with_context(
            "query2",
            vec![0.0, 1.0, 0.0],
            json!(2),
            Some(BranchContext::new("feature-x")),
            None,
            AIWorkloadContext::General,
            Vec::new(),
        );
        cache.insert_with_context(
            "query3",
            vec![0.0, 0.0, 1.0],
            json!(3),
            Some(BranchContext::new("feature-x")),
            None,
            AIWorkloadContext::General,
            Vec::new(),
        );

        assert_eq!(cache.len(), 3);

        // Invalidate feature-x branch
        let removed = cache.invalidate_branch(&"feature-x".to_string());
        assert_eq!(removed, 2);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_workload_ttl() {
        // RAG retrieval - short TTL (5 min)
        let rag_entry = SemanticEntry::new(1, "rag query", vec![], json!({}))
            .with_workload(AIWorkloadContext::RAGRetrieval);
        assert_eq!(rag_entry.workload_ttl(), Duration::from_secs(300));

        // Tool result - long TTL (24 hours)
        let tool_entry = SemanticEntry::new(2, "tool query", vec![], json!({}))
            .with_workload(AIWorkloadContext::ToolResult);
        assert_eq!(tool_entry.workload_ttl(), Duration::from_secs(86400));

        // Agent conversation - medium TTL (1 hour)
        let agent_entry = SemanticEntry::new(3, "agent query", vec![], json!({}))
            .with_workload(AIWorkloadContext::AgentConversation);
        assert_eq!(agent_entry.workload_ttl(), Duration::from_secs(3600));
    }

    #[test]
    fn test_find_similar_in_branch() {
        let cache = SemanticQueryCache::with_capacity(0.5, 100);
        let main = BranchContext::main();
        let feature = BranchContext::new("feature-x");

        // Insert entries in main
        for i in 0..3 {
            cache.insert_with_context(
                format!("main query {}", i),
                vec![1.0 - (i as f32 * 0.1), i as f32 * 0.1, 0.0],
                json!(i),
                Some(main.clone()),
                None,
                AIWorkloadContext::General,
                Vec::new(),
            );
        }

        // Insert entries in feature
        for i in 0..2 {
            cache.insert_with_context(
                format!("feature query {}", i),
                vec![0.5, 0.5 + (i as f32 * 0.1), 0.0],
                json!(100 + i),
                Some(feature.clone()),
                None,
                AIWorkloadContext::General,
                Vec::new(),
            );
        }

        // Find similar in main branch only
        let main_results = cache.find_similar_in_branch(&[1.0, 0.0, 0.0], &main, 5);
        assert_eq!(main_results.len(), 3);
        for r in &main_results {
            assert_eq!(r.entry.branch_context.as_ref().unwrap().branch, "main");
        }

        // Find similar in feature branch only
        let feature_results = cache.find_similar_in_branch(&[0.5, 0.5, 0.0], &feature, 5);
        assert_eq!(feature_results.len(), 2);
        for r in &feature_results {
            assert_eq!(r.entry.branch_context.as_ref().unwrap().branch, "feature-x");
        }
    }
}
