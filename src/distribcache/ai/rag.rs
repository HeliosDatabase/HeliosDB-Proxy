//! RAG chunk cache
//!
//! Caches document chunks for RAG (Retrieval-Augmented Generation) pipelines.
//! Optimized for embedding-based retrieval and document fetching.

use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Chunk identifier
pub type ChunkId = u64;

/// Embedding hash for cache lookup
pub type EmbeddingHash = u64;

/// Document chunk
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Chunk ID
    pub id: ChunkId,
    /// Parent document ID
    pub document_id: String,
    /// Chunk content
    pub content: String,
    /// Chunk embedding (optional, for similarity)
    pub embedding: Option<Vec<f32>>,
    /// Chunk position in document
    pub position: usize,
    /// Metadata
    pub metadata: Option<serde_json::Value>,
    /// Creation time
    pub created_at: Instant,
}

impl Chunk {
    /// Create a new chunk
    pub fn new(id: ChunkId, document_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            id,
            document_id: document_id.into(),
            content: content.into(),
            embedding: None,
            position: 0,
            metadata: None,
            created_at: Instant::now(),
        }
    }

    /// Add embedding
    pub fn with_embedding(mut self, embedding: Vec<f32>) -> Self {
        self.embedding = Some(embedding);
        self
    }

    /// Set position
    pub fn with_position(mut self, position: usize) -> Self {
        self.position = position;
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Approximate size in bytes
    pub fn size(&self) -> usize {
        self.content.len() +
        self.document_id.len() +
        self.embedding.as_ref().map(|e| e.len() * 4).unwrap_or(0) +
        64
    }
}

/// Hash an embedding vector
pub fn hash_embedding(embedding: &[f32]) -> EmbeddingHash {
    use std::hash::{Hash, Hasher};
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();

    // Quantize and hash
    for val in embedding {
        let quantized = (val * 1000.0) as i32;
        quantized.hash(&mut hasher);
    }

    hasher.finish()
}

/// RAG chunk cache
pub struct RagChunkCache {
    /// Chunk storage (id -> chunk)
    chunks: DashMap<ChunkId, Chunk>,

    /// Embedding to chunk IDs mapping
    embedding_to_chunks: DashMap<EmbeddingHash, Vec<ChunkId>>,

    /// Document to chunk IDs mapping
    document_to_chunks: DashMap<String, HashSet<ChunkId>>,

    /// Maximum cache size in MB
    max_size_mb: usize,

    /// Current size in bytes
    current_size: AtomicU64,

    /// Statistics
    stats: RagCacheStats,
}

/// RAG cache statistics
#[derive(Debug, Default)]
struct RagCacheStats {
    hits: AtomicU64,
    misses: AtomicU64,
    embedding_lookups: AtomicU64,
    embedding_cache_hits: AtomicU64,
}

impl RagChunkCache {
    /// Create a new RAG chunk cache
    pub fn new(max_size_mb: usize) -> Self {
        Self {
            chunks: DashMap::new(),
            embedding_to_chunks: DashMap::new(),
            document_to_chunks: DashMap::new(),
            max_size_mb,
            current_size: AtomicU64::new(0),
            stats: RagCacheStats::default(),
        }
    }

    /// Get a chunk by ID
    pub fn get_chunk(&self, id: ChunkId) -> Option<Chunk> {
        if let Some(chunk) = self.chunks.get(&id) {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            Some(chunk.clone())
        } else {
            self.stats.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Get chunks by embedding similarity
    pub fn get_chunks_by_embedding(&self, embedding: &[f32], k: usize) -> Vec<Chunk> {
        self.stats.embedding_lookups.fetch_add(1, Ordering::Relaxed);

        let hash = hash_embedding(embedding);

        if let Some(chunk_ids) = self.embedding_to_chunks.get(&hash) {
            self.stats.embedding_cache_hits.fetch_add(1, Ordering::Relaxed);

            let chunks: Vec<_> = chunk_ids.iter()
                .filter_map(|id| self.chunks.get(id).map(|c| c.clone()))
                .take(k)
                .collect();

            return chunks;
        }

        Vec::new()
    }

    /// Get all chunks for a document
    pub fn get_document_chunks(&self, document_id: &str) -> Vec<Chunk> {
        if let Some(ids) = self.document_to_chunks.get(document_id) {
            ids.iter()
                .filter_map(|id| self.chunks.get(id).map(|c| c.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Insert a chunk
    pub fn insert_chunk(&self, chunk: Chunk) {
        let size = chunk.size() as u64;
        let max_bytes = (self.max_size_mb * 1024 * 1024) as u64;

        // Evict if needed
        while self.current_size.load(Ordering::Relaxed) + size > max_bytes {
            if !self.evict_one() {
                break;
            }
        }

        // Index by document
        self.document_to_chunks
            .entry(chunk.document_id.clone())
            .or_default()
            .insert(chunk.id);

        // Index by embedding if available
        if let Some(ref embedding) = chunk.embedding {
            let hash = hash_embedding(embedding);
            self.embedding_to_chunks
                .entry(hash)
                .or_default()
                .push(chunk.id);
        }

        // Store chunk
        self.chunks.insert(chunk.id, chunk);
        self.current_size.fetch_add(size, Ordering::Relaxed);
    }

    /// Insert multiple chunks (batch)
    pub fn insert_chunks(&self, chunks: Vec<Chunk>) {
        for chunk in chunks {
            self.insert_chunk(chunk);
        }
    }

    /// Cache embedding to chunk ID mapping
    pub fn cache_embedding_result(&self, embedding: &[f32], chunk_ids: Vec<ChunkId>) {
        let hash = hash_embedding(embedding);
        self.embedding_to_chunks.insert(hash, chunk_ids);
    }

    /// Remove a chunk
    pub fn remove_chunk(&self, id: ChunkId) {
        if let Some((_, chunk)) = self.chunks.remove(&id) {
            self.current_size.fetch_sub(chunk.size() as u64, Ordering::Relaxed);

            // Remove from document index
            if let Some(mut ids) = self.document_to_chunks.get_mut(&chunk.document_id) {
                ids.remove(&id);
            }
        }
    }

    /// Remove all chunks for a document
    pub fn remove_document(&self, document_id: &str) {
        if let Some((_, ids)) = self.document_to_chunks.remove(document_id) {
            for id in ids {
                self.remove_chunk(id);
            }
        }
    }

    /// Evict one chunk (oldest by creation time)
    fn evict_one(&self) -> bool {
        let mut oldest_id = None;
        let mut oldest_time = Instant::now();

        for entry in self.chunks.iter() {
            if entry.created_at < oldest_time {
                oldest_time = entry.created_at;
                oldest_id = Some(*entry.key());
            }
        }

        if let Some(id) = oldest_id {
            self.remove_chunk(id);
            return true;
        }

        false
    }

    /// Get cache statistics
    pub fn stats(&self) -> RagCacheStatsSnapshot {
        RagCacheStatsSnapshot {
            chunk_count: self.chunks.len(),
            document_count: self.document_to_chunks.len(),
            size_bytes: self.current_size.load(Ordering::Relaxed),
            max_size_bytes: (self.max_size_mb * 1024 * 1024) as u64,
            hits: self.stats.hits.load(Ordering::Relaxed),
            misses: self.stats.misses.load(Ordering::Relaxed),
            embedding_lookups: self.stats.embedding_lookups.load(Ordering::Relaxed),
            embedding_cache_hit_rate: {
                let lookups = self.stats.embedding_lookups.load(Ordering::Relaxed);
                let hits = self.stats.embedding_cache_hits.load(Ordering::Relaxed);
                if lookups > 0 { hits as f64 / lookups as f64 } else { 0.0 }
            },
        }
    }

    /// Clear all cached chunks
    pub fn clear(&self) {
        self.chunks.clear();
        self.embedding_to_chunks.clear();
        self.document_to_chunks.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }
}

/// RAG cache statistics snapshot
#[derive(Debug, Clone)]
pub struct RagCacheStatsSnapshot {
    pub chunk_count: usize,
    pub document_count: usize,
    pub size_bytes: u64,
    pub max_size_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub embedding_lookups: u64,
    pub embedding_cache_hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_creation() {
        let chunk = Chunk::new(1, "doc-1", "This is a test chunk")
            .with_position(0);

        assert_eq!(chunk.id, 1);
        assert_eq!(chunk.document_id, "doc-1");
        assert_eq!(chunk.position, 0);
    }

    #[test]
    fn test_insert_and_get() {
        let cache = RagChunkCache::new(10);

        let chunk = Chunk::new(1, "doc-1", "Test content");
        cache.insert_chunk(chunk);

        let retrieved = cache.get_chunk(1);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().content, "Test content");
    }

    #[test]
    fn test_document_chunks() {
        let cache = RagChunkCache::new(10);

        cache.insert_chunk(Chunk::new(1, "doc-1", "Chunk 1").with_position(0));
        cache.insert_chunk(Chunk::new(2, "doc-1", "Chunk 2").with_position(1));
        cache.insert_chunk(Chunk::new(3, "doc-2", "Chunk 3").with_position(0));

        let doc1_chunks = cache.get_document_chunks("doc-1");
        assert_eq!(doc1_chunks.len(), 2);

        let doc2_chunks = cache.get_document_chunks("doc-2");
        assert_eq!(doc2_chunks.len(), 1);
    }

    #[test]
    fn test_embedding_lookup() {
        let cache = RagChunkCache::new(10);

        let embedding = vec![0.1, 0.2, 0.3];
        let chunk = Chunk::new(1, "doc-1", "Embedded content")
            .with_embedding(embedding.clone());

        cache.insert_chunk(chunk);

        // Cache the embedding result
        cache.cache_embedding_result(&embedding, vec![1]);

        // Lookup by embedding
        let results = cache.get_chunks_by_embedding(&embedding, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, 1);
    }

    #[test]
    fn test_remove_document() {
        let cache = RagChunkCache::new(10);

        cache.insert_chunk(Chunk::new(1, "doc-1", "Chunk 1"));
        cache.insert_chunk(Chunk::new(2, "doc-1", "Chunk 2"));

        cache.remove_document("doc-1");

        assert!(cache.get_chunk(1).is_none());
        assert!(cache.get_chunk(2).is_none());
    }

    #[test]
    fn test_stats() {
        let cache = RagChunkCache::new(10);

        cache.insert_chunk(Chunk::new(1, "doc-1", "Content"));
        cache.get_chunk(1); // Hit
        cache.get_chunk(2); // Miss

        let stats = cache.stats();
        assert_eq!(stats.chunk_count, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }
}
