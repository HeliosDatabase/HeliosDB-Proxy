//! L3 Semantic Cache
//!
//! Vector similarity cache for AI/RAG workloads.
//! Uses embeddings to find semantically similar queries.

use std::sync::RwLock;
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::Semaphore;

use super::config::L3Config;
use super::result::{CachedResult, L3Entry};
use super::CacheContext;

/// L3 semantic cache (vector similarity)
///
/// This cache stores query embeddings and uses cosine similarity
/// to find matches even when queries are not identical.
#[derive(Debug)]
pub struct L3SemanticCache {
    /// Configuration
    config: L3Config,

    /// Cache entries
    entries: RwLock<Vec<L3Entry>>,

    /// Embedding service client
    embedding_client: EmbeddingClient,

    /// Semaphore for limiting concurrent embedding requests
    embedding_semaphore: Semaphore,

    /// Cache for computed embeddings (query hash -> embedding)
    embedding_cache: DashMap<u64, Vec<f32>>,
}

/// Embedding service client (Ollama)
#[derive(Debug)]
pub struct EmbeddingClient {
    /// Ollama endpoint
    endpoint: String,

    /// Model name
    model: String,

    /// Expected embedding dimension
    dimension: usize,

    /// HTTP client
    client: reqwest::Client,
}

impl L3SemanticCache {
    /// Create a new L3 semantic cache
    pub fn new(config: L3Config) -> Self {
        let embedding_client = EmbeddingClient::new(
            config.embedding_endpoint.clone(),
            config.embedding_model.clone(),
            config.embedding_dim,
        );

        Self {
            config: config.clone(),
            entries: RwLock::new(Vec::with_capacity(config.max_entries)),
            embedding_client,
            embedding_semaphore: Semaphore::new(10), // Max 10 concurrent embedding requests
            embedding_cache: DashMap::new(),
        }
    }

    /// Look up a query using semantic similarity
    pub async fn get(&self, query: &str, context: &CacheContext) -> Option<CachedResult> {
        if !self.config.enabled {
            return None;
        }

        // Get embedding for the query
        let embedding = self.get_embedding(query).await?;

        // Find best match
        let entries = self.entries.read().ok()?;

        let mut best_match: Option<(f32, &L3Entry)> = None;

        for entry in entries.iter() {
            // Skip expired entries
            if entry.is_expired() {
                continue;
            }

            // Check context match (database, user for RLS)
            if entry.context.database != context.database {
                continue;
            }

            if entry.context.user != context.user {
                continue;
            }

            // Calculate similarity
            let similarity = entry.similarity(&embedding);

            if similarity >= self.config.similarity_threshold {
                if let Some((best_sim, _)) = best_match {
                    if similarity > best_sim {
                        best_match = Some((similarity, entry));
                    }
                } else {
                    best_match = Some((similarity, entry));
                }
            }
        }

        best_match.map(|(_, entry)| entry.result.clone())
    }

    /// Store a query and result in the semantic cache
    pub async fn put(&self, query: &str, context: &CacheContext, result: CachedResult) {
        if !self.config.enabled {
            return;
        }

        // Get embedding for the query
        let embedding = match self.get_embedding(query).await {
            Some(e) => e,
            None => return,
        };

        // Create entry
        let mut entry = L3Entry::new(
            query.to_string(),
            embedding,
            context.clone(),
            result,
        );

        // Enforce TTL from config
        if entry.result.ttl > self.config.ttl {
            entry.result.ttl = self.config.ttl;
        }

        let mut entries = match self.entries.write() {
            Ok(e) => e,
            Err(_) => return,
        };

        // Check capacity and evict if needed
        if entries.len() >= self.config.max_entries {
            self.evict(&mut entries);
        }

        entries.push(entry);
    }

    /// Clear all entries
    pub async fn clear(&self) {
        if let Ok(mut entries) = self.entries.write() {
            entries.clear();
        }
        self.embedding_cache.clear();
    }

    /// Get entry count
    pub fn len(&self) -> usize {
        self.entries.read().map(|e| e.len()).unwrap_or(0)
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get cache statistics
    pub fn stats(&self) -> L3CacheStats {
        let entries = self.entries.read().unwrap();

        let total_access: u64 = entries.iter().map(|e| e.access_count).sum();
        let avg_embedding_size = if entries.is_empty() {
            0
        } else {
            entries.first().map(|e| e.embedding.len()).unwrap_or(0)
        };

        L3CacheStats {
            entry_count: entries.len(),
            max_entries: self.config.max_entries,
            similarity_threshold: self.config.similarity_threshold,
            embedding_dimension: avg_embedding_size,
            total_accesses: total_access,
            embedding_cache_size: self.embedding_cache.len(),
        }
    }

    /// Get embedding for a query (cached)
    async fn get_embedding(&self, query: &str) -> Option<Vec<f32>> {
        // Check embedding cache first
        let query_hash = quick_hash(query);

        if let Some(cached) = self.embedding_cache.get(&query_hash) {
            return Some(cached.clone());
        }

        // Acquire semaphore to limit concurrent requests
        let _permit = self.embedding_semaphore.acquire().await.ok()?;

        // Call embedding service
        let embedding = self.embedding_client.embed(query).await?;

        // Cache the embedding
        self.embedding_cache.insert(query_hash, embedding.clone());

        Some(embedding)
    }

    /// Evict entries to make room for new ones
    fn evict(&self, entries: &mut Vec<L3Entry>) {
        // First, remove expired entries
        entries.retain(|e| !e.is_expired());

        // If still full, remove LRU entries
        while entries.len() >= self.config.max_entries {
            if let Some(lru_idx) = entries
                .iter()
                .enumerate()
                .min_by_key(|(_, e)| e.last_access)
                .map(|(i, _)| i)
            {
                entries.remove(lru_idx);
            } else {
                break;
            }
        }
    }

    /// Check if the embedding service is available
    pub async fn health_check(&self) -> bool {
        self.embedding_client.health_check().await
    }
}

impl EmbeddingClient {
    /// Create a new embedding client
    pub fn new(endpoint: String, model: String, dimension: usize) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        Self {
            endpoint,
            model,
            dimension,
            client,
        }
    }

    /// Generate embedding for text using Ollama
    pub async fn embed(&self, text: &str) -> Option<Vec<f32>> {
        let url = format!("{}/api/embeddings", self.endpoint);

        let request = serde_json::json!({
            "model": self.model,
            "prompt": text
        });

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .ok()?;

        if !response.status().is_success() {
            return None;
        }

        let body: serde_json::Value = response.json().await.ok()?;

        let embedding = body.get("embedding")?
            .as_array()?
            .iter()
            .filter_map(|v| v.as_f64().map(|f| f as f32))
            .collect::<Vec<f32>>();

        // Validate dimension
        if embedding.len() != self.dimension {
            // Try to handle dimension mismatch gracefully
            if embedding.len() > self.dimension {
                return Some(embedding[..self.dimension].to_vec());
            } else {
                // Pad with zeros (not ideal, but better than failing)
                let mut padded = embedding;
                padded.resize(self.dimension, 0.0);
                return Some(padded);
            }
        }

        Some(embedding)
    }

    /// Check if Ollama is available
    pub async fn health_check(&self) -> bool {
        let url = format!("{}/api/tags", self.endpoint);

        match self.client.get(&url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// List available models
    pub async fn list_models(&self) -> Option<Vec<String>> {
        let url = format!("{}/api/tags", self.endpoint);

        let response = self.client.get(&url).send().await.ok()?;
        let body: serde_json::Value = response.json().await.ok()?;

        let models = body.get("models")?
            .as_array()?
            .iter()
            .filter_map(|m| m.get("name")?.as_str().map(String::from))
            .collect();

        Some(models)
    }

    /// Pull a model if not available
    pub async fn pull_model(&self) -> Result<(), String> {
        let url = format!("{}/api/pull", self.endpoint);

        let request = serde_json::json!({
            "name": self.model
        });

        let response = self.client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("Failed to pull model: {}", response.status()))
        }
    }
}

/// L3 cache statistics
#[derive(Debug, Clone)]
pub struct L3CacheStats {
    /// Number of entries
    pub entry_count: usize,

    /// Maximum entries
    pub max_entries: usize,

    /// Similarity threshold
    pub similarity_threshold: f32,

    /// Embedding dimension
    pub embedding_dimension: usize,

    /// Total accesses
    pub total_accesses: u64,

    /// Embedding cache size
    pub embedding_cache_size: usize,
}

/// Quick hash for embedding cache key
fn quick_hash(s: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

/// Compute cosine similarity between two vectors
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot_product = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;

    for (x, y) in a.iter().zip(b.iter()) {
        dot_product += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    let norm_a = norm_a.sqrt();
    let norm_b = norm_b.sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot_product / (norm_a * norm_b)
}

/// Generate a random embedding for testing
#[cfg(test)]
fn random_embedding(dim: usize) -> Vec<f32> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    std::time::Instant::now().hash(&mut hasher);
    let seed = hasher.finish();

    (0..dim)
        .map(|i| {
            let x = ((seed.wrapping_add(i as u64) as f64) * 0.0001).sin() as f32;
            x
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_result(data: &str) -> CachedResult {
        CachedResult::new(
            Bytes::from(data.to_string()),
            1,
            Duration::from_secs(60),
            vec!["test".to_string()],
            Duration::from_millis(5),
        )
    }

    #[test]
    fn test_cosine_similarity() {
        // Same vector = 1.0
        let a = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &a) - 1.0).abs() < 0.001);

        // Orthogonal vectors = 0.0
        let b = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &b).abs() < 0.001);

        // Opposite vectors = -1.0
        let c = vec![-1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &c) + 1.0).abs() < 0.001);

        // Empty vectors = 0.0
        assert!(cosine_similarity(&[], &[]).abs() < 0.001);

        // Different lengths = 0.0
        let d = vec![1.0, 0.0];
        assert!(cosine_similarity(&a, &d).abs() < 0.001);
    }

    #[test]
    fn test_l3_entry_similarity() {
        let result = create_result("test");
        let ctx = CacheContext::default();

        let entry = L3Entry::new(
            "SELECT * FROM users".to_string(),
            vec![0.5, 0.5, 0.5, 0.5],
            ctx,
            result,
        );

        // High similarity
        let similar = vec![0.5, 0.5, 0.5, 0.5];
        assert!((entry.similarity(&similar) - 1.0).abs() < 0.001);

        // Moderate similarity
        let moderate = vec![0.5, 0.5, 0.0, 0.0];
        assert!(entry.similarity(&moderate) > 0.5);
        assert!(entry.similarity(&moderate) < 1.0);
    }

    #[test]
    fn test_quick_hash() {
        let hash1 = quick_hash("SELECT * FROM users");
        let hash2 = quick_hash("SELECT * FROM users");
        let hash3 = quick_hash("SELECT * FROM orders");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_random_embedding() {
        let emb = random_embedding(384);
        assert_eq!(emb.len(), 384);
    }

    #[tokio::test]
    async fn test_l3_cache_disabled() {
        let config = L3Config {
            enabled: false,
            ..Default::default()
        };
        let cache = L3SemanticCache::new(config);

        let ctx = CacheContext::default();
        let result = cache.get("test query", &ctx).await;
        assert!(result.is_none());
    }

    #[test]
    fn test_embedding_client_creation() {
        let client = EmbeddingClient::new(
            "http://localhost:11434".to_string(),
            "all-minilm".to_string(),
            384,
        );

        assert_eq!(client.endpoint, "http://localhost:11434");
        assert_eq!(client.model, "all-minilm");
        assert_eq!(client.dimension, 384);
    }

    #[test]
    fn test_l3_stats() {
        let config = L3Config {
            enabled: true,
            max_entries: 1000,
            similarity_threshold: 0.9,
            ..Default::default()
        };
        let cache = L3SemanticCache::new(config);

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.max_entries, 1000);
        assert!((stats.similarity_threshold - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_eviction() {
        // Test that eviction logic works
        let config = L3Config {
            enabled: true,
            max_entries: 3,
            ..Default::default()
        };
        let cache = L3SemanticCache::new(config);

        // Manually add entries for testing
        {
            let mut entries = cache.entries.write().unwrap();

            for i in 0..5 {
                let ctx = CacheContext::default();
                let result = create_result(&format!("result_{}", i));
                let embedding = random_embedding(384);

                entries.push(L3Entry::new(
                    format!("query_{}", i),
                    embedding,
                    ctx,
                    result,
                ));

                // Evict if needed
                cache.evict(&mut entries);
            }

            // Should have at most max_entries
            assert!(entries.len() <= 3);
        }
    }
}
