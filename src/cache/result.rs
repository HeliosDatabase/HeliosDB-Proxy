//! Cached Result Types
//!
//! Structures for storing and retrieving cached query results.

use bytes::Bytes;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

use super::normalizer::NormalizedQuery;
use super::CacheContext;

/// Cached query result
#[derive(Debug, Clone)]
pub struct CachedResult {
    /// Serialized result data
    pub data: Bytes,

    /// Number of rows in the result
    pub row_count: usize,

    /// When this result was cached
    pub cached_at: Instant,

    /// Time-to-live for this result
    pub ttl: Duration,

    /// Tables referenced by the query
    pub tables: Vec<String>,

    /// Original query execution time
    pub execution_time: Duration,
}

impl CachedResult {
    /// Create a new cached result
    pub fn new(
        data: Bytes,
        row_count: usize,
        ttl: Duration,
        tables: Vec<String>,
        execution_time: Duration,
    ) -> Self {
        Self {
            data,
            row_count,
            cached_at: Instant::now(),
            ttl,
            tables,
            execution_time,
        }
    }

    /// Check if this cached result has expired
    pub fn is_expired(&self) -> bool {
        self.cached_at.elapsed() > self.ttl
    }

    /// Get the age of this cached result
    pub fn age(&self) -> Duration {
        self.cached_at.elapsed()
    }

    /// Get remaining TTL
    pub fn remaining_ttl(&self) -> Duration {
        self.ttl.saturating_sub(self.cached_at.elapsed())
    }

    /// Get size in bytes
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// Cache key for lookup operations
#[derive(Debug, Clone)]
pub struct CacheKey {
    /// Hash of the normalized query
    pub query_hash: u64,

    /// Database name
    pub database: String,

    /// User (for RLS-aware caching)
    pub user: Option<String>,

    /// Branch (for HeliosDB branching support)
    pub branch: Option<String>,

    /// Pre-computed hash for fast lookups
    cached_hash: u64,
}

impl CacheKey {
    /// Create a new cache key from a normalized query and context
    pub fn new(normalized: &NormalizedQuery, context: &CacheContext) -> Self {
        let query_hash = normalized.hash;

        // Compute combined hash
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        query_hash.hash(&mut hasher);
        context.database.hash(&mut hasher);
        context.user.hash(&mut hasher);
        context.branch.hash(&mut hasher);
        let cached_hash = hasher.finish();

        Self {
            query_hash,
            database: context.database.clone(),
            user: context.user.clone(),
            branch: context.branch.clone(),
            cached_hash,
        }
    }

    /// Create a cache key from raw components
    pub fn from_parts(
        query_hash: u64,
        database: String,
        user: Option<String>,
        branch: Option<String>,
    ) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        query_hash.hash(&mut hasher);
        database.hash(&mut hasher);
        user.hash(&mut hasher);
        branch.hash(&mut hasher);
        let cached_hash = hasher.finish();

        Self {
            query_hash,
            database,
            user,
            branch,
            cached_hash,
        }
    }

    /// Get the pre-computed hash
    pub fn hash_value(&self) -> u64 {
        self.cached_hash
    }
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.cached_hash);
    }
}

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.cached_hash == other.cached_hash
            && self.query_hash == other.query_hash
            && self.database == other.database
            && self.user == other.user
            && self.branch == other.branch
    }
}

impl Eq for CacheKey {}

/// Entry in the L1 hot cache
#[derive(Debug, Clone)]
pub struct L1Entry {
    /// The cached result
    pub result: CachedResult,

    /// Original query string (for exact match)
    pub query: String,

    /// Access count for LRU tracking
    pub access_count: u64,

    /// Last access time
    pub last_access: Instant,
}

impl L1Entry {
    /// Create a new L1 cache entry
    pub fn new(query: String, result: CachedResult) -> Self {
        Self {
            result,
            query,
            access_count: 1,
            last_access: Instant::now(),
        }
    }

    /// Record an access to this entry
    pub fn touch(&mut self) {
        self.access_count += 1;
        self.last_access = Instant::now();
    }

    /// Check if this entry has expired
    pub fn is_expired(&self) -> bool {
        self.result.is_expired()
    }
}

/// Entry in the L2 warm cache
#[derive(Debug, Clone)]
pub struct L2Entry {
    /// The cached result
    pub result: CachedResult,

    /// Normalized query fingerprint
    pub fingerprint: String,

    /// Cache key
    pub key: CacheKey,

    /// Access count
    pub access_count: u64,

    /// Last access time
    pub last_access: Instant,

    /// Estimated memory size
    pub memory_size: usize,
}

impl L2Entry {
    /// Create a new L2 cache entry
    pub fn new(key: CacheKey, fingerprint: String, result: CachedResult) -> Self {
        let memory_size = result.size()
            + fingerprint.len()
            + std::mem::size_of::<Self>()
            + key.database.len()
            + key.user.as_ref().map(|s| s.len()).unwrap_or(0)
            + key.branch.as_ref().map(|s| s.len()).unwrap_or(0);

        Self {
            result,
            fingerprint,
            key,
            access_count: 1,
            last_access: Instant::now(),
            memory_size,
        }
    }

    /// Record an access to this entry
    pub fn touch(&mut self) {
        self.access_count += 1;
        self.last_access = Instant::now();
    }

    /// Check if this entry has expired
    pub fn is_expired(&self) -> bool {
        self.result.is_expired()
    }
}

/// Entry in the L3 semantic cache
#[derive(Debug, Clone)]
pub struct L3Entry {
    /// The cached result
    pub result: CachedResult,

    /// Original query string
    pub query: String,

    /// Query embedding vector
    pub embedding: Vec<f32>,

    /// Cache context
    pub context: CacheContext,

    /// Access count
    pub access_count: u64,

    /// Last access time
    pub last_access: Instant,
}

impl L3Entry {
    /// Create a new L3 cache entry
    pub fn new(query: String, embedding: Vec<f32>, context: CacheContext, result: CachedResult) -> Self {
        Self {
            result,
            query,
            embedding,
            context,
            access_count: 1,
            last_access: Instant::now(),
        }
    }

    /// Record an access to this entry
    pub fn touch(&mut self) {
        self.access_count += 1;
        self.last_access = Instant::now();
    }

    /// Check if this entry has expired
    pub fn is_expired(&self) -> bool {
        self.result.is_expired()
    }

    /// Compute cosine similarity with another embedding
    pub fn similarity(&self, other: &[f32]) -> f32 {
        if self.embedding.len() != other.len() {
            return 0.0;
        }

        let mut dot_product = 0.0f32;
        let mut norm_a = 0.0f32;
        let mut norm_b = 0.0f32;

        for (a, b) in self.embedding.iter().zip(other.iter()) {
            dot_product += a * b;
            norm_a += a * a;
            norm_b += b * b;
        }

        let norm_a = norm_a.sqrt();
        let norm_b = norm_b.sqrt();

        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }

        dot_product / (norm_a * norm_b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cached_result_expiry() {
        let result = CachedResult::new(
            Bytes::from("test"),
            1,
            Duration::from_millis(10),
            vec!["users".to_string()],
            Duration::from_millis(5),
        );

        assert!(!result.is_expired());

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(15));
        assert!(result.is_expired());
    }

    #[test]
    fn test_cache_key_equality() {
        let ctx1 = CacheContext {
            database: "db1".to_string(),
            user: Some("user1".to_string()),
            branch: None,
            connection_id: None,
        };

        let ctx2 = CacheContext {
            database: "db1".to_string(),
            user: Some("user1".to_string()),
            branch: None,
            connection_id: Some(123), // Different connection_id shouldn't matter
        };

        let normalized = NormalizedQuery {
            fingerprint: "SELECT * FROM users WHERE id = ?".to_string(),
            hash: 12345,
            tables: vec!["users".to_string()],
            parameters: vec!["1".to_string()],
        };

        let key1 = CacheKey::new(&normalized, &ctx1);
        let key2 = CacheKey::new(&normalized, &ctx2);

        assert_eq!(key1, key2);
    }

    #[test]
    fn test_cache_key_different_users() {
        let ctx1 = CacheContext {
            database: "db1".to_string(),
            user: Some("user1".to_string()),
            branch: None,
            connection_id: None,
        };

        let ctx2 = CacheContext {
            database: "db1".to_string(),
            user: Some("user2".to_string()),
            branch: None,
            connection_id: None,
        };

        let normalized = NormalizedQuery {
            fingerprint: "SELECT * FROM users".to_string(),
            hash: 12345,
            tables: vec!["users".to_string()],
            parameters: vec![],
        };

        let key1 = CacheKey::new(&normalized, &ctx1);
        let key2 = CacheKey::new(&normalized, &ctx2);

        // Different users should have different cache keys (for RLS)
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_l3_entry_similarity() {
        let result = CachedResult::new(
            Bytes::from("test"),
            1,
            Duration::from_secs(60),
            vec![],
            Duration::from_millis(5),
        );

        let ctx = CacheContext::default();

        let entry = L3Entry::new(
            "SELECT * FROM users".to_string(),
            vec![1.0, 0.0, 0.0],
            ctx,
            result,
        );

        // Same vector should have similarity 1.0
        assert!((entry.similarity(&[1.0, 0.0, 0.0]) - 1.0).abs() < 0.001);

        // Orthogonal vector should have similarity 0.0
        assert!((entry.similarity(&[0.0, 1.0, 0.0])).abs() < 0.001);

        // Opposite vector should have similarity -1.0
        assert!((entry.similarity(&[-1.0, 0.0, 0.0]) + 1.0).abs() < 0.001);
    }

    #[test]
    fn test_l1_entry_touch() {
        let result = CachedResult::new(
            Bytes::from("test"),
            1,
            Duration::from_secs(60),
            vec![],
            Duration::from_millis(5),
        );

        let mut entry = L1Entry::new("SELECT 1".to_string(), result);
        assert_eq!(entry.access_count, 1);

        entry.touch();
        assert_eq!(entry.access_count, 2);

        entry.touch();
        assert_eq!(entry.access_count, 3);
    }
}
