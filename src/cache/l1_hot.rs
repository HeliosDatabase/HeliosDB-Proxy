//! L1 Hot Cache
//!
//! Per-connection, exact-match cache with LRU eviction.
//! Provides sub-microsecond latency for repeated queries.
//!
//! Hits take only a read lock on the entries map; `L1Entry::access_count`
//! is an `AtomicU64`, so its increment doesn't require `&mut` access to
//! the entry. `parking_lot::RwLock` is used throughout — no poisoning,
//! no `.unwrap()` on every lock acquire.

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::RwLock;

use super::config::L1Config;
use super::result::{CachedResult, L1Entry};

/// L1 hot cache (per-connection)
///
/// This cache stores exact query matches for a single connection.
/// It uses LRU eviction when the cache is full.
#[derive(Debug)]
pub struct L1HotCache {
    /// Cache configuration
    config: L1Config,

    /// Cache entries indexed by exact query string
    entries: RwLock<HashMap<String, L1Entry>>,

    /// LRU order tracking (query string -> last access time)
    lru_order: RwLock<Vec<(String, Instant)>>,
}

impl L1HotCache {
    /// Create a new L1 hot cache with the given configuration
    pub fn new(config: L1Config) -> Self {
        let size = config.size;
        Self {
            config,
            entries: RwLock::new(HashMap::with_capacity(size)),
            lru_order: RwLock::new(Vec::with_capacity(size)),
        }
    }

    /// Look up a query in the cache.
    ///
    /// Hits take only a read lock on the entries map; the `touch()` call
    /// uses atomic `fetch_add` on `access_count` rather than exclusive
    /// access. Expired entries still need a write-lock to evict, but that
    /// is only the slow path.
    pub fn get(&self, query: &str) -> Option<CachedResult> {
        if !self.config.enabled {
            return None;
        }

        // Fast path: read lock.
        let (result, expired) = {
            let entries = self.entries.read();
            match entries.get(query) {
                None => return None,
                Some(entry) if entry.is_expired() => (None, true),
                Some(entry) => {
                    entry.touch();
                    (Some(entry.result.clone()), false)
                }
            }
        };

        if expired {
            // Slow path: escalate to a write lock to evict the dead entry.
            let mut entries = self.entries.write();
            entries.remove(query);
            drop(entries);
            self.remove_from_lru(query);
            return None;
        }

        // Hit: update LRU ordering after releasing the entries read lock,
        // so it contends only with other LRU updates, not with reads.
        self.update_lru(query);
        result
    }

    /// Store a query result in the cache
    pub fn put(&self, query: String, result: CachedResult) {
        if !self.config.enabled {
            return;
        }

        let mut entries = self.entries.write();

        // Check if we need to evict
        if entries.len() >= self.config.size && !entries.contains_key(&query) {
            self.evict_lru(&mut entries);
        }

        // Create TTL-adjusted result
        let mut adjusted_result = result;
        if adjusted_result.ttl > self.config.ttl {
            adjusted_result.ttl = self.config.ttl;
        }

        // Insert or update entry
        let entry = L1Entry::new(query.clone(), adjusted_result);
        entries.insert(query.clone(), entry);
        drop(entries);
        self.update_lru(&query);
    }

    /// Remove an entry from the cache
    pub fn remove(&self, query: &str) {
        self.entries.write().remove(query);
        self.remove_from_lru(query);
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.entries.write().clear();
        self.lru_order.write().clear();
    }

    /// Get current entry count
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get cache capacity
    pub fn capacity(&self) -> usize {
        self.config.size
    }

    /// Get hit statistics
    pub fn stats(&self) -> L1CacheStats {
        let entries = self.entries.read();
        let total_size: usize = entries.values().map(|e| e.result.size()).sum();
        let total_access: u64 = entries.values().map(|e| e.access_count()).sum();

        L1CacheStats {
            entry_count: entries.len(),
            capacity: self.config.size,
            total_size_bytes: total_size,
            total_accesses: total_access,
        }
    }

    /// Evict expired entries
    pub fn evict_expired(&self) {
        let mut entries = self.entries.write();
        let expired: Vec<String> = entries
            .iter()
            .filter(|(_, entry)| entry.is_expired())
            .map(|(key, _)| key.clone())
            .collect();

        for key in &expired {
            entries.remove(key);
        }
        drop(entries);

        for key in &expired {
            self.remove_from_lru(key);
        }
    }

    /// Update LRU tracking for a query
    fn update_lru(&self, query: &str) {
        let mut lru = self.lru_order.write();
        lru.retain(|(q, _)| q != query);
        lru.push((query.to_string(), Instant::now()));
    }

    /// Remove from LRU tracking
    fn remove_from_lru(&self, query: &str) {
        self.lru_order.write().retain(|(q, _)| q != query);
    }

    /// Evict least recently used entry
    fn evict_lru(&self, entries: &mut HashMap<String, L1Entry>) {
        let mut lru = self.lru_order.write();

        // First, try to evict expired entries
        let expired: Vec<String> = lru
            .iter()
            .filter(|(q, _)| entries.get(q).map(|e| e.is_expired()).unwrap_or(true))
            .map(|(q, _)| q.clone())
            .collect();

        for key in expired {
            entries.remove(&key);
            lru.retain(|(q, _)| q != &key);
        }

        // If still full, evict LRU entry
        if entries.len() >= self.config.size {
            if let Some((key, _)) = lru.first().cloned() {
                entries.remove(&key);
                lru.remove(0);
            }
        }
    }
}

/// L1 cache statistics
#[derive(Debug, Clone)]
pub struct L1CacheStats {
    /// Number of entries in cache
    pub entry_count: usize,

    /// Maximum capacity
    pub capacity: usize,

    /// Total size of cached data in bytes
    pub total_size_bytes: usize,

    /// Total number of accesses
    pub total_accesses: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

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
    fn test_basic_get_put() {
        let config = L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        let query = "SELECT * FROM users WHERE id = 1";
        let result = create_result("user data");

        // Initially empty
        assert!(cache.get(query).is_none());

        // Put and get
        cache.put(query.to_string(), result.clone());
        let cached = cache.get(query);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().data, result.data);
    }

    #[test]
    fn test_exact_match() {
        let config = L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        let query1 = "SELECT * FROM users WHERE id = 1";
        let query2 = "SELECT * FROM users WHERE id = 2";
        let result = create_result("user data");

        cache.put(query1.to_string(), result);

        // Exact match should hit
        assert!(cache.get(query1).is_some());

        // Different query should miss
        assert!(cache.get(query2).is_none());
    }

    #[test]
    fn test_expiration() {
        let config = L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_millis(10),
        };
        let cache = L1HotCache::new(config);

        let query = "SELECT 1";
        let result = create_result("1");

        cache.put(query.to_string(), result);
        assert!(cache.get(query).is_some());

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(15));
        assert!(cache.get(query).is_none());
    }

    #[test]
    fn test_lru_eviction() {
        let config = L1Config {
            enabled: true,
            size: 3,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        // Fill cache
        cache.put("query1".to_string(), create_result("1"));
        cache.put("query2".to_string(), create_result("2"));
        cache.put("query3".to_string(), create_result("3"));

        // Access query1 to make it recent
        cache.get("query1");

        // Add new entry - should evict query2 (LRU)
        cache.put("query4".to_string(), create_result("4"));

        assert!(cache.get("query1").is_some()); // Recently accessed
        assert!(cache.get("query2").is_none()); // Evicted
        assert!(cache.get("query3").is_some()); // Still present
        assert!(cache.get("query4").is_some()); // Newly added
    }

    #[test]
    fn test_clear() {
        let config = L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        cache.put("query1".to_string(), create_result("1"));
        cache.put("query2".to_string(), create_result("2"));

        assert_eq!(cache.len(), 2);

        cache.clear();

        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_remove() {
        let config = L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        cache.put("query1".to_string(), create_result("1"));
        cache.put("query2".to_string(), create_result("2"));

        cache.remove("query1");

        assert!(cache.get("query1").is_none());
        assert!(cache.get("query2").is_some());
    }

    #[test]
    fn test_disabled_cache() {
        let config = L1Config {
            enabled: false,
            size: 100,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        cache.put("query".to_string(), create_result("data"));
        assert!(cache.get("query").is_none());
    }

    #[test]
    fn test_stats() {
        let config = L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        cache.put("query1".to_string(), create_result("1"));
        cache.put("query2".to_string(), create_result("2"));

        // Access entries
        cache.get("query1");
        cache.get("query1");
        cache.get("query2");

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 2);
        assert_eq!(stats.capacity, 100);
        assert!(stats.total_size_bytes > 0);
        assert_eq!(stats.total_accesses, 5); // 2 puts + 3 gets
    }

    #[test]
    fn test_evict_expired() {
        let config = L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_millis(10),
        };
        let cache = L1HotCache::new(config);

        cache.put("query1".to_string(), create_result("1"));
        cache.put("query2".to_string(), create_result("2"));

        std::thread::sleep(Duration::from_millis(15));

        cache.evict_expired();

        assert!(cache.is_empty());
    }

    #[test]
    fn test_update_existing() {
        let config = L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        cache.put("query".to_string(), create_result("old"));
        cache.put("query".to_string(), create_result("new"));

        let cached = cache.get("query").unwrap();
        assert_eq!(cached.data, Bytes::from("new"));
    }

    /// Concurrent hits on the same key must not block each other and must
    /// all observe the cached result. Before the read-path refactor this
    /// test could not be written sensibly — every `get()` took a write
    /// lock, so reads serialised. With atomic access_count + read-locked
    /// hits, many threads can hit the same entry in parallel.
    #[test]
    fn test_concurrent_hits_read_lock_only() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(L1HotCache::new(L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_secs(60),
        }));
        cache.put("hot-query".to_string(), create_result("hot data"));

        const THREADS: usize = 16;
        const ITERS_PER_THREAD: usize = 500;

        let mut handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for _ in 0..ITERS_PER_THREAD {
                    let r = cache.get("hot-query").expect("hit expected");
                    assert_eq!(r.data, Bytes::from("hot data"));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let stats = cache.stats();
        // access_count starts at 1 (from put) and is bumped once per get.
        // Total: 1 (put) + THREADS * ITERS_PER_THREAD (gets).
        assert_eq!(
            stats.total_accesses,
            1 + (THREADS * ITERS_PER_THREAD) as u64
        );
    }
}
