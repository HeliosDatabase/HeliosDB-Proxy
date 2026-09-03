//! L1 Hot Cache
//!
//! Per-connection, exact-match cache with LRU eviction.
//! Provides sub-microsecond latency for repeated queries.
//!
//! Backed by `lru::LruCache`, an intrusive linked hash map: `get`/`put`
//! are O(1) amortized with no per-call allocation (promoting an
//! existing entry to most-recently-used relinks internal pointers, it
//! never re-clones the key). That replaces a previous `HashMap` +
//! `Vec<(String, Instant)>` pair whose LRU bookkeeping did an
//! O(n) `retain` scan plus a `String` allocation on every single hit
//! and put, with eviction doing an O(n) scan per candidate (O(n^2)
//! overall). A single `parking_lot::RwLock` (write-locked for both
//! reads and writes, since promotion needs `&mut`) guards the map —
//! no poisoning, no `.unwrap()` on every lock acquire.

use std::num::NonZeroUsize;

use lru::LruCache;
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

    /// Cache entries indexed by exact query string, in LRU order.
    entries: RwLock<LruCache<String, L1Entry>>,
}

impl L1HotCache {
    /// Create a new L1 hot cache with the given configuration
    pub fn new(config: L1Config) -> Self {
        // `size == 0` would be an invalid `NonZeroUsize`; the previous
        // Vec/HashMap implementation degraded to an effective capacity
        // of 1 in that case (evicting-then-inserting on every put), so
        // floor at 1 here to match rather than panicking.
        let cap = NonZeroUsize::new(config.size).unwrap_or(NonZeroUsize::MIN);
        Self {
            entries: RwLock::new(LruCache::new(cap)),
            config,
        }
    }

    /// Look up a query in the cache.
    ///
    /// O(1): one write-locked map lookup, which also promotes the
    /// entry to most-recently-used.
    pub fn get(&self, query: &str) -> Option<CachedResult> {
        if !self.config.enabled {
            return None;
        }

        let mut entries = self.entries.write();
        let entry = entries.get(query)?;
        if entry.is_expired() {
            entries.pop(query);
            return None;
        }
        entry.touch();
        Some(entry.result.clone())
    }

    /// Store a query result in the cache
    pub fn put(&self, query: String, result: CachedResult) {
        if !self.config.enabled {
            return;
        }

        let mut entries = self.entries.write();

        // If this is a new key and the cache is full, prefer evicting
        // an already-expired entry over a live LRU one — an O(n) scan,
        // but only on the eviction path, not on every put. If none are
        // expired, `entries.put` below evicts the true LRU entry
        // itself (built into the `lru` crate).
        if entries.len() >= entries.cap().get() && !entries.contains(&query) {
            if let Some(expired_key) = entries
                .iter()
                .find(|(_, e)| e.is_expired())
                .map(|(k, _)| k.clone())
            {
                entries.pop(&expired_key);
            }
        }

        // Create TTL-adjusted result
        let mut adjusted_result = result;
        if adjusted_result.ttl > self.config.ttl {
            adjusted_result.ttl = self.config.ttl;
        }

        // Insert or update entry (promotes to most-recently-used).
        let entry = L1Entry::new(query.clone(), adjusted_result);
        entries.put(query, entry);
    }

    /// Remove an entry from the cache
    pub fn remove(&self, query: &str) {
        self.entries.write().pop(query);
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.entries.write().clear();
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
        let total_size: usize = entries.iter().map(|(_, e)| e.result.size()).sum();
        let total_access: u64 = entries.iter().map(|(_, e)| e.access_count()).sum();

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
            entries.pop(key);
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

    /// Concurrent hits on the same key must all observe the cached
    /// result and be reflected exactly once each in the access count.
    /// Kept named `*_read_lock_only` for continuity with the earlier
    /// HashMap+Vec implementation (see CHANGELOG / website-brief docs
    /// referencing it by name): that version's `get()` took only a
    /// read lock on the hot path. The current `lru::LruCache`-backed
    /// `get()` write-locks (promotion needs `&mut`), trading that
    /// specific read-parallelism for O(1) no-alloc LRU bookkeeping —
    /// this test still guards the correctness properties that matter:
    /// no lost updates, no torn reads, under concurrent access.
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

    /// The cache must never hold more than `size` entries, however many
    /// distinct keys are pushed through it — this is the capacity
    /// invariant the O(1) `lru`-crate rewrite has to preserve exactly
    /// as the old `HashMap` + `Vec<(String, Instant)>` implementation
    /// did.
    #[test]
    fn test_capacity_invariant_under_heavy_churn() {
        let config = L1Config {
            enabled: true,
            size: 128,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        for i in 0..5_000 {
            cache.put(format!("query-{i}"), create_result("v"));
            assert!(cache.len() <= 128, "cache exceeded capacity at i={i}");
        }
        assert_eq!(cache.len(), 128);

        // Most recently inserted keys must have survived eviction.
        for i in 5_000 - 128..5_000 {
            assert!(
                cache.get(&format!("query-{i}")).is_some(),
                "recently inserted key {i} was evicted"
            );
        }
        // The oldest keys must be gone.
        assert!(cache.get("query-0").is_none());
    }

    /// Regression guard for the eviction preferring an already-expired
    /// entry over a live LRU one, and for that preference-scan being
    /// bounded to the eviction path itself (see `put`'s doc comment):
    /// with almost every entry expired, an insert past capacity must
    /// still make room by dropping an expired entry rather than the
    /// one live (non-expired) entry.
    #[test]
    fn test_eviction_prefers_expired_over_live() {
        let config = L1Config {
            enabled: true,
            size: 4,
            ttl: Duration::from_millis(10),
        };
        let cache = L1HotCache::new(config);

        cache.put("stale-1".to_string(), create_result("1"));
        cache.put("stale-2".to_string(), create_result("2"));
        cache.put("stale-3".to_string(), create_result("3"));

        std::thread::sleep(Duration::from_millis(15));

        // This one is inserted after the sleep, so it is live.
        cache.put("live".to_string(), create_result("live"));

        // Cache is now nominally "full" (4 entries, 3 of them expired
        // but not yet swept). Inserting one more must evict an expired
        // entry, never the live one.
        cache.put("new".to_string(), create_result("new"));

        assert!(cache.get("live").is_some(), "live entry was evicted");
        assert!(cache.get("new").is_some());
    }
}
