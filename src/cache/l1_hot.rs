//! L1 Hot Cache
//!
//! Per-connection, exact-match cache with LRU eviction.
//! Provides sub-microsecond latency for repeated queries.
//!
//! Backed by `lru::LruCache`, an intrusive linked hash map: `get`/`put`
//! are O(1), including eviction — when `put` inserts a new key past
//! capacity, `lru::LruCache` itself evicts the true least-recently-used
//! entry in O(1), with no scan over the map. That replaces a previous
//! `HashMap` + `Vec<(String, Instant)>` pair whose LRU bookkeeping did
//! an O(n) `retain` scan plus a `String` allocation on every single hit
//! and put, with eviction doing an O(n) scan per candidate (O(n^2)
//! overall). A single `parking_lot::RwLock` (write-locked for both
//! reads and writes, since promotion needs `&mut`) guards the map —
//! no poisoning, no `.unwrap()` on every lock acquire.
//!
//! Expired entries are not preferentially evicted on insert — that
//! would require an O(n) scan of the map on every full-cache put,
//! which is exactly the cost this rewrite removes. Instead, TTL
//! reclamation happens in two places: `get()` checks `is_expired()`
//! on the entry it just looked up and evicts it in place before
//! returning `None`, and the periodic `evict_expired()` sweep removes
//! any expired entries regardless of LRU order. Between those, an
//! expired-but-not-yet-swept entry can still be evicted by plain LRU
//! order ahead of a live entry that hasn't been touched recently —
//! that's an accepted trade of strict expiry-preference for O(1) puts.

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

        // No eviction bookkeeping here: `entries.put` below is O(1) and
        // evicts the true least-recently-used entry itself when the
        // cache is full and this is a new key (built into the `lru`
        // crate). Expired entries are reclaimed lazily — on `get()`,
        // when a lookup lands on one, or in bulk by `evict_expired()` —
        // not preferentially on insert, which would need an O(n) scan.

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
    /// result and be reflected exactly once each in the access count:
    /// no lost updates, no torn reads. `get()` write-locks the map
    /// (promoting to most-recently-used needs `&mut`), so hits on the
    /// same entry serialize briefly on that lock rather than running
    /// under a shared read lock the way the earlier HashMap+Vec
    /// implementation did — see the module doc comment and
    /// `docs/internal/website-brief-connection-routing.md`. What this
    /// test actually guards is unchanged: every thread gets the right
    /// data, and the access counter ends up exactly right.
    #[test]
    fn test_concurrent_hits_no_lost_updates() {
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

    /// Discriminates true LRU-order eviction from insertion-order
    /// eviction: with capacity 3, touching `a` via `get` must move it
    /// ahead of `b` in recency, so the next insert past capacity evicts
    /// `b` (the actual least-recently-used key), not `a` (the oldest
    /// insert). A cache that evicted by insertion order rather than
    /// access order would fail this.
    #[test]
    fn test_put_on_full_cache_evicts_least_recently_used() {
        let config = L1Config {
            enabled: true,
            size: 3,
            ttl: Duration::from_secs(60),
        };
        let cache = L1HotCache::new(config);

        cache.put("a".to_string(), create_result("a"));
        cache.put("b".to_string(), create_result("b"));
        cache.put("c".to_string(), create_result("c"));

        // Touch `a` so `b` becomes the least-recently-used key.
        assert!(cache.get("a").is_some());

        cache.put("d".to_string(), create_result("d"));

        assert!(
            cache.get("b").is_none(),
            "least-recently-used key b was not evicted"
        );
        assert!(
            cache.get("a").is_some(),
            "recently-touched key a was evicted"
        );
        assert!(cache.get("c").is_some(), "key c was evicted");
        assert!(cache.get("d").is_some(), "newly inserted key d was evicted");
    }

    /// An expired entry is never returned by `get`, and `get` removes
    /// it from the map on that read rather than leaving it to a later
    /// `evict_expired()` sweep.
    #[test]
    fn test_get_purges_expired_entry_on_read() {
        let config = L1Config {
            enabled: true,
            size: 100,
            ttl: Duration::from_millis(10),
        };
        let cache = L1HotCache::new(config);

        cache.put("query".to_string(), create_result("data"));
        assert_eq!(cache.len(), 1);

        std::thread::sleep(Duration::from_millis(15));

        assert!(cache.get("query").is_none(), "expired entry was returned");
        assert_eq!(
            cache.len(),
            0,
            "expired entry was not removed by the read that found it expired"
        );
    }
}
