//! L1 Hot Cache - uncompressed in-memory cache
//!
//! Features:
//! - LFU, LRU or FIFO eviction ([`EvictionPolicy`])
//! - Size-bounded (total bytes and per-entry bytes)
//! - Table index for invalidation

use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use super::{CacheEntry, EvictionPolicy, LFUEviction, TierStats};
use crate::distribcache::{CacheError, CacheResult, QueryFingerprint};

/// Insertion and last-access sequence numbers of one entry, from the cache's
/// logical clock. FIFO evicts the smallest `inserted`, LRU the smallest
/// `last_access`.
#[derive(Debug, Clone, Copy)]
struct AccessOrder {
    inserted: u64,
    last_access: u64,
}

/// L1 Hot Cache - in-memory with LFU, LRU or FIFO eviction
pub struct HotCache {
    /// Main cache storage
    cache: DashMap<u64, CacheEntry>,

    /// LFU eviction tracker
    eviction: Arc<LFUEviction>,

    /// Insertion / access order for LRU and FIFO eviction
    order: DashMap<u64, AccessOrder>,

    /// Logical clock for `order`
    clock: AtomicU64,

    /// Table to key index for invalidation
    table_index: DashMap<String, HashSet<u64>>,

    /// Current size in bytes
    current_size: AtomicUsize,

    /// Maximum size in bytes
    max_size: usize,

    /// Maximum entry size
    max_entry_size: usize,

    /// Eviction policy
    policy: EvictionPolicy,

    /// Statistics
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl HotCache {
    /// Create a new hot cache
    pub fn new(max_size: usize, max_entry_size: usize, policy: EvictionPolicy) -> Self {
        Self {
            cache: DashMap::new(),
            eviction: Arc::new(LFUEviction::new()),
            order: DashMap::new(),
            clock: AtomicU64::new(0),
            table_index: DashMap::new(),
            current_size: AtomicUsize::new(0),
            max_size,
            max_entry_size,
            policy,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::Relaxed)
    }

    /// Get an entry from the cache
    pub fn get(&self, fingerprint: &QueryFingerprint) -> Option<CacheEntry> {
        let key = self.fingerprint_to_hash(fingerprint);

        if let Some(mut entry) = self.cache.get_mut(&key) {
            // Check TTL
            if entry.is_expired() {
                drop(entry);
                self.remove_entry(key);
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            // Update access count for LFU and access order for LRU
            entry.access_count += 1;
            self.eviction.touch(key);
            let now = self.tick();
            if let Some(mut order) = self.order.get_mut(&key) {
                order.last_access = now;
            }
            self.hits.fetch_add(1, Ordering::Relaxed);

            Some(entry.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert an entry into the cache.
    ///
    /// Fails with [`CacheError::TooLarge`] when the entry exceeds the
    /// per-entry or total size limit; nothing is stored then.
    pub fn insert(&self, fingerprint: QueryFingerprint, entry: CacheEntry) -> CacheResult<()> {
        let entry_size = entry.size();

        let limit = self.max_entry_size.min(self.max_size);
        if entry_size > limit {
            return Err(CacheError::TooLarge(entry_size, limit));
        }

        let key = self.fingerprint_to_hash(&fingerprint);

        // Evict entries if needed
        while self.current_size.load(Ordering::Relaxed) + entry_size > self.max_size {
            if !self.evict_one() {
                break; // No more to evict
            }
        }

        // Remove old entry if exists
        self.remove_entry(key);

        // Index by tables for invalidation
        for table in &entry.tables {
            self.table_index
                .entry(table.clone())
                .or_default()
                .insert(key);
        }

        // Insert entry
        self.cache.insert(key, entry);
        self.current_size.fetch_add(entry_size, Ordering::Relaxed);
        self.eviction.insert(key);
        let now = self.tick();
        self.order.insert(
            key,
            AccessOrder {
                inserted: now,
                last_access: now,
            },
        );
        Ok(())
    }

    /// Key with the smallest order value under `pick`, if any.
    fn oldest_by(&self, pick: impl Fn(&AccessOrder) -> u64) -> Option<u64> {
        self.order
            .iter()
            .min_by_key(|e| pick(e.value()))
            .map(|e| *e.key())
    }

    /// Invalidate entries for a table
    pub fn invalidate_by_table(&self, table: &str) {
        if let Some((_, keys)) = self.table_index.remove(table) {
            for key in keys {
                self.remove_entry(key);
            }
        }
    }

    /// Invalidate a specific entry
    pub fn invalidate(&self, fingerprint: &QueryFingerprint) {
        let key = self.fingerprint_to_hash(fingerprint);
        self.remove_entry(key);
    }

    /// Remove an entry by key
    fn remove_entry(&self, key: u64) {
        if let Some((_, entry)) = self.cache.remove(&key) {
            self.current_size.fetch_sub(entry.size(), Ordering::Relaxed);
            self.eviction.remove(key);
            self.order.remove(&key);

            // Clean up table index
            for table in &entry.tables {
                if let Some(mut keys) = self.table_index.get_mut(table) {
                    keys.remove(&key);
                }
            }
        }
    }

    /// Evict one entry based on policy
    fn evict_one(&self) -> bool {
        let victim = match self.policy {
            EvictionPolicy::LFU => self.eviction.evict_one(),
            EvictionPolicy::LRU => self.oldest_by(|o| o.last_access),
            EvictionPolicy::FIFO => self.oldest_by(|o| o.inserted),
        };
        match victim {
            Some(key) => {
                self.remove_entry(key);
                self.evictions.fetch_add(1, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    /// Convert fingerprint to hash key
    fn fingerprint_to_hash(&self, fingerprint: &QueryFingerprint) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        fingerprint.template.hash(&mut hasher);
        if let Some(param) = fingerprint.param_hash {
            param.hash(&mut hasher);
        }
        hasher.finish()
    }

    /// Get cache statistics
    pub fn stats(&self) -> TierStats {
        TierStats {
            size_bytes: self.current_size.load(Ordering::Relaxed) as u64,
            max_size_bytes: self.max_size as u64,
            entry_count: self.cache.len() as u64,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            compression_ratio: None,
            peer_count: None,
            healthy_peers: None,
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.cache.clear();
        self.table_index.clear();
        self.order.clear();
        self.eviction.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Iterate over all entries (for L3 invalidation broadcast)
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = dashmap::mapref::multiple::RefMulti<'_, u64, CacheEntry>> {
        self.cache.iter()
    }

    /// Check if cache contains a fingerprint
    pub fn contains(&self, fingerprint: &QueryFingerprint) -> bool {
        let key = self.fingerprint_to_hash(fingerprint);
        self.cache.contains_key(&key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(table: &str) -> QueryFingerprint {
        QueryFingerprint::from_query(&format!("SELECT * FROM {}", table))
    }

    #[test]
    fn test_hot_cache_insert_get() {
        let cache = HotCache::new(1024 * 1024, 1024, EvictionPolicy::LFU);
        let fp = QueryFingerprint::from_query("SELECT * FROM users");

        let entry = CacheEntry::new(vec![1, 2, 3], vec!["users".to_string()], 1);
        cache.insert(fp.clone(), entry).unwrap();

        let result = cache.get(&fp);
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, vec![1, 2, 3]);
    }

    #[test]
    fn test_hot_cache_eviction() {
        // Small cache to force eviction
        let cache = HotCache::new(200, 100, EvictionPolicy::LFU);

        let table_names = [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
            "juliet",
        ];
        for name in &table_names {
            let entry = CacheEntry::new(vec![0; 50], vec![], 1);
            cache.insert(fp(name), entry).unwrap();
        }

        // Should have evicted some entries
        assert!(cache.len() < 10);
        assert!(cache.stats().evictions > 0);
    }

    /// LRU evicts the entry whose last access is oldest, not an arbitrary one.
    #[test]
    fn test_lru_evicts_least_recently_used() {
        // Each entry is 50 + 32 = 82 bytes; three fit in 250, a fourth does not.
        let cache = HotCache::new(250, 100, EvictionPolicy::LRU);
        for name in ["a", "b", "c"] {
            cache
                .insert(fp(name), CacheEntry::new(vec![0; 50], vec![], 1))
                .unwrap();
        }
        // Touch "a" and "b": "c" is now the least recently used.
        assert!(cache.get(&fp("a")).is_some());
        assert!(cache.get(&fp("b")).is_some());

        cache
            .insert(fp("d"), CacheEntry::new(vec![0; 50], vec![], 1))
            .unwrap();

        assert!(
            !cache.contains(&fp("c")),
            "LRU must evict the untouched entry"
        );
        for name in ["a", "b", "d"] {
            assert!(cache.contains(&fp(name)), "{name} was evicted");
        }
        assert_eq!(cache.stats().evictions, 1);
    }

    /// FIFO evicts the oldest insertion regardless of later accesses.
    #[test]
    fn test_fifo_evicts_oldest_insertion() {
        let cache = HotCache::new(250, 100, EvictionPolicy::FIFO);
        for name in ["a", "b", "c"] {
            cache
                .insert(fp(name), CacheEntry::new(vec![0; 50], vec![], 1))
                .unwrap();
        }
        // Accesses do not matter for FIFO.
        assert!(cache.get(&fp("a")).is_some());

        cache
            .insert(fp("d"), CacheEntry::new(vec![0; 50], vec![], 1))
            .unwrap();

        assert!(
            !cache.contains(&fp("a")),
            "FIFO must evict the first insertion"
        );
        for name in ["b", "c", "d"] {
            assert!(cache.contains(&fp(name)), "{name} was evicted");
        }
    }

    #[test]
    fn test_hot_cache_invalidate_by_table() {
        let cache = HotCache::new(1024 * 1024, 1024, EvictionPolicy::LFU);

        let fp1 = QueryFingerprint::from_query("SELECT * FROM users WHERE id = 1");
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders WHERE id = 1");

        cache
            .insert(
                fp1.clone(),
                CacheEntry::new(vec![1], vec!["users".to_string()], 1),
            )
            .unwrap();
        cache
            .insert(
                fp2.clone(),
                CacheEntry::new(vec![2], vec!["orders".to_string()], 1),
            )
            .unwrap();

        assert_eq!(cache.len(), 2);

        // Invalidate users table
        cache.invalidate_by_table("users");

        assert_eq!(cache.len(), 1);
        assert!(cache.get(&fp2).is_some());
    }

    #[test]
    fn test_hot_cache_stats() {
        let cache = HotCache::new(1024 * 1024, 1024, EvictionPolicy::LFU);
        let fp = QueryFingerprint::from_query("SELECT * FROM users");

        cache
            .insert(fp.clone(), CacheEntry::new(vec![1], vec![], 1))
            .unwrap();

        // Hit
        cache.get(&fp);
        // Miss
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders");
        cache.get(&fp2);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entry_count, 1);
    }

    /// An oversized entry is rejected with an explicit error, not dropped
    /// silently.
    #[test]
    fn test_max_entry_size() {
        let cache = HotCache::new(1024 * 1024, 100, EvictionPolicy::LFU);

        let fp = QueryFingerprint::from_query("SELECT *");
        let large_entry = CacheEntry::new(vec![0; 200], vec![], 1);
        let err = cache.insert(fp.clone(), large_entry).unwrap_err();
        assert!(matches!(err, CacheError::TooLarge(232, 100)), "got {err:?}");

        assert!(cache.is_empty());
    }
}
