//! L1 Hot Cache - In-memory cache with <100μs access time
//!
//! Features:
//! - LRU/LFU eviction with frequency aging
//! - Per-session affinity for connection locality
//! - Automatic size management

use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;

use super::{CacheEntry, CacheKey, EvictionPolicy, LFUEviction, TierStats};
use crate::distribcache::{QueryFingerprint, SessionId};

/// L1 Hot Cache - In-memory with LRU/LFU eviction
pub struct HotCache {
    /// Main cache storage
    cache: DashMap<u64, CacheEntry>,

    /// LFU eviction tracker
    eviction: Arc<LFUEviction>,

    /// Per-session affinity tracking
    session_affinity: DashMap<SessionId, HashSet<u64>>,

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
            session_affinity: DashMap::new(),
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

    /// Get an entry from the cache
    pub fn get(&self, fingerprint: &QueryFingerprint, session: SessionId) -> Option<CacheEntry> {
        let key = self.fingerprint_to_hash(fingerprint);

        if let Some(mut entry) = self.cache.get_mut(&key) {
            // Check TTL
            if entry.is_expired() {
                drop(entry);
                self.remove_entry(key);
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            // Update access count for LFU
            entry.access_count += 1;
            self.eviction.touch(key);
            self.hits.fetch_add(1, Ordering::Relaxed);

            Some(entry.clone())
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    /// Insert an entry into the cache
    pub fn insert(
        &self,
        fingerprint: QueryFingerprint,
        entry: CacheEntry,
        session: Option<SessionId>,
    ) {
        let entry_size = entry.size();

        // Skip if entry is too large
        if entry_size > self.max_entry_size {
            return;
        }

        let key = self.fingerprint_to_hash(&fingerprint);

        // Evict entries if needed
        while self.current_size.load(Ordering::Relaxed) + entry_size > self.max_size {
            if !self.evict_one() {
                break; // No more to evict
            }
        }

        // Remove old entry if exists
        if let Some((_, old_entry)) = self.cache.remove(&key) {
            self.current_size.fetch_sub(old_entry.size(), Ordering::Relaxed);
            self.eviction.remove(key);
        }

        // Index by tables for invalidation
        for table in &entry.tables {
            self.table_index
                .entry(table.clone())
                .or_default()
                .insert(key);
        }

        // Track session affinity
        if let Some(sid) = session {
            self.session_affinity
                .entry(sid)
                .or_default()
                .insert(key);
        }

        // Insert entry
        self.cache.insert(key, entry);
        self.current_size.fetch_add(entry_size, Ordering::Relaxed);
        self.eviction.insert(key);
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
        match self.policy {
            EvictionPolicy::LFU | EvictionPolicy::Adaptive => {
                if let Some(key) = self.eviction.evict_one() {
                    self.remove_entry(key);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            }
            EvictionPolicy::LRU => {
                // Find oldest entry (simplified - would need timestamp tracking)
                if let Some(entry) = self.cache.iter().next() {
                    let key = *entry.key();
                    drop(entry);
                    self.remove_entry(key);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            }
            EvictionPolicy::FIFO => {
                // Same as LRU for simplicity
                if let Some(entry) = self.cache.iter().next() {
                    let key = *entry.key();
                    drop(entry);
                    self.remove_entry(key);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            }
        }
        false
    }

    /// Convert fingerprint to hash key
    fn fingerprint_to_hash(&self, fingerprint: &QueryFingerprint) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

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
        self.session_affinity.clear();
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
    pub fn iter(&self) -> impl Iterator<Item = dashmap::mapref::multiple::RefMulti<'_, u64, CacheEntry>> {
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
    use std::time::Duration;

    #[test]
    fn test_hot_cache_insert_get() {
        let cache = HotCache::new(1024 * 1024, 1024, EvictionPolicy::LFU);
        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        let session = SessionId::new("sess-1");

        let entry = CacheEntry::new(vec![1, 2, 3], vec!["users".to_string()], 1);
        cache.insert(fp.clone(), entry, Some(session.clone()));

        let result = cache.get(&fp, session);
        assert!(result.is_some());
        assert_eq!(result.unwrap().data, vec![1, 2, 3]);
    }

    #[test]
    fn test_hot_cache_eviction() {
        // Small cache to force eviction
        let cache = HotCache::new(200, 100, EvictionPolicy::LFU);

        let table_names = ["alpha", "bravo", "charlie", "delta", "echo",
                           "foxtrot", "golf", "hotel", "india", "juliet"];
        for name in &table_names {
            let fp = QueryFingerprint::from_query(&format!("SELECT * FROM {}", name));
            let entry = CacheEntry::new(vec![0; 50], vec![], 1);
            cache.insert(fp, entry, None);
        }

        // Should have evicted some entries
        assert!(cache.len() < 10);
        assert!(cache.stats().evictions > 0);
    }

    #[test]
    fn test_hot_cache_invalidate_by_table() {
        let cache = HotCache::new(1024 * 1024, 1024, EvictionPolicy::LFU);

        let fp1 = QueryFingerprint::from_query("SELECT * FROM users WHERE id = 1");
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders WHERE id = 1");

        cache.insert(
            fp1.clone(),
            CacheEntry::new(vec![1], vec!["users".to_string()], 1),
            None,
        );
        cache.insert(
            fp2.clone(),
            CacheEntry::new(vec![2], vec!["orders".to_string()], 1),
            None,
        );

        assert_eq!(cache.len(), 2);

        // Invalidate users table
        cache.invalidate_by_table("users");

        assert_eq!(cache.len(), 1);
        assert!(cache.get(&fp2, SessionId::new("")).is_some());
    }

    #[test]
    fn test_hot_cache_stats() {
        let cache = HotCache::new(1024 * 1024, 1024, EvictionPolicy::LFU);
        let fp = QueryFingerprint::from_query("SELECT * FROM users");
        let session = SessionId::new("test");

        cache.insert(fp.clone(), CacheEntry::new(vec![1], vec![], 1), None);

        // Hit
        cache.get(&fp, session.clone());
        // Miss
        let fp2 = QueryFingerprint::from_query("SELECT * FROM orders");
        cache.get(&fp2, session);

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entry_count, 1);
    }

    #[test]
    fn test_max_entry_size() {
        let cache = HotCache::new(1024 * 1024, 100, EvictionPolicy::LFU);

        // Entry larger than max should not be inserted
        let fp = QueryFingerprint::from_query("SELECT *");
        let large_entry = CacheEntry::new(vec![0; 200], vec![], 1);
        cache.insert(fp.clone(), large_entry, None);

        assert!(cache.is_empty());
    }
}
