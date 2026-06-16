//! Per-edge query result cache.
//!
//! Hash-keyed by `(query_fingerprint, params_hash)`. Each entry
//! carries a monotonic `version` so an invalidation can sweep
//! everything older than a known commit point in one pass.
//!
//! LRU eviction kicks in at `max_entries`. Concurrent reads share a
//! parking_lot RwLock; concurrent writes are serialised by the same
//! lock — fine because writes are rare on the edge (only on
//! pull-on-miss + invalidation sweeps).

use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;

/// One cached query result.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Monotonic logical version. An invalidation drops every entry
    /// whose version <= invalidation.version.
    pub version: u64,
    /// Pre-encoded PostgreSQL wire-protocol response bytes ready
    /// to write back to the client.
    pub response_bytes: Vec<u8>,
    /// Tables the query touched, used by the home for invalidation
    /// fan-out — empty when the home didn't supply them.
    pub tables: Vec<String>,
    /// Wall-clock entry expiry.
    pub expires_at: Instant,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct EdgeCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub invalidations_received: u64,
    pub entries_evicted: u64,
    pub current_entries: usize,
}

/// LRU + version + TTL cache. Cheap to clone via Arc.
#[derive(Clone)]
pub struct EdgeCache {
    inner: Arc<EdgeCacheInner>,
}

struct EdgeCacheInner {
    max_entries: usize,
    map: RwLock<HashMap<CacheKey, CacheEntry>>,
    /// LRU ordering — newest at the back, oldest at the front.
    lru: RwLock<VecDeque<CacheKey>>,
    next_version: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    invalidations: AtomicU64,
    evictions: AtomicU64,
}

/// Cache key — the (fingerprint, params_hash) pair. Both sides are
/// strings so the same key works whether params are plain text or
/// hashed by an upstream caller.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub fingerprint: String,
    pub params_hash: String,
}

impl CacheKey {
    pub fn new(fingerprint: impl Into<String>, params_hash: impl Into<String>) -> Self {
        Self {
            fingerprint: fingerprint.into(),
            params_hash: params_hash.into(),
        }
    }
}

impl EdgeCache {
    pub fn new(max_entries: usize) -> Self {
        assert!(max_entries > 0, "max_entries must be > 0");
        Self {
            inner: Arc::new(EdgeCacheInner {
                max_entries,
                map: RwLock::new(HashMap::new()),
                lru: RwLock::new(VecDeque::new()),
                next_version: AtomicU64::new(1),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                inserts: AtomicU64::new(0),
                invalidations: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
            }),
        }
    }

    /// Mint a fresh logical version. Used by the home when assigning
    /// version stamps to writes; also used by the cache itself when
    /// inserting locally-cached entries.
    pub fn next_version(&self) -> u64 {
        self.inner.next_version.fetch_add(1, Ordering::Relaxed)
    }

    /// Look up a cache entry. Returns None on miss or expired TTL.
    /// Bumps the LRU on hit; increments hit/miss counters either way.
    pub fn get(&self, key: &CacheKey) -> Option<CacheEntry> {
        let now = Instant::now();
        let map = self.inner.map.read();
        let entry = match map.get(key) {
            Some(e) => e.clone(),
            None => {
                self.inner.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        drop(map);

        if entry.expires_at <= now {
            // Lazy expiry: evict on read so we don't bloat memory
            // with stale entries even when nothing else touches them.
            self.inner.map.write().remove(key);
            self.inner.lru.write().retain(|k| k != key);
            self.inner.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // Bump LRU.
        let mut lru = self.inner.lru.write();
        lru.retain(|k| k != key);
        lru.push_back(key.clone());

        self.inner.hits.fetch_add(1, Ordering::Relaxed);
        Some(entry)
    }

    /// Insert / overwrite an entry. Triggers LRU eviction if
    /// over capacity.
    pub fn insert(&self, key: CacheKey, entry: CacheEntry) {
        {
            let mut map = self.inner.map.write();
            let mut lru = self.inner.lru.write();
            if map.insert(key.clone(), entry).is_none() {
                lru.push_back(key.clone());
            } else {
                lru.retain(|k| k != &key);
                lru.push_back(key);
            }
            self.inner.inserts.fetch_add(1, Ordering::Relaxed);

            // LRU eviction.
            while map.len() > self.inner.max_entries {
                if let Some(victim) = lru.pop_front() {
                    map.remove(&victim);
                    self.inner.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }
        }
    }

    /// Drop every entry whose version <= `up_to_version` AND whose
    /// `tables` overlaps with `tables` (empty `tables` invalidates
    /// every entry meeting the version bound). Returns the count
    /// dropped.
    pub fn invalidate(&self, up_to_version: u64, tables: &[String]) -> u64 {
        self.inner.invalidations.fetch_add(1, Ordering::Relaxed);
        let mut map = self.inner.map.write();
        let mut lru = self.inner.lru.write();
        let mut drop_keys = Vec::new();
        for (k, e) in map.iter() {
            if e.version > up_to_version {
                continue;
            }
            if tables.is_empty() {
                drop_keys.push(k.clone());
                continue;
            }
            if e.tables.iter().any(|t| tables.contains(t)) {
                drop_keys.push(k.clone());
            }
        }
        for k in &drop_keys {
            map.remove(k);
            lru.retain(|x| x != k);
        }
        drop_keys.len() as u64
    }

    pub fn stats(&self) -> EdgeCacheStats {
        EdgeCacheStats {
            hits: self.inner.hits.load(Ordering::Relaxed),
            misses: self.inner.misses.load(Ordering::Relaxed),
            inserts: self.inner.inserts.load(Ordering::Relaxed),
            invalidations_received: self
                .inner
                .invalidations
                .load(Ordering::Relaxed),
            entries_evicted: self.inner.evictions.load(Ordering::Relaxed),
            current_entries: self.inner.map.read().len(),
        }
    }

    /// Test-only: deterministic insert with explicit version + TTL.
    pub fn insert_with(&self, key: CacheKey, entry: CacheEntry) {
        self.insert(key, entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(version: u64, body: &[u8], tables: &[&str], ttl: Duration) -> CacheEntry {
        CacheEntry {
            version,
            response_bytes: body.to_vec(),
            tables: tables.iter().map(|s| s.to_string()).collect(),
            expires_at: Instant::now() + ttl,
        }
    }

    #[test]
    fn insert_then_get_returns_value() {
        let c = EdgeCache::new(10);
        let k = CacheKey::new("fp1", "p1");
        c.insert(k.clone(), entry(1, b"row", &["users"], Duration::from_secs(60)));
        let got = c.get(&k).expect("hit");
        assert_eq!(got.response_bytes, b"row");
    }

    #[test]
    fn miss_returns_none() {
        let c = EdgeCache::new(10);
        assert!(c.get(&CacheKey::new("fp1", "p1")).is_none());
        assert_eq!(c.stats().misses, 1);
    }

    #[test]
    fn expired_entry_is_dropped_on_read() {
        let c = EdgeCache::new(10);
        let k = CacheKey::new("fp1", "p1");
        // Insert with a 0-duration TTL — already expired.
        let mut e = entry(1, b"x", &[], Duration::from_secs(0));
        e.expires_at = Instant::now() - Duration::from_millis(1);
        c.insert(k.clone(), e);
        assert!(c.get(&k).is_none());
        assert_eq!(c.stats().current_entries, 0);
    }

    #[test]
    fn lru_evicts_oldest_when_over_capacity() {
        let c = EdgeCache::new(3);
        for i in 0..5 {
            let k = CacheKey::new(format!("fp{}", i), "p");
            c.insert(k, entry(i, b"x", &[], Duration::from_secs(60)));
        }
        // Capacity 3, inserted 5 → 2 evictions.
        assert_eq!(c.stats().entries_evicted, 2);
        assert_eq!(c.stats().current_entries, 3);
        // The two oldest (fp0, fp1) should be gone.
        assert!(c.get(&CacheKey::new("fp0", "p")).is_none());
        assert!(c.get(&CacheKey::new("fp1", "p")).is_none());
        assert!(c.get(&CacheKey::new("fp4", "p")).is_some());
    }

    #[test]
    fn lru_promotes_recently_read_entries() {
        let c = EdgeCache::new(3);
        for i in 0..3 {
            let k = CacheKey::new(format!("fp{}", i), "p");
            c.insert(k, entry(i, b"x", &[], Duration::from_secs(60)));
        }
        // Read fp0 — promotes it to the back of the LRU.
        let _ = c.get(&CacheKey::new("fp0", "p"));
        // Insert one more, should evict fp1 (now the oldest).
        c.insert(
            CacheKey::new("fp3", "p"),
            entry(3, b"x", &[], Duration::from_secs(60)),
        );
        assert!(c.get(&CacheKey::new("fp0", "p")).is_some());
        assert!(c.get(&CacheKey::new("fp1", "p")).is_none());
        assert!(c.get(&CacheKey::new("fp2", "p")).is_some());
        assert!(c.get(&CacheKey::new("fp3", "p")).is_some());
    }

    #[test]
    fn invalidate_drops_old_versions_only() {
        let c = EdgeCache::new(10);
        c.insert(
            CacheKey::new("fp1", "p"),
            entry(5, b"v5", &["users"], Duration::from_secs(60)),
        );
        c.insert(
            CacheKey::new("fp2", "p"),
            entry(10, b"v10", &["users"], Duration::from_secs(60)),
        );
        let dropped = c.invalidate(7, &["users".to_string()]);
        assert_eq!(dropped, 1);
        assert!(c.get(&CacheKey::new("fp1", "p")).is_none());
        assert!(c.get(&CacheKey::new("fp2", "p")).is_some());
    }

    #[test]
    fn invalidate_filters_by_tables() {
        let c = EdgeCache::new(10);
        c.insert(
            CacheKey::new("fp1", "p"),
            entry(5, b"x", &["users"], Duration::from_secs(60)),
        );
        c.insert(
            CacheKey::new("fp2", "p"),
            entry(5, b"y", &["orders"], Duration::from_secs(60)),
        );
        let dropped = c.invalidate(100, &["users".to_string()]);
        assert_eq!(dropped, 1);
        assert!(c.get(&CacheKey::new("fp1", "p")).is_none());
        assert!(c.get(&CacheKey::new("fp2", "p")).is_some());
    }

    #[test]
    fn invalidate_with_no_tables_drops_everything_within_version() {
        let c = EdgeCache::new(10);
        c.insert(
            CacheKey::new("fp1", "p"),
            entry(5, b"x", &["users"], Duration::from_secs(60)),
        );
        c.insert(
            CacheKey::new("fp2", "p"),
            entry(10, b"y", &["orders"], Duration::from_secs(60)),
        );
        let dropped = c.invalidate(7, &[]);
        assert_eq!(dropped, 1, "fp1 (v5) should be dropped, fp2 (v10) kept");
    }

    #[test]
    fn next_version_is_monotonic() {
        let c = EdgeCache::new(10);
        let v1 = c.next_version();
        let v2 = c.next_version();
        let v3 = c.next_version();
        assert!(v1 < v2 && v2 < v3);
    }

    #[test]
    fn stats_track_hits_and_misses() {
        let c = EdgeCache::new(10);
        let k = CacheKey::new("fp1", "p");
        c.insert(k.clone(), entry(1, b"x", &[], Duration::from_secs(60)));
        let _ = c.get(&k);
        let _ = c.get(&k);
        let _ = c.get(&CacheKey::new("missing", "p"));
        let s = c.stats();
        assert_eq!(s.hits, 2);
        assert_eq!(s.misses, 1);
        assert_eq!(s.inserts, 1);
    }

    #[test]
    fn panics_on_zero_capacity() {
        let res = std::panic::catch_unwind(|| EdgeCache::new(0));
        assert!(res.is_err());
    }
}
