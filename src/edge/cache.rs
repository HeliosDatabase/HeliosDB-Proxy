//! Per-edge query result cache.
//!
//! Keyed by `(fingerprint, params_hash, database, user)`. The tenant
//! identity (database/user) is carried **verbatim** in the key — a
//! 64-bit `params_hash` collision alone can never alias two tenants'
//! entries (mirrors the query-cache `CacheKey`).
//!
//! Internals: one `std::sync::Mutex` over the LRU map plus a
//! `table -> keys` reverse index — `get`/`insert` are O(1), and a
//! table-targeted `invalidate` touches only the keys registered under
//! the written tables (O(matching entries), not O(all entries)). Only
//! an *empty* table set (an unparseable write) falls back to the full
//! scan. The lock is never held across an await (every method is sync).
//!
//! ## Version domains
//!
//! Entry `version` stamps and invalidation `up_to_version` bounds must
//! come from the SAME clock or sweeps are meaningless:
//!
//! - **Home role**: one local counter (`next_version`) stamps read
//!   misses and mints write versions — a single domain.
//! - **Edge role**: writes are versioned by the HOME, so edge entries
//!   are stamped with the **last observed home version**
//!   (`observed_home_version`), never with local mints. Any read that
//!   began before a home write therefore carries a stamp `<` that
//!   write's version and is always swept by its invalidation.
//!
//! Store races are closed per role: the home re-checks the
//! `invalidated_hwm` under the map lock (`insert_if_fresh`), the edge
//! re-checks the invalidation epoch under the map lock
//! (`insert_if_epoch`) — both piggyback on `invalidate()` bumping its
//! atomics *before* taking the map lock, so either the store observes
//! the bump and skips, or the sweep runs after the insert and drops it.
//!
//! ## Home epochs
//!
//! The home's version clock is per-process; a home restart resets it.
//! Every `InvalidationEvent` (and the subscribe-time hello frame)
//! carries the home's per-boot `epoch`; when the edge observes an
//! epoch change (`on_home_epoch`) it flushes everything and resets its
//! observed-home clock, so post-restart invalidations sweep correctly
//! instead of silently no-op'ing against pre-restart stamps.

use bytes::Bytes;
use lru::LruCache;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use serde::Serialize;

/// One cached query result.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    /// Logical version in the invalidation clock's domain (home role:
    /// local mint; edge role: last observed home version). An
    /// invalidation drops every entry whose version <=
    /// invalidation.version.
    pub version: u64,
    /// Pre-encoded PostgreSQL wire-protocol response bytes ready
    /// to write back to the client. `Bytes` so the store path shares
    /// one refcounted buffer with the query-cache instead of copying.
    pub response_bytes: Bytes,
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

/// Map + reverse index, guarded by ONE mutex so they can never drift.
struct MapState {
    lru: LruCache<CacheKey, Arc<CacheEntry>>,
    /// table -> keys of live entries referencing it. Maintained on
    /// every insert/pop/eviction, so `invalidate(tables)` visits only
    /// candidate keys instead of scanning the whole LRU.
    by_table: HashMap<String, HashSet<CacheKey>>,
}

impl MapState {
    fn index_add(&mut self, key: &CacheKey, tables: &[String]) {
        for t in tables {
            self.by_table
                .entry(t.clone())
                .or_default()
                .insert(key.clone());
        }
    }

    fn index_remove(&mut self, key: &CacheKey, tables: &[String]) {
        for t in tables {
            if let Some(set) = self.by_table.get_mut(t) {
                set.remove(key);
                if set.is_empty() {
                    self.by_table.remove(t);
                }
            }
        }
    }

    /// Pop `key` and unindex it. Returns the removed entry.
    fn pop(&mut self, key: &CacheKey) -> Option<Arc<CacheEntry>> {
        let entry = self.lru.pop(key)?;
        let tables = entry.tables.clone();
        self.index_remove(key, &tables);
        Some(entry)
    }
}

struct EdgeCacheInner {
    /// Single lock over the LRU map + table index. Entries are
    /// `Arc`-wrapped so a hit clones a pointer, not the response bytes.
    map: Mutex<MapState>,
    next_version: AtomicU64,
    /// Highest home version observed (edge role) — SSE invalidations
    /// and the hello frame feed it. Edge-role reads are stamped with
    /// this value so home invalidations (which are strictly greater)
    /// always sweep them.
    observed_home: AtomicU64,
    /// The home's per-boot epoch as last seen by this edge (0 =
    /// unknown / never seen). A change means the home restarted and
    /// its version clock reset — flush everything.
    home_epoch: AtomicU64,
    /// This process's own per-boot epoch (home role: carried in every
    /// broadcast so edges can detect a restart). Never 0.
    epoch: u64,
    /// Highest `up_to_version` seen by `invalidate()`. Home-role reads
    /// stamped at or below this must not be cached — a write
    /// invalidated their tables while they were in flight.
    invalidated_hwm: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
    /// Also serves as the store-race "epoch": bumped BEFORE the map
    /// lock is taken by `invalidate`/`flush_all`, and re-checked under
    /// the map lock by `insert_if_epoch`.
    invalidations: AtomicU64,
    evictions: AtomicU64,
}

/// Cache key. `database`/`user` are verbatim tenant identity — two
/// tenants can never alias through a `params_hash` collision.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub fingerprint: String,
    pub params_hash: String,
    pub database: String,
    pub user: String,
}

impl CacheKey {
    /// Tenant-less constructor kept for tests/tools; production call
    /// sites populate `database`/`user` explicitly.
    pub fn new(fingerprint: impl Into<String>, params_hash: impl Into<String>) -> Self {
        Self {
            fingerprint: fingerprint.into(),
            params_hash: params_hash.into(),
            database: String::new(),
            user: String::new(),
        }
    }
}

/// Per-boot epoch: unique across restarts of the same host with
/// overwhelming probability (nanos since UNIX epoch XOR rotated pid),
/// and never the 0 legacy sentinel.
fn mint_process_epoch() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = (std::process::id() as u64).rotate_left(48);
    (nanos ^ pid).max(1)
}

impl EdgeCache {
    pub fn new(max_entries: usize) -> Self {
        let cap = NonZeroUsize::new(max_entries).expect("max_entries must be > 0");
        Self {
            inner: Arc::new(EdgeCacheInner {
                map: Mutex::new(MapState {
                    lru: LruCache::new(cap),
                    by_table: HashMap::new(),
                }),
                next_version: AtomicU64::new(1),
                observed_home: AtomicU64::new(0),
                home_epoch: AtomicU64::new(0),
                epoch: mint_process_epoch(),
                invalidated_hwm: AtomicU64::new(0),
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                inserts: AtomicU64::new(0),
                invalidations: AtomicU64::new(0),
                evictions: AtomicU64::new(0),
            }),
        }
    }

    /// Lock the map, recovering from poisoning — the cache holds no
    /// cross-entry invariants worth propagating a panic for (the
    /// index is rebuilt consistently by every mutation path).
    fn lock(&self) -> MutexGuard<'_, MapState> {
        self.inner.map.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// This process's per-boot epoch (home role: stamped into every
    /// broadcast invalidation + the subscribe hello frame).
    pub fn epoch(&self) -> u64 {
        self.inner.epoch
    }

    /// Mint a fresh logical version (home role: stamps read misses and
    /// write invalidations from one local clock).
    pub fn next_version(&self) -> u64 {
        self.inner.next_version.fetch_add(1, Ordering::Relaxed)
    }

    /// Current value of the local version counter, without minting.
    /// Strictly greater than every version this process has stamped.
    pub fn current_version(&self) -> u64 {
        self.inner.next_version.load(Ordering::Relaxed)
    }

    /// Record a version observed from the home (edge role: SSE
    /// invalidations + the hello frame carry the home's clock). Edge
    /// reads are stamped with `observed_home_version()`, i.e. *at* the
    /// last home version — any later home write mints a strictly
    /// greater version, so its `<=` sweep always catches them.
    pub fn observe_home_version(&self, v: u64) {
        self.inner.observed_home.fetch_max(v, Ordering::Relaxed);
        self.inner
            .next_version
            .fetch_max(v.saturating_add(1), Ordering::Relaxed);
    }

    /// Last home version observed (0 until the first event/hello).
    /// The edge-role read stamp.
    pub fn observed_home_version(&self) -> u64 {
        self.inner.observed_home.load(Ordering::Relaxed)
    }

    /// Handle the home's per-boot epoch carried by an event. Returns
    /// the number of entries flushed. `0` (legacy/absent) is ignored.
    /// First sighting just records the epoch; a CHANGE means the home
    /// restarted with a reset version clock — every cached stamp is
    /// from an incomparable clock, so flush everything and reset the
    /// observed-home clock (the same event's `observe_home_version`
    /// re-syncs it).
    pub fn on_home_epoch(&self, epoch: u64) -> usize {
        if epoch == 0 {
            return 0;
        }
        let prev = self.inner.home_epoch.swap(epoch, Ordering::Relaxed);
        if prev == 0 || prev == epoch {
            return 0;
        }
        self.inner.observed_home.store(0, Ordering::Relaxed);
        self.flush_all()
    }

    /// Drop every entry. Bumps the invalidation counter BEFORE taking
    /// the map lock so in-flight epoch-gated stores are rejected (same
    /// ordering contract as `invalidate`). Returns the count dropped.
    pub fn flush_all(&self) -> usize {
        self.inner.invalidations.fetch_add(1, Ordering::Relaxed);
        let mut map = self.lock();
        let n = map.lru.len();
        map.lru.clear();
        map.by_table.clear();
        n
    }

    /// Whether a home-role read stamped with `version` is still safe
    /// to cache. False once an invalidation with `up_to_version >=
    /// version` has been applied — the read may predate the write that
    /// triggered it. (Cheap unlocked fast-path; the authoritative
    /// re-check is `insert_if_fresh`.)
    pub fn should_cache(&self, version: u64) -> bool {
        version > self.inner.invalidated_hwm.load(Ordering::Relaxed)
    }

    /// Monotonic count of invalidation events applied. Edge-role reads
    /// snapshot it before forwarding; `insert_if_epoch` re-checks it
    /// under the map lock, so a store whose flight overlapped ANY
    /// invalidation is skipped (table-agnostic and conservative — the
    /// next identical read simply re-stores).
    pub fn invalidation_epoch(&self) -> u64 {
        self.inner.invalidations.load(Ordering::Relaxed)
    }

    /// Look up a cache entry. Returns None on miss or expired TTL.
    /// Bumps the LRU on hit (O(1)); increments hit/miss counters
    /// either way. Expired entries are removed lazily here.
    pub fn get(&self, key: &CacheKey) -> Option<Arc<CacheEntry>> {
        let now = Instant::now();
        let mut map = self.lock();
        match map.lru.get(key) {
            Some(e) if e.expires_at > now => {
                let out = Arc::clone(e);
                drop(map);
                self.inner.hits.fetch_add(1, Ordering::Relaxed);
                Some(out)
            }
            Some(_) => {
                // Lazy expiry: evict on read so we don't bloat memory
                // with stale entries even when nothing else touches
                // them. Counted as a miss, not an eviction.
                map.pop(key);
                drop(map);
                self.inner.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
            None => {
                drop(map);
                self.inner.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert / overwrite an entry. The LRU victim is auto-evicted
    /// when at capacity (O(1)). Prefer the race-checked variants on
    /// production store paths.
    pub fn insert(&self, key: CacheKey, entry: CacheEntry) {
        let mut map = self.lock();
        self.insert_locked(&mut map, key, entry);
    }

    /// Home-role store: re-checks the invalidation high-water mark
    /// UNDER the map lock, closing the TOCTOU between an unlocked
    /// `should_cache` and the push (a concurrent `invalidate` bumps
    /// the hwm before taking this same lock, so either we see the bump
    /// here and skip, or our insert lands first and its sweep drops
    /// the entry). Returns whether the entry was stored.
    pub fn insert_if_fresh(&self, key: CacheKey, entry: CacheEntry) -> bool {
        let mut map = self.lock();
        if entry.version <= self.inner.invalidated_hwm.load(Ordering::Relaxed) {
            return false;
        }
        self.insert_locked(&mut map, key, entry);
        true
    }

    /// Edge-role store: entry stamps live in the home's clock, so the
    /// hwm gate is inert (stamps == hwm right after every event).
    /// Instead the store is valid only if NO invalidation was applied
    /// since `epoch` was snapshotted (before the read was forwarded).
    /// Checked under the map lock — same ordering proof as
    /// `insert_if_fresh` (invalidate bumps the counter before locking).
    /// Returns whether the entry was stored.
    pub fn insert_if_epoch(&self, key: CacheKey, entry: CacheEntry, epoch: u64) -> bool {
        let mut map = self.lock();
        if self.inner.invalidations.load(Ordering::Relaxed) != epoch {
            return false;
        }
        self.insert_locked(&mut map, key, entry);
        true
    }

    fn insert_locked(&self, map: &mut MapState, key: CacheKey, entry: CacheEntry) {
        let tables = entry.tables.clone();
        // `push` returns the displaced pair: the old value when the
        // key was already present (an update, not an eviction), or
        // the LRU victim when capacity forced one out.
        let updating = map.lru.contains(&key);
        map.index_add(&key, &tables);
        let displaced = map.lru.push(key, Arc::new(entry));
        if let Some((old_key, old_entry)) = &displaced {
            if updating {
                // Same-key overwrite: drop index links the new entry
                // no longer has (index_add above re-added the shared
                // ones).
                let stale: Vec<String> = old_entry
                    .tables
                    .iter()
                    .filter(|t| !tables.contains(t))
                    .cloned()
                    .collect();
                map.index_remove(old_key, &stale);
            } else {
                let victim_tables = old_entry.tables.clone();
                map.index_remove(old_key, &victim_tables);
            }
        }
        self.inner.inserts.fetch_add(1, Ordering::Relaxed);
        if displaced.is_some() && !updating {
            self.inner.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Drop every entry whose version <= `up_to_version` AND whose
    /// `tables` overlaps with `tables` (empty `tables` invalidates
    /// every entry meeting the version bound). Also raises the
    /// high-water mark consulted by `should_cache` and bumps the
    /// invalidation epoch — both BEFORE the map lock is taken, which
    /// the `insert_if_*` race checks rely on. Returns the count
    /// dropped.
    ///
    /// Table-targeted sweeps walk only the keys registered under the
    /// written tables (reverse index) — O(matching entries). Only the
    /// empty-table wildcard falls back to a full scan.
    pub fn invalidate(&self, up_to_version: u64, tables: &[String]) -> usize {
        self.inner.invalidations.fetch_add(1, Ordering::Relaxed);
        self.inner
            .invalidated_hwm
            .fetch_max(up_to_version, Ordering::Relaxed);
        let mut map = self.lock();
        let drop_keys: Vec<CacheKey> = if tables.is_empty() {
            map.lru
                .iter()
                .filter(|(_, e)| e.version <= up_to_version)
                .map(|(k, _)| k.clone())
                .collect()
        } else {
            let mut keys: HashSet<CacheKey> = HashSet::new();
            for t in tables {
                if let Some(set) = map.by_table.get(t) {
                    for k in set {
                        keys.insert(k.clone());
                    }
                }
            }
            keys.into_iter()
                .filter(|k| {
                    map.lru
                        .peek(k)
                        .map(|e| e.version <= up_to_version)
                        .unwrap_or(false)
                })
                .collect()
        };
        for k in &drop_keys {
            map.pop(k);
        }
        drop_keys.len()
    }

    pub fn stats(&self) -> EdgeCacheStats {
        EdgeCacheStats {
            hits: self.inner.hits.load(Ordering::Relaxed),
            misses: self.inner.misses.load(Ordering::Relaxed),
            inserts: self.inner.inserts.load(Ordering::Relaxed),
            invalidations_received: self.inner.invalidations.load(Ordering::Relaxed),
            entries_evicted: self.inner.evictions.load(Ordering::Relaxed),
            current_entries: self.lock().lru.len(),
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
    use std::time::Duration;

    fn entry(version: u64, body: &[u8], tables: &[&str], ttl: Duration) -> CacheEntry {
        CacheEntry {
            version,
            response_bytes: Bytes::copy_from_slice(body),
            tables: tables.iter().map(|s| s.to_string()).collect(),
            expires_at: Instant::now() + ttl,
        }
    }

    #[test]
    fn insert_then_get_returns_value() {
        let c = EdgeCache::new(10);
        let k = CacheKey::new("fp1", "p1");
        c.insert(
            k.clone(),
            entry(1, b"row", &["users"], Duration::from_secs(60)),
        );
        let got = c.get(&k).expect("hit");
        assert_eq!(&got.response_bytes[..], b"row");
    }

    #[test]
    fn miss_returns_none() {
        let c = EdgeCache::new(10);
        assert!(c.get(&CacheKey::new("fp1", "p1")).is_none());
        assert_eq!(c.stats().misses, 1);
    }

    #[test]
    fn key_isolates_database_and_user_verbatim() {
        // F13: tenant identity is part of key EQUALITY, not just the
        // hash — a params_hash collision alone can never alias tenants.
        let c = EdgeCache::new(10);
        let a = CacheKey {
            fingerprint: "FP".into(),
            params_hash: "SAMEHASH".into(),
            database: "tenant_a".into(),
            user: "alice".into(),
        };
        let b = CacheKey {
            database: "tenant_b".into(),
            ..a.clone()
        };
        let u = CacheKey {
            user: "bob".into(),
            ..a.clone()
        };
        assert_ne!(a, b);
        assert_ne!(a, u);
        c.insert(
            a.clone(),
            entry(1, b"a-rows", &["t"], Duration::from_secs(60)),
        );
        assert!(c.get(&b).is_none(), "other database must not alias");
        assert!(c.get(&u).is_none(), "other user must not alias");
        assert!(c.get(&a).is_some());
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
        let s = c.stats();
        assert_eq!(s.current_entries, 0);
        assert_eq!(s.misses, 1, "expired read counts as a miss");
        assert_eq!(s.entries_evicted, 0, "TTL expiry is not an LRU eviction");
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
        // Read fp0 — promotes it to most-recently-used.
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
    fn eviction_counter_respects_lru_order_at_capacity() {
        // Insert A, B at cap 2; touch A; insert C → B (LRU) evicted.
        let c = EdgeCache::new(2);
        c.insert(
            CacheKey::new("a", "p"),
            entry(1, b"a", &[], Duration::from_secs(60)),
        );
        c.insert(
            CacheKey::new("b", "p"),
            entry(2, b"b", &[], Duration::from_secs(60)),
        );
        assert_eq!(c.stats().entries_evicted, 0);
        assert!(c.get(&CacheKey::new("a", "p")).is_some());
        c.insert(
            CacheKey::new("c", "p"),
            entry(3, b"c", &[], Duration::from_secs(60)),
        );
        let s = c.stats();
        assert_eq!(s.entries_evicted, 1);
        assert_eq!(s.current_entries, 2);
        assert!(c.get(&CacheKey::new("b", "p")).is_none(), "b was the LRU");
        assert!(c.get(&CacheKey::new("a", "p")).is_some());
        assert!(c.get(&CacheKey::new("c", "p")).is_some());
    }

    #[test]
    fn overwrite_same_key_is_not_an_eviction() {
        let c = EdgeCache::new(2);
        let k = CacheKey::new("a", "p");
        c.insert(k.clone(), entry(1, b"v1", &[], Duration::from_secs(60)));
        c.insert(k.clone(), entry(2, b"v2", &[], Duration::from_secs(60)));
        let s = c.stats();
        assert_eq!(s.entries_evicted, 0);
        assert_eq!(s.inserts, 2);
        assert_eq!(&c.get(&k).expect("hit").response_bytes[..], b"v2");
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
    fn invalidate_matches_any_of_the_entry_tables() {
        // Reverse-index sweep must catch an entry via its SECOND table.
        let c = EdgeCache::new(10);
        c.insert(
            CacheKey::new("fp1", "p"),
            entry(5, b"x", &["users", "orders"], Duration::from_secs(60)),
        );
        let dropped = c.invalidate(100, &["orders".to_string()]);
        assert_eq!(dropped, 1);
        assert!(c.get(&CacheKey::new("fp1", "p")).is_none());
    }

    #[test]
    fn table_index_survives_eviction_and_overwrite() {
        // Evicted / overwritten entries must leave no stale index links
        // (a stale link would inflate later sweeps or leak memory).
        let c = EdgeCache::new(2);
        c.insert(
            CacheKey::new("a", "p"),
            entry(1, b"a", &["users"], Duration::from_secs(60)),
        );
        // Overwrite a with a different table set.
        c.insert(
            CacheKey::new("a", "p"),
            entry(2, b"a2", &["orders"], Duration::from_secs(60)),
        );
        // Fill + force eviction of `a` (LRU).
        c.insert(
            CacheKey::new("b", "p"),
            entry(3, b"b", &["users"], Duration::from_secs(60)),
        );
        c.insert(
            CacheKey::new("c", "p"),
            entry(4, b"c", &["users"], Duration::from_secs(60)),
        );
        // `a` (orders) was evicted; sweeping orders must drop nothing.
        assert_eq!(c.invalidate(100, &["orders".to_string()]), 0);
        // Sweeping the stale pre-overwrite table of `a` (users) drops
        // only the live users entries (b, c).
        assert_eq!(c.invalidate(100, &["users".to_string()]), 2);
        assert_eq!(c.stats().current_entries, 0);
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
        assert!(c.get(&CacheKey::new("fp2", "p")).is_some());
    }

    #[test]
    fn next_version_is_monotonic() {
        let c = EdgeCache::new(10);
        let v1 = c.next_version();
        let v2 = c.next_version();
        let v3 = c.next_version();
        assert!(v1 < v2 && v2 < v3);
        assert_eq!(v1, 1, "version counter starts at 1");
    }

    #[test]
    fn should_cache_gated_by_invalidation_hwm() {
        let c = EdgeCache::new(10);
        // Nothing invalidated yet — every positive version is cacheable.
        assert!(c.should_cache(1));
        let _ = c.invalidate(7, &["users".to_string()]);
        assert!(!c.should_cache(7), "version == hwm must not be cached");
        assert!(!c.should_cache(3), "version < hwm must not be cached");
        assert!(c.should_cache(8), "version > hwm is cacheable");
        // The hwm only ratchets up: a lower invalidation doesn't lower it.
        let _ = c.invalidate(2, &[]);
        assert!(!c.should_cache(7));
        assert!(c.should_cache(8));
    }

    #[test]
    fn insert_if_fresh_rejects_read_invalidated_in_flight() {
        // F18 repro: gate check passes, a full invalidation completes,
        // THEN the insert runs — the locked re-check must reject it.
        let c = EdgeCache::new(10);
        let read_version = c.next_version();
        assert!(c.should_cache(read_version)); // unlocked fast-path passes
        let w = c.next_version();
        let _ = c.invalidate(w, &["users".to_string()]); // completes fully
        let stored = c.insert_if_fresh(
            CacheKey::new("fp", "p"),
            entry(read_version, b"stale", &["users"], Duration::from_secs(60)),
        );
        assert!(!stored, "stale store must be rejected under the lock");
        assert!(c.get(&CacheKey::new("fp", "p")).is_none());
        // A read stamped after the invalidation stores fine.
        let fresh = c.next_version();
        assert!(c.insert_if_fresh(
            CacheKey::new("fp", "p"),
            entry(fresh, b"fresh", &["users"], Duration::from_secs(60)),
        ));
    }

    #[test]
    fn insert_if_epoch_rejects_after_any_invalidation() {
        // F1 edge-role store gate: snapshot before forwarding, reject
        // when any invalidation (or flush) landed in between.
        let c = EdgeCache::new(10);
        let epoch = c.invalidation_epoch();
        let _ = c.invalidate(5, &["users".to_string()]);
        let stored = c.insert_if_epoch(
            CacheKey::new("fp", "p"),
            entry(0, b"stale", &["users"], Duration::from_secs(60)),
            epoch,
        );
        assert!(!stored);
        // Caching resumes with a fresh snapshot.
        let epoch2 = c.invalidation_epoch();
        assert!(c.insert_if_epoch(
            CacheKey::new("fp", "p"),
            entry(
                c.observed_home_version(),
                b"fresh",
                &["users"],
                Duration::from_secs(60)
            ),
            epoch2,
        ));
        assert!(c.get(&CacheKey::new("fp", "p")).is_some());
    }

    #[test]
    fn edge_stamps_in_home_domain_are_swept_by_next_home_write() {
        // F1 repro (old scheme cached entries ABOVE the home clock, so
        // the next home invalidation swept nothing): entries stamped
        // with the observed home version must be dropped by the next
        // home write's `<=` sweep.
        let c = EdgeCache::new(10);
        c.observe_home_version(5);
        let stamp = c.observed_home_version();
        assert_eq!(stamp, 5);
        c.insert(
            CacheKey::new("fp", "p"),
            entry(stamp, b"rows", &["users"], Duration::from_secs(60)),
        );
        // Home's next write mints 6 and sweeps <= 6.
        assert_eq!(c.invalidate(6, &["users".to_string()]), 1);
        assert!(c.get(&CacheKey::new("fp", "p")).is_none());
    }

    #[test]
    fn fresh_boot_edge_entries_swept_by_first_invalidation() {
        // Before any home contact observed_home is 0; the first real
        // invalidation must still sweep those entries.
        let c = EdgeCache::new(10);
        assert_eq!(c.observed_home_version(), 0);
        c.insert(
            CacheKey::new("fp", "p"),
            entry(0, b"rows", &["users"], Duration::from_secs(60)),
        );
        assert_eq!(c.invalidate(1, &["users".to_string()]), 1);
    }

    #[test]
    fn observe_home_version_advances_counter() {
        let c = EdgeCache::new(10);
        c.observe_home_version(100);
        assert_eq!(c.observed_home_version(), 100);
        assert!(c.next_version() > 100);
        // Observing an older version never moves the clocks backwards.
        c.observe_home_version(5);
        assert_eq!(c.observed_home_version(), 100);
        assert!(c.next_version() > 100);
    }

    #[test]
    fn home_epoch_change_flushes_and_resets_observed_home() {
        // F9: a home restart resets its version clock. The epoch change
        // must flush everything (old stamps are incomparable) and reset
        // the observed-home clock so re-sync starts clean.
        let c = EdgeCache::new(10);
        assert_eq!(c.on_home_epoch(1111), 0, "first sighting records only");
        c.observe_home_version(1_000_000);
        c.insert(
            CacheKey::new("fp", "p"),
            entry(1_000_000, b"old", &["users"], Duration::from_secs(60)),
        );
        // Same epoch again: nothing happens.
        assert_eq!(c.on_home_epoch(1111), 0);
        assert!(c.get(&CacheKey::new("fp", "p")).is_some());
        // Restarted home (new epoch): flush + reset.
        assert_eq!(c.on_home_epoch(2222), 1);
        assert!(c.get(&CacheKey::new("fp", "p")).is_none());
        assert_eq!(c.observed_home_version(), 0);
        // Legacy events (epoch 0) never trigger a flush.
        c.insert(
            CacheKey::new("fp2", "p"),
            entry(1, b"x", &[], Duration::from_secs(60)),
        );
        assert_eq!(c.on_home_epoch(0), 0);
        assert!(c.get(&CacheKey::new("fp2", "p")).is_some());
    }

    #[test]
    fn process_epoch_is_stable_and_nonzero() {
        let c = EdgeCache::new(2);
        assert_ne!(c.epoch(), 0);
        assert_eq!(c.epoch(), c.epoch());
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
    fn invalidate_bumps_received_counter() {
        let c = EdgeCache::new(10);
        let _ = c.invalidate(1, &[]);
        let _ = c.invalidate(2, &["users".to_string()]);
        assert_eq!(c.stats().invalidations_received, 2);
    }

    #[test]
    fn concurrent_get_insert_invalidate() {
        // Smoke-test the single lock under contention: no deadlock,
        // no panic, and the map stays within capacity.
        let c = EdgeCache::new(64);
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let c = c.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..500u64 {
                    let k = CacheKey::new(format!("fp{}", (t * 500 + i) % 100), "p");
                    if i % 7 == 0 {
                        let v = c.next_version();
                        let _ = c.invalidate(v, &["users".to_string()]);
                    } else if i % 3 == 0 {
                        let v = c.next_version();
                        c.insert(k, entry(v, b"x", &["users"], Duration::from_secs(60)));
                    } else {
                        let _ = c.get(&k);
                    }
                }
            }));
        }
        for h in handles {
            h.join().expect("worker panicked");
        }
        assert!(c.stats().current_entries <= 64);
    }

    #[test]
    fn panics_on_zero_capacity() {
        let res = std::panic::catch_unwind(|| EdgeCache::new(0));
        assert!(res.is_err());
    }
}
