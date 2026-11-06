//! Refresh-ahead: single-flight refills of hot entries before they expire
//! (`[cache] refresh_ahead_ratio`, P3b of the distribcache merge).
//!
//! Once an entry's age reaches `ratio` × its TTL, the next read that finds it
//! (in L2, or its own L1 copy, which then consults L2) *claims* the refill:
//! [`super::QueryCache`] treats that one read as a miss (it bypasses its own
//! L1 copy too), so it runs on its own backend connection and its result is
//! stored through the normal store path, replacing the entry. Every other
//! read keeps being served the current entry until the refill lands or the
//! entry's TTL ends. A hot key then costs one backend read per refresh window
//! instead of a stampede of misses when it expires.
//!
//! A claim ends exactly once: when the claimant's refill is stored
//! (`completed`), or when it is not (`abandoned`): the store refused it (too
//! large, a write raced the fetch), or the read ended without a store and
//! dropped its `RefreshClaim` (an error response, an oversized capture, a
//! standby served it, the client went away, …). A claim older than
//! `refresh_ahead_claim_timeout_ms` is taken over by the next reader that
//! finds the entry due (counted `abandoned`), so a lost or slow claimant never
//! blocks refreshes. Claims live in a map keyed by the L2 key's hash and are
//! removed when they end, so the map holds at most one claim per refill in
//! flight. Time comes in as a parameter (`now`), never read here, so tests
//! move it instead of sleeping.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use super::result::CachedResult;

/// Refresh-ahead state of a [`super::QueryCache`]: only built when
/// `refresh_ahead_ratio` is in (0, 1) and the shared tier exists.
#[derive(Debug)]
pub(crate) struct RefreshAhead {
    /// Fraction of an entry's TTL after which its refill is claimed.
    ratio: f64,
    /// Age at which a claim is taken over by the next reader.
    claim_timeout: Duration,
    /// Claims in flight, by L2 key hash.
    claims: DashMap<u64, Claim>,
    /// Source of claim tokens: a claim ends only the record it made.
    next_token: AtomicU64,
    /// Claims made (takeovers included).
    claimed: AtomicU64,
    /// Claims whose refill was stored.
    completed: AtomicU64,
    /// Claims that ended without a stored refill, or were taken over.
    abandoned: AtomicU64,
}

/// One claim in flight.
#[derive(Debug, Clone, Copy)]
struct Claim {
    token: u64,
    at: Instant,
}

/// Refresh-ahead counters, for `/metrics` (`query_cache.refresh_ahead`) and
/// `heliosdb_proxy_cache_refresh_ahead_*_total`. Every claim ends as exactly
/// one of `completed` or `abandoned`, so `claims - completed - abandoned` is
/// the number in flight.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct RefreshAheadStats {
    /// Reads that claimed a refill (and went to the backend as a miss).
    pub claims: u64,
    /// Claims whose refill was stored, replacing the entry.
    pub completed: u64,
    /// Claims that ended without a stored refill, or that a later reader
    /// took over after `refresh_ahead_claim_timeout_ms`.
    pub abandoned: u64,
}

impl RefreshAhead {
    /// The state for `ratio` and `claim_timeout`, or `None` (off) unless
    /// `ratio` is in (0, 1): `0` is the documented off value, and anything
    /// else (NaN, ≥ 1, a library caller that skipped validation) is off too,
    /// never a panic on the read path.
    pub(crate) fn new(ratio: f64, claim_timeout: Duration) -> Option<Arc<Self>> {
        (ratio > 0.0 && ratio < 1.0).then(|| {
            Arc::new(Self {
                ratio,
                claim_timeout,
                claims: DashMap::new(),
                next_token: AtomicU64::new(1),
                claimed: AtomicU64::new(0),
                completed: AtomicU64::new(0),
                abandoned: AtomicU64::new(0),
            })
        })
    }

    /// Whether `result` is past its refresh point at `now`: its age is at
    /// least `ratio` × its own TTL.
    #[inline]
    pub(crate) fn due(&self, result: &CachedResult, now: Instant) -> bool {
        now.saturating_duration_since(result.cached_at)
            .as_secs_f64()
            >= result.ttl.as_secs_f64() * self.ratio
    }

    /// Claim the refill of the entry whose L2 key hashes to `key`, at `now`.
    /// `None` while another reader's claim is younger than the claim
    /// timeout; an older one is taken over (and counted abandoned).
    pub(crate) fn try_claim(self: &Arc<Self>, key: u64, now: Instant) -> Option<RefreshClaim> {
        let live = |claim: &Claim| now.saturating_duration_since(claim.at) < self.claim_timeout;
        // Every reader that finds the entry due while its refill is in flight
        // asks; a shared lock answers them.
        if self.claims.get(&key).is_some_and(|c| live(&c)) {
            return None;
        }
        let token = match self.claims.entry(key) {
            Entry::Vacant(vacant) => {
                let token = self.next_token.fetch_add(1, Ordering::Relaxed);
                vacant.insert(Claim { token, at: now });
                token
            }
            Entry::Occupied(mut occupied) => {
                if live(occupied.get()) {
                    return None;
                }
                // The claimant is lost or slow: its claim ends here, and its
                // own end later finds another token and counts nothing.
                self.abandoned.fetch_add(1, Ordering::Relaxed);
                let token = self.next_token.fetch_add(1, Ordering::Relaxed);
                occupied.insert(Claim { token, at: now });
                token
            }
        };
        self.claimed.fetch_add(1, Ordering::Relaxed);
        Some(RefreshClaim {
            owner: Arc::clone(self),
            key,
            token,
            ended: AtomicBool::new(false),
        })
    }

    /// End the claim `token` on `key` if it is still the one recorded.
    /// Returns whether it was.
    fn end(&self, key: u64, token: u64) -> bool {
        self.claims
            .remove_if(&key, |_, claim| claim.token == token)
            .is_some()
    }

    /// The counters.
    pub(crate) fn stats(&self) -> RefreshAheadStats {
        RefreshAheadStats {
            claims: self.claimed.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
        }
    }

    /// Claims in flight (recorded and not ended).
    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        self.claims.len()
    }
}

/// A reader's claim on a refill. It ends once: [`Self::end`] when the store
/// path is done with the refill, [`Self::withdraw`] when the lookup finds the
/// refill already landed, or on drop (the read ended without a store).
#[derive(Debug)]
pub(crate) struct RefreshClaim {
    owner: Arc<RefreshAhead>,
    key: u64,
    token: u64,
    ended: AtomicBool,
}

impl RefreshClaim {
    /// End the claim after the store path ran: `stored` when the refill was
    /// stored (counted completed), else abandoned. Nothing is counted for a
    /// claim already taken over, or already ended.
    pub(crate) fn end(&self, stored: bool) {
        if self.ended.swap(true, Ordering::AcqRel) {
            return;
        }
        if self.owner.end(self.key, self.token) {
            let counter = if stored {
                &self.owner.completed
            } else {
                &self.owner.abandoned
            };
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Give the claim back without a refill of its own: another reader's
    /// refill landed between the lookup and the claim. It is not counted at
    /// all (the claim is taken back out of `claims`).
    pub(crate) fn withdraw(self) {
        if !self.ended.swap(true, Ordering::AcqRel) && self.owner.end(self.key, self.token) {
            self.owner.claimed.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for RefreshClaim {
    fn drop(&mut self) {
        self.end(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    const TIMEOUT: Duration = Duration::from_secs(5);

    fn ra(ratio: f64) -> Arc<RefreshAhead> {
        RefreshAhead::new(ratio, TIMEOUT).expect("on")
    }

    /// A result stored at `cached_at` with `ttl`.
    fn entry(cached_at: Instant, ttl: Duration) -> CachedResult {
        CachedResult {
            cached_at,
            ..CachedResult::new(Bytes::from_static(b"r"), 1, ttl, Vec::new(), Duration::ZERO)
        }
    }

    #[test]
    fn off_unless_the_ratio_is_inside_zero_one() {
        for ratio in [0.0, -0.5, 1.0, 1.5, f64::NAN, f64::INFINITY, 1e300] {
            assert!(RefreshAhead::new(ratio, TIMEOUT).is_none(), "{ratio}");
        }
        for ratio in [f64::MIN_POSITIVE, 0.5, 0.999] {
            assert!(RefreshAhead::new(ratio, TIMEOUT).is_some(), "{ratio}");
        }
    }

    #[test]
    fn due_at_ratio_times_the_entrys_own_ttl() {
        let ra = ra(0.8);
        let t0 = Instant::now();
        let e = entry(t0, Duration::from_secs(10));
        assert!(!ra.due(&e, t0));
        assert!(!ra.due(&e, t0 + Duration::from_millis(7_999)));
        assert!(ra.due(&e, t0 + Duration::from_secs(8)));
        assert!(ra.due(&e, t0 + Duration::from_secs(30)), "expired is due");
        // A shorter TTL (a hint, a table TTL) moves the point with it.
        let short = entry(t0, Duration::from_secs(1));
        assert!(ra.due(&short, t0 + Duration::from_millis(800)));
        // An instant before the entry (another clock read) is never due.
        let later = entry(t0 + Duration::from_secs(1), Duration::from_secs(10));
        assert!(!ra.due(&later, t0));
    }

    #[test]
    fn one_claim_per_key_until_it_ends() {
        let ra = ra(0.5);
        let t0 = Instant::now();
        let claim = ra.try_claim(1, t0).expect("first reader claims");
        assert!(ra.try_claim(1, t0).is_none(), "second reader is served");
        assert!(
            ra.try_claim(1, t0 + TIMEOUT - Duration::from_millis(1))
                .is_none(),
            "until the timeout"
        );
        let other = ra.try_claim(2, t0).expect("another key is independent");
        assert_eq!(ra.in_flight(), 2);
        claim.end(true);
        other.end(false);
        assert_eq!(ra.in_flight(), 0);
        assert_eq!(
            ra.stats(),
            RefreshAheadStats {
                claims: 2,
                completed: 1,
                abandoned: 1
            }
        );
        // Ending twice counts once.
        claim.end(false);
        drop(other);
        assert_eq!(ra.stats().abandoned, 1);
        assert!(ra.try_claim(1, t0).is_some(), "released: claimable again");
    }

    #[test]
    fn dropping_a_claim_abandons_it() {
        let ra = ra(0.5);
        let t0 = Instant::now();
        drop(ra.try_claim(7, t0).unwrap());
        assert_eq!(ra.in_flight(), 0);
        assert_eq!(ra.stats().abandoned, 1);
        assert!(ra.try_claim(7, t0).is_some());
    }

    /// A lost claimant never blocks refreshes: at the timeout the next reader
    /// takes over, and the lost claimant's late end counts nothing (its
    /// refill may still be stored; the takeover already counted the claim
    /// abandoned).
    #[test]
    fn a_claim_past_the_timeout_is_taken_over() {
        let ra = ra(0.5);
        let t0 = Instant::now();
        let lost = ra.try_claim(1, t0).unwrap();
        let taker = ra.try_claim(1, t0 + TIMEOUT).expect("taken over");
        assert_eq!(
            ra.stats(),
            RefreshAheadStats {
                claims: 2,
                completed: 0,
                abandoned: 1
            }
        );
        lost.end(true);
        assert_eq!(ra.stats().completed, 0, "the lost claim ended earlier");
        assert_eq!(ra.in_flight(), 1, "the taker's claim is still recorded");
        assert!(ra.try_claim(1, t0 + TIMEOUT).is_none());
        taker.end(true);
        assert_eq!(
            ra.stats(),
            RefreshAheadStats {
                claims: 2,
                completed: 1,
                abandoned: 1
            }
        );
        assert_eq!(ra.in_flight(), 0);
    }

    #[test]
    fn a_withdrawn_claim_is_not_counted() {
        let ra = ra(0.5);
        let t0 = Instant::now();
        ra.try_claim(3, t0).unwrap().withdraw();
        assert_eq!(ra.stats(), RefreshAheadStats::default());
        assert_eq!(ra.in_flight(), 0);
        // A withdrawn takeover keeps the abandonment it caused.
        let lost = ra.try_claim(3, t0).unwrap();
        ra.try_claim(3, t0 + TIMEOUT).unwrap().withdraw();
        assert_eq!(
            ra.stats(),
            RefreshAheadStats {
                claims: 1,
                completed: 0,
                abandoned: 1
            }
        );
        drop(lost);
        assert_eq!(ra.stats().abandoned, 1);
    }

    /// Of many threads asking at once, exactly one claims.
    #[test]
    fn exactly_one_of_many_concurrent_readers_claims() {
        let ra = ra(0.5);
        let t0 = Instant::now();
        let threads = 16;
        let barrier = Arc::new(std::sync::Barrier::new(threads));
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let ra = Arc::clone(&ra);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    ra.try_claim(42, t0)
                })
            })
            .collect();
        let claims: Vec<RefreshClaim> = handles
            .into_iter()
            .filter_map(|h| h.join().unwrap())
            .collect();
        assert_eq!(claims.len(), 1);
        assert_eq!(ra.stats().claims, 1);
    }
}

/// The query cache's side: the `cache_refresh` hint (P3a) and refresh-ahead
/// through [`super::QueryCache`]'s lookup and store, with the time injected
/// (entries stored already aged, and the lookup's clock moved) instead of
/// sleeps.
#[cfg(test)]
mod lookup_tests {
    use super::super::{
        CacheConfig, CacheContext, CacheKey, CacheLevel, CacheLookup, CachedResult, L1Config,
        L3Config, QueryCache, QueryPrep, RefreshPick,
    };
    use super::*;
    use bytes::Bytes;

    const TTL: Duration = Duration::from_secs(60);
    /// Past a 0.5 refresh point of [`TTL`], well before its end.
    const DUE: Duration = Duration::from_secs(31);
    const YOUNG: Duration = Duration::from_secs(1);
    const SQL: &str = "SELECT v FROM kv WHERE id = 1";

    /// L1 admits the whole TTL (as the daemon's does: the longest TTL).
    fn config(ratio: f64) -> CacheConfig {
        CacheConfig {
            default_ttl: TTL,
            l1: L1Config {
                ttl: TTL,
                ..Default::default()
            },
            refresh_ahead_ratio: ratio,
            ..Default::default()
        }
    }

    fn ctx(conn: u64) -> CacheContext {
        CacheContext {
            database: "db".into(),
            connection_id: Some(conn),
            ..Default::default()
        }
    }

    /// Store `payload` for `sql` as a fill `age` ago would have: in L2, and
    /// in the L1 of each of `conns`.
    async fn store_aged(
        cache: &QueryCache,
        sql: &str,
        conns: &[u64],
        age: Duration,
        payload: &'static [u8],
    ) -> CachedResult {
        let normalized = cache.normalizer.normalize_with(sql, false);
        let result = CachedResult {
            data: Bytes::from_static(payload),
            row_count: 1,
            cached_at: Instant::now()
                .checked_sub(age)
                .expect("the monotonic clock is older than the test's ages"),
            ttl: TTL,
            tables: normalized.tables.clone(),
            execution_time: Duration::ZERO,
            generation: cache.generation_of(&normalized.tables),
        };
        let l2 = cache.l2_cache.as_ref().expect("L2 on");
        l2.put(CacheKey::new(&normalized, &ctx(0)), result.clone())
            .await;
        for conn in conns {
            cache
                .get_l1_cache(*conn)
                .put(sql.to_string(), result.clone());
        }
        result
    }

    async fn lookup_at(
        cache: &QueryCache,
        sql: &str,
        conn: u64,
        now: Instant,
    ) -> (CacheLookup, QueryPrep) {
        cache.lookup(sql, None, &ctx(conn), || now).await
    }

    /// The payload and tier of a hit, `None` for a miss.
    fn served(lookup: &CacheLookup) -> Option<(&[u8], CacheLevel)> {
        match lookup {
            CacheLookup::Hit { result, level } => Some((&result.data[..], *level)),
            CacheLookup::Miss => None,
        }
    }

    async fn store(
        cache: &QueryCache,
        sql: &str,
        conn: u64,
        prep: &QueryPrep,
        payload: &'static [u8],
    ) {
        cache
            .put_prepared(
                sql,
                &ctx(conn),
                prep,
                Bytes::from_static(payload),
                1,
                Duration::ZERO,
            )
            .await;
    }

    fn ra(cache: &QueryCache) -> &Arc<RefreshAhead> {
        cache.refresh_ahead.as_ref().expect("refresh-ahead on")
    }

    /// The heatmap sees every refresh-ahead path: the claimant's read is a
    /// miss on the statement's tables (not an expiry), the readers served
    /// their own due L1 copy or the shared entry while the claim is held are
    /// hits, and so is the refill served from L2 afterwards.
    #[tokio::test]
    async fn the_heatmap_counts_refresh_ahead_claims_and_serves() {
        let cache = QueryCache::new(CacheConfig {
            heatmap: crate::cache::HeatmapConfig {
                enabled: true,
                ..Default::default()
            },
            ..config(0.5)
        });
        // Another statement of the table stored: `kv` is tracked.
        cache
            .put(
                "SELECT v FROM kv WHERE id = 2",
                &ctx(9),
                Bytes::from_static(b"two"),
                1,
                Duration::ZERO,
            )
            .await;
        store_aged(&cache, SQL, &[1, 2], DUE, b"old").await;
        let now = Instant::now();
        let (claimed, prep) = lookup_at(&cache, SQL, 1, now).await;
        assert!(matches!(claimed, CacheLookup::Miss), "the claimant");
        assert_eq!(
            served(&lookup_at(&cache, SQL, 2, now).await.0),
            Some((&b"old"[..], CacheLevel::L1Hot)),
            "its own due L1 copy while the claim is held"
        );
        assert_eq!(
            served(&lookup_at(&cache, SQL, 3, now).await.0),
            Some((&b"old"[..], CacheLevel::L2Warm)),
            "the shared entry while the claim is held"
        );
        store(&cache, SQL, 1, &prep, b"new").await;
        assert_eq!(
            served(&lookup_at(&cache, SQL, 2, Instant::now()).await.0),
            Some((&b"new"[..], CacheLevel::L2Warm)),
            "the refill, picked up from L2"
        );
        let s = cache.heatmap_snapshot().expect("heatmap on");
        assert_eq!(s.untracked_misses, 0, "{s:?}");
        let kv = s.tables.iter().find(|t| t.table == "kv").expect("kv");
        assert_eq!(
            (kv.hits, kv.misses, kv.expired, kv.stores),
            (3, 1, 0, 2),
            "{kv:?}"
        );
    }

    #[tokio::test]
    async fn off_by_default_and_without_l2() {
        assert!(QueryCache::new(CacheConfig::default())
            .refresh_ahead
            .is_none());
        let cache = QueryCache::new(config(0.0));
        assert!(cache.refresh_ahead.is_none());
        store_aged(&cache, SQL, &[1], DUE, b"old").await;
        let now = Instant::now();
        // Past any refresh point: an ordinary hit on both tiers, no claim.
        assert_eq!(
            served(&lookup_at(&cache, SQL, 1, now).await.0),
            Some((&b"old"[..], CacheLevel::L1Hot))
        );
        assert_eq!(
            served(&lookup_at(&cache, SQL, 2, now).await.0),
            Some((&b"old"[..], CacheLevel::L2Warm))
        );
        assert_eq!(cache.refresh_ahead_stats(), RefreshAheadStats::default());
        // Refresh-ahead needs the shared tier.
        let mut no_l2 = config(0.5);
        no_l2.l2.enabled = false;
        assert!(QueryCache::new(no_l2).refresh_ahead.is_none());
        assert!(QueryCache::new(config(0.5)).refresh_ahead.is_some());
    }

    #[tokio::test]
    async fn a_young_entry_is_served_without_a_claim() {
        let cache = QueryCache::new(config(0.5));
        store_aged(&cache, SQL, &[1], YOUNG, b"old").await;
        let now = Instant::now();
        assert_eq!(
            served(&lookup_at(&cache, SQL, 1, now).await.0),
            Some((&b"old"[..], CacheLevel::L1Hot))
        );
        assert_eq!(
            served(&lookup_at(&cache, SQL, 2, now).await.0),
            Some((&b"old"[..], CacheLevel::L2Warm))
        );
        assert_eq!(cache.refresh_ahead_stats().claims, 0);
    }

    /// The first reader past the refresh point claims the refill, bypassing
    /// its own L1 copy; every other reader is served the entry (its own L1
    /// copy, or L2) while the refill is in flight; the stored refill
    /// replaces the entry, and the other connections' L1 copies pick it up.
    #[tokio::test]
    async fn one_reader_claims_and_the_others_are_served_until_the_refill() {
        let cache = QueryCache::new(config(0.5));
        store_aged(&cache, SQL, &[1, 2, 3], DUE, b"old").await;
        let now = Instant::now();
        let (lookup, claimant) = lookup_at(&cache, SQL, 1, now).await;
        assert!(
            matches!(lookup, CacheLookup::Miss),
            "the claimant bypasses its own L1 copy"
        );
        assert!(claimant.refresh_claim.is_some() && claimant.storable());
        for conn in [2, 3] {
            assert_eq!(
                served(&lookup_at(&cache, SQL, conn, now).await.0),
                Some((&b"old"[..], CacheLevel::L1Hot)),
                "conn {conn}: its own copy"
            );
        }
        for conn in [4, 5] {
            assert_eq!(
                served(&lookup_at(&cache, SQL, conn, now).await.0),
                Some((&b"old"[..], CacheLevel::L2Warm)),
                "conn {conn}: the shared entry"
            );
        }
        assert_eq!(cache.stats().total_misses, 1);
        assert_eq!(ra(&cache).in_flight(), 1);

        store(&cache, SQL, 1, &claimant, b"new").await;
        assert_eq!(
            cache.refresh_ahead_stats(),
            RefreshAheadStats {
                claims: 1,
                completed: 1,
                abandoned: 0
            }
        );
        assert_eq!(ra(&cache).in_flight(), 0, "the claim ended at the store");
        let now = Instant::now();
        assert_eq!(
            served(&lookup_at(&cache, SQL, 1, now).await.0),
            Some((&b"new"[..], CacheLevel::L1Hot)),
            "the claimant's own L1 holds the refill"
        );
        // A due L1 copy (conn 2), and one promoted from L2 during the refill
        // (conn 4), check the shared tier and take the refill from it.
        for conn in [2, 4] {
            assert_eq!(
                served(&lookup_at(&cache, SQL, conn, now).await.0),
                Some((&b"new"[..], CacheLevel::L2Warm)),
                "conn {conn}"
            );
            assert_eq!(
                served(&lookup_at(&cache, SQL, conn, now).await.0),
                Some((&b"new"[..], CacheLevel::L1Hot)),
                "conn {conn}: promoted"
            );
        }
        assert_eq!(cache.refresh_ahead_stats().claims, 1, "one refill");
        assert_eq!(cache.stats().total_misses, 1);
    }

    /// A claim that ends without a stored refill is released at once
    /// (counted abandoned), so the next reader claims again: an oversized
    /// result, a write that raced the fetch, a HeliosDB flag set meanwhile,
    /// and a prep dropped without a store (an error response, an oversized
    /// capture, a standby's answer, a client that went away).
    #[tokio::test]
    async fn a_claim_ends_on_every_path_without_a_store() {
        async fn claimed() -> (QueryCache, QueryPrep) {
            let cache = QueryCache::new(CacheConfig {
                max_result_size: 4,
                ..config(0.5)
            });
            store_aged(&cache, SQL, &[], DUE, b"old").await;
            let (lookup, prep) = lookup_at(&cache, SQL, 1, Instant::now()).await;
            assert!(matches!(lookup, CacheLookup::Miss));
            assert!(prep.refresh_claim.is_some());
            (cache, prep)
        }
        async fn reclaimable(cache: &QueryCache) {
            assert_eq!(
                cache.refresh_ahead_stats(),
                RefreshAheadStats {
                    claims: 1,
                    completed: 0,
                    abandoned: 1
                }
            );
            assert_eq!(ra(cache).in_flight(), 0);
            let (lookup, prep) = lookup_at(cache, SQL, 2, Instant::now()).await;
            assert!(matches!(lookup, CacheLookup::Miss), "claimed again");
            assert!(prep.refresh_claim.is_some());
        }

        let (cache, prep) = claimed().await;
        store(&cache, SQL, 1, &prep, b"too large").await;
        assert_eq!(cache.stats().size_exceeded, 1);
        reclaimable(&cache).await;

        let (cache, prep) = claimed().await;
        cache.mark_written(&["kv".to_string()]);
        store(&cache, SQL, 1, &prep, b"new").await;
        assert_eq!(cache.fills_raced(), 1);
        assert_eq!(ra(&cache).in_flight(), 0);
        assert_eq!(cache.refresh_ahead_stats().abandoned, 1);
        // The write retired the entry itself: the next read is a plain miss.
        let (lookup, prep) = lookup_at(&cache, SQL, 2, Instant::now()).await;
        assert!(matches!(lookup, CacheLookup::Miss) && prep.refresh_claim.is_none());

        let (cache, prep) = claimed().await;
        assert!(cache.note_heliosdb_backend());
        store(&cache, SQL, 1, &prep, b"new").await;
        assert_eq!(cache.fills_raced(), 1, "keyed the other way");
        assert_eq!(cache.refresh_ahead_stats().abandoned, 1);
        assert_eq!(ra(&cache).in_flight(), 0);

        let (cache, prep) = claimed().await;
        drop(prep);
        reclaimable(&cache).await;
    }

    /// A claimant that neither stores nor fails within the claim timeout is
    /// taken over by the next reader that finds the entry due; its own late
    /// store still lands, but completes nothing.
    #[tokio::test]
    async fn a_lost_claimant_never_blocks_refreshes() {
        let cache = QueryCache::new(CacheConfig {
            refresh_ahead_claim_timeout: Duration::from_secs(5),
            ..config(0.5)
        });
        store_aged(&cache, SQL, &[], DUE, b"old").await;
        let t = Instant::now();
        let (lookup, lost) = lookup_at(&cache, SQL, 1, t).await;
        assert!(matches!(lookup, CacheLookup::Miss));
        let just_before = t + Duration::from_millis(4_999);
        assert_eq!(
            served(&lookup_at(&cache, SQL, 2, just_before).await.0),
            Some((&b"old"[..], CacheLevel::L2Warm))
        );
        let (lookup, taker) = lookup_at(&cache, SQL, 3, t + Duration::from_secs(5)).await;
        assert!(matches!(lookup, CacheLookup::Miss), "taken over");
        assert!(taker.refresh_claim.is_some());
        assert_eq!(
            cache.refresh_ahead_stats(),
            RefreshAheadStats {
                claims: 2,
                completed: 0,
                abandoned: 1
            }
        );
        store(&cache, SQL, 1, &lost, b"late").await;
        assert_eq!(cache.refresh_ahead_stats().completed, 0);
        assert_eq!(cache.stats().puts, 1, "the late refill is still stored");
        store(&cache, SQL, 3, &taker, b"new").await;
        assert_eq!(
            cache.refresh_ahead_stats(),
            RefreshAheadStats {
                claims: 2,
                completed: 1,
                abandoned: 1
            }
        );
        assert_eq!(ra(&cache).in_flight(), 0);
    }

    /// A refill that lands between a reader's L2 lookup and its claim (the
    /// claimant ended its claim just before) is served instead: claiming
    /// again would refill twice.
    #[tokio::test]
    async fn a_refill_landed_before_the_claim_supersedes_it() {
        let cache = QueryCache::new(config(0.5));
        let seen = store_aged(&cache, SQL, &[], DUE, b"old").await;
        store_aged(&cache, SQL, &[], YOUNG, b"new").await;
        let normalized = cache.normalizer.normalize_with(SQL, false);
        let key = CacheKey::new(&normalized, &ctx(1));
        let now = Instant::now();
        match cache
            .refresh_ahead_pick(ra(&cache), &key, None, Some(seen), now, None)
            .await
        {
            RefreshPick::Serve(result, CacheLevel::L2Warm) => {
                assert_eq!(&result.data[..], b"new")
            }
            RefreshPick::Serve(_, level) => panic!("served from {level}"),
            RefreshPick::Claimed(_) => panic!("claimed a landed refill"),
            RefreshPick::Miss => panic!("missed"),
        }
        assert_eq!(cache.refresh_ahead_stats(), RefreshAheadStats::default());
        assert_eq!(ra(&cache).in_flight(), 0);
    }

    /// Of many readers looking a due entry up at once (on several threads),
    /// exactly one claims it; every other is served the current entry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_readers_claim_once() {
        let cache = Arc::new(QueryCache::new(config(0.5)));
        let readers = 32u64;
        let conns: Vec<u64> = (1..=readers / 2).collect();
        store_aged(&cache, SQL, &conns, DUE, b"old").await;
        let barrier = Arc::new(tokio::sync::Barrier::new(readers as usize));
        let tasks: Vec<_> = (1..=readers)
            .map(|conn| {
                let cache = Arc::clone(&cache);
                let barrier = Arc::clone(&barrier);
                tokio::spawn(async move {
                    barrier.wait().await;
                    cache.get_with_prep(SQL, &ctx(conn)).await
                })
            })
            .collect();
        let mut misses = 0;
        let mut preps = Vec::new();
        for task in tasks {
            let (lookup, prep) = task.await.unwrap();
            match served(&lookup) {
                Some((data, _)) => assert_eq!(data, b"old"),
                None => misses += 1,
            }
            preps.push(prep);
        }
        assert_eq!(misses, 1);
        assert_eq!(cache.refresh_ahead_stats().claims, 1);
        assert_eq!(
            preps.iter().filter(|p| p.refresh_claim.is_some()).count(),
            1
        );
    }

    /// `cache_refresh` looks nothing up (L1, L2 or L3, even with entries for
    /// the text) and stores the result through the normal path, replacing the
    /// entries; it is counted apart from misses and never claims a
    /// refresh-ahead refill.
    #[tokio::test]
    async fn the_refresh_hint_skips_every_tier_and_stores() {
        let cache = QueryCache::new(CacheConfig {
            l3: L3Config {
                enabled: true,
                similarity_threshold: 1.0,
                ..Default::default()
            },
            ..config(0.5)
        });
        let hinted = "SELECT v FROM kv WHERE id = 1 /*helios:cache=semantic,cache_refresh*/";
        let semantic = "SELECT v FROM kv WHERE id = 1 /*helios:cache=semantic*/";
        cache
            .put(
                hinted,
                &ctx(1),
                Bytes::from_static(b"old"),
                1,
                Duration::ZERO,
            )
            .await;
        cache
            .put(
                semantic,
                &ctx(1),
                Bytes::from_static(b"old"),
                1,
                Duration::ZERO,
            )
            .await;
        // Past the refresh point too: a hinted read still claims nothing.
        store_aged(&cache, SQL, &[], DUE, b"old").await;
        let misses = cache.stats().total_misses;
        let later = Instant::now() + TTL / 2;
        for conn in [1, 2] {
            let (lookup, prep) = lookup_at(&cache, hinted, conn, later).await;
            assert!(matches!(lookup, CacheLookup::Miss), "conn {conn}");
            assert!(prep.storable() && prep.refresh_claim.is_none());
            assert!(prep.normalized.is_some() && prep.generation.is_some());
        }
        assert_eq!(cache.refresh_hints(), 2);
        assert_eq!(cache.stats().total_misses, misses, "not counted as misses");
        assert_eq!(cache.stats().total_skips, 0);
        assert_eq!(cache.refresh_ahead_stats().claims, 0);
        // The client's text carries it when the forwarded one lost it.
        let (lookup, _) = cache.get_with_prep_hinted(SQL, Some(hinted), &ctx(1)).await;
        assert!(matches!(lookup, CacheLookup::Miss));

        let (_, prep) = cache.get_with_prep(hinted, &ctx(3)).await;
        store(&cache, hinted, 3, &prep, b"fresh").await;
        // The shared entry (any text of the statement), this text's L1 copy
        // and the semantic entry now hold the refill.
        assert_eq!(
            served(&cache.get(SQL, &ctx(4)).await),
            Some((&b"fresh"[..], CacheLevel::L2Warm))
        );
        let l1 = cache.get_l1_cache(3).get(hinted).unwrap();
        assert_eq!(&l1.data[..], b"fresh");
        assert_eq!(
            served(
                &cache
                    .get(
                        "select v from kv x where id = 1 /*helios:cache=semantic*/",
                        &ctx(5)
                    )
                    .await
            ),
            Some((&b"fresh"[..], CacheLevel::L3Semantic))
        );
    }

    /// Every store guard applies to a refresh: the size cap, the C-02
    /// generation taken at the lookup, and a skip, which wins.
    #[tokio::test]
    async fn the_refresh_hint_keeps_every_store_guard() {
        let hinted = "SELECT v FROM kv WHERE id = 1 /*helios:cache_refresh*/";
        let cache = QueryCache::new(CacheConfig {
            max_result_size: 4,
            ..CacheConfig::default()
        });
        store_aged(&cache, SQL, &[], YOUNG, b"old").await;
        let (_, prep) = cache.get_with_prep(hinted, &ctx(1)).await;
        store(&cache, hinted, 1, &prep, b"too large").await;
        assert_eq!(cache.stats().size_exceeded, 1);
        let (_, prep) = cache.get_with_prep(hinted, &ctx(1)).await;
        cache.mark_written(&["kv".to_string()]);
        store(&cache, hinted, 1, &prep, b"new").await;
        assert_eq!(cache.fills_raced(), 1, "a write raced the refresh");
        assert_eq!(cache.stats().puts, 0);
        assert!(matches!(cache.get(SQL, &ctx(2)).await, CacheLookup::Miss));

        let both = "SELECT v FROM kv WHERE id = 1 /*helios:cache=skip,cache_refresh*/";
        let (_, prep) = cache.get_with_prep(both, &ctx(1)).await;
        assert!(!prep.storable(), "a skip wins");
        store(&cache, both, 1, &prep, b"new").await;
        assert_eq!(cache.stats().puts, 0);
        assert_eq!(cache.stats().total_skips, 1);
        assert_eq!(cache.refresh_hints(), 2);
    }
}
