//! Per-table cache heatmap with recommendations (`[cache] heatmap_*`,
//! `GET /api/cache/heatmap`).
//!
//! For every table the cache attributes a read or a write to, the heatmap
//! counts, in a ring of `heatmap_buckets` time buckets of
//! `heatmap_bucket_secs` each:
//!
//! - `hits`: hits on any tier, on the tables of the served entry (the C-02
//!   table list every entry keeps), a refresh-ahead serve of an L1 copy past
//!   its refresh point included;
//! - `misses`: lookups that went to the backend, on the statement's tables
//!   (already extracted by then: no parse is added for the heatmap), a
//!   refresh-ahead claimant's included, and of those `expired`: the L2 entry
//!   was there but past its TTL (an entry that does not decompress is not);
//!   a `cache_refresh` read looks nothing up and is not counted;
//! - `stores` and `stored_bytes`: results stored (their size before any L2
//!   compression, not what is retained now);
//! - `invalidations`: the C-02 write marks of the table
//!   ([`super::QueryCache::mark_written`]);
//! - `stale_rejected` and `fills_raced`: entries refused because the table was
//!   written since their fetch (per tier, like
//!   `heliosdb_proxy_cache_stale_rejected_total`), and fills not stored
//!   because a write raced their fetch.
//!
//! A snapshot sums the buckets still inside the window, ranks the tables by
//! reads into temperatures, and derives recommendations that name a real
//! `[cache]` key, only from those counters and only for a table with at least
//! `heatmap_min_samples` reads in the window.
//!
//! The ideas (temperature, bucket ring, recommendations) come from
//! `distribcache::heatmap`; its locking does not, since that module takes a
//! global write lock on every access. Here recording is lock-free:
//!
//! - the table set is an immutable map behind an [`ArcSwap`], loaded without
//!   a shared write; a table seen for the first time copies the map under a
//!   mutex only such recorders take (at most `heatmap_max_tables` times, and
//!   once per bucket to prune once the set is full);
//! - each table owns its ring, and a record is a relaxed `fetch_add` per
//!   counter on the current slot. The first record of a new bucket resets the
//!   slot it reuses with a compare-and-swap; a concurrent recorder never waits
//!   for it (a sample landing during the reset's few stores is dropped);
//! - with `heatmap_max_tables` tables tracked, a sample of another table is
//!   counted in `overflow` instead, and once per bucket the tables with
//!   nothing left in the window are dropped to make room. A plain miss never
//!   adds a table (it counts in `untracked_misses` until a hit, store, write
//!   or rejection of the table does), so reads of tables that do not exist
//!   cannot fill the set.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use serde::Serialize;

/// Upper bound of `heatmap_buckets × heatmap_max_tables`
/// ([`crate::config::CACHE_HEATMAP_MAX_SLOTS`]): every tracked table owns
/// `heatmap_buckets` slots of [`SLOT_BYTES`] bytes.
pub use crate::config::CACHE_HEATMAP_MAX_SLOTS as HEATMAP_MAX_SLOTS;

/// Bytes one bucket slot of one table takes.
pub const SLOT_BYTES: usize = std::mem::size_of::<Slot>();

/// The `[cache] heatmap_*` keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeatmapConfig {
    /// `heatmap_enabled`: build the heatmap at all.
    pub enabled: bool,
    /// `heatmap_bucket_secs`: length of one bucket, seconds.
    pub bucket_secs: u64,
    /// `heatmap_buckets`: buckets in the ring; the window is
    /// `bucket_secs × buckets`.
    pub buckets: usize,
    /// `heatmap_max_tables`: tables tracked at most.
    pub max_tables: usize,
    /// `heatmap_min_samples`: reads (hits + misses) a table needs in the
    /// window before any recommendation is made for it.
    pub min_samples: u64,
}

impl Default for HeatmapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bucket_secs: 300,
            buckets: 288,
            max_tables: 1024,
            min_samples: 100,
        }
    }
}

impl HeatmapConfig {
    /// Length of the window, seconds.
    pub fn window_secs(&self) -> u64 {
        self.bucket_secs.saturating_mul(self.buckets as u64)
    }

    /// Why the configuration cannot be built, if it cannot (the runtime check
    /// of `CacheConfig::validate`; the daemon's `[cache]` validation names
    /// the keys).
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }
        for (name, zero) in [
            ("bucket_secs", self.bucket_secs == 0),
            ("buckets", self.buckets == 0),
            ("max_tables", self.max_tables == 0),
            ("min_samples", self.min_samples == 0),
        ] {
            if zero {
                return Err(format!("heatmap {name} cannot be 0 when enabled"));
            }
        }
        if self.buckets.saturating_mul(self.max_tables) > HEATMAP_MAX_SLOTS {
            return Err(format!(
                "heatmap buckets x max_tables must be at most {HEATMAP_MAX_SLOTS}"
            ));
        }
        Ok(())
    }
}

/// What a bucket counts, as indexes into its counters.
#[derive(Debug, Clone, Copy)]
enum Counter {
    Hits,
    Misses,
    Expired,
    Stores,
    StoredBytes,
    Invalidations,
    StaleRejected,
    FillsRaced,
}

/// Number of [`Counter`]s.
const COUNTERS: usize = 8;

/// A slot's tag while a recorder zeroes it for a new bucket.
const RESETTING: u64 = u64::MAX;

/// One bucket of one table.
#[derive(Debug)]
struct Slot {
    /// 1 + the bucket number the counts belong to; 0 = never used,
    /// [`RESETTING`] while a recorder zeroes the counts for a new bucket.
    tag: AtomicU64,
    counts: [AtomicU64; COUNTERS],
}

impl Slot {
    fn new() -> Self {
        Self {
            tag: AtomicU64::new(0),
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Whether a slot tagged `tag` holds a bucket inside the `len` buckets that
/// end with bucket `now`.
fn in_window(tag: u64, now: u64, len: u64) -> bool {
    if tag == 0 || tag == RESETTING {
        return false;
    }
    let bucket = tag - 1;
    bucket <= now && now - bucket < len
}

/// A ring of bucket slots.
#[derive(Debug)]
struct Ring {
    slots: Box<[Slot]>,
}

impl Ring {
    fn new(len: usize) -> Self {
        Self {
            slots: (0..len.max(1)).map(|_| Slot::new()).collect(),
        }
    }

    /// Add `adds` to bucket `bucket`, resetting the slot it reuses first.
    /// False when the sample was dropped: the slot is being reset by another
    /// recorder, or already holds a newer bucket (a recorder that read the
    /// clock a whole window ago).
    fn add(&self, bucket: u64, adds: &[(Counter, u64)]) -> bool {
        let slot = &self.slots[(bucket % self.slots.len() as u64) as usize];
        let tag = bucket + 1;
        let seen = slot.tag.load(Ordering::Acquire);
        if seen != tag && !Self::reset(slot, seen, tag) {
            return false;
        }
        for &(counter, n) in adds {
            slot.counts[counter as usize].fetch_add(n, Ordering::Relaxed);
        }
        true
    }

    /// Move `slot` from tag `seen` to `tag` (zeroing its counts) unless it is
    /// newer or another recorder is at it; true when the slot now holds `tag`.
    fn reset(slot: &Slot, seen: u64, tag: u64) -> bool {
        if seen > tag {
            // RESETTING included: never wait for another recorder.
            return false;
        }
        if seen == 0 {
            // Never used: its counts are zero, so it is claimed in one step
            // and no concurrent recorder of the same bucket drops a sample.
            return match slot
                .tag
                .compare_exchange(0, tag, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => true,
                Err(now) => now == tag,
            };
        }
        match slot
            .tag
            .compare_exchange(seen, RESETTING, Ordering::Acquire, Ordering::Acquire)
        {
            Ok(_) => {
                for count in &slot.counts {
                    count.store(0, Ordering::Relaxed);
                }
                slot.tag.store(tag, Ordering::Release);
                true
            }
            Err(now) => now == tag,
        }
    }

    /// The counts of the buckets inside the window that ends with `now`.
    fn window(&self, now: u64) -> [u64; COUNTERS] {
        let len = self.slots.len() as u64;
        let mut sum = [0u64; COUNTERS];
        for slot in self.slots.iter() {
            let tag = slot.tag.load(Ordering::Acquire);
            if !in_window(tag, now, len) {
                continue;
            }
            let counts: [u64; COUNTERS] =
                std::array::from_fn(|i| slot.counts[i].load(Ordering::Relaxed));
            std::sync::atomic::fence(Ordering::Acquire);
            if slot.tag.load(Ordering::Relaxed) != tag {
                // Reset for a newer bucket while it was read.
                continue;
            }
            for (s, c) in sum.iter_mut().zip(counts) {
                *s = s.saturating_add(c);
            }
        }
        sum
    }

    /// Whether any slot holds a bucket inside the window that ends with `now`.
    fn active(&self, now: u64) -> bool {
        let len = self.slots.len() as u64;
        self.slots
            .iter()
            .any(|s| in_window(s.tag.load(Ordering::Relaxed), now, len))
    }
}

/// One tracked table.
#[derive(Debug)]
struct TableHeat {
    ring: Ring,
    /// 1 + the second (since the heatmap's epoch) of its last hit or miss;
    /// 0 = none yet.
    last_read: AtomicU64,
}

impl TableHeat {
    fn new(buckets: usize) -> Self {
        Self {
            ring: Ring::new(buckets),
            last_read: AtomicU64::new(0),
        }
    }

    /// Add `adds` to bucket `bucket` and, for a hit or a miss, note the read.
    fn add(&self, bucket: u64, adds: &[(Counter, u64)], kind: Kind, secs: u64) {
        self.ring.add(bucket, adds);
        if kind != Kind::Other {
            self.touch(secs);
        }
    }

    /// Note a read at `secs`: a store only when the second changed, so
    /// concurrent readers of a hot table write the line once a second.
    fn touch(&self, secs: u64) {
        let v = secs.saturating_add(1);
        if self.last_read.load(Ordering::Relaxed) < v {
            self.last_read.store(v, Ordering::Relaxed);
        }
    }
}

/// Tracked tables by name (the names the C-02 generations use).
type TableMap = HashMap<Box<str>, Arc<TableHeat>>;

/// Temperature of a table: the quartile of its reads (hits + misses) in the
/// window among the tracked tables (the top quartile is `hot`); a table with
/// no read in the window is `frozen`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Temperature {
    Frozen,
    Cold,
    Warm,
    Hot,
}

impl Temperature {
    /// The class of `reads` given the reads of all tables, sorted ascending
    /// (`distribcache::heatmap`'s quartile rule).
    fn of(reads: u64, sorted: &[u64]) -> Self {
        let quartile = |q: usize| sorted.get(sorted.len() * q / 4).copied().unwrap_or(0);
        if reads == 0 {
            Temperature::Frozen
        } else if reads >= quartile(3) {
            Temperature::Hot
        } else if reads >= quartile(2) {
            Temperature::Warm
        } else if reads >= quartile(1) {
            Temperature::Cold
        } else {
            Temperature::Frozen
        }
    }
}

/// One table's counts over the window.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TableHeatSnapshot {
    pub table: String,
    pub hits: u64,
    pub misses: u64,
    /// Of `misses`: the L2 entry was found past its TTL.
    pub expired: u64,
    pub stores: u64,
    /// Bytes of the results stored (not what is retained now).
    pub stored_bytes: u64,
    pub invalidations: u64,
    pub stale_rejected: u64,
    pub fills_raced: u64,
    /// `hits / (hits + misses)`; `None` without a read in the window.
    pub hit_ratio: Option<f64>,
    pub temperature: Temperature,
    /// Seconds since its last hit or miss; `None` if it never had one.
    pub last_access_secs_ago: Option<u64>,
}

impl TableHeatSnapshot {
    fn reads(&self) -> u64 {
        self.hits.saturating_add(self.misses)
    }
}

/// A recommendation: a change to a real `[cache]` key for one table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Recommendation {
    pub table: String,
    /// The dotted config key path (`cache.exclude_tables`,
    /// `cache.tables.ttl_secs`).
    pub key: &'static str,
    /// `add` (to the list) or `raise`.
    pub action: &'static str,
    pub reason: String,
}

/// The heatmap's view of the window, for `GET /api/cache/heatmap`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HeatmapSnapshot {
    pub window_secs: u64,
    pub bucket_secs: u64,
    pub buckets: usize,
    pub max_tables: usize,
    pub min_samples: u64,
    /// Tables tracked (some may have nothing left in the window: they are
    /// not listed, and are the first dropped when the set is full).
    pub tables_tracked: usize,
    /// Samples of a table the full set could not take, since startup.
    pub overflow: u64,
    /// Misses in the window not attributed to a tracked table: a read with
    /// no extracted table counts once, a read of an untracked table once per
    /// such table.
    pub untracked_misses: u64,
    /// Whole-cache invalidations (unknown write scope) in the window.
    pub invalidate_all: u64,
    /// Tables with anything in the window, hottest first.
    pub tables: Vec<TableHeatSnapshot>,
    pub recommendations: Vec<Recommendation>,
}

/// TTLs the recommendations quote: the cache's `ttl_secs` and a table's own
/// `[[cache.tables]] ttl_secs`.
pub struct TtlView<'a> {
    pub default: Duration,
    pub table: &'a dyn Fn(&str) -> Option<Duration>,
}

/// The per-table heatmap of one query cache.
pub struct CacheHeatmap {
    config: HeatmapConfig,
    /// Time zero of the bucket numbering.
    epoch: Instant,
    tables: ArcSwap<TableMap>,
    /// Taken only to add (or prune) tables; never by a recorder of a tracked
    /// table.
    grow: Mutex<()>,
    /// Untracked misses ([`Counter::Misses`]) and whole-cache invalidations
    /// ([`Counter::Invalidations`]).
    global: Ring,
    overflow: AtomicU64,
    /// 1 + the bucket the full set was last pruned in; 0 = never.
    pruned: AtomicU64,
}

impl CacheHeatmap {
    /// A heatmap with `config`'s bucket length, window and table bound
    /// (each at least 1).
    pub fn new(config: HeatmapConfig) -> Self {
        let config = HeatmapConfig {
            bucket_secs: config.bucket_secs.max(1),
            buckets: config.buckets.max(1),
            max_tables: config.max_tables.max(1),
            ..config
        };
        Self {
            global: Ring::new(config.buckets),
            config,
            epoch: Instant::now(),
            tables: ArcSwap::from_pointee(TableMap::new()),
            grow: Mutex::new(()),
            overflow: AtomicU64::new(0),
            pruned: AtomicU64::new(0),
        }
    }

    /// Its configuration.
    pub fn config(&self) -> &HeatmapConfig {
        &self.config
    }

    /// Seconds from the epoch to `at` (0 before it).
    fn secs(&self, at: Instant) -> u64 {
        at.saturating_duration_since(self.epoch).as_secs()
    }

    /// A hit on an entry of `tables`.
    pub fn record_hit(&self, tables: &[String], at: Instant) {
        self.record_at(tables, &[(Counter::Hits, 1)], Kind::Read, self.secs(at));
    }

    /// A lookup of a read of `tables` that went to the backend; `expired`
    /// when its L2 entry was there but past its TTL.
    pub fn record_miss(&self, tables: &[String], expired: bool, at: Instant) {
        let adds: &[(Counter, u64)] = if expired {
            &[(Counter::Misses, 1), (Counter::Expired, 1)]
        } else {
            &[(Counter::Misses, 1)]
        };
        self.record_at(tables, adds, Kind::Miss, self.secs(at));
    }

    /// A stored result of `bytes` bytes that read `tables`.
    pub fn record_store(&self, tables: &[String], bytes: usize, at: Instant) {
        self.record_at(
            tables,
            &[(Counter::Stores, 1), (Counter::StoredBytes, bytes as u64)],
            Kind::Other,
            self.secs(at),
        );
    }

    /// A C-02 write mark of `tables`.
    pub fn record_invalidation(&self, tables: &[String], at: Instant) {
        self.record_at(
            tables,
            &[(Counter::Invalidations, 1)],
            Kind::Other,
            self.secs(at),
        );
    }

    /// A whole-cache invalidation (write of unknown scope).
    pub fn record_invalidate_all(&self, at: Instant) {
        let bucket = self.secs(at) / self.config.bucket_secs;
        self.global.add(bucket, &[(Counter::Invalidations, 1)]);
    }

    /// An entry of `tables` refused because one of them was written since
    /// its fetch.
    pub fn record_stale_rejected(&self, tables: &[String], at: Instant) {
        self.record_at(
            tables,
            &[(Counter::StaleRejected, 1)],
            Kind::Other,
            self.secs(at),
        );
    }

    /// A fill of `tables` not stored because a write raced its fetch.
    pub fn record_fill_raced(&self, tables: &[String], at: Instant) {
        self.record_at(
            tables,
            &[(Counter::FillsRaced, 1)],
            Kind::Other,
            self.secs(at),
        );
    }

    /// Add `adds` to the current bucket of each of `tables` at `secs`.
    fn record_at(&self, tables: &[String], adds: &[(Counter, u64)], kind: Kind, secs: u64) {
        let bucket = secs / self.config.bucket_secs;
        if tables.is_empty() {
            if kind == Kind::Miss {
                self.global.add(bucket, &[(Counter::Misses, 1)]);
            }
            return;
        }
        let map = self.tables.load();
        for table in tables {
            if let Some(heat) = map.get(table.as_str()) {
                heat.add(bucket, adds, kind, secs);
            } else if kind == Kind::Miss {
                self.global.add(bucket, &[(Counter::Misses, 1)]);
            } else if let Some(heat) = self.track(table, map.len(), bucket) {
                heat.add(bucket, adds, kind, secs);
            } else {
                self.overflow.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// The heat of `table`, added to the set (copy-on-write) unless the set
    /// is full; once full, the tables with nothing in the window are dropped
    /// first, at most once per bucket. `len` is the size of the set the
    /// caller loaded: a full set already pruned in this bucket is refused
    /// without the lock.
    fn track(&self, table: &str, len: usize, bucket: u64) -> Option<Arc<TableHeat>> {
        let max = self.config.max_tables;
        if len >= max && self.pruned.load(Ordering::Relaxed) == bucket + 1 {
            return None;
        }
        let _grow = self.grow.lock().unwrap_or_else(|e| e.into_inner());
        let current = self.tables.load_full();
        if let Some(heat) = current.get(table) {
            return Some(heat.clone());
        }
        let mut next = if current.len() >= max {
            if self.pruned.swap(bucket + 1, Ordering::Relaxed) == bucket + 1 {
                return None;
            }
            let kept: TableMap = current
                .iter()
                .filter(|(_, heat)| heat.ring.active(bucket))
                .map(|(name, heat)| (name.clone(), heat.clone()))
                .collect();
            if kept.len() >= max {
                return None;
            }
            kept
        } else {
            (*current).clone()
        };
        let heat = Arc::new(TableHeat::new(self.config.buckets));
        next.insert(table.into(), heat.clone());
        self.tables.store(Arc::new(next));
        Some(heat)
    }

    /// The window as of `at`, with recommendations quoting `ttls`.
    pub fn snapshot(&self, at: Instant, ttls: &TtlView<'_>) -> HeatmapSnapshot {
        self.snapshot_at(self.secs(at), ttls)
    }

    /// [`Self::snapshot`] at `secs` since the epoch.
    fn snapshot_at(&self, secs: u64, ttls: &TtlView<'_>) -> HeatmapSnapshot {
        let bucket = secs / self.config.bucket_secs;
        let map = self.tables.load_full();
        let mut tables: Vec<TableHeatSnapshot> = map
            .iter()
            .filter_map(|(name, heat)| {
                let c = heat.ring.window(bucket);
                if c.iter().all(|&n| n == 0) {
                    return None;
                }
                let (hits, misses) = (c[Counter::Hits as usize], c[Counter::Misses as usize]);
                let reads = hits.saturating_add(misses);
                let last = heat.last_read.load(Ordering::Relaxed);
                Some(TableHeatSnapshot {
                    table: name.to_string(),
                    hits,
                    misses,
                    expired: c[Counter::Expired as usize],
                    stores: c[Counter::Stores as usize],
                    stored_bytes: c[Counter::StoredBytes as usize],
                    invalidations: c[Counter::Invalidations as usize],
                    stale_rejected: c[Counter::StaleRejected as usize],
                    fills_raced: c[Counter::FillsRaced as usize],
                    hit_ratio: if reads > 0 {
                        Some(hits as f64 / reads as f64)
                    } else {
                        None
                    },
                    temperature: Temperature::Frozen,
                    last_access_secs_ago: if last > 0 {
                        Some(secs.saturating_sub(last - 1))
                    } else {
                        None
                    },
                })
            })
            .collect();
        // Quartiles of the tables that were read at all (the rest are frozen).
        let mut reads: Vec<u64> = tables
            .iter()
            .map(TableHeatSnapshot::reads)
            .filter(|&r| r > 0)
            .collect();
        reads.sort_unstable();
        for t in &mut tables {
            t.temperature = Temperature::of(t.reads(), &reads);
        }
        tables.sort_by(|a, b| {
            b.reads()
                .cmp(&a.reads())
                .then_with(|| a.table.cmp(&b.table))
        });
        let recommendations = tables
            .iter()
            .filter_map(|t| self.recommend(t, ttls))
            .collect();
        let global = self.global.window(bucket);
        HeatmapSnapshot {
            window_secs: self.config.window_secs(),
            bucket_secs: self.config.bucket_secs,
            buckets: self.config.buckets,
            max_tables: self.config.max_tables,
            min_samples: self.config.min_samples,
            tables_tracked: map.len(),
            overflow: self.overflow.load(Ordering::Relaxed),
            untracked_misses: global[Counter::Misses as usize],
            invalidate_all: global[Counter::Invalidations as usize],
            tables,
            recommendations,
        }
    }

    /// The recommendation for `t`, if its counters support one. Only for a
    /// table with `min_samples` reads in the window whose name a `[cache]`
    /// table key accepts; churn first, since an excluded table needs no TTL:
    ///
    /// - **churn**: more invalidations than hits: writes retire its entries
    ///   faster than reads reuse them, so caching it costs a store per fill
    ///   and serves little; add it to `cache.exclude_tables`;
    /// - **expiry**: more of its misses found the entry expired than not:
    ///   entries outlive no write but their TTL, so raise its
    ///   `[[cache.tables]] ttl_secs`.
    fn recommend(&self, t: &TableHeatSnapshot, ttls: &TtlView<'_>) -> Option<Recommendation> {
        if t.reads() < self.config.min_samples
            || crate::config::cache_table_name_error("", &t.table).is_some()
        {
            return None;
        }
        let window = self.config.window_secs();
        let own_ttl = (ttls.table)(&t.table);
        if t.invalidations > t.hits {
            return Some(Recommendation {
                table: t.table.clone(),
                key: "cache.exclude_tables",
                action: "add",
                reason: format!(
                    "{} invalidations against {} hits in the last {window} s: writes retire its \
                     entries faster than reads reuse them{}",
                    t.invalidations,
                    t.hits,
                    if own_ttl.is_some() {
                        " (also remove its [[cache.tables]] ttl_secs entry)"
                    } else {
                        ""
                    }
                ),
            });
        }
        // Saturating: the counts of a slot are read one by one, so a
        // concurrent expired miss may show in `expired` but not `misses`.
        if t.expired > t.misses.saturating_sub(t.expired) {
            let ttl = match own_ttl {
                Some(ttl) => format!("its [[cache.tables]] ttl_secs = {}", ttl.as_secs()),
                None => format!(
                    "cache.ttl_secs = {}; a [[cache.tables]] ttl_secs above that applies only \
                     to single-table reads of the semantic grammar",
                    ttls.default.as_secs()
                ),
            };
            return Some(Recommendation {
                table: t.table.clone(),
                key: "cache.tables.ttl_secs",
                action: "raise",
                reason: format!(
                    "{} of its {} misses in the last {window} s found the entry expired ({ttl}): \
                     a longer TTL would have served them unless a write retired them first",
                    t.expired, t.misses
                ),
            });
        }
        None
    }
}

/// How a record touches the table set and its last read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A hit: tracks the table, notes the read.
    Read,
    /// A miss: notes the read of a tracked table; an untracked one is
    /// counted in `untracked_misses`, never added.
    Miss,
    /// Anything else: tracks the table.
    Other,
}

/// A query cache's heatmap, or none: with `heatmap_enabled = false` every
/// record is one branch (no allocation, no atomic).
#[derive(Default)]
pub struct HeatRecorder(Option<Box<CacheHeatmap>>);

impl HeatRecorder {
    /// The heatmap `config` asks for (none when disabled).
    pub fn new(config: &HeatmapConfig) -> Self {
        Self(
            config
                .enabled
                .then(|| Box::new(CacheHeatmap::new(config.clone()))),
        )
    }

    /// The heatmap, when enabled.
    pub fn heatmap(&self) -> Option<&CacheHeatmap> {
        self.0.as_deref()
    }

    /// [`CacheHeatmap::record_hit`], when enabled.
    #[inline]
    pub fn record_hit(&self, tables: &[String], at: Instant) {
        if let Some(h) = &self.0 {
            h.record_hit(tables, at);
        }
    }

    /// [`CacheHeatmap::record_miss`], when enabled.
    #[inline]
    pub fn record_miss(&self, tables: &[String], expired: bool, at: Instant) {
        if let Some(h) = &self.0 {
            h.record_miss(tables, expired, at);
        }
    }

    /// [`CacheHeatmap::record_store`], when enabled.
    #[inline]
    pub fn record_store(&self, tables: &[String], bytes: usize, at: Instant) {
        if let Some(h) = &self.0 {
            h.record_store(tables, bytes, at);
        }
    }

    /// [`CacheHeatmap::record_stale_rejected`], when enabled.
    #[inline]
    pub fn record_stale_rejected(&self, tables: &[String], at: Instant) {
        if let Some(h) = &self.0 {
            h.record_stale_rejected(tables, at);
        }
    }

    /// [`CacheHeatmap::record_fill_raced`], when enabled (reads the clock
    /// only then).
    #[inline]
    pub fn record_fill_raced(&self, tables: &[String]) {
        if let Some(h) = &self.0 {
            h.record_fill_raced(tables, Instant::now());
        }
    }

    /// [`CacheHeatmap::record_invalidation`], when enabled (reads the clock
    /// only then).
    #[inline]
    pub fn record_invalidation(&self, tables: &[String]) {
        if let Some(h) = &self.0 {
            h.record_invalidation(tables, Instant::now());
        }
    }

    /// [`CacheHeatmap::record_invalidate_all`], when enabled (reads the
    /// clock only then).
    #[inline]
    pub fn record_invalidate_all(&self) {
        if let Some(h) = &self.0 {
            h.record_invalidate_all(Instant::now());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heatmap(bucket_secs: u64, buckets: usize, max_tables: usize) -> CacheHeatmap {
        CacheHeatmap::new(HeatmapConfig {
            enabled: true,
            bucket_secs,
            buckets,
            max_tables,
            min_samples: 100,
        })
    }

    fn names(tables: &[&str]) -> Vec<String> {
        tables.iter().map(|t| t.to_string()).collect()
    }

    fn no_table_ttl(_: &str) -> Option<Duration> {
        None
    }

    fn ttls() -> TtlView<'static> {
        TtlView {
            default: Duration::from_secs(300),
            table: &no_table_ttl,
        }
    }

    fn snap(h: &CacheHeatmap, secs: u64) -> HeatmapSnapshot {
        h.snapshot_at(secs, &ttls())
    }

    fn row<'a>(s: &'a HeatmapSnapshot, table: &str) -> Option<&'a TableHeatSnapshot> {
        s.tables.iter().find(|t| t.table == table)
    }

    fn hits(h: &CacheHeatmap, table: &str, n: u64, secs: u64) {
        let tables = names(&[table]);
        for _ in 0..n {
            h.record_at(&tables, &[(Counter::Hits, 1)], Kind::Read, secs);
        }
    }

    fn misses(h: &CacheHeatmap, table: &str, n: u64, expired: bool, secs: u64) {
        let tables = names(&[table]);
        let adds: &[(Counter, u64)] = if expired {
            &[(Counter::Misses, 1), (Counter::Expired, 1)]
        } else {
            &[(Counter::Misses, 1)]
        };
        for _ in 0..n {
            h.record_at(&tables, adds, Kind::Miss, secs);
        }
    }

    fn invalidations(h: &CacheHeatmap, table: &str, n: u64, secs: u64) {
        let tables = names(&[table]);
        for _ in 0..n {
            h.record_at(&tables, &[(Counter::Invalidations, 1)], Kind::Other, secs);
        }
    }

    /// Buckets rotate with the (injected) clock: a sample counts while its
    /// bucket is one of the last `buckets`, then leaves the window; a table
    /// with nothing left is no longer listed.
    #[test]
    fn buckets_rotate_and_old_samples_leave_the_window() {
        let h = heatmap(10, 3, 16);
        hits(&h, "t", 1, 0); // bucket 0
        hits(&h, "t", 1, 15); // bucket 1
        hits(&h, "t", 1, 25); // bucket 2
        assert_eq!(snap(&h, 25).tables[0].hits, 3);
        assert_eq!(snap(&h, 29).tables[0].hits, 3);
        // Bucket 3: bucket 0 is out.
        assert_eq!(snap(&h, 30).tables[0].hits, 2);
        // Bucket 4: bucket 1 is out too.
        assert_eq!(snap(&h, 45).tables[0].hits, 1);
        let s = snap(&h, 50);
        assert!(s.tables.is_empty(), "nothing left in the window");
        assert_eq!(s.tables_tracked, 1);
        assert_eq!(s.window_secs, 30);
        assert_eq!((s.bucket_secs, s.buckets), (10, 3));
    }

    /// A slot reused for a new bucket starts from zero: the counts of the
    /// bucket a window earlier are not carried into it.
    #[test]
    fn a_reused_slot_is_reset_for_its_new_bucket() {
        let h = heatmap(10, 3, 16);
        hits(&h, "t", 5, 0); // bucket 0, slot 0
        hits(&h, "t", 1, 30); // bucket 3, slot 0 again
        assert_eq!(snap(&h, 30).tables[0].hits, 1);
        // A recorder that read the clock a window ago lands on a slot that
        // already holds a newer bucket: dropped, never counted there.
        hits(&h, "t", 1, 0);
        assert_eq!(snap(&h, 30).tables[0].hits, 1);
    }

    /// The table set never grows past `max_tables`: a sample of another
    /// table counts in `overflow`, and a plain miss never adds a table (it
    /// counts in `untracked_misses`).
    #[test]
    fn the_table_set_is_bounded_and_counts_overflow() {
        let h = heatmap(10, 3, 2);
        invalidations(&h, "a", 1, 0);
        hits(&h, "b", 1, 0);
        hits(&h, "c", 2, 0);
        invalidations(&h, "d", 1, 0);
        misses(&h, "e", 3, false, 0);
        let s = snap(&h, 0);
        assert_eq!(s.tables_tracked, 2);
        assert_eq!(s.overflow, 3, "two hits of c, one write of d");
        assert_eq!(s.untracked_misses, 3);
        let tracked: Vec<&str> = s.tables.iter().map(|t| t.table.as_str()).collect();
        assert_eq!(tracked, ["b", "a"]);
        // Tracked tables keep counting; a miss of one is its own.
        hits(&h, "a", 1, 1);
        misses(&h, "a", 1, false, 1);
        let s = snap(&h, 1);
        assert_eq!(
            (row(&s, "a").unwrap().hits, row(&s, "a").unwrap().misses),
            (1, 1)
        );
        assert_eq!(s.untracked_misses, 3);
        // A read with no extracted table is an untracked miss once.
        h.record_at(&[], &[(Counter::Misses, 1)], Kind::Miss, 1);
        assert_eq!(snap(&h, 1).untracked_misses, 4);
    }

    /// Once full, the tables with nothing left in the window make room, at
    /// most once per bucket; a table still in the window stays.
    #[test]
    fn a_full_set_drops_tables_idle_for_the_window() {
        let h = heatmap(10, 3, 2);
        hits(&h, "a", 1, 0); // bucket 0
        hits(&h, "b", 1, 45); // bucket 4
        hits(&h, "c", 1, 50); // bucket 5: a is out of the window, b is not
        let s = snap(&h, 50);
        assert_eq!(s.tables_tracked, 2);
        assert_eq!(s.overflow, 0);
        let tracked: Vec<&str> = s.tables.iter().map(|t| t.table.as_str()).collect();
        assert_eq!(tracked, ["b", "c"]);
        // Full again, already pruned in this bucket: d overflows.
        hits(&h, "d", 1, 51);
        assert_eq!(snap(&h, 51).overflow, 1);
        // The next bucket prunes again, but b and c are still in the window.
        hits(&h, "d", 1, 60);
        let s = snap(&h, 60);
        assert_eq!((s.tables_tracked, s.overflow), (2, 2));
    }

    /// Off, the recorder holds nothing and every record is a no-op.
    #[test]
    fn a_disabled_recorder_records_nothing() {
        let off = HeatRecorder::new(&HeatmapConfig::default());
        assert!(off.heatmap().is_none());
        let t = names(&["t"]);
        let now = Instant::now();
        off.record_hit(&t, now);
        off.record_miss(&t, true, now);
        off.record_store(&t, 10, now);
        off.record_stale_rejected(&t, now);
        off.record_fill_raced(&t);
        off.record_invalidation(&t);
        off.record_invalidate_all();
        assert!(off.heatmap().is_none());
        let on = HeatRecorder::new(&HeatmapConfig {
            enabled: true,
            ..Default::default()
        });
        on.record_store(&t, 10, now);
        on.record_hit(&t, now);
        on.record_miss(&t, true, now);
        on.record_stale_rejected(&t, now);
        on.record_fill_raced(&t);
        on.record_invalidation(&t);
        on.record_invalidate_all();
        let s = on.heatmap().unwrap().snapshot(Instant::now(), &ttls());
        let r = row(&s, "t").unwrap();
        assert_eq!(
            (
                r.hits,
                r.misses,
                r.expired,
                r.stores,
                r.stored_bytes,
                r.invalidations,
                r.stale_rejected,
                r.fills_raced
            ),
            (1, 1, 1, 1, 10, 1, 1, 1)
        );
        assert_eq!(r.hit_ratio, Some(0.5));
        assert_eq!(s.invalidate_all, 1);
    }

    /// Temperatures are the quartiles of the tables' reads; a table with no
    /// read in the window is frozen, and the list runs hottest first.
    #[test]
    fn temperature_ranks_tables_by_reads() {
        let h = heatmap(10, 3, 16);
        hits(&h, "a", 1, 0);
        hits(&h, "b", 10, 0);
        hits(&h, "c", 100, 0);
        hits(&h, "d", 1000, 0);
        invalidations(&h, "w", 1, 0);
        let s = snap(&h, 0);
        let order: Vec<(&str, Temperature)> = s
            .tables
            .iter()
            .map(|t| (t.table.as_str(), t.temperature))
            .collect();
        assert_eq!(
            order,
            [
                ("d", Temperature::Hot),
                ("c", Temperature::Warm),
                ("b", Temperature::Cold),
                ("a", Temperature::Frozen),
                ("w", Temperature::Frozen),
            ]
        );
        assert_eq!(row(&s, "w").unwrap().hit_ratio, None);
        // One table alone is hot.
        let h = heatmap(10, 3, 16);
        hits(&h, "only", 1, 0);
        assert_eq!(snap(&h, 0).tables[0].temperature, Temperature::Hot);
    }

    /// `last_access_secs_ago` follows the last hit or miss, not writes.
    #[test]
    fn last_access_follows_reads() {
        let h = heatmap(10, 30, 16);
        hits(&h, "t", 1, 5);
        invalidations(&h, "t", 1, 50);
        invalidations(&h, "w", 1, 50);
        let s = snap(&h, 65);
        assert_eq!(row(&s, "t").unwrap().last_access_secs_ago, Some(60));
        assert_eq!(row(&s, "w").unwrap().last_access_secs_ago, None);
        misses(&h, "t", 1, false, 64);
        assert_eq!(
            row(&snap(&h, 65), "t").unwrap().last_access_secs_ago,
            Some(1)
        );
    }

    /// Churn: more invalidations than hits, with `min_samples` reads, names
    /// `cache.exclude_tables`; not at equality, and not below the sample
    /// floor.
    #[test]
    fn churn_recommends_excluding_the_table() {
        let h = heatmap(10, 3, 16);
        hits(&h, "orders", 10, 0);
        misses(&h, "orders", 90, false, 0);
        invalidations(&h, "orders", 10, 0);
        assert!(snap(&h, 0).recommendations.is_empty(), "10 vs 10");
        invalidations(&h, "orders", 1, 0);
        let s = snap(&h, 0);
        assert_eq!(s.recommendations.len(), 1);
        let r = &s.recommendations[0];
        assert_eq!(
            (r.table.as_str(), r.key, r.action),
            ("orders", "cache.exclude_tables", "add")
        );
        assert!(
            r.reason.contains("11 invalidations against 10 hits"),
            "{r:?}"
        );
        assert!(!r.reason.contains("[[cache.tables]]"), "{r:?}");

        // Below the floor (99 reads): nothing.
        let h = heatmap(10, 3, 16);
        hits(&h, "orders", 10, 0);
        misses(&h, "orders", 89, false, 0);
        invalidations(&h, "orders", 50, 0);
        assert!(snap(&h, 0).recommendations.is_empty());

        // A table with its own TTL: the reason says to drop that entry too.
        let h = heatmap(10, 3, 16);
        misses(&h, "orders", 1, false, 0); // untracked: no table yet
        invalidations(&h, "orders", 1, 0);
        misses(&h, "orders", 100, false, 0);
        let own: &dyn Fn(&str) -> Option<Duration> = &|_| Some(Duration::from_secs(60));
        let s = h.snapshot_at(
            0,
            &TtlView {
                default: Duration::from_secs(300),
                table: own,
            },
        );
        assert!(s.recommendations[0].reason.contains("[[cache.tables]]"));
    }

    /// Expiry: more of the misses found the entry expired than not names
    /// `cache.tables.ttl_secs`; not at equality. Churn wins over it.
    #[test]
    fn expiry_recommends_raising_the_table_ttl() {
        let h = heatmap(10, 3, 16);
        hits(&h, "fx", 40, 0);
        misses(&h, "fx", 30, true, 0);
        misses(&h, "fx", 30, false, 0);
        assert!(snap(&h, 0).recommendations.is_empty(), "30 vs 30");
        misses(&h, "fx", 1, true, 0);
        let s = snap(&h, 0);
        let r = &s.recommendations[0];
        assert_eq!(
            (r.table.as_str(), r.key, r.action),
            ("fx", "cache.tables.ttl_secs", "raise")
        );
        assert!(r.reason.contains("31 of its 61 misses"), "{r:?}");
        assert!(r.reason.contains("cache.ttl_secs = 300"), "{r:?}");
        // With more invalidations than hits, churn is the recommendation.
        invalidations(&h, "fx", 41, 0);
        let s = snap(&h, 0);
        assert_eq!(s.recommendations.len(), 1);
        assert_eq!(s.recommendations[0].key, "cache.exclude_tables");
    }

    /// A name no `[cache]` table key accepts (a reserved word, a non-ASCII
    /// name) gets no recommendation.
    #[test]
    fn no_recommendation_for_a_name_the_keys_refuse() {
        let h = heatmap(10, 3, 16);
        for table in ["order", "caf\u{e9}"] {
            misses(&h, table, 1, false, 0);
            invalidations(&h, table, 5, 0);
            misses(&h, table, 100, false, 0);
        }
        let s = snap(&h, 0);
        assert_eq!(s.tables.len(), 2);
        assert!(s.recommendations.is_empty());
    }

    /// The recommendation keys are real config keys.
    #[test]
    fn recommendation_keys_are_config_keys() {
        let known = crate::config::known_config_keys();
        for key in ["cache.exclude_tables", "cache.tables.ttl_secs"] {
            assert!(known.contains(key), "{key}");
        }
    }

    /// Concurrent recorders of one table lose no sample within a bucket.
    #[test]
    fn concurrent_hits_of_one_table_are_all_counted() {
        let h = Arc::new(heatmap(300, 288, 16));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let h = h.clone();
                std::thread::spawn(move || hits(&h, "hot", 10_000, 7))
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(snap(&h, 7).tables[0].hits, 80_000);
    }

    #[test]
    fn config_validation() {
        assert!(HeatmapConfig::default().validate().is_ok(), "off");
        let on = HeatmapConfig {
            enabled: true,
            ..Default::default()
        };
        assert!(on.validate().is_ok());
        assert_eq!(on.window_secs(), 86_400);
        for broken in [
            HeatmapConfig {
                bucket_secs: 0,
                ..on.clone()
            },
            HeatmapConfig {
                buckets: 0,
                ..on.clone()
            },
            HeatmapConfig {
                max_tables: 0,
                ..on.clone()
            },
            HeatmapConfig {
                min_samples: 0,
                ..on.clone()
            },
            HeatmapConfig {
                buckets: HEATMAP_MAX_SLOTS,
                max_tables: 2,
                ..on.clone()
            },
        ] {
            assert!(broken.validate().is_err(), "{broken:?}");
        }
        // Zeroes are floored at 1 rather than dividing by zero.
        let h = CacheHeatmap::new(HeatmapConfig {
            bucket_secs: 0,
            buckets: 0,
            max_tables: 0,
            ..on
        });
        hits(&h, "t", 1, 3);
        assert_eq!(snap(&h, 3).tables[0].hits, 1);
        assert_eq!(SLOT_BYTES, 72);
    }
}
