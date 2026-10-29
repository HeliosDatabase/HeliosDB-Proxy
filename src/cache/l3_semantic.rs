//! L3 Semantic Cache
//!
//! The tier serves only statements of one small single-relation SELECT
//! grammar (an allow-list, [`super::canonical`]); anything else is
//! ineligible and left to L1/L2. A read is served from an entry with the
//! same form, which `L3Config::semantic_match` picks:
//!
//! - verbatim (the daemon's default): the statement text exactly as the
//!   backend receives it, hint comments included. Sound on every backend.
//! - canonical (PostgreSQL semantics only): hint comments, whitespace,
//!   keyword case, `!=` / `<>`, the relation's alias, `AS`, and which alias a
//!   column qualifier uses do not matter. Names keep their case (relation,
//!   schema, columns, output labels, `ORDER BY` / `GROUP BY` keys: HeliosDB-
//!   Lite does not fold them and HeliosDB-Nano reports a label as written),
//!   a qualifier must be spelled exactly like the alias (or, without one,
//!   the relation), whether a column is qualified matters (`t.f` may be the
//!   function `f(t)`), and any other comment makes the statement ineligible;
//!   everything that decides which rows come back, and the RowDescription,
//!   is compared token for token. Once a backend reports a HeliosDB
//!   `server_version`, the query cache neither stores nor serves this form.
//!
//! Neither needs an embedding service.
//!
//! The only approximate part is optional (canonical form, a similarity
//! threshold below 1.0): in a statement without `DISTINCT` or `GROUP BY`,
//! an unqualified select-list column without an output alias may be served
//! from an entry
//! that lists another unqualified column at the same position when the two
//! column NAMES embed at or above the threshold (a qualified `t.f` may be
//! the call `f(t)`, so it is never substituted). The rows are the stored
//! statement's (the same rows by construction); the values and the
//! RowDescription are the stored column's. Approximate hits are counted.
//!
//! Embeddings are computed in the background, only for column names, and
//! remembered in a bounded LRU that takes at most a quarter of the byte
//! budget and yields to the entries; a lookup or a store never waits on the
//! embedding endpoint, and no name is sent while the entries leave the memo
//! less room than the names one match needs together (they could not all be
//! kept: approximate matching pauses until the entries shrink). A failing
//! endpoint is left alone for `embedding_retry_after`, then probed by
//! exactly one call at a time; exact hits are unaffected meanwhile.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashSet;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Semaphore;

use super::canonical::{self, Canonical};
use super::config::L3Config;
use super::normalizer::QueryNormalizer;
use super::result::CachedResult;
use super::CacheContext;

/// Bytes of JSON one embedding value may take in a response (sign, digits,
/// exponent, separator), for the response-size cap.
const BODY_BYTES_PER_DIM: usize = 32;
/// Response bytes allowed on top of the per-value budget (the object, the
/// `"embedding"` key, whitespace).
const BODY_SLACK_BYTES: usize = 4096;
/// Text the startup probe embeds (never remembered).
const PROBE_TEXT: &str = "heliosdb proxy semantic cache probe";
/// Remembered column-name embeddings take at most `max_bytes /
/// MEMO_SHARE_DIVISOR` (a quarter) of the byte budget, and only what the
/// entries leave of it: approximate matching can never starve the entries.
const MEMO_SHARE_DIVISOR: usize = 4;

/// The embedding endpoint as the tier last saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EndpointState {
    /// No call has completed yet (or the threshold is 1.0: never called).
    Unknown,
    /// The last call that counts succeeded.
    Up,
    /// A call failed; calls pause for `embedding_retry_after`, then one
    /// probes.
    Down,
}

impl EndpointState {
    /// Lower-case name, as on `/metrics`.
    pub fn as_str(self) -> &'static str {
        match self {
            EndpointState::Unknown => "unknown",
            EndpointState::Up => "up",
            EndpointState::Down => "down",
        }
    }
}

/// The semantic tier's health and counters (`/metrics` `query_cache.semantic`).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SemanticHealth {
    /// The embedding endpoint's state.
    pub endpoint: EndpointState,
    /// Embedding calls made (background and probe).
    pub embed_requests: u64,
    /// Embedding calls that failed (error, timeout, non-2xx, bad JSON, wrong
    /// dimension, oversized body).
    pub embed_errors: u64,
    /// Embeddings not requested: already remembered or in flight, no room
    /// left for it in the memo (the entries hold the byte budget), no permit
    /// free, or the endpoint is down.
    pub embed_skipped: u64,
    /// Seconds spent in embedding calls, summed.
    pub embed_seconds_sum: f64,
    /// Embedding calls timed.
    pub embed_seconds_count: u64,
    /// Hits that substituted a similar column (subset of the L3 hits).
    pub approximate_hits: u64,
    /// Hinted reads the tier refused (outside its grammar, or the relation
    /// was not extracted): they use L1/L2 only.
    pub ineligible: u64,
    /// Results not stored because their entry alone would exceed
    /// `semantic_max_bytes` (storing one would evict every entry and
    /// embedding, then itself). They use L1/L2 only.
    pub too_large: u64,
    /// Entries retained.
    pub entries: u64,
    /// Column-name embeddings remembered.
    pub embeddings: u64,
}

/// What a lookup found.
#[derive(Debug)]
pub(crate) enum SemanticLookup {
    /// A result to serve; `approximate` when a column was substituted.
    Hit {
        result: CachedResult,
        approximate: bool,
    },
    /// Only entries a write retired (C-02) matched.
    Stale,
    /// Nothing matched.
    Miss,
}

/// L3 semantic cache: exact canonical matches, plus opt-in column
/// substitution by name similarity.
pub struct L3SemanticCache {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for L3SemanticCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L3SemanticCache")
            .field("entries", &self.len())
            .field("health", &self.health())
            .finish()
    }
}

struct Inner {
    config: L3Config,
    store: RwLock<Store>,
    /// Column-name embeddings, least recently used first.
    memo: Mutex<lru::LruCache<String, Arc<[f32]>>>,
    /// Bytes and names the memo holds (read without its lock).
    memo_bytes: AtomicUsize,
    memo_len: AtomicUsize,
    /// Bytes and count of the entries (mirrors of the store's, lock-free).
    entry_bytes: AtomicUsize,
    entry_count: AtomicUsize,
    client: EmbeddingClient,
    permits: Arc<Semaphore>,
    /// Column names being embedded right now.
    in_flight: DashSet<String>,
    health: Health,
    counters: Counters,
}

#[derive(Default)]
struct Counters {
    requests: AtomicU64,
    errors: AtomicU64,
    skipped: AtomicU64,
    nanos: AtomicU64,
    timed: AtomicU64,
    hits: AtomicU64,
    approximate_hits: AtomicU64,
    ineligible: AtomicU64,
    too_large: AtomicU64,
}

/// One cached result of one canonical statement in one session context.
struct Slot {
    seq: u64,
    database: String,
    user: Option<String>,
    branch: Option<String>,
    shape: String,
    select: String,
    columns: Vec<Option<String>>,
    result: CachedResult,
    charge: usize,
}

impl Slot {
    /// Same statement shape in the same context (a bucket can, after a hash
    /// collision, hold others).
    fn matches(&self, c: &Canonical, ctx: &CacheContext) -> bool {
        self.shape == c.shape
            && self.database == ctx.database
            && self.user == ctx.user
            && self.branch == ctx.branch
    }
}

#[derive(Default)]
struct Store {
    /// Slots by bucket (statement shape + context), oldest first.
    buckets: HashMap<u64, Vec<Slot>>,
    /// Insertion order for eviction, cleaned lazily: (bucket, seq).
    fifo: VecDeque<(u64, u64)>,
    next_seq: u64,
    len: usize,
    bytes: usize,
}

/// Endpoint state, epoch and retry lease.
struct Health {
    /// `epoch << 2 | state`: one word, so a transition and its epoch move
    /// together.
    word: AtomicU64,
    /// Milliseconds since `origin` before which no call may go while down.
    down_until_ms: AtomicU64,
    /// Whether the one call allowed while down (the probe) is in flight. Held
    /// for the call's whole life, however long its timeout is next to the
    /// retry window, so probes never pile up on a hung endpoint.
    probing: AtomicBool,
    origin: Instant,
    retry: Duration,
}

/// Permission for one embedding call: its epoch, and whether it is the
/// probe made while the endpoint is down (whose lease ends with the call).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Claim {
    epoch: u64,
    probe: bool,
}

/// Ends a probe's lease when its call ends (or is never started): the next
/// probe may go once the retry window, which a failure renewed, is over.
struct ProbeLease {
    inner: Arc<Inner>,
}

impl Drop for ProbeLease {
    fn drop(&mut self) {
        self.inner.health.probing.store(false, Ordering::Release);
    }
}

const STATE_UNKNOWN: u64 = 0;
const STATE_UP: u64 = 1;
const STATE_DOWN: u64 = 2;

impl Health {
    fn new(retry: Duration) -> Self {
        Self {
            word: AtomicU64::new(STATE_UNKNOWN),
            down_until_ms: AtomicU64::new(0),
            probing: AtomicBool::new(false),
            origin: Instant::now(),
            retry,
        }
    }

    fn now_ms(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    fn retry_ms(&self) -> u64 {
        u64::try_from(self.retry.as_millis()).unwrap_or(u64::MAX)
    }

    fn state(&self) -> EndpointState {
        match self.word.load(Ordering::Acquire) & 3 {
            STATE_UP => EndpointState::Up,
            STATE_DOWN => EndpointState::Down,
            _ => EndpointState::Unknown,
        }
    }

    /// May a call go now? `Some` with the epoch to tag it with. While down,
    /// only once the retry window is over, and then only for the one caller
    /// that takes the probe lease, which it holds until its call ends
    /// ([`ProbeLease`]): a call may outlast the window (`embedding_timeout`
    /// longer than `embedding_retry_after`), and a second probe must not
    /// start meanwhile.
    fn claim(&self) -> Option<Claim> {
        let word = self.word.load(Ordering::Acquire);
        let epoch = word >> 2;
        if word & 3 != STATE_DOWN {
            return Some(Claim {
                epoch,
                probe: false,
            });
        }
        if self.now_ms() < self.down_until_ms.load(Ordering::Acquire) {
            return None;
        }
        self.probing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| Claim { epoch, probe: true })
    }

    /// A call succeeded: up, with a new epoch on a transition. Returns the
    /// state it left, if it changed.
    fn mark_up(&self) -> Option<EndpointState> {
        let mut word = self.word.load(Ordering::Acquire);
        loop {
            if word & 3 == STATE_UP {
                return None;
            }
            let next = (((word >> 2) + 1) << 2) | STATE_UP;
            match self
                .word
                .compare_exchange(word, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(if word & 3 == STATE_DOWN {
                        EndpointState::Down
                    } else {
                        EndpointState::Unknown
                    })
                }
                Err(seen) => word = seen,
            }
        }
    }

    /// A call tagged `epoch` failed: down for a retry window, unless a
    /// success since it started moved the epoch (a straggler cannot flap the
    /// state). Returns whether this was a transition to down.
    fn mark_down_if_epoch(&self, epoch: u64) -> bool {
        let mut word = self.word.load(Ordering::Acquire);
        loop {
            if word >> 2 != epoch {
                return false;
            }
            self.down_until_ms.fetch_max(
                self.now_ms().saturating_add(self.retry_ms()),
                Ordering::AcqRel,
            );
            if word & 3 == STATE_DOWN {
                return false;
            }
            let next = (epoch << 2) | STATE_DOWN;
            match self
                .word
                .compare_exchange(word, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(seen) => word = seen,
            }
        }
    }
}

/// Removes a name from the in-flight set when its embedding call ends (or
/// is never started).
struct InFlight {
    inner: Arc<Inner>,
    name: String,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.inner.in_flight.remove(&self.name);
    }
}

/// Heap bytes one remembered embedding takes.
fn memo_charge(name: &str, v: &[f32]) -> usize {
    memo_charge_for(name, v.len())
}

/// Heap bytes the embedding of `name` takes once remembered, with `dim`
/// values.
fn memo_charge_for(name: &str, dim: usize) -> usize {
    name.len()
        + dim.saturating_mul(std::mem::size_of::<f32>())
        + std::mem::size_of::<(String, Arc<[f32]>)>()
}

/// The column names one lookup or store needs remembered at the same time,
/// kept within what the memo can hold next to the entries. A set of names
/// that must be in the memo together (a candidate's `want` and `have` names)
/// is fetched only when it fits whole: fetching one of a set that does not
/// fit evicts another of it (least recently used), so the next lookup
/// fetches that one again, and no approximate hit can ever happen.
struct MemoPlan<'a> {
    room: usize,
    max_names: usize,
    dim: usize,
    names: HashSet<&'a str>,
    bytes: usize,
}

impl<'a> MemoPlan<'a> {
    /// Reserve room for every name of `set` (with the names reserved
    /// before); `false`, reserving nothing, when they do not all fit.
    fn reserve(&mut self, set: &[&'a str]) -> bool {
        let mut new: Vec<&'a str> = set
            .iter()
            .copied()
            .filter(|n| !self.names.contains(n))
            .collect();
        new.sort_unstable();
        new.dedup();
        let bytes = new
            .iter()
            .map(|n| memo_charge_for(n, self.dim))
            .fold(self.bytes, usize::saturating_add);
        if bytes > self.room || self.names.len().saturating_add(new.len()) > self.max_names {
            return false;
        }
        self.bytes = bytes;
        self.names.extend(new);
        true
    }
}

/// Bucket of a canonical statement in a session context.
fn bucket_key(c: &Canonical, ctx: &CacheContext) -> u64 {
    let mut h = DefaultHasher::new();
    c.guard.hash(&mut h);
    ctx.database.hash(&mut h);
    ctx.user.hash(&mut h);
    ctx.branch.hash(&mut h);
    h.finish()
}

fn expired(result: &CachedResult, now: Instant) -> bool {
    now.saturating_duration_since(result.cached_at) > result.ttl
}

impl Inner {
    /// The approximate part is on (it is the only user of the endpoint).
    fn approximate_on(&self) -> bool {
        self.config.similarity_threshold < 1.0
    }

    /// Evict oldest-inserted entries while `over` holds. Publishes the
    /// entries' count and bytes. The slot a store just pushed is the newest,
    /// so it goes last.
    fn evict_while(&self, store: &mut Store, over: impl Fn(&Store) -> bool) {
        while over(store) {
            let Some((key, seq)) = store.fifo.pop_front() else {
                break;
            };
            if let Some(bucket) = store.buckets.get_mut(&key) {
                if let Some(at) = bucket.iter().position(|s| s.seq == seq) {
                    let slot = bucket.remove(at);
                    store.len -= 1;
                    store.bytes -= slot.charge;
                }
                if bucket.is_empty() {
                    store.buckets.remove(&key);
                }
            }
        }
        if store.fifo.len() > 2 * store.len + 1 {
            let Store { buckets, fifo, .. } = store;
            fifo.retain(|(key, seq)| {
                buckets
                    .get(key)
                    .is_some_and(|b| b.iter().any(|s| s.seq == *seq))
            });
        }
        self.entry_bytes.store(store.bytes, Ordering::Release);
        self.entry_count.store(store.len, Ordering::Relaxed);
    }

    /// Bytes the remembered embeddings may take next to `entry_bytes` of
    /// entries: at most a [`MEMO_SHARE_DIVISOR`]th of `max_bytes`, and only
    /// what the entries leave of it (entries are never evicted to make room
    /// for embeddings). Unbounded without a byte budget.
    fn memo_room(&self, entry_bytes: usize) -> usize {
        let max_bytes = self.config.max_bytes;
        if max_bytes == 0 {
            return usize::MAX;
        }
        (max_bytes / MEMO_SHARE_DIVISOR).min(max_bytes.saturating_sub(entry_bytes))
    }

    /// An empty [`MemoPlan`] for the memo's room next to the entries now.
    fn memo_plan<'a>(&self) -> MemoPlan<'a> {
        MemoPlan {
            room: self.memo_room(self.entry_bytes.load(Ordering::Acquire)),
            max_names: self.config.max_entries,
            dim: self.config.embedding_dim,
            names: HashSet::new(),
            bytes: 0,
        }
    }

    /// Drop least recently used embeddings until the memo holds at most
    /// `max_entries` names within [`Self::memo_room`], then publish its
    /// bytes and names. The caller holds the memo lock.
    fn trim_memo(
        &self,
        memo: &mut lru::LruCache<String, Arc<[f32]>>,
        mut memo_bytes: usize,
        entry_bytes: usize,
    ) {
        let room = self.memo_room(entry_bytes);
        while memo.len() > self.config.max_entries || memo_bytes > room {
            let Some((name, v)) = memo.pop_lru() else {
                break;
            };
            memo_bytes -= memo_charge(&name, &v);
        }
        self.memo_bytes.store(memo_bytes, Ordering::Relaxed);
        self.memo_len.store(memo.len(), Ordering::Relaxed);
    }

    /// Remember a column name's embedding within the memo's bounds; never
    /// touches an entry. A store that runs meanwhile trims the memo again
    /// under the same lock, with the entries' published bytes.
    fn memoize(&self, name: String, v: Arc<[f32]>) {
        let mut memo = self.memo.lock();
        let mut memo_bytes = self.memo_bytes.load(Ordering::Relaxed) + memo_charge(&name, &v);
        if let Some((old_name, old)) = memo.push(name, v) {
            memo_bytes -= memo_charge(&old_name, &old);
        }
        self.trim_memo(
            &mut memo,
            memo_bytes,
            self.entry_bytes.load(Ordering::Acquire),
        );
    }

    /// One embedding call tagged `epoch`; the result is remembered when
    /// `remember`. Returns the embedding's length on success.
    async fn embed_one(&self, name: &str, epoch: u64, remember: bool) -> Option<usize> {
        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = self.client.try_embed(name).await;
        self.counters.nanos.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.counters.timed.fetch_add(1, Ordering::Relaxed);
        match result {
            Ok(v) => {
                if self.health.mark_up() == Some(EndpointState::Down) {
                    tracing::info!(
                        endpoint = %self.client.endpoint,
                        "semantic cache: the embedding endpoint answers again; approximate \
                         matching resumes"
                    );
                }
                let dim = v.len();
                if remember {
                    self.memoize(name.to_string(), v.into());
                }
                Some(dim)
            }
            Err(error) => {
                self.counters.errors.fetch_add(1, Ordering::Relaxed);
                if self.health.mark_down_if_epoch(epoch) {
                    tracing::warn!(
                        endpoint = %self.client.endpoint,
                        error = %error,
                        retry_secs = self.health.retry.as_secs_f64(),
                        "semantic cache: the embedding endpoint failed; approximate matching \
                         pauses (exact canonical hits continue) and one call probes it after \
                         the retry window"
                    );
                }
                None
            }
        }
    }
}

impl L3SemanticCache {
    /// Create a new L3 semantic cache. Needs no runtime; embedding calls are
    /// spawned on the current tokio runtime when one is running.
    pub fn new(config: L3Config) -> Self {
        let client = EmbeddingClient::with_options(
            config.embedding_endpoint.clone(),
            config.embedding_model.clone(),
            config.embedding_dim,
            config.embedding_timeout,
        );
        let permits = Arc::new(Semaphore::new(
            config
                .embedding_concurrency
                .clamp(1, Semaphore::MAX_PERMITS),
        ));
        let health = Health::new(config.embedding_retry_after);
        Self {
            inner: Arc::new(Inner {
                config,
                store: RwLock::new(Store::default()),
                memo: Mutex::new(lru::LruCache::unbounded()),
                memo_bytes: AtomicUsize::new(0),
                memo_len: AtomicUsize::new(0),
                entry_bytes: AtomicUsize::new(0),
                entry_count: AtomicUsize::new(0),
                client,
                permits,
                in_flight: DashSet::new(),
                health,
                counters: Counters::default(),
            }),
        }
    }

    /// Look up `c` in context `ctx`. Synchronous, no I/O: the newest
    /// current entry with the same select list is an exact hit; otherwise,
    /// in an approximable statement with a threshold below 1.0, the newest
    /// entry whose differing columns all embed at or above the threshold is
    /// an approximate hit. A candidate with a column name that has no
    /// embedding yet is skipped this time; its stored names (backend-
    /// validated: an entry is only stored from a result) are embedded in the
    /// background, only when the memo can hold all of the candidate's names
    /// together. The client's own names are never sent from here: they are
    /// embedded once a statement listing them is stored. The memo lock is
    /// held only to fetch each distinct name once. Expired entries and ones
    /// `is_current` refuses (C-02) are skipped and purged from the bucket
    /// when its lock is free.
    pub(crate) fn lookup(
        &self,
        c: &Canonical,
        ctx: &CacheContext,
        is_current: &dyn Fn(&CachedResult) -> bool,
    ) -> SemanticLookup {
        self.lookup_within(c, ctx, is_current, None)
    }

    /// [`Self::lookup`] serving only an entry at most `max_age` old (a
    /// client's `cache_ttl` hint): an older one is skipped, as if absent,
    /// and kept for reads without that bound.
    pub(crate) fn lookup_within(
        &self,
        c: &Canonical,
        ctx: &CacheContext,
        is_current: &dyn Fn(&CachedResult) -> bool,
        max_age: Option<Duration>,
    ) -> SemanticLookup {
        let inner = &self.inner;
        if !inner.config.enabled {
            return SemanticLookup::Miss;
        }
        let key = bucket_key(c, ctx);
        let now = Instant::now();
        let approximate = c.approximable && inner.approximate_on();
        let mut dead = false;
        let mut stale = 0usize;
        let mut exact = None;
        let mut candidates: Vec<(Vec<Option<String>>, CachedResult)> = Vec::new();
        {
            let store = inner.store.read();
            for slot in store.buckets.get(&key).into_iter().flatten().rev() {
                if !slot.matches(c, ctx) {
                    continue;
                }
                if expired(&slot.result, now) {
                    dead = true;
                    continue;
                }
                if !is_current(&slot.result) {
                    stale += 1;
                    dead = true;
                    continue;
                }
                if max_age
                    .is_some_and(|age| now.saturating_duration_since(slot.result.cached_at) > age)
                {
                    continue;
                }
                if slot.select == c.select {
                    exact = Some(slot.result.clone());
                    break;
                }
                if approximate {
                    candidates.push((slot.columns.clone(), slot.result.clone()));
                }
            }
        }
        if dead {
            self.purge_bucket(key, now, is_current);
        }
        if let Some(result) = exact {
            inner.counters.hits.fetch_add(1, Ordering::Relaxed);
            return SemanticLookup::Hit {
                result,
                approximate: false,
            };
        }
        if !candidates.is_empty() {
            let best = self.score_candidates(c, &candidates);
            if let Some(at) = best {
                inner.counters.hits.fetch_add(1, Ordering::Relaxed);
                inner
                    .counters
                    .approximate_hits
                    .fetch_add(1, Ordering::Relaxed);
                return SemanticLookup::Hit {
                    result: candidates.swap_remove(at).1,
                    approximate: true,
                };
            }
        }
        if stale > 0 {
            SemanticLookup::Stale
        } else {
            SemanticLookup::Miss
        }
    }

    /// The best candidate of an approximate lookup of `c`: the one whose
    /// differing columns all embed at or above the threshold with the
    /// highest lowest cosine (the newest of equals). The memo lock is held
    /// only to fetch each distinct name once, never per candidate or slot.
    /// A candidate with a name lacking an embedding is skipped; its stored
    /// names are fetched in the background when all of its names fit in the
    /// memo together ([`MemoPlan`]), else counted as skipped.
    fn score_candidates(
        &self,
        c: &Canonical,
        candidates: &[(Vec<Option<String>>, CachedResult)],
    ) -> Option<usize> {
        let inner = &self.inner;
        let threshold = inner.config.similarity_threshold;
        // Every candidate's differing slots, (want, have), and where each
        // candidate's run of them ends.
        let mut slots: Vec<(&str, &str)> = Vec::new();
        let mut ends: Vec<usize> = Vec::with_capacity(candidates.len());
        for (columns, _) in candidates {
            slots.extend(c.columns.iter().zip(columns).filter_map(|pair| match pair {
                (Some(want), Some(have)) if want != have => Some((want.as_str(), have.as_str())),
                _ => None,
            }));
            ends.push(slots.len());
        }
        let mut names: Vec<&str> = slots.iter().flat_map(|&(w, h)| [w, h]).collect();
        names.sort_unstable();
        names.dedup();
        let embeddings: Vec<Option<Arc<[f32]>>> = {
            let mut memo = inner.memo.lock();
            names.iter().map(|n| memo.get(*n).cloned()).collect()
        };
        let embedding = |name: &str| {
            names
                .binary_search_by(|probe| (**probe).cmp(name))
                .ok()
                .and_then(|i| embeddings[i].as_deref())
        };
        let mut plan = inner.memo_plan();
        let mut missing: Vec<&str> = Vec::new();
        let mut refused: Vec<&str> = Vec::new();
        let mut best: Option<(f32, usize)> = None;
        let mut start = 0;
        for (at, &end) in ends.iter().enumerate() {
            let run = &slots[start..end];
            start = end;
            // The candidate's names must be remembered together: its absent
            // ones are fetched only when all of them fit next to the newer
            // candidates' ([`MemoPlan`]).
            let needed: Vec<&str> = run.iter().flat_map(|&(w, h)| [w, h]).collect();
            let fits = plan.reserve(&needed);
            let mut complete = true;
            for &(want, have) in run {
                complete &= embedding(want).is_some();
                if embedding(have).is_none() {
                    complete = false;
                    let to = if fits { &mut missing } else { &mut refused };
                    to.push(have);
                }
            }
            if !complete {
                continue;
            }
            let mut score = 1.0f32;
            let mut matched = true;
            for &(want, have) in run {
                let (Some(a), Some(b)) = (embedding(want), embedding(have)) else {
                    matched = false;
                    break;
                };
                let similarity = cosine_similarity(a, b);
                // Fail closed: `f32::min` would ignore a NaN.
                if similarity.is_nan() || similarity < threshold {
                    matched = false;
                    break;
                }
                score = score.min(similarity);
            }
            if matched && best.is_none_or(|(s, _)| score > s) {
                best = Some((score, at));
            }
        }
        missing.sort_unstable();
        missing.dedup();
        refused.sort_unstable();
        refused.dedup();
        refused.retain(|n| missing.binary_search(n).is_err());
        if !refused.is_empty() {
            inner
                .counters
                .skipped
                .fetch_add(refused.len() as u64, Ordering::Relaxed);
        }
        if !missing.is_empty() {
            self.schedule_embed(missing.into_iter().map(str::to_string).collect());
        }
        best.map(|(_, at)| at)
    }

    /// Drop the expired and retired slots of one bucket, unless its lock is
    /// contended (the next lookup or store tries again).
    fn purge_bucket(&self, key: u64, now: Instant, is_current: &dyn Fn(&CachedResult) -> bool) {
        let Some(mut guard) = self.inner.store.try_write() else {
            return;
        };
        let store = &mut *guard;
        let Some(bucket) = store.buckets.get_mut(&key) else {
            return;
        };
        let mut freed = 0;
        let mut removed = 0;
        bucket.retain(|s| {
            let alive = !expired(&s.result, now) && is_current(&s.result);
            if !alive {
                freed += s.charge;
                removed += 1;
            }
            alive
        });
        if bucket.is_empty() {
            store.buckets.remove(&key);
        }
        store.len -= removed;
        store.bytes -= freed;
        self.inner.entry_bytes.store(store.bytes, Ordering::Relaxed);
        self.inner.entry_count.store(store.len, Ordering::Relaxed);
    }

    /// Store `result` for `c` in context `ctx`. Synchronous, no I/O: the
    /// bucket's expired, retired (`is_current` false) and same-select slots
    /// are replaced, a full bucket (`max_variants`) drops its oldest, and
    /// the tier evicts oldest-inserted entries to `max_entries`. Within
    /// `max_bytes` the remembered embeddings yield first (least recently used
    /// first); entries are evicted only while they alone exceed it, so never
    /// to make room for embeddings. An entry that alone exceeds `max_bytes`
    /// could never be kept: it is not stored (counted in `too_large`), and
    /// nothing is evicted or trimmed for it. The entry's TTL is capped at
    /// `ttl`. In an approximable statement (threshold below 1.0), its column
    /// names without an embedding are embedded in the background.
    pub(crate) fn store(
        &self,
        c: &Canonical,
        ctx: &CacheContext,
        mut result: CachedResult,
        is_current: &dyn Fn(&CachedResult) -> bool,
    ) {
        let inner = &self.inner;
        if !inner.config.enabled {
            return;
        }
        if result.ttl > inner.config.ttl {
            result.ttl = inner.config.ttl;
        }
        let key = bucket_key(c, ctx);
        let now = Instant::now();
        let charge = result.size()
            + result.tables.iter().map(String::len).sum::<usize>()
            + c.shape.len()
            + c.select.len()
            + c.columns.iter().flatten().map(String::len).sum::<usize>()
            + ctx.database.len()
            + ctx.user.as_ref().map_or(0, String::len)
            + ctx.branch.as_ref().map_or(0, String::len)
            + std::mem::size_of::<Slot>();
        if inner.config.max_bytes > 0 && charge > inner.config.max_bytes {
            inner.counters.too_large.fetch_add(1, Ordering::Relaxed);
            return;
        }
        {
            let mut guard = inner.store.write();
            let store = &mut *guard;
            let seq = store.next_seq;
            store.next_seq += 1;
            let bucket = store.buckets.entry(key).or_default();
            let mut freed = 0;
            let mut removed = 0;
            bucket.retain(|s| {
                let replace = s.matches(c, ctx)
                    && (expired(&s.result, now) || !is_current(&s.result) || s.select == c.select);
                if replace {
                    freed += s.charge;
                    removed += 1;
                }
                !replace
            });
            if bucket.len() >= inner.config.max_variants.max(1) {
                let oldest = bucket.remove(0);
                freed += oldest.charge;
                removed += 1;
            }
            bucket.push(Slot {
                seq,
                database: ctx.database.clone(),
                user: ctx.user.clone(),
                branch: ctx.branch.clone(),
                shape: c.shape.clone(),
                select: c.select.clone(),
                columns: c.columns.clone(),
                result,
                charge,
            });
            store.fifo.push_back((key, seq));
            store.len = store.len + 1 - removed;
            store.bytes = store.bytes + charge - freed;
            let max_entries = inner.config.max_entries;
            inner.evict_while(store, |s| s.len > max_entries);
            // Entries are evicted only when they alone exceed the byte
            // budget, so never for the memo.
            let max_bytes = inner.config.max_bytes;
            inner.evict_while(store, |s| max_bytes > 0 && s.bytes > max_bytes);
        }
        // Within the byte budget the embeddings yield first: the memo is
        // trimmed to what the entries (as published) leave. Under its own
        // lock, after the store's is released: a lookup holding the memo lock
        // never stalls a store that holds the store lock, and through it
        // every other session's lookup.
        let unknown: Vec<String> = {
            let mut memo = inner.memo.lock();
            inner.trim_memo(
                &mut memo,
                inner.memo_bytes.load(Ordering::Relaxed),
                inner.entry_bytes.load(Ordering::Acquire),
            );
            if c.approximable && inner.approximate_on() {
                let names: Vec<&str> = c.columns.iter().flatten().map(String::as_str).collect();
                let mut unknown: Vec<String> = names
                    .iter()
                    .filter(|n| !memo.contains(**n))
                    .map(|n| n.to_string())
                    .collect();
                unknown.sort();
                unknown.dedup();
                // The statement's names are matched together: fetched only
                // when they all fit in the memo ([`MemoPlan`]).
                if !unknown.is_empty() && !inner.memo_plan().reserve(&names) {
                    inner
                        .counters
                        .skipped
                        .fetch_add(unknown.len() as u64, Ordering::Relaxed);
                    unknown.clear();
                }
                unknown
            } else {
                Vec::new()
            }
        };
        if !unknown.is_empty() {
            self.schedule_embed(unknown);
        }
    }

    /// Embed `names` in the background, each at most once at a time, within
    /// the permit budget, while the endpoint may be called. Never waits:
    /// whatever cannot start now is skipped (counted). A name whose
    /// embedding the memo has no room for (the entries hold the byte budget,
    /// [`Inner::memo_room`]) is not fetched: it would be dropped on arrival.
    /// The callers pass only names whose whole set fits ([`MemoPlan`]).
    fn schedule_embed(&self, names: Vec<String>) {
        let inner = &self.inner;
        if !inner.approximate_on() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let skip = || {
            inner.counters.skipped.fetch_add(1, Ordering::Relaxed);
        };
        let room = inner.memo_room(inner.entry_bytes.load(Ordering::Acquire));
        for name in names {
            if room < memo_charge_for(&name, inner.config.embedding_dim)
                || inner.memo.lock().contains(&name)
                || !inner.in_flight.insert(name.clone())
            {
                skip();
                continue;
            }
            let guard = InFlight {
                inner: inner.clone(),
                name,
            };
            let Ok(permit) = inner.permits.clone().try_acquire_owned() else {
                skip();
                continue;
            };
            let Some(claim) = inner.health.claim() else {
                skip();
                continue;
            };
            let lease = claim.probe.then(|| ProbeLease {
                inner: inner.clone(),
            });
            runtime.spawn(async move {
                let _permit = permit;
                let _lease = lease;
                let inner = guard.inner.clone();
                inner.embed_one(&guard.name, claim.epoch, true).await;
                drop(guard);
            });
        }
    }

    /// One embedding of a fixed text, not remembered, to learn the
    /// endpoint's state at startup (and log it). Only when the approximate
    /// part is on; never needed for a read.
    pub(crate) async fn probe(&self) {
        let inner = &self.inner;
        if !inner.config.enabled || !inner.approximate_on() {
            return;
        }
        let Ok(_permit) = inner.permits.clone().try_acquire_owned() else {
            return;
        };
        let Some(claim) = inner.health.claim() else {
            return;
        };
        let _lease = claim.probe.then(|| ProbeLease {
            inner: inner.clone(),
        });
        if let Some(dim) = inner.embed_one(PROBE_TEXT, claim.epoch, false).await {
            tracing::info!(
                endpoint = %inner.client.endpoint,
                model = %inner.client.model,
                dim,
                "semantic cache: embedding endpoint probe succeeded"
            );
        }
    }

    /// A hinted read the tier refused (outside its grammar).
    pub(crate) fn record_ineligible(&self) {
        self.inner
            .counters
            .ineligible
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Health and counters.
    pub fn health(&self) -> SemanticHealth {
        let c = &self.inner.counters;
        SemanticHealth {
            endpoint: self.inner.health.state(),
            embed_requests: c.requests.load(Ordering::Relaxed),
            embed_errors: c.errors.load(Ordering::Relaxed),
            embed_skipped: c.skipped.load(Ordering::Relaxed),
            embed_seconds_sum: Duration::from_nanos(c.nanos.load(Ordering::Relaxed)).as_secs_f64(),
            embed_seconds_count: c.timed.load(Ordering::Relaxed),
            approximate_hits: c.approximate_hits.load(Ordering::Relaxed),
            ineligible: c.ineligible.load(Ordering::Relaxed),
            too_large: c.too_large.load(Ordering::Relaxed),
            entries: self.inner.entry_count.load(Ordering::Relaxed) as u64,
            embeddings: self.inner.memo_len.load(Ordering::Relaxed) as u64,
        }
    }

    /// Look up a query by its canonical form (library API; the query cache
    /// calls the crate-internal lookup with its C-02 check). Never waits on
    /// the embedding endpoint.
    pub async fn get(&self, query: &str, context: &CacheContext) -> Option<CachedResult> {
        let c = self.form(query)?;
        match self.lookup(&c, context, &|_| true) {
            SemanticLookup::Hit { result, .. } => Some(result),
            SemanticLookup::Stale | SemanticLookup::Miss => None,
        }
    }

    /// Store a query's result by its canonical form (library API). A query
    /// without one is not stored. Never waits on the embedding endpoint.
    pub async fn put(&self, query: &str, context: &CacheContext, result: CachedResult) {
        if let Some(c) = self.form(query) {
            self.store(&c, context, result, &|_| true);
        }
    }

    /// The form the library API keys `query` by (`semantic_match`).
    fn form(&self, query: &str) -> Option<Canonical> {
        let tables = QueryNormalizer::new().normalize(query).tables;
        let config = &self.inner.config;
        canonical::semantic_form(
            query,
            &tables,
            config.max_statement_bytes,
            config.semantic_match,
        )
    }

    /// Clear all entries and remembered embeddings
    pub async fn clear(&self) {
        let inner = &self.inner;
        {
            let mut store = inner.store.write();
            *store = Store::default();
            inner.entry_bytes.store(0, Ordering::Relaxed);
            inner.entry_count.store(0, Ordering::Relaxed);
        }
        let mut memo = inner.memo.lock();
        memo.clear();
        inner.memo_bytes.store(0, Ordering::Relaxed);
        inner.memo_len.store(0, Ordering::Relaxed);
    }

    /// Bytes retained: entries plus remembered embeddings (C-01).
    pub fn retained_bytes(&self) -> usize {
        self.inner.entry_bytes.load(Ordering::Relaxed)
            + self.inner.memo_bytes.load(Ordering::Relaxed)
    }

    /// Get entry count
    pub fn len(&self) -> usize {
        self.inner.entry_count.load(Ordering::Relaxed)
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get cache statistics
    pub fn stats(&self) -> L3CacheStats {
        let embeddings = self.inner.memo_len.load(Ordering::Relaxed);
        L3CacheStats {
            entry_count: self.len(),
            max_entries: self.inner.config.max_entries,
            similarity_threshold: self.inner.config.similarity_threshold,
            embedding_dimension: if embeddings == 0 {
                0
            } else {
                self.inner.config.embedding_dim
            },
            total_accesses: self.inner.counters.hits.load(Ordering::Relaxed),
            embedding_cache_size: embeddings,
        }
    }

    /// Check if the embedding service is available (one call, now).
    pub async fn health_check(&self) -> bool {
        self.inner.client.health_check().await
    }
}

/// Embedding service client (Ollama-compatible `POST {endpoint}/api/embeddings`).
#[derive(Debug)]
pub struct EmbeddingClient {
    /// Endpoint base URL, without a trailing `/`
    endpoint: String,

    /// Model name
    model: String,

    /// Expected embedding dimension
    dimension: usize,

    /// HTTP client (`None` if it could not be built: every call fails)
    client: Option<reqwest::Client>,
}

impl EmbeddingClient {
    /// Create a new embedding client with the default call timeout.
    pub fn new(endpoint: String, model: String, dimension: usize) -> Self {
        Self::with_options(
            endpoint,
            model,
            dimension,
            L3Config::default().embedding_timeout,
        )
    }

    /// Create a client whose calls end after `timeout` (DNS, connect and
    /// body). System HTTP proxies are not used; a trailing `/` of
    /// `endpoint` is ignored.
    pub fn with_options(
        endpoint: String,
        model: String,
        dimension: usize,
        timeout: Duration,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .no_proxy()
            .build()
            .ok();
        Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            model,
            dimension,
            client,
        }
    }

    /// Largest response body accepted: `dimension * 32 + 4096` bytes.
    fn max_body(&self) -> usize {
        self.dimension
            .saturating_mul(BODY_BYTES_PER_DIM)
            .saturating_add(BODY_SLACK_BYTES)
    }

    /// Embed `text`. Fails on a transport error or timeout, a non-2xx
    /// status, a body over `dimension * 32 + 4096` bytes, bad JSON, a
    /// non-finite value, or a length other than the configured dimension.
    pub async fn try_embed(&self, text: &str) -> Result<Vec<f32>, String> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| "the HTTP client could not be built".to_string())?;
        let url = format!("{}/api/embeddings", self.endpoint);
        let request = serde_json::json!({
            "model": self.model,
            "prompt": text
        });
        let mut response = client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !response.status().is_success() {
            return Err(format!("HTTP {}", response.status()));
        }
        let cap = self.max_body();
        if let Some(len) = response.content_length() {
            if len > cap as u64 {
                return Err(format!("response of {len} bytes exceeds {cap}"));
            }
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
            if body.len() + chunk.len() > cap {
                return Err(format!("response exceeds {cap} bytes"));
            }
            body.extend_from_slice(&chunk);
        }
        let value: serde_json::Value =
            serde_json::from_slice(&body).map_err(|e| format!("bad JSON: {e}"))?;
        let values = value
            .get("embedding")
            .and_then(|e| e.as_array())
            .ok_or_else(|| "no \"embedding\" array".to_string())?;
        if values.len() != self.dimension {
            return Err(format!(
                "embedding has {} values, expected {}",
                values.len(),
                self.dimension
            ));
        }
        values
            .iter()
            .map(|v| {
                v.as_f64()
                    .map(|f| f as f32)
                    .filter(|f| f.is_finite())
                    .ok_or_else(|| "a non-finite or non-numeric value".to_string())
            })
            .collect()
    }

    /// Generate embedding for text (`None` on any failure, including a
    /// wrong dimension: it is never padded or truncated).
    pub async fn embed(&self, text: &str) -> Option<Vec<f32>> {
        self.try_embed(text).await.ok()
    }

    /// Check if Ollama is available
    pub async fn health_check(&self) -> bool {
        let Some(client) = &self.client else {
            return false;
        };
        let url = format!("{}/api/tags", self.endpoint);

        match client.get(&url).send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// List available models
    pub async fn list_models(&self) -> Option<Vec<String>> {
        let url = format!("{}/api/tags", self.endpoint);

        let response = self.client.as_ref()?.get(&url).send().await.ok()?;
        let body: serde_json::Value = response.json().await.ok()?;

        let models = body
            .get("models")?
            .as_array()?
            .iter()
            .filter_map(|m| m.get("name")?.as_str().map(String::from))
            .collect();

        Some(models)
    }

    /// Pull a model if not available
    pub async fn pull_model(&self) -> Result<(), String> {
        let url = format!("{}/api/pull", self.endpoint);

        let request = serde_json::json!({
            "name": self.model
        });

        let response = self
            .client
            .as_ref()
            .ok_or_else(|| "the HTTP client could not be built".to_string())?
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("Failed to pull model: {}", response.status()))
        }
    }
}

/// L3 cache statistics
#[derive(Debug, Clone)]
pub struct L3CacheStats {
    /// Number of entries
    pub entry_count: usize,

    /// Maximum entries
    pub max_entries: usize,

    /// Similarity threshold
    pub similarity_threshold: f32,

    /// Embedding dimension (0 while no embedding is remembered)
    pub embedding_dimension: usize,

    /// Hits served (exact and approximate)
    pub total_accesses: u64,

    /// Remembered column-name embeddings
    pub embedding_cache_size: usize,
}

/// Compute cosine similarity between two vectors (0.0 for different lengths
/// or a zero vector). Accumulated in f64, so finite inputs give a finite
/// result: in f32 the square of a component of magnitude about 1.8e19 or
/// more overflows to infinity and the quotient is NaN.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }

    let mut dot_product = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;

    for (&x, &y) in a.iter().zip(b.iter()) {
        let (x, y) = (f64::from(x), f64::from(y));
        dot_product += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }

    let norm_a = norm_a.sqrt();
    let norm_b = norm_b.sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    (dot_product / (norm_a * norm_b)) as f32
}

/// A local HTTP/1.1 stand-in for an Ollama-compatible embedding endpoint,
/// for tests (no new dev-dependency): deterministic vectors per name (a
/// synonym group shares one), switchable failure modes, and a record of
/// what it was asked.
#[cfg(test)]
pub(crate) mod fake_embedder {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// How the fake answers.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Mode {
        /// 200 with a vector of the configured dimension.
        Ok,
        /// 500.
        Error,
        /// Never answers (well past any test timeout).
        Hang,
        /// 200 with one value too many.
        WrongDim,
        /// 200 with a body far over the client's cap.
        Oversize,
        /// 200 with the usual vector scaled to finite values whose f32
        /// squares overflow (1e20).
        Huge,
    }

    #[derive(Debug)]
    struct State {
        mode: Mode,
        dim: usize,
        synonyms: Vec<Vec<String>>,
        /// Per-prompt override: wait, then answer in this mode.
        delayed: HashMap<String, (Duration, Mode)>,
        requests: usize,
        last_path: String,
        last_prompt: String,
    }

    /// A running fake; stops with the test's runtime.
    #[derive(Clone)]
    pub(crate) struct FakeEmbedder {
        pub(crate) url: String,
        state: Arc<parking_lot::Mutex<State>>,
    }

    impl FakeEmbedder {
        pub(crate) async fn start(dim: usize) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let state = Arc::new(parking_lot::Mutex::new(State {
                mode: Mode::Ok,
                dim,
                synonyms: Vec::new(),
                delayed: HashMap::new(),
                requests: 0,
                last_path: String::new(),
                last_prompt: String::new(),
            }));
            let shared = state.clone();
            tokio::spawn(async move {
                while let Ok((sock, _)) = listener.accept().await {
                    tokio::spawn(serve(sock, shared.clone()));
                }
            });
            Self { url, state }
        }

        pub(crate) fn set_mode(&self, mode: Mode) {
            self.state.lock().mode = mode;
        }

        pub(crate) fn set_synonyms(&self, groups: &[&[&str]]) {
            self.state.lock().synonyms = groups
                .iter()
                .map(|g| g.iter().map(|s| s.to_string()).collect())
                .collect();
        }

        pub(crate) fn delay(&self, prompt: &str, wait: Duration, then: Mode) {
            self.state
                .lock()
                .delayed
                .insert(prompt.to_string(), (wait, then));
        }

        pub(crate) fn requests(&self) -> usize {
            self.state.lock().requests
        }

        pub(crate) fn last_path(&self) -> String {
            self.state.lock().last_path.clone()
        }

        pub(crate) fn last_prompt(&self) -> String {
            self.state.lock().last_prompt.clone()
        }
    }

    /// A deterministic pseudo-random vector for `seed`.
    fn vector(seed: u64, dim: usize) -> Vec<f64> {
        let mut x = seed | 1;
        (0..dim)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                (x % 2001) as f64 / 1000.0 - 1.0
            })
            .collect()
    }

    fn seed_of(name: &str, synonyms: &[Vec<String>]) -> u64 {
        use std::hash::{Hash, Hasher};
        let key = synonyms
            .iter()
            .position(|g| g.iter().any(|s| s == name))
            .map_or_else(|| format!("name:{name}"), |g| format!("group:{g}"));
        let mut h = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h);
        h.finish()
    }

    async fn serve(mut sock: TcpStream, state: Arc<parking_lot::Mutex<State>>) {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let head_end = loop {
            if let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break at + 4;
            }
            match sock.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
        let length = head
            .lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        while buf.len() < head_end + length {
            match sock.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
            }
        }
        let body: serde_json::Value =
            serde_json::from_slice(&buf[head_end..head_end + length]).unwrap_or_default();
        let prompt = body["prompt"].as_str().unwrap_or("").to_string();
        let (mode, dim, seed, wait) = {
            let mut s = state.lock();
            s.requests += 1;
            s.last_path = path;
            s.last_prompt = prompt.clone();
            let (wait, mode) = s
                .delayed
                .get(&prompt)
                .copied()
                .unwrap_or((Duration::ZERO, s.mode));
            (mode, s.dim, seed_of(&prompt, &s.synonyms), wait)
        };
        tokio::time::sleep(wait).await;
        let (status, payload) = match mode {
            Mode::Ok => (
                "200 OK",
                serde_json::json!({ "embedding": vector(seed, dim) }).to_string(),
            ),
            Mode::WrongDim => (
                "200 OK",
                serde_json::json!({ "embedding": vector(seed, dim + 1) }).to_string(),
            ),
            Mode::Huge => (
                "200 OK",
                serde_json::json!({
                    "embedding": vector(seed, dim).iter().map(|v| v * 1e20).collect::<Vec<_>>()
                })
                .to_string(),
            ),
            Mode::Oversize => (
                "200 OK",
                format!(
                    "{{\"embedding\": [], \"pad\": \"{}\"}}",
                    "x".repeat(dim * 64 + 8192)
                ),
            ),
            Mode::Error => ("500 Internal Server Error", "{}".to_string()),
            Mode::Hang => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                return;
            }
        };
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: close\r\n\r\n{payload}",
            payload.len()
        );
        let _ = sock.write_all(response.as_bytes()).await;
        let _ = sock.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::fake_embedder::{FakeEmbedder, Mode};
    use super::*;
    use crate::cache::result::L3Entry;
    use bytes::Bytes;

    fn create_result(data: &str) -> CachedResult {
        CachedResult::new(
            Bytes::from(data.to_string()),
            1,
            Duration::from_secs(60),
            vec!["test".to_string()],
            Duration::from_millis(5),
        )
    }

    fn ctx() -> CacheContext {
        CacheContext {
            database: "db".to_string(),
            user: Some("u".to_string()),
            ..Default::default()
        }
    }

    fn canon(sql: &str) -> Canonical {
        let tables = QueryNormalizer::new().normalize(sql).tables;
        canonical::canonicalize(sql, &tables, usize::MAX).unwrap_or_else(|| panic!("{sql} refused"))
    }

    fn config(endpoint: &str, threshold: f32) -> L3Config {
        L3Config {
            enabled: true,
            similarity_threshold: threshold,
            embedding_endpoint: endpoint.to_string(),
            embedding_dim: 64,
            embedding_timeout: Duration::from_millis(300),
            embedding_retry_after: Duration::from_millis(300),
            ..Default::default()
        }
    }

    fn hit(l: SemanticLookup) -> Option<(Bytes, bool)> {
        match l {
            SemanticLookup::Hit {
                result,
                approximate,
            } => Some((result.data, approximate)),
            _ => None,
        }
    }

    fn look(cache: &L3SemanticCache, sql: &str) -> Option<(Bytes, bool)> {
        hit(cache.lookup(&canon(sql), &ctx(), &|_| true))
    }

    fn put(cache: &L3SemanticCache, sql: &str, data: &str) {
        cache.store(&canon(sql), &ctx(), create_result(data), &|_| true);
    }

    /// Poll `cond` for up to 3 s.
    async fn eventually(what: &str, cond: impl Fn() -> bool) {
        for _ in 0..300 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// A port nothing listens on.
    fn closed_endpoint() -> String {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", l.local_addr().unwrap())
    }

    #[test]
    fn test_cosine_similarity() {
        // Same vector = 1.0
        let a = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &a) - 1.0).abs() < 0.001);

        // Orthogonal vectors = 0.0
        let b = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &b).abs() < 0.001);

        // Opposite vectors = -1.0
        let c = vec![-1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &c) + 1.0).abs() < 0.001);

        // Empty vectors = 0.0
        assert!(cosine_similarity(&[], &[]).abs() < 0.001);

        // Different lengths = 0.0
        let d = vec![1.0, 0.0];
        assert!(cosine_similarity(&a, &d).abs() < 0.001);

        // Huge finite components stay finite (f32 sums of squares overflowed
        // to infinity, and the quotient was NaN).
        let h = [1e20f32, 0.0, 0.0];
        let k = [1e20f32, 1e20, 0.0];
        assert!((cosine_similarity(&h, &h) - 1.0).abs() < 0.001);
        assert!((cosine_similarity(&h, &k) - std::f32::consts::FRAC_1_SQRT_2).abs() < 0.001);
        assert!((cosine_similarity(&[f32::MAX], &[-f32::MAX]) + 1.0).abs() < 0.001);
    }

    #[test]
    fn test_l3_entry_similarity() {
        let result = create_result("test");
        let ctx = CacheContext::default();

        let entry = L3Entry::new(
            "SELECT * FROM users".to_string(),
            vec![0.5, 0.5, 0.5, 0.5],
            ctx,
            result,
        );

        // High similarity
        let similar = vec![0.5, 0.5, 0.5, 0.5];
        assert!((entry.similarity(&similar) - 1.0).abs() < 0.001);

        // Moderate similarity
        let moderate = vec![0.5, 0.5, 0.0, 0.0];
        assert!(entry.similarity(&moderate) > 0.5);
        assert!(entry.similarity(&moderate) < 1.0);
    }

    #[tokio::test]
    async fn test_l3_cache_disabled() {
        let config = L3Config {
            max_bytes: usize::MAX,
            enabled: false,
            ..Default::default()
        };
        let cache = L3SemanticCache::new(config);

        let ctx = CacheContext::default();
        cache.put("SELECT v FROM t", &ctx, create_result("r")).await;
        assert!(cache.get("SELECT v FROM t", &ctx).await.is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn test_embedding_client_creation() {
        let client = EmbeddingClient::new(
            "http://localhost:11434/".to_string(),
            "all-minilm".to_string(),
            384,
        );

        assert_eq!(client.endpoint, "http://localhost:11434");
        assert_eq!(client.model, "all-minilm");
        assert_eq!(client.dimension, 384);
        assert_eq!(client.max_body(), 384 * 32 + 4096);
    }

    #[test]
    fn test_l3_stats() {
        let config = L3Config {
            max_bytes: usize::MAX,
            enabled: true,
            max_entries: 1000,
            similarity_threshold: 0.9,
            ..Default::default()
        };
        let cache = L3SemanticCache::new(config);

        let stats = cache.stats();
        assert_eq!(stats.entry_count, 0);
        assert_eq!(stats.max_entries, 1000);
        assert!((stats.similarity_threshold - 0.9).abs() < 0.001);
        assert_eq!(cache.health().endpoint, EndpointState::Unknown);
    }

    /// Exact canonical matches are synchronous and never touch the endpoint
    /// (no runtime here, so nothing could even be spawned).
    #[test]
    fn exact_canonical_hit_needs_no_endpoint() {
        let cache = L3SemanticCache::new(config(&closed_endpoint(), 0.9));
        put(&cache, "SELECT val FROM kv WHERE id = 1", "one");
        assert_eq!(
            look(
                &cache,
                "select val from kv as t where id = 1 /*helios:cache=semantic*/"
            ),
            Some((Bytes::from("one"), false))
        );
        // Another spelling of a select-list column labels the output column
        // differently where the backend does not fold it (HeliosDB-Nano), and
        // is another column where it does not fold names at all
        // (HeliosDB-Lite): never an exact hit. Below 1.0 it is another name in
        // the same slot, matched only through embeddings (none here).
        assert_eq!(look(&cache, "SELECT VAL FROM kv WHERE id = 1"), None);
        // A qualified column may be a function of the row: never the bare one.
        assert_eq!(look(&cache, "SELECT t.val FROM kv t WHERE t.id = 1"), None);
        assert_eq!(look(&cache, "SELECT val FROM kv WHERE id = 2"), None);
        // `x = 1.e3` compares with 1000; `x = 1. e3` is outside the grammar.
        put(&cache, "SELECT val FROM kv WHERE x = 1.e3", "thousand");
        assert!(look(&cache, "SELECT val FROM kv WHERE x = 1.e3").is_some());
        for other in [
            "SELECT val FROM kv WHERE x = 1. e3",
            "SELECT val FROM kv WHERE x = 1./**/e3",
        ] {
            let tables = QueryNormalizer::new().normalize(other).tables;
            assert_eq!(
                canonical::canonicalize(other, &tables, usize::MAX),
                None,
                "{other}"
            );
        }
        assert_eq!(cache.health().embed_requests, 0);
        assert_eq!(cache.health().endpoint, EndpointState::Unknown);
    }

    /// A `cache_ttl` hint bounds the age of what a lookup serves: an older
    /// entry is a miss for that lookup only (neither stale nor purged).
    #[test]
    fn a_max_age_skips_older_entries_without_dropping_them() {
        let cache = L3SemanticCache::new(config(&closed_endpoint(), 1.0));
        let sql = "SELECT val FROM kv WHERE id = 1";
        put(&cache, sql, "one");
        std::thread::sleep(Duration::from_millis(30));
        let c = canon(sql);
        assert!(matches!(
            cache.lookup_within(&c, &ctx(), &|_| true, Some(Duration::from_millis(10))),
            SemanticLookup::Miss
        ));
        assert_eq!(
            hit(cache.lookup_within(&c, &ctx(), &|_| true, Some(Duration::from_secs(60)))),
            Some((Bytes::from("one"), false))
        );
        assert_eq!(look(&cache, sql), Some((Bytes::from("one"), false)));
    }

    #[tokio::test]
    async fn approximate_hit_substitutes_a_similar_column_position_by_position() {
        let fake = FakeEmbedder::start(64).await;
        fake.set_synonyms(&[&["val", "alt"]]);
        let cache = L3SemanticCache::new(config(&fake.url, 0.9));
        put(&cache, "SELECT val FROM kv WHERE id = 3", "val-of-3");
        eventually("val embedded", || cache.health().embeddings >= 1).await;

        let alt = "SELECT alt FROM kv AS k WHERE id = 3";
        // `alt` has no embedding yet: a miss. A lookup never sends the
        // client's own names; a stored statement listing `alt` does.
        assert_eq!(look(&cache, alt), None);
        put(&cache, "SELECT alt FROM kv WHERE id = 40", "alt-of-40");
        eventually("alt embedded", || cache.health().embeddings >= 2).await;
        assert_eq!(look(&cache, alt), Some((Bytes::from("val-of-3"), true)));
        assert_eq!(cache.health().approximate_hits, 1);
        // Never another row, and never a qualified column (which may be a
        // function) from a bare one.
        assert_eq!(look(&cache, "SELECT alt FROM kv WHERE id = 4"), None);
        assert_eq!(look(&cache, "SELECT k.alt FROM kv AS k WHERE id = 3"), None);

        // Position by position: a swapped list of dissimilar names misses.
        put(&cache, "SELECT name, email FROM users WHERE id = 1", "ne");
        let swapped = "SELECT email, name FROM users WHERE id = 1";
        assert_eq!(look(&cache, swapped), None);
        eventually("names embedded", || cache.health().embeddings >= 4).await;
        assert_eq!(look(&cache, swapped), None);
        // A dissimilar name misses too.
        let other = "SELECT zzz FROM kv WHERE id = 3";
        assert_eq!(look(&cache, other), None);
        put(&cache, "SELECT zzz FROM kv WHERE id = 30", "zzz-of-30");
        eventually("zzz embedded", || cache.health().embeddings >= 5).await;
        assert_eq!(look(&cache, other), None);
        assert_eq!(cache.health().approximate_hits, 1);
        assert!(fake.last_prompt().len() <= 5, "only column names are sent");
    }

    /// Finite embeddings with huge components made the f32 cosine NaN, and
    /// `f32::min` ignored it: every name then matched every other.
    #[tokio::test]
    async fn huge_finite_embeddings_never_match_dissimilar_names() {
        let fake = FakeEmbedder::start(64).await;
        fake.set_mode(Mode::Huge);
        fake.set_synonyms(&[&["salary", "pay"]]);
        let cache = L3SemanticCache::new(config(&fake.url, 0.9));
        put(&cache, "SELECT salary FROM emp WHERE id = 1", "salary-of-1");
        eventually("salary embedded", || cache.health().embeddings >= 1).await;
        let bonus = "SELECT bonus FROM emp WHERE id = 1";
        put(&cache, "SELECT bonus FROM emp WHERE id = 2", "bonus-of-2");
        eventually("bonus embedded", || cache.health().embeddings >= 2).await;
        assert_eq!(look(&cache, bonus), None, "a dissimilar name never matches");
        // The same direction still matches: the similarity is computed.
        let pay = "SELECT pay FROM emp WHERE id = 1";
        put(&cache, "SELECT pay FROM emp WHERE id = 3", "pay-of-3");
        eventually("pay embedded", || cache.health().embeddings >= 3).await;
        assert_eq!(look(&cache, pay), Some((Bytes::from("salary-of-1"), true)));
        assert_eq!(cache.health().approximate_hits, 1);
    }

    #[tokio::test]
    async fn threshold_one_never_calls_the_endpoint() {
        let fake = FakeEmbedder::start(64).await;
        let cache = L3SemanticCache::new(config(&fake.url, 1.0));
        put(&cache, "SELECT val FROM kv WHERE id = 1", "r");
        assert!(look(&cache, "SELECT val FROM kv k WHERE id = 1").is_some());
        assert!(look(&cache, "SELECT alt FROM kv WHERE id = 1").is_none());
        cache.probe().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fake.requests(), 0);
        assert_eq!(cache.health().endpoint, EndpointState::Unknown);
    }

    #[test]
    fn different_context_never_matches() {
        let cache = L3SemanticCache::new(config(&closed_endpoint(), 1.0));
        let c = canon("SELECT val FROM kv WHERE id = 1");
        cache.store(&c, &ctx(), create_result("r"), &|_| true);
        for other in [
            CacheContext {
                user: Some("other".into()),
                ..ctx()
            },
            CacheContext {
                database: "db2".into(),
                ..ctx()
            },
            CacheContext {
                branch: Some("b".into()),
                ..ctx()
            },
        ] {
            assert!(hit(cache.lookup(&c, &other, &|_| true)).is_none());
        }
        assert!(hit(cache.lookup(&c, &ctx(), &|_| true)).is_some());
    }

    /// A retired entry is skipped (Stale) and purged; a refill replaces it
    /// and hits. The old tier kept the stale entry and shadowed every refill
    /// with it, so it never hit again after a write.
    #[test]
    fn stale_slot_is_skipped_purged_and_a_refill_replaces_it() {
        let cache = L3SemanticCache::new(config(&closed_endpoint(), 1.0));
        let c = canon("SELECT val FROM kv WHERE id = 1");
        cache.store(&c, &ctx(), create_result("old"), &|_| true);
        assert!(matches!(
            cache.lookup(&c, &ctx(), &|_| false),
            SemanticLookup::Stale
        ));
        assert_eq!(cache.len(), 0, "purged");
        assert!(matches!(
            cache.lookup(&c, &ctx(), &|_| true),
            SemanticLookup::Miss
        ));
        cache.store(&c, &ctx(), create_result("new"), &|_| true);
        cache.store(&c, &ctx(), create_result("newer"), &|_| true);
        assert_eq!(cache.len(), 1, "a refill replaces its slot");
        assert_eq!(
            hit(cache.lookup(&c, &ctx(), &|_| true)),
            Some((Bytes::from("newer"), false))
        );
        // Expired entries go the same way.
        let mut short = create_result("short");
        short.ttl = Duration::ZERO;
        short.cached_at = Instant::now() - Duration::from_millis(5);
        let d = canon("SELECT val FROM kv WHERE id = 2");
        cache.store(&d, &ctx(), short, &|_| true);
        assert!(matches!(
            cache.lookup(&d, &ctx(), &|_| true),
            SemanticLookup::Miss
        ));
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn endpoint_is_never_awaited_by_lookup_or_store() {
        let fake = FakeEmbedder::start(64).await;
        fake.set_mode(Mode::Hang);
        let cache = L3SemanticCache::new(config(&fake.url, 0.9));
        let started = Instant::now();
        put(&cache, "SELECT val FROM kv WHERE id = 1", "r");
        assert_eq!(look(&cache, "SELECT alt FROM kv WHERE id = 1"), None);
        assert!(
            started.elapsed() < Duration::from_millis(50),
            "{:?}",
            started.elapsed()
        );
        eventually("down after the call timeout", || {
            cache.health().endpoint == EndpointState::Down
        })
        .await;
        // Exact hits keep working while it is down.
        assert!(look(&cache, "SELECT val FROM kv WHERE id = 1").is_some());
    }

    #[tokio::test]
    async fn failing_endpoint_backs_off_and_probes_once_per_window() {
        let fake = FakeEmbedder::start(64).await;
        fake.set_mode(Mode::Error);
        let cache = L3SemanticCache::new(config(&fake.url, 0.9));
        cache.schedule_embed(vec!["a".into()]);
        eventually("down", || cache.health().endpoint == EndpointState::Down).await;
        assert_eq!(fake.requests(), 1);
        // Inside the window: no call, counted as skipped.
        let skipped = cache.health().embed_skipped;
        cache.schedule_embed(vec!["b".into()]);
        assert_eq!(cache.health().embed_skipped, skipped + 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fake.requests(), 1);
        // After the window: exactly one probe, whoever asks.
        fake.set_mode(Mode::Ok);
        tokio::time::sleep(Duration::from_millis(350)).await;
        cache.schedule_embed(vec!["c".into(), "d".into()]);
        eventually("up", || cache.health().endpoint == EndpointState::Up).await;
        assert_eq!(fake.requests(), 2);
        assert_eq!(cache.health().embed_errors, 1);
    }

    /// While down, the probe's lease lasts as long as its call, not one
    /// retry window: with a call timeout longer than the window, a probe
    /// hanging on a wedged endpoint used to let the next caller of every
    /// window start another, up to `embedding_concurrency` at once.
    #[tokio::test]
    async fn a_hung_probe_holds_its_lease_past_the_retry_window() {
        let fake = FakeEmbedder::start(64).await;
        fake.set_mode(Mode::Error);
        let cache = L3SemanticCache::new(L3Config {
            embedding_concurrency: 4,
            embedding_timeout: Duration::from_secs(5),
            embedding_retry_after: Duration::from_millis(40),
            ..config(&fake.url, 0.9)
        });
        cache.schedule_embed(vec!["a".into()]);
        eventually("down", || cache.health().endpoint == EndpointState::Down).await;
        fake.set_mode(Mode::Hang);
        tokio::time::sleep(Duration::from_millis(60)).await;
        cache.schedule_embed(vec!["b".into()]);
        eventually("the probe in flight", || fake.requests() == 2).await;
        // Several retry windows pass while the probe hangs: no other call.
        for name in ["c", "d", "e", "f", "g"] {
            tokio::time::sleep(Duration::from_millis(60)).await;
            cache.schedule_embed(vec![name.into()]);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fake.requests(), 2, "one probe in flight while down");
        assert_eq!(cache.health().endpoint, EndpointState::Down);
    }

    #[tokio::test]
    async fn busy_permits_skip_and_identical_names_embed_once() {
        let fake = FakeEmbedder::start(64).await;
        fake.set_mode(Mode::Hang);
        let cache = L3SemanticCache::new(L3Config {
            embedding_concurrency: 1,
            embedding_timeout: Duration::from_secs(5),
            ..config(&fake.url, 0.9)
        });
        cache.schedule_embed(vec!["a".into()]);
        eventually("a in flight", || fake.requests() == 1).await;
        let skipped = cache.health().embed_skipped;
        cache.schedule_embed(vec!["b".into()]);
        assert_eq!(
            cache.health().embed_skipped,
            skipped + 1,
            "no permit: skipped"
        );

        let wide = L3SemanticCache::new(L3Config {
            embedding_concurrency: 4,
            embedding_timeout: Duration::from_secs(5),
            ..config(&fake.url, 0.9)
        });
        for _ in 0..8 {
            wide.schedule_embed(vec!["same".into()]);
        }
        eventually("same in flight", || fake.requests() == 2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fake.requests(), 2, "eight schedules, one call");
        assert_eq!(wide.health().embed_skipped, 7);
    }

    #[tokio::test]
    async fn late_failure_does_not_flip_up_to_down() {
        let fake = FakeEmbedder::start(64).await;
        fake.delay("slow", Duration::from_millis(200), Mode::Error);
        let cache = L3SemanticCache::new(config(&fake.url, 0.9));
        cache.schedule_embed(vec!["slow".into()]);
        cache.schedule_embed(vec!["fast".into()]);
        eventually("up", || cache.health().endpoint == EndpointState::Up).await;
        eventually("the slow failure", || cache.health().embed_errors == 1).await;
        assert_eq!(cache.health().endpoint, EndpointState::Up);
    }

    #[tokio::test]
    async fn wrong_dimension_and_oversized_body_are_endpoint_errors() {
        for mode in [Mode::WrongDim, Mode::Oversize] {
            let fake = FakeEmbedder::start(64).await;
            fake.set_mode(mode);
            let cache = L3SemanticCache::new(config(&fake.url, 0.9));
            cache.schedule_embed(vec!["val".into()]);
            eventually("down", || cache.health().endpoint == EndpointState::Down).await;
            let h = cache.health();
            assert_eq!((h.embed_errors, h.embeddings), (1, 0), "{mode:?}");
            let client = EmbeddingClient::new(fake.url.clone(), "m".into(), 64);
            assert!(client.embed("val").await.is_none(), "{mode:?}");
        }
    }

    /// Through the embedding endpoint: the memo keeps at most a quarter of
    /// the byte budget (least recently used names go), and the entries all
    /// stay. The memo used to evict entries to make room for itself: with
    /// 4096-dimension embeddings this tier ended with no entries at all.
    #[tokio::test]
    async fn budget_counts_entries_and_embeddings() {
        let fake = FakeEmbedder::start(4096).await;
        let one_vector = memo_charge("c00", &[0.0f32; 4096]);
        let budget = 8 * one_vector;
        let cache = L3SemanticCache::new(L3Config {
            embedding_dim: 4096,
            max_bytes: budget,
            ..config(&fake.url, 0.9)
        });
        for i in 0..6usize {
            put(
                &cache,
                &format!("SELECT c{i:02} FROM t{i} WHERE id = 1"),
                "r",
            );
            eventually("embedded", || cache.health().embed_requests > i as u64).await;
            eventually("remembered", || {
                cache.health().embeddings as usize == (i + 1).min(2)
            })
            .await;
            assert!(
                cache.retained_bytes() <= budget,
                "{} > {budget}",
                cache.retained_bytes()
            );
        }
        let h = cache.health();
        assert_eq!((h.entries, h.embeddings), (6, 2), "{h:?}");
        cache.clear().await;
        assert_eq!(cache.retained_bytes(), 0);
        assert_eq!(cache.health().embeddings, 0);
    }

    /// The starvation scenario, without an endpoint: remembered embeddings
    /// never evict an entry. They take at most a quarter of the budget; a
    /// store that needs their room trims them (least recently used first)
    /// before any entry, and keeps the slot it just pushed; a name remembered
    /// with no room left is dropped at once.
    #[test]
    fn embeddings_never_starve_entries() {
        let dim = 4096;
        let one_vector = memo_charge("c00", &[0.0f32; 4096]);
        let budget = 8 * one_vector;
        let cache = L3SemanticCache::new(L3Config {
            embedding_dim: dim,
            max_bytes: budget,
            ..config(&closed_endpoint(), 0.9)
        });
        let inner = &cache.inner;
        let vector = || Arc::<[f32]>::from(vec![0.5f32; dim]);
        for i in 0..6 {
            put(
                &cache,
                &format!("SELECT c{i:02} FROM t{i} WHERE id = 1"),
                "r",
            );
            inner.memoize(format!("c{i:02}"), vector());
            assert!(cache.retained_bytes() <= budget);
        }
        let h = cache.health();
        assert_eq!((h.entries, h.embeddings), (6, 2), "{h:?}");
        assert!(inner.memo_bytes.load(Ordering::Relaxed) <= budget / MEMO_SHARE_DIVISOR);
        {
            let memo = inner.memo.lock();
            assert!(memo.contains("c05") && memo.contains("c04"));
        }

        // An entry that leaves less than one embedding's room: the memo
        // yields, every entry stays, the new one included.
        let entries = inner.entry_bytes.load(Ordering::Relaxed);
        let big = budget - entries - one_vector / 2;
        let result = CachedResult::new(
            Bytes::from(vec![b'x'; big]),
            1,
            Duration::from_secs(60),
            vec!["t9".to_string()],
            Duration::ZERO,
        );
        let sql = "SELECT big FROM t9 WHERE id = 1";
        cache.store(&canon(sql), &ctx(), result, &|_| true);
        let h = cache.health();
        assert_eq!((h.entries, h.embeddings), (7, 0), "{h:?}");
        assert!(look(&cache, sql).is_some());
        assert!(look(&cache, "SELECT c00 FROM t0 WHERE id = 1").is_some());
        assert!(cache.retained_bytes() <= budget);
        // No room: a remembered name is dropped, no entry is evicted.
        inner.memoize("c99".to_string(), vector());
        let h = cache.health();
        assert_eq!((h.entries, h.embeddings), (7, 0), "{h:?}");
        assert!(cache.retained_bytes() <= budget);
        // Entries alone over the budget: the oldest go.
        let result = CachedResult::new(
            Bytes::from(vec![b'x'; budget / 2]),
            1,
            Duration::from_secs(60),
            vec!["t8".to_string()],
            Duration::ZERO,
        );
        let sql = "SELECT huge FROM t8 WHERE id = 1";
        cache.store(&canon(sql), &ctx(), result, &|_| true);
        assert!(look(&cache, sql).is_some());
        assert!(look(&cache, "SELECT c00 FROM t0 WHERE id = 1").is_none());
        assert!(cache.retained_bytes() <= budget);
        assert_eq!(cache.health().too_large, 0);
    }

    /// An entry larger than the whole byte budget could never be kept: it is
    /// not stored (counted in `too_large`), and nothing is evicted or trimmed
    /// for it. Before, storing it dropped every remembered embedding, evicted
    /// every entry oldest first, then evicted itself: one large hinted read
    /// emptied the tier.
    #[test]
    fn an_entry_over_the_whole_budget_is_not_stored() {
        let dim = 64;
        let budget = 64 * memo_charge("c00", &[0.0f32; 64]);
        let cache = L3SemanticCache::new(L3Config {
            max_bytes: budget,
            ..config(&closed_endpoint(), 0.9)
        });
        let inner = &cache.inner;
        let stored = |i: usize| format!("SELECT c{i:02} FROM t{i} WHERE id = 1");
        for i in 0..8 {
            put(&cache, &stored(i), "r");
            inner.memoize(format!("c{i:02}"), Arc::from(vec![0.5f32; dim]));
        }
        let before = cache.health();
        assert_eq!((before.entries, before.embeddings), (8, 8), "{before:?}");
        let retained = cache.retained_bytes();
        let sql = "SELECT doc FROM blobs WHERE id = 7";
        for size in [budget, 4 * budget] {
            let result = CachedResult::new(
                Bytes::from(vec![b'x'; size]),
                1,
                Duration::from_secs(60),
                vec!["blobs".to_string()],
                Duration::ZERO,
            );
            cache.store(&canon(sql), &ctx(), result, &|_| true);
        }
        let h = cache.health();
        assert_eq!((h.entries, h.embeddings, h.too_large), (8, 8, 2), "{h:?}");
        assert_eq!(cache.retained_bytes(), retained);
        assert_eq!(look(&cache, sql), None);
        for i in 0..8 {
            assert_eq!(look(&cache, &stored(i)), Some((Bytes::from("r"), false)));
        }
        // Without a byte budget nothing is too large.
        let unbounded = L3SemanticCache::new(L3Config {
            max_bytes: 0,
            ..config(&closed_endpoint(), 0.9)
        });
        put(&unbounded, sql, &"x".repeat(budget));
        assert_eq!((unbounded.len(), unbounded.health().too_large), (1, 0));
    }

    /// Entries holding the byte budget leave the memo less room than one
    /// embedding: no name is sent to the endpoint (counted as skipped), since
    /// its embedding would be dropped on arrival. Before, every hinted store
    /// and every lookup with candidates fetched the same names again, and
    /// approximate matching never found them. Once the entries shrink,
    /// names are embedded again.
    #[tokio::test]
    async fn no_embedding_is_fetched_without_memo_room() {
        let fake = FakeEmbedder::start(64).await;
        fake.set_synonyms(&[&["val", "alt"]]);
        let one = memo_charge_for("val", 64);
        let budget = 64 * one;
        let stored = "SELECT val FROM kv WHERE id = 3";
        // The bytes one entry of `stored` takes besides its result.
        let overhead = {
            let probe = L3SemanticCache::new(L3Config {
                max_bytes: budget,
                ..config(&closed_endpoint(), 1.0)
            });
            put(&probe, stored, "");
            probe.inner.entry_bytes.load(Ordering::Relaxed)
        };
        let cache = L3SemanticCache::new(L3Config {
            max_bytes: budget,
            ..config(&fake.url, 0.9)
        });
        let big = CachedResult::new(
            Bytes::from(vec![b'x'; budget - overhead - one / 2]),
            1,
            Duration::from_secs(60),
            vec!["test".to_string()],
            Duration::ZERO,
        );
        cache.store(&canon(stored), &ctx(), big, &|_| true);
        let entries = cache.inner.entry_bytes.load(Ordering::Relaxed);
        assert!(entries <= budget, "{entries} > {budget}");
        assert!(cache.inner.memo_room(entries) < one);
        assert_eq!(cache.health().embed_skipped, 1, "the store's name");
        let paraphrase = "SELECT alt FROM kv WHERE id = 3";
        for _ in 0..20 {
            assert_eq!(look(&cache, paraphrase), None);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let h = cache.health();
        assert_eq!(fake.requests(), 0);
        assert_eq!((h.embed_requests, h.embeddings), (0, 0), "{h:?}");
        // Each lookup refuses the stored name (`alt`, the client's, is never
        // sent from a lookup).
        assert_eq!(h.embed_skipped, 1 + 20, "{h:?}");
        assert_eq!(cache.len(), 1, "no entry was evicted");

        // Room again: the names are embedded and matched.
        cache.clear().await;
        put(&cache, stored, "val-of-3");
        put(&cache, "SELECT alt FROM kv WHERE id = 30", "alt-of-30");
        assert_eq!(look(&cache, paraphrase), None);
        eventually("both names embedded", || cache.health().embeddings >= 2).await;
        assert_eq!(
            look(&cache, paraphrase),
            Some((Bytes::from("val-of-3"), true))
        );
    }

    /// The entries leave the memo room for one embedding and a half: each
    /// stored name is fetched once (`alt`, remembered last, evicts `val`),
    /// but a candidate needs `val` and `alt` remembered together, so a
    /// lookup never fetches `val` again (counted as skipped). Before, each
    /// lookup fetched the missing name, remembering it evicted the other
    /// one, and the next lookup fetched that one again: an endpoint call per
    /// hinted lookup and never an approximate hit.
    #[tokio::test]
    async fn a_candidate_is_fetched_only_when_its_names_fit_together() {
        let fake = FakeEmbedder::start(64).await;
        fake.set_synonyms(&[&["val", "alt"]]);
        let one = memo_charge_for("val", 64);
        let budget = 64 * one;
        let stored = "SELECT val FROM kv WHERE id = 3";
        let other = "SELECT alt FROM kv WHERE id = 30";
        // The bytes one entry of each statement takes besides its result.
        let overhead = |sql: &str| {
            let probe = L3SemanticCache::new(L3Config {
                max_bytes: budget,
                ..config(&closed_endpoint(), 1.0)
            });
            put(&probe, sql, "");
            probe.inner.entry_bytes.load(Ordering::Relaxed)
        };
        let small = overhead(other) + 1;
        let cache = L3SemanticCache::new(L3Config {
            max_bytes: budget,
            ..config(&fake.url, 0.9)
        });
        let big = CachedResult::new(
            Bytes::from(vec![b'x'; budget - overhead(stored) - small - 3 * one / 2]),
            1,
            Duration::from_secs(60),
            vec!["test".to_string()],
            Duration::ZERO,
        );
        cache.store(&canon(stored), &ctx(), big, &|_| true);
        eventually("val embedded", || cache.health().embeddings == 1).await;
        put(&cache, other, "a");
        let room = cache
            .inner
            .memo_room(cache.inner.entry_bytes.load(Ordering::Relaxed));
        assert!(one <= room && room < 2 * one, "{room} vs {one}");
        eventually("alt embedded", || cache.inner.memo.lock().contains("alt")).await;
        assert_eq!(fake.requests(), 2);
        assert_eq!(cache.health().embeddings, 1, "val made room for alt");
        let skipped = cache.health().embed_skipped;
        let paraphrase = "SELECT alt FROM kv WHERE id = 3";
        for _ in 0..20 {
            assert_eq!(look(&cache, paraphrase), None);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let h = cache.health();
        assert_eq!(fake.requests(), 2, "{h:?}");
        assert_eq!((h.embed_requests, h.embeddings), (2, 1), "{h:?}");
        assert_eq!(h.embed_skipped, skipped + 20, "{h:?}");
        assert_eq!(h.approximate_hits, 0);
        assert!(cache.inner.memo.lock().contains("alt"));
        assert_eq!(cache.len(), 2, "no entry was evicted");

        // Room for both: each name fetched once more (the clear dropped
        // them), then approximate hits.
        cache.clear().await;
        put(&cache, stored, "val-of-3");
        put(&cache, other, "a");
        eventually("both embedded", || cache.health().embeddings >= 2).await;
        for _ in 0..5 {
            assert_eq!(
                look(&cache, paraphrase),
                Some((Bytes::from("val-of-3"), true))
            );
        }
        assert_eq!(fake.requests(), 4);
    }

    /// A lookup never sends the client's own names: they come from a
    /// statement no backend has answered yet (the lookup precedes it), so
    /// any word a client writes left the proxy and churned the memo. Only a
    /// stored entry's names are fetched, here `val`, which its store could
    /// not embed while the endpoint was down.
    #[tokio::test]
    async fn a_lookup_sends_only_stored_names() {
        let fake = FakeEmbedder::start(64).await;
        fake.set_mode(Mode::Error);
        let cache = L3SemanticCache::new(config(&fake.url, 0.9));
        put(&cache, "SELECT val FROM kv WHERE id = 1", "r");
        eventually("down", || cache.health().endpoint == EndpointState::Down).await;
        fake.set_mode(Mode::Ok);
        tokio::time::sleep(Duration::from_millis(350)).await;
        // The retry window is over: this lookup's candidate lacks `val`.
        assert_eq!(look(&cache, "SELECT x0000 FROM kv WHERE id = 1"), None);
        eventually("val embedded", || cache.health().embeddings == 1).await;
        assert_eq!(cache.health().endpoint, EndpointState::Up);
        // The endpoint answers, and every candidate name is remembered: the
        // lookups' own names are still never sent.
        for i in 1..=20 {
            let sql = format!("SELECT x{i:04} FROM kv WHERE id = 1");
            assert_eq!(look(&cache, &sql), None);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fake.requests(), 2, "the failed call, then val");
        assert_eq!(fake.last_prompt(), "val");
        assert!(cache.inner.memo.lock().contains("val"));
        assert_eq!(cache.health().embeddings, 1);
    }

    /// A store never waits on the memo lock while it holds the store lock.
    /// A lookup scoring candidates holds the memo lock; a concurrent store
    /// used to park on it inside its store write lock, and through it every
    /// other session's lookup (exact hits included) blocked its worker.
    #[test]
    fn a_store_waiting_on_the_memo_never_holds_the_store_lock() {
        let cache = Arc::new(L3SemanticCache::new(config(&closed_endpoint(), 0.9)));
        let memo = cache.inner.memo.lock();
        let storing = {
            let cache = cache.clone();
            std::thread::spawn(move || put(&cache, "SELECT val FROM kv WHERE id = 1", "r"))
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while cache.inner.entry_count.load(Ordering::Relaxed) == 0 {
            assert!(Instant::now() < deadline, "the store never ran");
            std::thread::yield_now();
        }
        // The entry is in; the store now waits on the memo lock.
        let free = loop {
            if cache.inner.store.try_read().is_some() {
                break true;
            }
            if Instant::now() > deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        drop(memo);
        storing.join().unwrap();
        assert!(free, "the store lock was held while waiting on the memo");
        assert!(look(&cache, "SELECT val FROM kv WHERE id = 1").is_some());
    }

    /// Several candidates: the names they need together are reserved
    /// newest first, and a candidate whose names do not fit next to those
    /// is not fetched (it would evict theirs).
    #[test]
    fn memo_plan_reserves_whole_sets() {
        let one = memo_charge_for("val", 64);
        let mut plan = MemoPlan {
            room: 3 * one,
            max_names: 10,
            dim: 64,
            names: HashSet::new(),
            bytes: 0,
        };
        assert!(plan.reserve(&["val", "alt"]));
        assert!(plan.reserve(&["val", "alt", "val"]), "nothing new");
        assert!(plan.reserve(&["alt", "abc"]));
        assert!(!plan.reserve(&["val", "xyz"]), "a fourth name");
        assert_eq!(plan.names.len(), 3);
        assert_eq!(plan.bytes, 3 * one);
        let mut names = MemoPlan {
            room: usize::MAX,
            max_names: 2,
            dim: 64,
            names: HashSet::new(),
            bytes: 0,
        };
        assert!(!names.reserve(&["a", "b", "c"]), "over the name bound");
        assert!(names.names.is_empty());
    }

    /// Without a byte budget the memo is bounded by `max_entries` names.
    #[test]
    fn memo_names_are_bounded_without_a_byte_budget() {
        let cache = L3SemanticCache::new(L3Config {
            max_bytes: 0,
            max_entries: 3,
            ..config(&closed_endpoint(), 0.9)
        });
        for i in 0..5 {
            cache
                .inner
                .memoize(format!("n{i}"), Arc::from(vec![1.0f32; 64]));
        }
        assert_eq!(cache.health().embeddings, 3);
        assert!(!cache.inner.memo.lock().contains("n0"));
        assert!(cache.inner.memo.lock().contains("n4"));
    }

    #[test]
    fn fifo_eviction_and_variant_cap() {
        let cache = L3SemanticCache::new(L3Config {
            max_entries: 3,
            max_variants: 2,
            ..config(&closed_endpoint(), 0.9)
        });
        for i in 0..4 {
            put(&cache, &format!("SELECT v FROM t WHERE id = {i}"), "r");
        }
        assert_eq!(cache.len(), 3);
        assert!(look(&cache, "SELECT v FROM t WHERE id = 0").is_none());
        assert!(look(&cache, "SELECT v FROM t WHERE id = 3").is_some());

        let variants = L3SemanticCache::new(L3Config {
            max_variants: 2,
            ..config(&closed_endpoint(), 0.9)
        });
        for col in ["a", "b", "c"] {
            put(&variants, &format!("SELECT {col} FROM t WHERE id = 1"), col);
        }
        assert_eq!(variants.len(), 2);
        assert!(look(&variants, "SELECT a FROM t WHERE id = 1").is_none());
        assert!(look(&variants, "SELECT c FROM t WHERE id = 1").is_some());
    }

    #[tokio::test]
    async fn endpoint_trailing_slash_is_trimmed() {
        let fake = FakeEmbedder::start(8).await;
        let client = EmbeddingClient::new(format!("{}/", fake.url), "m".into(), 8);
        assert_eq!(client.try_embed("val").await.unwrap().len(), 8);
        assert_eq!(fake.last_path(), "/api/embeddings");
        assert_eq!(fake.last_prompt(), "val");
    }

    #[tokio::test]
    async fn legacy_get_put_wrappers_round_trip() {
        let cache = L3SemanticCache::new(config(&closed_endpoint(), 1.0));
        let ctx = ctx();
        cache
            .put("SELECT val FROM kv WHERE id = 1", &ctx, create_result("r"))
            .await;
        assert_eq!(
            cache
                .get("select val from kv k where id = 1", &ctx)
                .await
                .map(|r| r.data),
            Some(Bytes::from("r"))
        );
        // Another spelling of the select-list column is another label.
        assert!(cache
            .get("SELECT VAL FROM kv WHERE id = 1", &ctx)
            .await
            .is_none());
        // No canonical form: neither stored nor found.
        cache.put("SELECT 1", &ctx, create_result("x")).await;
        assert!(cache.get("SELECT 1", &ctx).await.is_none());
        assert_eq!(cache.len(), 1);
        let stats = cache.stats();
        assert_eq!((stats.entry_count, stats.total_accesses), (1, 1));
        cache.clear().await;
        assert!(cache.is_empty());
    }
}
