//! MCP tool-result cache (`[mcp] tool_cache_*`, Sprinter 20bcc218385f).
//!
//! The MCP gateway's `query` and `list_tables` results are SQL results, so
//! they have a source of truth and the C-02 write generations
//! ([`TableGenerations`]) retire them soundly, with a TTL for the writes the
//! proxy does not see. `explain` is never cached (`EXPLAIN ANALYZE` runs the
//! statement, and plans follow statistics no generation tracks).
//!
//! - **Key**: the owner (principal, tenant), the tool and the final backend
//!   SQL (after `[query_rewrite]` and the tenant filter), compared on the
//!   full bytes. The map hashes with a per-process random SipHash key
//!   (`RandomState`), so no client can aim keys at one bucket. Everything
//!   else that decides the backend's answer (role, database, contract,
//!   result caps) is fixed for the one `McpServer` the cache belongs to: a
//!   hit returns only what the same request could have fetched.
//! - **Eligibility** (`query`): one plain `SELECT` with no `INTO`, no
//!   locking clause and nothing volatile (the result caches' own scan, with
//!   `[cache] extra_volatile_functions`), naming no system catalog, no
//!   backend-ambiguous text or time-travel comment, no excluded table, at
//!   least one table, at most `tool_cache_max_statement_bytes` long.
//!   `list_tables` reads the catalog: it is valid while the global
//!   generation (DDL, `GRANT`, `REVOKE` through the proxy) is unchanged, for
//!   at most `tool_cache_catalog_ttl_secs`.
//! - **Freshness**: a hit needs its tables' generation sum unchanged and its
//!   TTL unexpired; a fill takes the sum before its fetch and is stored only
//!   if it is still the same (the C-02 fill-race rule). The TTL is
//!   `tool_cache_ttl_secs`, or a shorter `[[cache.tables]] ttl_secs` of a
//!   table the text names (fail-closed scan).
//! - **Bounds**: per owner and global, in entries and bytes (keys counted);
//!   one FIFO per owner and one global FIFO with lazy deletion, so every
//!   operation is O(1) amortized. Over an owner's bound only that owner's
//!   oldest entries go; over the global bound the globally oldest. One
//!   mutex, never held across an `.await`.
//!
//! The cache belongs to one `McpServer`; `[mcp]` is read at startup only (a
//! reload that rebuilt the gateway would have to clear it).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::time::Instant;

use crate::cache::{QueryNormalizer, TableGenerations};
use crate::config::{McpConfig, ProxyConfig};
use crate::mcp::{CachedTool, ToolOutput};
use crate::sql_text::{ShorterTtls, TableExclusions, VolatileScan};

/// The `_meta` key a tool result's cache outcome is reported under.
pub const META_KEY: &str = "heliosdb.com/cache";

/// A FIFO is compacted (its stale, lazily deleted slots dropped) once it
/// holds more than this many slots per live entry, which keeps every
/// operation O(1) amortized and the FIFOs within a constant factor of the
/// entry count.
const FIFO_SLACK_FACTOR: usize = 2;

/// Why a tool call was neither served from nor stored in the cache (the
/// `reason` label of `heliosdb_proxy_mcp_cache_skipped_total`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SkipReason {
    /// Not one plain `SELECT` (a write, DDL, `INTO`, several statements).
    Write,
    /// A volatile function or literal, an extra of `[cache]
    /// extra_volatile_functions`, or a locking clause.
    Volatile,
    /// Names a system catalog (or `list_tables` with
    /// `tool_cache_catalog_ttl_secs = 0`).
    Catalog,
    /// Names a `[cache]` excluded table.
    Excluded,
    /// Backend-ambiguous text or a time-travel comment.
    Ambiguous,
    /// No table found (`SELECT 1`).
    NoTables,
    /// Longer than `tool_cache_max_statement_bytes`.
    StatementTooLarge,
    /// Key and result over `tool_cache_max_entry_bytes`.
    EntryTooLarge,
    /// The call failed (`isError`): never stored.
    Error,
    /// `"fresh": true`: the lookup was skipped (the result is stored).
    Fresh,
}

impl SkipReason {
    /// Number of reasons.
    pub const COUNT: usize = 10;
    /// Every reason, in label order.
    pub const ALL: [SkipReason; SkipReason::COUNT] = [
        SkipReason::Write,
        SkipReason::Volatile,
        SkipReason::Catalog,
        SkipReason::Excluded,
        SkipReason::Ambiguous,
        SkipReason::NoTables,
        SkipReason::StatementTooLarge,
        SkipReason::EntryTooLarge,
        SkipReason::Error,
        SkipReason::Fresh,
    ];

    /// The metric label and `_meta` value.
    pub fn as_str(self) -> &'static str {
        match self {
            SkipReason::Write => "write",
            SkipReason::Volatile => "volatile",
            SkipReason::Catalog => "catalog",
            SkipReason::Excluded => "excluded",
            SkipReason::Ambiguous => "ambiguous",
            SkipReason::NoTables => "no_tables",
            SkipReason::StatementTooLarge => "statement_too_large",
            SkipReason::EntryTooLarge => "entry_too_large",
            SkipReason::Error => "error",
            SkipReason::Fresh => "fresh",
        }
    }

    fn index(self) -> usize {
        SkipReason::ALL
            .iter()
            .position(|r| *r == self)
            .unwrap_or_default()
    }
}

/// The isolation unit of the cache: the request's principal (its bearer
/// token, `[[mcp.principals]]`) and tenant (`tenant_header`). Every key and
/// every budget starts with it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Owner {
    principal: Arc<str>,
    tenant: Option<Arc<str>>,
}

impl Owner {
    /// The owner of a request by `principal` for `tenant`.
    pub fn new(principal: Arc<str>, tenant: Option<&str>) -> Self {
        Self {
            principal,
            tenant: tenant.map(Arc::from),
        }
    }

    /// The principal id.
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// The tenant, when the gateway applies tenancy.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }

    fn bytes(&self) -> usize {
        self.principal.len() + self.tenant.as_deref().map_or(0, str::len)
    }
}

/// The full key: compared on every byte.
#[derive(Debug, PartialEq, Eq, Hash)]
struct EntryKey {
    owner: Owner,
    tool: CachedTool,
    text: Box<str>,
}

impl EntryKey {
    fn bytes(&self) -> usize {
        self.owner.bytes() + self.text.len()
    }
}

/// One cached tool result.
struct Entry {
    output: Arc<ToolOutput>,
    /// The tables the result read (empty for `list_tables`: the global
    /// generation alone).
    tables: Vec<String>,
    /// Their generation sum before the fetch.
    generation: u64,
    stored_at: Instant,
    ttl: Duration,
    /// Insertion number: a FIFO slot whose number differs is stale.
    seq: u64,
    /// Key, result and table-name bytes.
    bytes: usize,
}

/// A FIFO of `(seq, key)` slots, oldest first, with lazy deletion.
type Fifo = VecDeque<(u64, Arc<EntryKey>)>;

/// One owner's share of the store.
#[derive(Default)]
struct OwnerState {
    fifo: Fifo,
    entries: usize,
    bytes: usize,
}

/// Everything under the one mutex.
#[derive(Default)]
struct Store {
    map: HashMap<Arc<EntryKey>, Entry>,
    fifo: Fifo,
    owners: HashMap<Owner, OwnerState>,
    bytes: usize,
    next_seq: u64,
}

impl Store {
    /// Remove `key`'s entry (its FIFO slots go stale); its bytes.
    fn remove(&mut self, key: &EntryKey) -> Option<Entry> {
        let entry = self.map.remove(key)?;
        self.bytes -= entry.bytes;
        if let Some(owner) = self.owners.get_mut(&key.owner) {
            owner.entries -= 1;
            owner.bytes -= entry.bytes;
            if owner.entries == 0 {
                self.owners.remove(&key.owner);
            }
        }
        Some(entry)
    }

    /// Whether the slot `(seq, key)` is the live entry's.
    fn live(&self, seq: u64, key: &EntryKey) -> bool {
        self.map.get(key).is_some_and(|e| e.seq == seq)
    }

    /// Evict the globally oldest live entry. False when none is left.
    fn evict_global_oldest(&mut self) -> bool {
        while let Some((seq, key)) = self.fifo.pop_front() {
            if self.live(seq, &key) {
                self.remove(&key);
                return true;
            }
        }
        false
    }

    /// Evict `owner`'s oldest live entry. False when it has none.
    fn evict_owner_oldest(&mut self, owner: &Owner) -> bool {
        loop {
            let Some((seq, key)) = self.owners.get_mut(owner).and_then(|o| o.fifo.pop_front())
            else {
                return false;
            };
            if self.live(seq, &key) {
                self.remove(&key);
                return true;
            }
        }
    }

    /// Drop the stale slots of the global FIFO, and of `owner`'s, once they
    /// outnumber the live entries by [`FIFO_SLACK_FACTOR`].
    fn compact(&mut self, owner: &Owner) {
        if self.fifo.len() > FIFO_SLACK_FACTOR * self.map.len().max(1) {
            let map = &self.map;
            self.fifo
                .retain(|(seq, key)| map.get(&**key).is_some_and(|e| e.seq == *seq));
        }
        if let Some(state) = self.owners.get_mut(owner) {
            if state.fifo.len() > FIFO_SLACK_FACTOR * state.entries.max(1) {
                let map = &self.map;
                state
                    .fifo
                    .retain(|(seq, key)| map.get(&**key).is_some_and(|e| e.seq == *seq));
            }
        }
    }
}

/// The `[mcp] tool_cache_*` bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolCacheSettings {
    /// `tool_cache_ttl_secs`.
    pub ttl: Duration,
    /// `tool_cache_catalog_ttl_secs` (zero: `list_tables` is not cached).
    pub catalog_ttl: Duration,
    /// `tool_cache_max_entries`.
    pub max_entries: usize,
    /// `tool_cache_max_bytes`.
    pub max_bytes: usize,
    /// `tool_cache_max_entries_per_principal` (per owner).
    pub max_entries_per_owner: usize,
    /// `tool_cache_max_bytes_per_principal` (per owner).
    pub max_bytes_per_owner: usize,
    /// `tool_cache_max_entry_bytes`.
    pub max_entry_bytes: usize,
    /// `tool_cache_max_statement_bytes`.
    pub max_statement_bytes: usize,
}

impl ToolCacheSettings {
    /// The bounds `[mcp]` configures (validated by `ProxyConfig::validate`).
    pub fn from_config(mcp: &McpConfig) -> Self {
        Self {
            ttl: Duration::from_secs(mcp.tool_cache_ttl_secs),
            catalog_ttl: Duration::from_secs(mcp.tool_cache_catalog_ttl_secs),
            max_entries: mcp.tool_cache_max_entries,
            max_bytes: mcp.tool_cache_max_bytes,
            max_entries_per_owner: mcp.tool_cache_max_entries_per_principal,
            max_bytes_per_owner: mcp.tool_cache_max_bytes_per_principal,
            max_entry_bytes: mcp.tool_cache_max_entry_bytes,
            max_statement_bytes: mcp.tool_cache_max_statement_bytes,
        }
    }
}

/// Process-lifetime counters (`/metrics`, `/metrics/prometheus`).
#[derive(Default)]
struct Stats {
    hits: [AtomicU64; CachedTool::COUNT],
    misses: [AtomicU64; CachedTool::COUNT],
    stores: AtomicU64,
    stale_rejected: AtomicU64,
    expired: AtomicU64,
    fills_raced: AtomicU64,
    skipped: [AtomicU64; SkipReason::COUNT],
    evictions_owner: AtomicU64,
    evictions_global: AtomicU64,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn read(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

/// What [`McpToolCache::plan`] made of a call it may cache.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Plan {
    tables: Vec<String>,
    ttl: Duration,
}

/// How a call left the cache, for its `_meta`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Served from the cache.
    Hit { age: Duration, expires_in: Duration },
    /// Went to the backend; `stored` says whether the result is now cached,
    /// `skipped` why it was not looked up or not stored (`raced`: a write
    /// moved the generations during the fetch).
    Miss {
        stored: bool,
        skipped: Option<&'static str>,
    },
}

impl Outcome {
    /// The `_meta` object of the tool result: `{"heliosdb.com/cache": …}`.
    /// Never carries text derived from data.
    pub fn meta(&self) -> Value {
        let inner = match self {
            Outcome::Hit { age, expires_in } => json!({
                "hit": true,
                "age_ms": age.as_millis() as u64,
                "expires_in_ms": expires_in.as_millis() as u64,
            }),
            Outcome::Miss { stored, skipped } => {
                let mut v = json!({ "hit": false, "stored": stored });
                if let Some(reason) = skipped {
                    v["skipped"] = json!(reason);
                }
                v
            }
        };
        json!({ META_KEY: inner })
    }
}

/// Why a fill was not stored.
enum Refused {
    TooLarge,
    Raced,
}

/// One owner's share, for `GET /api/mcp/cache`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct OwnerUsage {
    pub principal: String,
    pub tenant: Option<String>,
    pub entries: usize,
    pub bytes: usize,
}

/// The MCP gateway's tool-result cache.
pub struct McpToolCache {
    settings: ToolCacheSettings,
    generations: Arc<TableGenerations>,
    volatile: Arc<VolatileScan>,
    exclusions: Option<Arc<TableExclusions>>,
    /// `[[cache.tables]]` TTLs shorter than `tool_cache_ttl_secs`.
    shorter_ttls: Option<ShorterTtls>,
    normalizer: QueryNormalizer,
    store: parking_lot::Mutex<Store>,
    stats: Stats,
}

impl McpToolCache {
    /// A cache with `settings`, retired by `generations`, refusing what
    /// `volatile` and `exclusions` refuse (the result caches' own scans),
    /// with the shorter of `table_ttls` (`[[cache.tables]]`) applied.
    pub fn new<'a>(
        settings: ToolCacheSettings,
        generations: Arc<TableGenerations>,
        volatile: Arc<VolatileScan>,
        exclusions: Option<Arc<TableExclusions>>,
        table_ttls: impl IntoIterator<Item = (&'a str, Duration)>,
    ) -> Self {
        Self {
            shorter_ttls: ShorterTtls::new(table_ttls, settings.ttl),
            settings,
            generations,
            volatile,
            exclusions,
            normalizer: QueryNormalizer::new(),
            store: parking_lot::Mutex::new(Store::default()),
            stats: Stats::default(),
        }
    }

    /// The cache `config` configures, on the daemon's shared objects.
    pub fn from_config(
        config: &ProxyConfig,
        generations: Arc<TableGenerations>,
        volatile: Arc<VolatileScan>,
        exclusions: Option<Arc<TableExclusions>>,
    ) -> Self {
        Self::new(
            ToolCacheSettings::from_config(&config.mcp),
            generations,
            volatile,
            exclusions,
            config.cache.tables.iter().filter_map(|t| {
                t.ttl_secs
                    .map(|secs| (t.name.as_str(), Duration::from_secs(secs)))
            }),
        )
    }

    /// The bounds.
    pub fn settings(&self) -> &ToolCacheSettings {
        &self.settings
    }

    /// Whether `tool`'s result for the final statement `sql` may be cached,
    /// and if so its tables and TTL.
    fn plan(&self, tool: CachedTool, sql: &str) -> Result<Plan, SkipReason> {
        if tool == CachedTool::ListTables {
            if self.settings.catalog_ttl.is_zero() {
                return Err(SkipReason::Catalog);
            }
            return Ok(Plan {
                tables: Vec::new(),
                ttl: self.settings.catalog_ttl,
            });
        }
        if sql.len() > self.settings.max_statement_bytes {
            return Err(SkipReason::StatementTooLarge);
        }
        if !crate::server::ProxyServer::is_cacheable_read_sql_with(sql, &self.volatile) {
            let select = crate::protocol::starts_with_ci(sql.trim_start(), "SELECT");
            return Err(if select && self.volatile.refuses(sql) {
                SkipReason::Volatile
            } else {
                SkipReason::Write
            });
        }
        if crate::sql_text::names_system_catalog(sql.as_bytes()) {
            return Err(SkipReason::Catalog);
        }
        if crate::sql_text::uncacheable_text(sql).is_some() {
            return Err(SkipReason::Ambiguous);
        }
        if self.exclusions.as_ref().is_some_and(|x| x.matches(sql)) {
            return Err(SkipReason::Excluded);
        }
        let tables = self.normalizer.statement_tables(sql);
        if tables.is_empty() {
            return Err(SkipReason::NoTables);
        }
        let ttl = self
            .shorter_ttls
            .as_ref()
            .and_then(|s| s.min_in(sql))
            .map_or(self.settings.ttl, |t| t.min(self.settings.ttl));
        Ok(Plan { tables, ttl })
    }

    /// Serve `tool` for `owner` and the final statement `sql`: from the
    /// cache when a current entry holds it (and `fresh` is not set), else
    /// from `fetch`, whose result is then stored when eligible. Returns the
    /// result and how it left the cache.
    pub async fn serve<F, Fut>(
        &self,
        owner: &Owner,
        tool: CachedTool,
        sql: &str,
        fresh: bool,
        fetch: F,
    ) -> (Result<ToolOutput, String>, Outcome)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<ToolOutput, String>>,
    {
        let plan = match self.plan(tool, sql) {
            Ok(plan) => plan,
            Err(reason) => {
                bump(&self.stats.skipped[reason.index()]);
                bump(&self.stats.misses[tool.index()]);
                let result = fetch().await;
                let skipped = match (&result, reason) {
                    // A refused statement that also failed: the failure is
                    // what the agent needs to know, not the refusal.
                    (Err(_), _) => SkipReason::Error,
                    (Ok(_), reason) => reason,
                };
                return (
                    result,
                    Outcome::Miss {
                        stored: false,
                        skipped: Some(skipped.as_str()),
                    },
                );
            }
        };
        let key = EntryKey {
            owner: owner.clone(),
            tool,
            text: Box::from(sql),
        };
        if fresh {
            bump(&self.stats.skipped[SkipReason::Fresh.index()]);
        } else if let Some((output, outcome)) = self.lookup(&key) {
            bump(&self.stats.hits[tool.index()]);
            return (Ok((*output).clone()), outcome);
        }
        bump(&self.stats.misses[tool.index()]);
        // C-02: the generation before the fetch; a write during it means the
        // rows may predate the write, so they are not stored.
        let before = self.generations.generation_of(&plan.tables);
        let result = fetch().await;
        let outcome = match &result {
            Err(_) => {
                bump(&self.stats.skipped[SkipReason::Error.index()]);
                Outcome::Miss {
                    stored: false,
                    skipped: Some(SkipReason::Error.as_str()),
                }
            }
            Ok(output) => match self.insert(key, plan, before, output.clone()) {
                Ok(()) => Outcome::Miss {
                    stored: true,
                    skipped: fresh.then_some(SkipReason::Fresh.as_str()),
                },
                Err(Refused::TooLarge) => {
                    bump(&self.stats.skipped[SkipReason::EntryTooLarge.index()]);
                    Outcome::Miss {
                        stored: false,
                        skipped: Some(SkipReason::EntryTooLarge.as_str()),
                    }
                }
                Err(Refused::Raced) => Outcome::Miss {
                    stored: false,
                    skipped: Some("raced"),
                },
            },
        };
        (result, outcome)
    }

    /// A current, unexpired entry for `key`; a stale or expired one is
    /// removed.
    fn lookup(&self, key: &EntryKey) -> Option<(Arc<ToolOutput>, Outcome)> {
        let now = Instant::now();
        let mut store = self.store.lock();
        let verdict = {
            let entry = store.map.get(key)?;
            let age = now.saturating_duration_since(entry.stored_at);
            if age >= entry.ttl {
                Err(&self.stats.expired)
            } else if entry.generation != self.generations.generation_of(&entry.tables) {
                Err(&self.stats.stale_rejected)
            } else {
                Ok((
                    entry.output.clone(),
                    Outcome::Hit {
                        age,
                        expires_in: entry.ttl - age,
                    },
                ))
            }
        };
        match verdict {
            Ok(hit) => Some(hit),
            Err(counter) => {
                store.remove(key);
                bump(counter);
                None
            }
        }
    }

    /// Store a fill whose tables had generation sum `before` when its fetch
    /// started, evicting what the bounds require.
    fn insert(
        &self,
        key: EntryKey,
        plan: Plan,
        before: u64,
        output: ToolOutput,
    ) -> Result<(), Refused> {
        let structured = output
            .1
            .as_ref()
            .map_or(0, |v| serde_json::to_string(v).map_or(0, |s| s.len()));
        let bytes = key.bytes()
            + output.0.len()
            + structured
            + plan.tables.iter().map(String::len).sum::<usize>();
        if bytes > self.settings.max_entry_bytes {
            return Err(Refused::TooLarge);
        }
        let now = Instant::now();
        let mut store = self.store.lock();
        if self.generations.generation_of(&plan.tables) != before {
            bump(&self.stats.fills_raced);
            return Err(Refused::Raced);
        }
        // Expired (and lazily deleted) entries at the front go first.
        while let Some((seq, front)) = store.fifo.front().cloned() {
            // `Some(expired)` for the live entry, `None` for a stale slot.
            let live = match store.map.get(&*front) {
                Some(e) if e.seq == seq => {
                    Some(now.saturating_duration_since(e.stored_at) >= e.ttl)
                }
                _ => None,
            };
            match live {
                Some(false) => break,
                Some(true) => {
                    store.fifo.pop_front();
                    store.remove(&front);
                    bump(&self.stats.expired);
                }
                None => {
                    store.fifo.pop_front();
                }
            }
        }
        store.remove(&key);
        let owner = key.owner.clone();
        // This owner's bounds: only its own oldest entries go.
        loop {
            let (entries, held) = store
                .owners
                .get(&owner)
                .map_or((0, 0), |o| (o.entries, o.bytes));
            if entries < self.settings.max_entries_per_owner
                && held + bytes <= self.settings.max_bytes_per_owner
            {
                break;
            }
            if !store.evict_owner_oldest(&owner) {
                break;
            }
            bump(&self.stats.evictions_owner);
        }
        // The global bounds: the globally oldest entries go.
        while store.map.len() >= self.settings.max_entries
            || store.bytes + bytes > self.settings.max_bytes
        {
            if !store.evict_global_oldest() {
                break;
            }
            bump(&self.stats.evictions_global);
        }
        let seq = store.next_seq;
        store.next_seq += 1;
        let key = Arc::new(key);
        store.fifo.push_back((seq, key.clone()));
        let state = store.owners.entry(owner.clone()).or_default();
        state.fifo.push_back((seq, key.clone()));
        state.entries += 1;
        state.bytes += bytes;
        store.bytes += bytes;
        store.map.insert(
            key,
            Entry {
                output: Arc::new(output),
                tables: plan.tables,
                generation: before,
                stored_at: now,
                ttl: plan.ttl,
                seq,
                bytes,
            },
        );
        store.compact(&owner);
        bump(&self.stats.stores);
        Ok(())
    }

    /// Remove every entry of the owners `principal` and `tenant` select
    /// (`None` = any): after a write the proxy did not see. The number
    /// removed.
    pub fn clear(&self, principal: Option<&str>, tenant: Option<&str>) -> usize {
        let mut store = self.store.lock();
        let owners: Vec<Owner> = store
            .owners
            .keys()
            .filter(|o| {
                principal.is_none_or(|p| o.principal() == p)
                    && tenant.is_none_or(|t| o.tenant() == Some(t))
            })
            .cloned()
            .collect();
        let mut removed = 0;
        for owner in owners {
            let Some(state) = store.owners.remove(&owner) else {
                continue;
            };
            for (seq, key) in state.fifo {
                let live = store.map.get(&*key).is_some_and(|e| e.seq == seq);
                let entry = if live { store.map.remove(&*key) } else { None };
                if let Some(entry) = entry {
                    store.bytes -= entry.bytes;
                    removed += 1;
                }
            }
        }
        if store.map.is_empty() {
            store.fifo.clear();
        }
        removed
    }

    /// Entries held now.
    pub fn entries(&self) -> usize {
        self.store.lock().map.len()
    }

    /// Bytes held now (keys, results and table names).
    pub fn retained_bytes(&self) -> usize {
        self.store.lock().bytes
    }

    /// Owners holding entries now.
    pub fn owners(&self) -> usize {
        self.store.lock().owners.len()
    }

    /// Every owner's entries and bytes, ordered by principal then tenant.
    pub fn owner_usage(&self) -> Vec<OwnerUsage> {
        let store = self.store.lock();
        let mut usage: Vec<OwnerUsage> = store
            .owners
            .iter()
            .map(|(o, s)| OwnerUsage {
                principal: o.principal().to_string(),
                tenant: o.tenant().map(str::to_string),
                entries: s.entries,
                bytes: s.bytes,
            })
            .collect();
        usage.sort_by(|a, b| (&a.principal, &a.tenant).cmp(&(&b.principal, &b.tenant)));
        usage
    }

    /// The counters and gauges as JSON (`/metrics` `mcp_cache`, `GET
    /// /api/mcp/cache`). No principal or tenant appears here.
    pub fn stats_json(&self) -> Value {
        let s = &self.stats;
        let by_tool = |counters: &[AtomicU64; CachedTool::COUNT]| {
            let mut m = serde_json::Map::new();
            for tool in CachedTool::ALL {
                m.insert(tool.as_str().into(), read(&counters[tool.index()]).into());
            }
            Value::Object(m)
        };
        let skipped: BTreeMap<&str, u64> = SkipReason::ALL
            .iter()
            .map(|r| (r.as_str(), read(&s.skipped[r.index()])))
            .collect();
        let (entries, bytes, owners) = {
            let store = self.store.lock();
            (store.map.len(), store.bytes, store.owners.len())
        };
        json!({
            "hits": by_tool(&s.hits),
            "misses": by_tool(&s.misses),
            "stores": read(&s.stores),
            "stale_rejected": read(&s.stale_rejected),
            "expired": read(&s.expired),
            "fills_raced": read(&s.fills_raced),
            "skipped": skipped,
            "evictions": {
                "principal": read(&s.evictions_owner),
                "global": read(&s.evictions_global),
            },
            "entries": entries,
            "retained_bytes": bytes,
            "owners": owners,
        })
    }

    /// Append the `heliosdb_proxy_mcp_cache_*` series to a Prometheus body
    /// (no principal or tenant label: bounded cardinality).
    pub fn write_prometheus(&self, out: &mut String) {
        use std::fmt::Write as _;
        let s = &self.stats;
        let head = |out: &mut String, name: &str, kind: &str, help: &str| {
            let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
        };
        for (name, help, counters) in [
            (
                "heliosdb_proxy_mcp_cache_hits_total",
                "MCP tool calls served from the tool-result cache",
                &s.hits,
            ),
            (
                "heliosdb_proxy_mcp_cache_misses_total",
                "MCP tool calls of a cacheable tool that went to the backend",
                &s.misses,
            ),
        ] {
            head(out, name, "counter", help);
            for tool in CachedTool::ALL {
                let _ = writeln!(
                    out,
                    "{name}{{tool=\"{}\"}} {}",
                    tool.as_str(),
                    read(&counters[tool.index()])
                );
            }
        }
        for (name, help, value) in [
            (
                "heliosdb_proxy_mcp_cache_stores_total",
                "MCP tool results stored in the tool-result cache",
                read(&s.stores),
            ),
            (
                "heliosdb_proxy_mcp_cache_stale_rejected_total",
                "MCP cache entries refused because a table they read was written",
                read(&s.stale_rejected),
            ),
            (
                "heliosdb_proxy_mcp_cache_expired_total",
                "MCP cache entries dropped past their TTL",
                read(&s.expired),
            ),
            (
                "heliosdb_proxy_mcp_cache_fills_raced_total",
                "MCP tool results not stored because a write raced their fetch",
                read(&s.fills_raced),
            ),
        ] {
            head(out, name, "counter", help);
            let _ = writeln!(out, "{name} {value}");
        }
        let name = "heliosdb_proxy_mcp_cache_skipped_total";
        head(
            out,
            name,
            "counter",
            "MCP tool calls the tool-result cache neither served nor stored, by reason",
        );
        for reason in SkipReason::ALL {
            let _ = writeln!(
                out,
                "{name}{{reason=\"{}\"}} {}",
                reason.as_str(),
                read(&s.skipped[reason.index()])
            );
        }
        let name = "heliosdb_proxy_mcp_cache_evictions_total";
        head(
            out,
            name,
            "counter",
            "MCP cache entries evicted by the per-principal or the global bounds",
        );
        let _ = writeln!(
            out,
            "{name}{{scope=\"principal\"}} {}",
            read(&s.evictions_owner)
        );
        let _ = writeln!(
            out,
            "{name}{{scope=\"global\"}} {}",
            read(&s.evictions_global)
        );
        let (entries, bytes, owners) = {
            let store = self.store.lock();
            (store.map.len(), store.bytes, store.owners.len())
        };
        for (name, help, value) in [
            (
                "heliosdb_proxy_mcp_cache_entries",
                "Entries in the MCP tool-result cache",
                entries,
            ),
            (
                "heliosdb_proxy_mcp_cache_retained_bytes",
                "Bytes (keys and results) the MCP tool-result cache holds",
                bytes,
            ),
            (
                "heliosdb_proxy_mcp_cache_owners",
                "Owners (principal, tenant) holding MCP cache entries",
                owners,
            ),
        ] {
            head(out, name, "gauge", help);
            let _ = writeln!(out, "{name} {value}");
        }
    }

    /// Counter read for tests.
    #[cfg(test)]
    fn skipped(&self, reason: SkipReason) -> u64 {
        read(&self.stats.skipped[reason.index()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const Q: &str = "SELECT id, status FROM orders WHERE id = 42";

    fn settings() -> ToolCacheSettings {
        ToolCacheSettings::from_config(&McpConfig::default())
    }

    fn cache_with(settings: ToolCacheSettings) -> (McpToolCache, Arc<TableGenerations>) {
        let generations = Arc::new(TableGenerations::new());
        let cache = McpToolCache::new(
            settings,
            generations.clone(),
            Arc::new(VolatileScan::new(["my_volatile_fn"])),
            Some(Arc::new(TableExclusions::new(["payroll"]))),
            [("audit", Duration::from_secs(5))],
        );
        (cache, generations)
    }

    fn cache() -> (McpToolCache, Arc<TableGenerations>) {
        cache_with(settings())
    }

    fn owner(principal: &str, tenant: Option<&str>) -> Owner {
        Owner::new(Arc::from(principal), tenant)
    }

    fn rows(text: &str) -> ToolOutput {
        (text.to_string(), None)
    }

    /// Serve `sql` with a fetch that answers `answer` and counts its calls.
    async fn call(
        cache: &McpToolCache,
        who: &Owner,
        tool: CachedTool,
        sql: &str,
        fresh: bool,
        answer: Result<ToolOutput, String>,
        fetches: &std::cell::Cell<usize>,
    ) -> (Result<ToolOutput, String>, Outcome) {
        cache
            .serve(who, tool, sql, fresh, || async {
                fetches.set(fetches.get() + 1);
                answer
            })
            .await
    }

    fn is_hit(o: &Outcome) -> bool {
        matches!(o, Outcome::Hit { .. })
    }

    #[tokio::test]
    async fn a_miss_stores_and_the_same_request_hits_with_identical_content() {
        let (cache, _) = cache();
        let who = owner("default", None);
        let n = std::cell::Cell::new(0);
        let (miss, o1) = call(
            &cache,
            &who,
            CachedTool::Query,
            Q,
            false,
            Ok(rows("42 | shipped")),
            &n,
        )
        .await;
        assert_eq!(
            o1,
            Outcome::Miss {
                stored: true,
                skipped: None
            }
        );
        let (hit, o2) = call(
            &cache,
            &who,
            CachedTool::Query,
            Q,
            false,
            Ok(rows("other")),
            &n,
        )
        .await;
        assert!(is_hit(&o2), "{o2:?}");
        assert_eq!(hit, miss, "content is byte-identical on a hit");
        assert_eq!(n.get(), 1, "the hit did not fetch");
        assert_eq!(cache.entries(), 1);
        let meta = o2.meta();
        assert_eq!(meta[META_KEY]["hit"], true);
        assert!(meta[META_KEY]["age_ms"].is_u64());
        assert!(meta[META_KEY]["expires_in_ms"].as_u64().unwrap() <= 60_000);
        let miss_meta = o1.meta();
        assert_eq!(miss_meta[META_KEY], json!({"hit": false, "stored": true}));
        // A truncated result keeps its structured note on a hit too.
        let truncated: ToolOutput = ("x".into(), Some(json!({"truncated": true})));
        let sql = "SELECT v FROM big";
        let _ = call(
            &cache,
            &who,
            CachedTool::Query,
            sql,
            false,
            Ok(truncated.clone()),
            &n,
        )
        .await;
        let (again, o) = call(
            &cache,
            &who,
            CachedTool::Query,
            sql,
            false,
            Ok(rows("y")),
            &n,
        )
        .await;
        assert!(is_hit(&o));
        assert_eq!(again, Ok(truncated));
    }

    #[tokio::test]
    async fn another_principal_tenant_or_text_misses() {
        let (cache, _) = cache();
        let n = std::cell::Cell::new(0);
        let a = owner("etl-agent", Some("acme"));
        let _ = call(
            &cache,
            &a,
            CachedTool::Query,
            Q,
            false,
            Ok(rows("acme")),
            &n,
        )
        .await;
        for (who, sql) in [
            (owner("bi-agent", Some("acme")), Q),
            (owner("etl-agent", Some("globex")), Q),
            (owner("etl-agent", None), Q),
            // A rewrite or a tenant filter changes the final text: another key.
            (
                a.clone(),
                "SELECT id, status FROM orders WHERE id = 42 AND tid = 'acme'",
            ),
            (a.clone(), "SELECT id, status FROM orders WHERE id = 42 "),
        ] {
            let (_, o) = call(
                &cache,
                &who,
                CachedTool::Query,
                sql,
                false,
                Ok(rows("x")),
                &n,
            )
            .await;
            assert!(!is_hit(&o), "{who:?} {sql}");
        }
        let (_, o) = call(&cache, &a, CachedTool::Query, Q, false, Ok(rows("x")), &n).await;
        assert!(is_hit(&o));
        // list_tables and query never share an entry.
        let (_, o) = call(
            &cache,
            &a,
            CachedTool::ListTables,
            Q,
            false,
            Ok(rows("x")),
            &n,
        )
        .await;
        assert!(!is_hit(&o));
    }

    #[tokio::test]
    async fn a_write_retires_the_entry_and_a_raced_fill_is_not_stored() {
        let (cache, g) = cache();
        let who = owner("default", None);
        let n = std::cell::Cell::new(0);
        let _ = call(
            &cache,
            &who,
            CachedTool::Query,
            Q,
            false,
            Ok(rows("old")),
            &n,
        )
        .await;
        g.mark_written(&["orders".to_string()]);
        let (r, o) = call(
            &cache,
            &who,
            CachedTool::Query,
            Q,
            false,
            Ok(rows("new")),
            &n,
        )
        .await;
        assert_eq!(r, Ok(rows("new")));
        assert_eq!(
            o,
            Outcome::Miss {
                stored: true,
                skipped: None
            }
        );
        assert_eq!(read(&cache.stats.stale_rejected), 1);
        // Another table's write leaves it served.
        g.mark_written(&["customers".to_string()]);
        let (_, o) = call(&cache, &who, CachedTool::Query, Q, false, Ok(rows("x")), &n).await;
        assert!(is_hit(&o));

        // A write while the fetch runs: the result is served, not stored.
        let sql = "SELECT * FROM orders WHERE id = 7";
        let (r, o) = cache
            .serve(&who, CachedTool::Query, sql, false, || async {
                g.note_write("UPDATE orders SET status = 'x' WHERE id = 7");
                Ok(rows("pre-write"))
            })
            .await;
        assert_eq!(r, Ok(rows("pre-write")));
        assert_eq!(
            o,
            Outcome::Miss {
                stored: false,
                skipped: Some("raced")
            }
        );
        assert_eq!(read(&cache.stats.fills_raced), 1);
        let (_, o) = call(
            &cache,
            &who,
            CachedTool::Query,
            sql,
            false,
            Ok(rows("x")),
            &n,
        )
        .await;
        assert!(!is_hit(&o), "nothing was stored");
    }

    #[tokio::test]
    async fn list_tables_follows_the_global_generation_and_its_own_ttl() {
        let (cache, g) = cache();
        let who = owner("default", None);
        let n = std::cell::Cell::new(0);
        let sql = "SELECT table_schema, table_name FROM information_schema.tables";
        let _ = call(
            &cache,
            &who,
            CachedTool::ListTables,
            sql,
            false,
            Ok(rows("t")),
            &n,
        )
        .await;
        let (_, o) = call(
            &cache,
            &who,
            CachedTool::ListTables,
            sql,
            false,
            Ok(rows("t")),
            &n,
        )
        .await;
        match o {
            Outcome::Hit { expires_in, .. } => assert!(expires_in <= Duration::from_secs(30)),
            other => panic!("{other:?}"),
        }
        // A table write leaves the listing; DDL (unknown scope) retires it.
        g.mark_written(&["orders".to_string()]);
        let (_, o) = call(
            &cache,
            &who,
            CachedTool::ListTables,
            sql,
            false,
            Ok(rows("t")),
            &n,
        )
        .await;
        assert!(is_hit(&o));
        g.invalidate_all();
        let (_, o) = call(
            &cache,
            &who,
            CachedTool::ListTables,
            sql,
            false,
            Ok(rows("t2")),
            &n,
        )
        .await;
        assert!(!is_hit(&o));

        // tool_cache_catalog_ttl_secs = 0: list_tables is not cached.
        let (off, _) = cache_with(ToolCacheSettings {
            catalog_ttl: Duration::ZERO,
            ..settings()
        });
        let (_, o) = call(
            &off,
            &who,
            CachedTool::ListTables,
            sql,
            false,
            Ok(rows("t")),
            &n,
        )
        .await;
        assert_eq!(
            o,
            Outcome::Miss {
                stored: false,
                skipped: Some("catalog")
            }
        );
        assert_eq!(off.entries(), 0);
    }

    #[tokio::test]
    async fn every_skip_reason_is_neither_served_nor_stored() {
        let (cache, _) = cache_with(ToolCacheSettings {
            max_statement_bytes: 64,
            max_entry_bytes: 120,
            ..settings()
        });
        let who = owner("default", None);
        let n = std::cell::Cell::new(0);
        for (sql, reason) in [
            ("UPDATE orders SET status = 'x'", SkipReason::Write),
            ("SELECT * FROM orders; SELECT 1", SkipReason::Write),
            ("SELECT * INTO copy FROM orders", SkipReason::Write),
            ("WITH x AS (SELECT 1) SELECT * FROM x", SkipReason::Write),
            ("SELECT now(), id FROM orders", SkipReason::Volatile),
            ("SELECT * FROM orders FOR UPDATE", SkipReason::Volatile),
            (
                "SELECT my_volatile_fn(id) FROM orders",
                SkipReason::Volatile,
            ),
            (
                "SELECT * FROM orders WHERE d > 'today'",
                SkipReason::Volatile,
            ),
            ("SELECT * FROM pg_settings", SkipReason::Catalog),
            (
                "SELECT * FROM orders /* as of now */",
                SkipReason::Ambiguous,
            ),
            (
                "SELECT * FROM orders WHERE p = 'C:\\' OR 1 = 1",
                SkipReason::Ambiguous,
            ),
            ("SELECT * FROM payroll", SkipReason::Excluded),
            ("SELECT 1", SkipReason::NoTables),
            (
                "SELECT id, status, total, customer FROM orders WHERE id = 1234567",
                SkipReason::StatementTooLarge,
            ),
        ] {
            let before = cache.skipped(reason);
            let (r, o) = call(
                &cache,
                &who,
                CachedTool::Query,
                sql,
                false,
                Ok(rows("r")),
                &n,
            )
            .await;
            assert_eq!(r, Ok(rows("r")), "{sql}");
            assert_eq!(
                o,
                Outcome::Miss {
                    stored: false,
                    skipped: Some(reason.as_str())
                },
                "{sql}"
            );
            assert_eq!(cache.skipped(reason), before + 1, "{sql}");
        }
        assert_eq!(cache.entries(), 0);

        // Entry too large: key and result over max_entry_bytes.
        let big = "x".repeat(200);
        let (_, o) = call(
            &cache,
            &who,
            CachedTool::Query,
            Q,
            false,
            Ok(rows(&big)),
            &n,
        )
        .await;
        assert_eq!(
            o,
            Outcome::Miss {
                stored: false,
                skipped: Some("entry_too_large")
            }
        );
        // An error (isError) is never stored.
        let (r, o) = call(
            &cache,
            &who,
            CachedTool::Query,
            Q,
            false,
            Err("boom".into()),
            &n,
        )
        .await;
        assert_eq!(r, Err("boom".to_string()));
        assert_eq!(
            o,
            Outcome::Miss {
                stored: false,
                skipped: Some("error")
            }
        );
        assert_eq!(cache.entries(), 0);
        assert_eq!(cache.skipped(SkipReason::Error), 1);
    }

    #[tokio::test]
    async fn fresh_skips_the_lookup_and_refreshes_the_entry() {
        let (cache, _) = cache();
        let who = owner("default", None);
        let n = std::cell::Cell::new(0);
        let _ = call(
            &cache,
            &who,
            CachedTool::Query,
            Q,
            false,
            Ok(rows("v1")),
            &n,
        )
        .await;
        let (r, o) = call(&cache, &who, CachedTool::Query, Q, true, Ok(rows("v2")), &n).await;
        assert_eq!(r, Ok(rows("v2")));
        assert_eq!(
            o,
            Outcome::Miss {
                stored: true,
                skipped: Some("fresh")
            }
        );
        let (r, o) = call(
            &cache,
            &who,
            CachedTool::Query,
            Q,
            false,
            Ok(rows("v3")),
            &n,
        )
        .await;
        assert!(is_hit(&o));
        assert_eq!(r, Ok(rows("v2")), "the refreshed entry");
        assert_eq!(cache.entries(), 1);
        assert_eq!(cache.skipped(SkipReason::Fresh), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn entries_expire_at_their_ttl_and_a_shorter_table_ttl_applies() {
        let (cache, _) = cache();
        let who = owner("default", None);
        let n = std::cell::Cell::new(0);
        let _ = call(&cache, &who, CachedTool::Query, Q, false, Ok(rows("a")), &n).await;
        let audit = "SELECT * FROM audit WHERE id = 1";
        let _ = call(
            &cache,
            &who,
            CachedTool::Query,
            audit,
            false,
            Ok(rows("b")),
            &n,
        )
        .await;
        tokio::time::advance(Duration::from_secs(4)).await;
        let (_, o) = call(
            &cache,
            &who,
            CachedTool::Query,
            audit,
            false,
            Ok(rows("b")),
            &n,
        )
        .await;
        assert!(is_hit(&o), "within the 5 s table TTL");
        tokio::time::advance(Duration::from_secs(2)).await;
        let (_, o) = call(
            &cache,
            &who,
            CachedTool::Query,
            audit,
            false,
            Ok(rows("b2")),
            &n,
        )
        .await;
        assert!(!is_hit(&o), "past the [[cache.tables]] TTL");
        assert_eq!(read(&cache.stats.expired), 1);
        let (_, o) = call(&cache, &who, CachedTool::Query, Q, false, Ok(rows("a")), &n).await;
        assert!(is_hit(&o), "the 60 s default still holds");
        tokio::time::advance(Duration::from_secs(55)).await;
        let (_, o) = call(
            &cache,
            &who,
            CachedTool::Query,
            Q,
            false,
            Ok(rows("a2")),
            &n,
        )
        .await;
        assert!(!is_hit(&o), "past tool_cache_ttl_secs");
        // The lookup dropped Q's expired entry; the refill's store then
        // dropped the expired `audit` entry at the front of the FIFO.
        assert_eq!(read(&cache.stats.expired), 3);
        assert_eq!(cache.entries(), 1);
    }

    #[tokio::test]
    async fn an_owner_over_its_bound_evicts_only_its_own_oldest() {
        let (cache, _) = cache_with(ToolCacheSettings {
            max_entries_per_owner: 2,
            ..settings()
        });
        let n = std::cell::Cell::new(0);
        let a = owner("a", None);
        let b = owner("b", None);
        let sql = |i: usize| format!("SELECT v FROM t{i}");
        let _ = call(
            &cache,
            &b,
            CachedTool::Query,
            &sql(0),
            false,
            Ok(rows("b0")),
            &n,
        )
        .await;
        for i in 0..3 {
            let _ = call(
                &cache,
                &a,
                CachedTool::Query,
                &sql(i),
                false,
                Ok(rows("a")),
                &n,
            )
            .await;
        }
        assert_eq!(read(&cache.stats.evictions_owner), 1);
        assert_eq!(read(&cache.stats.evictions_global), 0);
        let (_, o) = call(
            &cache,
            &a,
            CachedTool::Query,
            &sql(0),
            false,
            Ok(rows("a")),
            &n,
        )
        .await;
        assert!(!is_hit(&o), "a's oldest went");
        let (_, o) = call(
            &cache,
            &b,
            CachedTool::Query,
            &sql(0),
            false,
            Ok(rows("b")),
            &n,
        )
        .await;
        assert!(is_hit(&o), "b untouched");
        let usage = cache.owner_usage();
        assert_eq!(usage.len(), 2);
        assert_eq!((usage[0].principal.as_str(), usage[0].entries), ("a", 2));
        assert_eq!((usage[1].principal.as_str(), usage[1].entries), ("b", 1));
    }

    #[tokio::test]
    async fn the_global_bounds_evict_the_globally_oldest_and_accounting_returns_to_zero() {
        let (cache, _) = cache_with(ToolCacheSettings {
            max_entries: 3,
            ..settings()
        });
        let n = std::cell::Cell::new(0);
        let sql = |i: usize| format!("SELECT v FROM t{i}");
        for i in 0..5 {
            let who = owner(&format!("p{i}"), None);
            let _ = call(
                &cache,
                &who,
                CachedTool::Query,
                &sql(i),
                false,
                Ok(rows("r")),
                &n,
            )
            .await;
        }
        assert_eq!(cache.entries(), 3);
        assert_eq!(read(&cache.stats.evictions_global), 2);
        assert_eq!(cache.owners(), 3, "emptied owners are dropped");
        let (_, o) = call(
            &cache,
            &owner("p0", None),
            CachedTool::Query,
            &sql(0),
            false,
            Ok(rows("r")),
            &n,
        )
        .await;
        assert!(!is_hit(&o));

        // Byte bound: a large entry evicts as many old ones as it needs.
        let (bytes_cache, g) = cache_with(ToolCacheSettings {
            max_bytes: 400,
            max_bytes_per_owner: 400,
            max_entry_bytes: 400,
            ..settings()
        });
        let who = owner("p", None);
        for i in 0..4 {
            let _ = call(
                &bytes_cache,
                &who,
                CachedTool::Query,
                &sql(i),
                false,
                Ok(rows(&"x".repeat(50))),
                &n,
            )
            .await;
        }
        assert!(bytes_cache.retained_bytes() <= 400);
        let _ = call(
            &bytes_cache,
            &who,
            CachedTool::Query,
            &sql(9),
            false,
            Ok(rows(&"y".repeat(300))),
            &n,
        )
        .await;
        assert!(
            bytes_cache.retained_bytes() <= 400,
            "{}",
            bytes_cache.retained_bytes()
        );
        assert!(
            read(&bytes_cache.stats.evictions_owner) + read(&bytes_cache.stats.evictions_global)
                >= 2
        );

        // Stale and replaced entries leave the accounting exact.
        g.invalidate_all();
        for i in 0..4 {
            let _ = call(
                &bytes_cache,
                &who,
                CachedTool::Query,
                &sql(i),
                false,
                Ok(rows("z")),
                &n,
            )
            .await;
        }
        let held = bytes_cache.entries();
        assert!(held >= 4);
        assert_eq!(bytes_cache.clear(None, None), held);
        assert_eq!(bytes_cache.entries(), 0);
        assert_eq!(bytes_cache.retained_bytes(), 0);
        assert_eq!(bytes_cache.owners(), 0);
    }

    #[tokio::test]
    async fn clear_by_owner_leaves_the_others() {
        let (cache, _) = cache();
        let n = std::cell::Cell::new(0);
        for (p, t) in [("a", Some("x")), ("a", Some("y")), ("b", Some("x"))] {
            let _ = call(
                &cache,
                &owner(p, t),
                CachedTool::Query,
                Q,
                false,
                Ok(rows("r")),
                &n,
            )
            .await;
        }
        assert_eq!(cache.clear(Some("a"), Some("x")), 1);
        assert_eq!(cache.entries(), 2);
        assert_eq!(cache.clear(None, Some("x")), 1);
        assert_eq!(cache.clear(Some("a"), None), 1);
        assert_eq!(cache.entries(), 0);
        assert_eq!(cache.retained_bytes(), 0);
        assert_eq!(cache.clear(None, None), 0);
    }

    #[tokio::test]
    async fn replacing_an_entry_many_times_keeps_the_fifos_bounded() {
        let (cache, _) = cache();
        let who = owner("default", None);
        let n = std::cell::Cell::new(0);
        for _ in 0..100 {
            let _ = call(&cache, &who, CachedTool::Query, Q, true, Ok(rows("r")), &n).await;
        }
        let store = cache.store.lock();
        assert_eq!(store.map.len(), 1);
        assert!(
            store.fifo.len() <= FIFO_SLACK_FACTOR + 1,
            "{}",
            store.fifo.len()
        );
        let owner_fifo = store.owners.values().next().unwrap().fifo.len();
        assert!(owner_fifo <= FIFO_SLACK_FACTOR + 1, "{owner_fifo}");
    }

    #[test]
    fn metrics_carry_no_principal_or_tenant() {
        let (cache, _) = cache();
        let mut out = String::new();
        cache.write_prometheus(&mut out);
        for series in [
            "heliosdb_proxy_mcp_cache_hits_total{tool=\"query\"} 0",
            "heliosdb_proxy_mcp_cache_misses_total{tool=\"list_tables\"} 0",
            "heliosdb_proxy_mcp_cache_stores_total 0",
            "heliosdb_proxy_mcp_cache_stale_rejected_total 0",
            "heliosdb_proxy_mcp_cache_expired_total 0",
            "heliosdb_proxy_mcp_cache_fills_raced_total 0",
            "heliosdb_proxy_mcp_cache_skipped_total{reason=\"statement_too_large\"} 0",
            "heliosdb_proxy_mcp_cache_evictions_total{scope=\"principal\"} 0",
            "heliosdb_proxy_mcp_cache_evictions_total{scope=\"global\"} 0",
            "heliosdb_proxy_mcp_cache_entries 0",
            "heliosdb_proxy_mcp_cache_retained_bytes 0",
            "heliosdb_proxy_mcp_cache_owners 0",
        ] {
            assert!(out.contains(series), "{series} missing:\n{out}");
        }
        assert!(!out.contains("principal=") && !out.contains("tenant="));
        let json = cache.stats_json();
        assert_eq!(json["hits"]["query"], 0);
        assert_eq!(json["skipped"]["fresh"], 0);
    }
}
