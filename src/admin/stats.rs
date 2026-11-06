//! `GET /api/stats`, schema_version 1 (Sprinter c3397d898b25).
//!
//! One JSON document for external pollers (HeliosDB-Lite's proxy system
//! views, dashboards) instead of stitching `/metrics`, `/nodes`,
//! `/topology`, `/api/circuit`, `/api/cache/heatmap`, `/api/mcp/cache` and
//! `/sessions` together. It reads only the snapshots those endpoints already
//! take, plus configuration values: it adds no counter to the data path.
//!
//! The contract is docs/admin-api.md ("GET /api/stats — schema_version 1")
//! and tests/fixtures/api-stats-v1.schema.json:
//! - every section is always present; a section whose feature is compiled
//!   out or switched off is `{"enabled": false, "reason": "..."}`;
//! - within version 1 fields are only ever added; a breaking change bumps
//!   `schema_version`;
//! - a field ending in `_total` counts since startup, any other number is a
//!   value now;
//! - a list is sorted and cut at `[admin] stats_max_items`, next to its full
//!   length (`count`) and `truncated`.

use super::{AdminServer, AdminState};
use crate::config::ProxyConfig;
use crate::server::ServerMetricsSnapshot;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

/// The document's version. Bumped only by a breaking change.
const SCHEMA_VERSION: u32 = 1;

/// A section: `{"enabled": true, ...fields}` or `{"enabled": false,
/// "reason": "..."}`, never absent.
#[derive(Serialize)]
struct Section<T> {
    enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
    #[serde(flatten)]
    data: Option<T>,
}

impl<T> Section<T> {
    fn on(data: T) -> Self {
        Self {
            enabled: true,
            reason: None,
            data: Some(data),
        }
    }

    fn off(reason: &'static str) -> Self {
        Self {
            enabled: false,
            reason: Some(reason),
            data: None,
        }
    }
}

/// The data of a section whose feature this build does not compile: it is
/// always `{"enabled": false, ...}`.
#[cfg(any(
    not(feature = "pool-modes"),
    not(feature = "query-cache"),
    not(feature = "rate-limiting")
))]
#[derive(Serialize)]
enum Never {}

#[cfg(feature = "pool-modes")]
type PoolsData = Pools;
#[cfg(not(feature = "pool-modes"))]
type PoolsData = Never;
#[cfg(feature = "query-cache")]
type CacheData = Cache;
#[cfg(not(feature = "query-cache"))]
type CacheData = Never;
#[cfg(feature = "query-cache")]
type ToolCacheData = ToolCache;
#[cfg(not(feature = "query-cache"))]
type ToolCacheData = Never;
#[cfg(feature = "rate-limiting")]
type RateLimitData = RateLimit;
#[cfg(not(feature = "rate-limiting"))]
type RateLimitData = Never;

/// The `GET /api/stats` document.
#[derive(Serialize)]
pub(super) struct ProxyStats {
    schema_version: u32,
    proxy_version: &'static str,
    generated_at: String,
    uptime_secs: u64,
    nodes: Section<Nodes>,
    pools: Section<PoolsData>,
    sessions: Section<Sessions>,
    traffic: Section<Traffic>,
    cache: Section<CacheData>,
    tenants: Section<Tenants>,
    rate_limit: Section<RateLimitData>,
    transaction_replay: Section<TransactionReplay>,
    gateways: Section<Gateways>,
}

/// The document's top-level sections, in order.
#[cfg(test)]
const SECTIONS: [&str; 9] = [
    "nodes",
    "pools",
    "sessions",
    "traffic",
    "cache",
    "tenants",
    "rate_limit",
    "transaction_replay",
    "gateways",
];

/// Build the document from `state`. Configuration values come from the
/// configuration the proxy started with (the defaults when none is
/// attached, as in library use).
pub(super) async fn build(state: &Arc<AdminState>) -> ProxyStats {
    let config = state.proxy_config.read().await.clone().unwrap_or_default();
    let max_items = config.admin.stats_max_items.max(1);
    let metrics = state.metrics.read().await.clone();
    ProxyStats {
        schema_version: SCHEMA_VERSION,
        proxy_version: crate::VERSION,
        generated_at: rfc3339(chrono::Utc::now()),
        uptime_secs: state.started_at.elapsed().as_secs(),
        nodes: Section::on(nodes(state, &config, &metrics, max_items).await),
        pools: pools(state, &config).await,
        sessions: Section::on(sessions(state, &metrics).await),
        traffic: Section::on(Traffic::from(&metrics)),
        cache: cache(state, &metrics).await,
        tenants: tenants(state, &config, max_items).await,
        rate_limit: rate_limit(&config, &metrics),
        transaction_replay: transaction_replay(state, &config, &metrics).await,
        gateways: Section::on(gateways(state, &config).await),
    }
}

/// RFC 3339, UTC, millisecond precision (`2026-11-06T10:15:02.123Z`).
fn rfc3339(t: chrono::DateTime<chrono::Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Cut the sorted `items` to `max`; the full length and whether it was cut.
fn cap<T>(items: &mut Vec<T>, max: usize) -> (usize, bool) {
    let count = items.len();
    items.truncate(max);
    (count, count > max)
}

// ---------------------------------------------------------------------------
// nodes
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Nodes {
    /// The current write target, as `/topology` `currentPrimary`.
    write_leader: Option<String>,
    load_balancer: crate::config::Strategy,
    failovers_total: u64,
    reconnect_attempts_total: u64,
    backend_capacity_waits_total: u64,
    backend_capacity_refusals_total: u64,
    /// `[limits] max_concurrent_reconnects_per_node`; `None` when it is 0.
    reconnects: Option<Reconnects>,
    count: usize,
    healthy_count: usize,
    truncated: bool,
    items: Vec<NodeStats>,
}

#[derive(Serialize)]
struct NodeStats {
    address: String,
    /// The configured role; `None` for a node the proxy learned later.
    role: Option<crate::config::NodeRole>,
    enabled: Option<bool>,
    weight: Option<u32>,
    write_leader: bool,
    healthy: Option<bool>,
    latency_ms: Option<f64>,
    failure_count: Option<u32>,
    last_check: Option<String>,
    last_error: Option<String>,
    lag_bytes: Option<u64>,
    lag_sampled_at: Option<String>,
    #[serde(rename = "override")]
    override_: Option<crate::node_overrides::OverrideKind>,
    circuit: Option<&'static str>,
    backend_connections: Option<BackendConnections>,
}

#[derive(Serialize)]
struct BackendConnections {
    limit: usize,
    in_use: usize,
    waits_total: u64,
    refusals_total: u64,
}

#[derive(Serialize)]
struct Reconnects {
    limit_per_node: usize,
    in_progress: usize,
    peak_per_node: usize,
    waits_total: u64,
    refusals_total: u64,
}

async fn nodes(
    state: &Arc<AdminState>,
    config: &ProxyConfig,
    metrics: &ServerMetricsSnapshot,
    max_items: usize,
) -> Nodes {
    let write_leader = AdminServer::compute_topology(state).await.current_primary;
    let overrides = AdminServer::node_overrides(state).await;
    let circuits = circuit_states(state).await;
    let health = state.node_health.read().await.clone();
    let slots = state.node_slots.read().await.clone();
    let mut capped: HashMap<String, BackendConnections> = HashMap::new();
    let mut reconnects = None;
    if let Some(slots) = slots.as_ref() {
        for s in slots.stats() {
            capped.insert(
                s.node,
                BackendConnections {
                    limit: s.limit,
                    in_use: s.in_use,
                    waits_total: s.waits,
                    refusals_total: s.refusals,
                },
            );
        }
        reconnects = slots.reconnect_stats().map(|r| Reconnects {
            limit_per_node: r.limit_per_node,
            in_progress: r.in_progress,
            peak_per_node: r.peak_per_node,
            waits_total: r.waits,
            refusals_total: r.refusals,
        });
    }
    let configured: HashMap<&str, &crate::config::NodeConfig> =
        config.nodes.iter().map(|n| (n.address(), n)).collect();
    let addresses: BTreeSet<&str> = configured
        .keys()
        .copied()
        .chain(health.keys().map(String::as_str))
        .collect();
    let mut items: Vec<NodeStats> = addresses
        .into_iter()
        .map(|address| {
            let node = configured.get(address);
            let h = health.get(address);
            NodeStats {
                address: address.to_string(),
                role: node.map(|n| n.role),
                enabled: node.map(|n| n.enabled),
                weight: node.map(|n| n.weight),
                write_leader: write_leader.as_deref() == Some(address),
                healthy: h.map(|h| h.healthy),
                latency_ms: h.map(|h| h.latency_ms),
                failure_count: h.map(|h| h.failure_count),
                last_check: h.map(|h| rfc3339(h.last_check)),
                last_error: h.and_then(|h| h.last_error.clone()),
                lag_bytes: h.and_then(|h| h.replication_lag_bytes),
                lag_sampled_at: h.and_then(|h| h.lag_sampled_at.map(rfc3339)),
                override_: overrides.get(address).map(|o| o.kind),
                // A node without a breaker yet has had no request: closed.
                circuit: circuits
                    .as_ref()
                    .map(|c| c.get(address).copied().unwrap_or("closed")),
                backend_connections: capped.remove(address),
            }
        })
        .collect();
    let healthy_count = items.iter().filter(|n| n.healthy == Some(true)).count();
    let (count, truncated) = cap(&mut items, max_items);
    Nodes {
        write_leader,
        load_balancer: config.load_balancer.read_strategy,
        failovers_total: metrics.failovers,
        reconnect_attempts_total: metrics.reconnect_attempts,
        backend_capacity_waits_total: metrics.backend_capacity_waits,
        backend_capacity_refusals_total: metrics.backend_capacity_refusals,
        reconnects,
        count,
        healthy_count,
        truncated,
        items,
    }
}

/// Each node's circuit state (`closed` / `open` / `half_open`) from the
/// attached breaker manager, without creating a breaker; `None` when no
/// manager is attached.
#[cfg(feature = "circuit-breaker")]
async fn circuit_states(state: &Arc<AdminState>) -> Option<HashMap<String, &'static str>> {
    use crate::circuit_breaker::CircuitState;
    let manager = state.circuit_breaker.read().await.clone()?;
    Some(
        manager
            .get_all_states()
            .into_iter()
            .map(|(node, s)| {
                let name = match s {
                    CircuitState::Closed => "closed",
                    CircuitState::Open => "open",
                    CircuitState::HalfOpen => "half_open",
                };
                (node, name)
            })
            .collect(),
    )
}

#[cfg(not(feature = "circuit-breaker"))]
async fn circuit_states(_state: &Arc<AdminState>) -> Option<HashMap<String, &'static str>> {
    None
}

// ---------------------------------------------------------------------------
// pools
// ---------------------------------------------------------------------------

#[cfg(feature = "pool-modes")]
#[derive(Serialize)]
struct Pools {
    mode: crate::config::PoolingMode,
    max_pool_size: u32,
    min_idle: u32,
    max_total_idle: usize,
    idle_timeout_secs: u64,
    acquire_timeout_secs: u64,
    /// The data-path idle pool (transaction / statement mode); `None` in
    /// session mode.
    idle_pool: Option<IdlePool>,
}

#[cfg(feature = "pool-modes")]
#[derive(Serialize)]
struct IdlePool {
    idle: usize,
    reuses_total: u64,
    parked_total: u64,
    over_capacity_total: u64,
    stale_evicted_total: u64,
    reaped_total: u64,
    resets_skipped_total: u64,
    capacity_evictions_total: u64,
}

#[cfg(feature = "pool-modes")]
async fn pools(state: &Arc<AdminState>, config: &ProxyConfig) -> Section<PoolsData> {
    let idle_pool = state.backend_pool.read().await.as_ref().map(|p| IdlePool {
        idle: p.idle_count(),
        reuses_total: p.reuses(),
        parked_total: p.parked(),
        over_capacity_total: p.over_capacity(),
        stale_evicted_total: p.stale_evicted(),
        reaped_total: p.reaped(),
        resets_skipped_total: p.resets_skipped(),
        capacity_evictions_total: p.capacity_evictions(),
    });
    let pm = &config.pool_mode;
    Section::on(Pools {
        mode: pm.mode,
        max_pool_size: pm.max_pool_size,
        min_idle: pm.min_idle,
        max_total_idle: config.limits.max_total_idle_backend_conns,
        idle_timeout_secs: pm.idle_timeout_secs,
        acquire_timeout_secs: pm.acquire_timeout_secs,
        idle_pool,
    })
}

#[cfg(not(feature = "pool-modes"))]
async fn pools(_state: &Arc<AdminState>, _config: &ProxyConfig) -> Section<PoolsData> {
    Section::off("pool-modes feature not compiled in")
}

// ---------------------------------------------------------------------------
// sessions, traffic
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Sessions {
    active: u64,
    connections_open: u64,
    waiting_admission: u64,
    accepted_total: u64,
    closed_total: u64,
    rejected_total: u64,
    admission_waited_total: u64,
    admission_timeouts_total: u64,
    startup_waits_total: u64,
    startup_wait_timeouts_total: u64,
}

async fn sessions(state: &Arc<AdminState>, m: &ServerMetricsSnapshot) -> Sessions {
    Sessions {
        active: *state.active_sessions.read().await,
        connections_open: m.connections_accepted.saturating_sub(m.connections_closed),
        waiting_admission: m.admission_queue_depth,
        accepted_total: m.connections_accepted,
        closed_total: m.connections_closed,
        rejected_total: m.connections_rejected,
        admission_waited_total: m.admission_waited,
        admission_timeouts_total: m.admission_timeouts,
        startup_waits_total: m.startup_waits,
        startup_wait_timeouts_total: m.startup_wait_timeouts,
    }
}

#[derive(Serialize)]
struct Traffic {
    queries_total: u64,
    bytes_received_total: u64,
    bytes_sent_total: u64,
    reads_offloaded_total: u64,
    read_mirror_failures_total: u64,
}

impl From<&ServerMetricsSnapshot> for Traffic {
    fn from(m: &ServerMetricsSnapshot) -> Self {
        Self {
            queries_total: m.queries_processed,
            bytes_received_total: m.bytes_received,
            bytes_sent_total: m.bytes_sent,
            reads_offloaded_total: m.reads_offloaded,
            read_mirror_failures_total: m.read_mirror_failures,
        }
    }
}

// ---------------------------------------------------------------------------
// cache
// ---------------------------------------------------------------------------

#[cfg(feature = "query-cache")]
#[derive(Serialize)]
struct Cache {
    tiers: CacheTiers,
    misses_total: u64,
    skips_total: u64,
    puts_total: u64,
    invalidations_total: u64,
    tables_invalidated_total: u64,
    clears_total: u64,
    stale_rejected_total: u64,
    fills_raced_total: u64,
    unseen_write_invalidations_total: u64,
    refresh_hint_total: u64,
    compression_saved_bytes: u64,
    refresh_ahead: RefreshAhead,
    not_cached: NotCached,
    heatmap: Section<HeatmapSummary>,
}

#[cfg(feature = "query-cache")]
#[derive(Serialize)]
struct CacheTiers {
    l1: CacheTier,
    l2: CacheTier,
    l3: CacheTier,
}

#[cfg(feature = "query-cache")]
#[derive(Serialize)]
struct CacheTier {
    entries: u64,
    bytes: u64,
    hits_total: u64,
    evictions_total: u64,
}

#[cfg(feature = "query-cache")]
#[derive(Serialize)]
struct RefreshAhead {
    claims_total: u64,
    completed_total: u64,
    abandoned_total: u64,
}

/// Cacheable reads no result cache tier stored (oversize) or looked up.
#[cfg(feature = "query-cache")]
#[derive(Serialize)]
struct NotCached {
    oversize_total: u64,
    excluded_total: u64,
    catalog_total: u64,
    time_travel_total: u64,
    ambiguous_text_total: u64,
}

#[cfg(feature = "query-cache")]
#[derive(Serialize)]
struct HeatmapSummary {
    window_secs: u64,
    bucket_secs: u64,
    max_tables: usize,
    tables_tracked: usize,
    tables_in_window: usize,
    recommendations: usize,
    untracked_misses: u64,
    invalidate_all: u64,
    overflow_total: u64,
}

#[cfg(feature = "query-cache")]
async fn cache(state: &Arc<AdminState>, m: &ServerMetricsSnapshot) -> Section<CacheData> {
    let Some(cache) = state.query_cache.read().await.clone() else {
        return Section::off("the query cache is off ([cache] enabled = false)");
    };
    let s = cache.stats();
    let entries = cache.entry_counts();
    let bytes = cache.retained_bytes();
    let refresh = cache.refresh_ahead_stats();
    let tier =
        |entries: u64, bytes: u64, level: &crate::cache::CacheStatsLevelSnapshot| CacheTier {
            entries,
            bytes,
            hits_total: level.hits,
            evictions_total: level.evictions,
        };
    let heatmap = match cache.heatmap_snapshot() {
        Some(h) => Section::on(HeatmapSummary {
            window_secs: h.window_secs,
            bucket_secs: h.bucket_secs,
            max_tables: h.max_tables,
            tables_tracked: h.tables_tracked,
            tables_in_window: h.tables.len(),
            recommendations: h.recommendations.len(),
            untracked_misses: h.untracked_misses,
            invalidate_all: h.invalidate_all,
            overflow_total: h.overflow,
        }),
        None => Section::off("the cache heatmap is off ([cache] heatmap_enabled = false)"),
    };
    Section::on(Cache {
        tiers: CacheTiers {
            l1: tier(entries.l1, bytes.l1, &s.l1),
            l2: tier(entries.l2, bytes.l2, &s.l2),
            l3: tier(entries.l3, bytes.l3, &s.l3),
        },
        misses_total: s.total_misses,
        skips_total: s.total_skips,
        puts_total: s.puts,
        invalidations_total: s.invalidations,
        tables_invalidated_total: s.tables_invalidated,
        clears_total: s.clears,
        stale_rejected_total: cache.stale_rejected(),
        fills_raced_total: cache.fills_raced(),
        unseen_write_invalidations_total: cache.unseen_write_invalidations(),
        refresh_hint_total: cache.refresh_hints(),
        compression_saved_bytes: cache.compression_saved_bytes(),
        refresh_ahead: RefreshAhead {
            claims_total: refresh.claims,
            completed_total: refresh.completed,
            abandoned_total: refresh.abandoned,
        },
        not_cached: NotCached {
            oversize_total: m.cache_capture_oversize,
            excluded_total: m.cache_excluded,
            catalog_total: m.cache_catalog_skipped,
            time_travel_total: m.cache_time_travel_skipped,
            ambiguous_text_total: m.cache_ambiguous_skipped,
        },
        heatmap,
    })
}

#[cfg(not(feature = "query-cache"))]
async fn cache(_state: &Arc<AdminState>, _m: &ServerMetricsSnapshot) -> Section<CacheData> {
    Section::off("query-cache feature not compiled in")
}

// ---------------------------------------------------------------------------
// tenants
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Tenants {
    count: usize,
    truncated: bool,
    items: Vec<TenantStats>,
}

#[derive(Serialize)]
struct TenantStats {
    tenant: String,
    /// The client cap; `None` for a tenant without one.
    sessions: Option<TenantSessions>,
    /// The active-query cap; `None` for a tenant without one.
    queries: Option<TenantQueries>,
    /// The replay-byte budget; `None` for a tenant without one.
    replay: Option<TenantReplay>,
}

#[derive(Serialize)]
struct TenantSessions {
    limit: usize,
    active: usize,
    waiting: usize,
    waits_total: u64,
    refusals_total: u64,
}

#[derive(Serialize)]
struct TenantQueries {
    limit: usize,
    active: usize,
    idle: usize,
    waiting: usize,
    admitted_total: u64,
    waits_total: u64,
    refusals_total: u64,
    wait_cancels_total: u64,
    idle_permit_timeouts_total: u64,
}

#[derive(Serialize)]
struct TenantReplay {
    byte_limit: usize,
    bytes: usize,
    refusals_total: u64,
}

async fn tenants(
    state: &Arc<AdminState>,
    config: &ProxyConfig,
    max_items: usize,
) -> Section<Tenants> {
    if !config.multi_tenancy.enabled {
        return Section::off("[multi_tenancy] enabled = false");
    }
    let mut by_tenant: BTreeMap<String, TenantStats> = BTreeMap::new();
    if let Some(slots) = state.tenant_slots.read().await.clone() {
        for s in slots.stats() {
            tenant_entry(&mut by_tenant, &s.tenant).sessions = Some(TenantSessions {
                limit: s.limit,
                active: s.in_use,
                waiting: s.waiting,
                waits_total: s.waits,
                refusals_total: s.refusals,
            });
        }
        for b in slots.budget_stats() {
            let t = tenant_entry(&mut by_tenant, &b.tenant);
            if b.query_limit > 0 {
                t.queries = Some(TenantQueries {
                    limit: b.query_limit,
                    active: b.queries_active,
                    idle: b.queries_idle,
                    waiting: b.queries_waiting,
                    admitted_total: b.queries_admitted,
                    waits_total: b.query_waits,
                    refusals_total: b.query_refusals,
                    wait_cancels_total: b.query_wait_cancels,
                    idle_permit_timeouts_total: b.idle_permit_timeouts,
                });
            }
            if b.replay_byte_limit > 0 {
                t.replay = Some(TenantReplay {
                    byte_limit: b.replay_byte_limit,
                    bytes: b.replay_bytes,
                    refusals_total: b.replay_refusals,
                });
            }
        }
    }
    let mut items: Vec<TenantStats> = by_tenant.into_values().collect();
    let (count, truncated) = cap(&mut items, max_items);
    Section::on(Tenants {
        count,
        truncated,
        items,
    })
}

/// The entry of `tenant`, created empty.
fn tenant_entry<'m>(
    map: &'m mut BTreeMap<String, TenantStats>,
    tenant: &str,
) -> &'m mut TenantStats {
    map.entry(tenant.to_string())
        .or_insert_with(|| TenantStats {
            tenant: tenant.to_string(),
            sessions: None,
            queries: None,
            replay: None,
        })
}

// ---------------------------------------------------------------------------
// rate_limit, transaction_replay
// ---------------------------------------------------------------------------

#[cfg(feature = "rate-limiting")]
#[derive(Serialize)]
struct RateLimit {
    key_by: crate::config::RateLimitKeyBy,
    max_concurrent: u32,
    concurrency_refused_total: u64,
}

#[cfg(feature = "rate-limiting")]
fn rate_limit(config: &ProxyConfig, m: &ServerMetricsSnapshot) -> Section<RateLimitData> {
    let rl = &config.rate_limit;
    if !rl.enabled {
        return Section::off("[rate_limit] enabled = false");
    }
    Section::on(RateLimit {
        key_by: rl.key_by,
        max_concurrent: rl.max_concurrent,
        concurrency_refused_total: m.rate_limit_concurrency_refused,
    })
}

#[cfg(not(feature = "rate-limiting"))]
fn rate_limit(_config: &ProxyConfig, _m: &ServerMetricsSnapshot) -> Section<RateLimitData> {
    Section::off("rate-limiting feature not compiled in")
}

#[derive(Serialize)]
struct TransactionReplay {
    mode: crate::config::TrMode,
    failovers_total: u64,
    statements_reexecuted_total: u64,
    transactions_replayed_total: u64,
    replay_failures_total: u64,
    unknown_outcome_errors_total: u64,
    replay_cap_exceeded_total: u64,
    session_set_cap_exceeded_total: u64,
    session_set_untracked_total: u64,
    /// By reason; `None` when the latency histograms are not attached.
    refusals_total: Option<BTreeMap<&'static str, u64>>,
    journal: Journal,
}

#[derive(Serialize)]
struct Journal {
    committed_total: u64,
    rolled_back_total: u64,
    statements_total: u64,
}

async fn transaction_replay(
    state: &Arc<AdminState>,
    config: &ProxyConfig,
    m: &ServerMetricsSnapshot,
) -> Section<TransactionReplay> {
    if !config.tr_enabled {
        return Section::off("tr_enabled = false");
    }
    let refusals_total = state.latency.read().await.as_ref().map(|l| {
        crate::metrics_hist::ReplayRefusal::ALL
            .iter()
            .map(|r| (r.label(), l.refusals(*r)))
            .collect()
    });
    let tr = &m.tr;
    Section::on(TransactionReplay {
        mode: config.effective_tr_mode(),
        failovers_total: tr.failovers,
        statements_reexecuted_total: tr.statements_reexecuted,
        transactions_replayed_total: tr.transactions_replayed,
        replay_failures_total: tr.replay_failures,
        unknown_outcome_errors_total: tr.unknown_outcome_errors,
        replay_cap_exceeded_total: tr.replay_cap_exceeded,
        session_set_cap_exceeded_total: tr.session_set_cap_exceeded,
        session_set_untracked_total: tr.session_set_untracked,
        refusals_total,
        journal: Journal {
            committed_total: m.journal_committed,
            rolled_back_total: m.journal_rolled_back,
            statements_total: m.journal_statements,
        },
    })
}

// ---------------------------------------------------------------------------
// gateways
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Gateways {
    http: Section<Gateway>,
    graphql: Section<Gateway>,
    mcp: Section<McpGateway>,
}

#[derive(Serialize)]
struct Gateway {
    /// The gateway's backend pool; `None` when not attached.
    pool: Option<GatewayPool>,
    results_refused_total: u64,
    /// `None` without the query-cache feature.
    write_invalidations_total: Option<u64>,
}

#[derive(Serialize)]
struct McpGateway {
    pool: Option<GatewayPool>,
    results_truncated_total: u64,
    write_invalidations_total: Option<u64>,
    tool_cache: Section<ToolCacheData>,
}

#[derive(Serialize)]
struct GatewayPool {
    idle: usize,
    reuses_total: u64,
    misses_total: u64,
    discards_total: u64,
}

impl From<&crate::gateway_pool::BackendClientPool> for GatewayPool {
    fn from(p: &crate::gateway_pool::BackendClientPool) -> Self {
        Self {
            idle: p.idle_count(),
            reuses_total: p.reuse_count(),
            misses_total: p.miss_count(),
            discards_total: p.discard_count(),
        }
    }
}

#[cfg(feature = "query-cache")]
#[derive(Serialize)]
struct ToolCache {
    entries: u64,
    bytes: u64,
    owners: u64,
    hits_total: u64,
    misses_total: u64,
    stores_total: u64,
    stale_rejected_total: u64,
    expired_total: u64,
    fills_raced_total: u64,
    evictions_total: u64,
}

async fn gateways(state: &Arc<AdminState>, config: &ProxyConfig) -> Gateways {
    use crate::gateway_policy::{results_refused_total, GatewayKind, ResultGateway};
    let pools = state.gateway_pools.read().await.clone();
    let pools = pools.as_deref();
    let http = if config.http_gateway.enabled {
        Section::on(Gateway {
            pool: pools.map(|p| GatewayPool::from(&*p.http)),
            results_refused_total: results_refused_total(ResultGateway::Http),
            write_invalidations_total: write_invalidations(GatewayKind::Http),
        })
    } else {
        Section::off("[http_gateway] enabled = false")
    };
    let graphql = if !cfg!(feature = "graphql-gateway") {
        Section::off("graphql-gateway feature not compiled in")
    } else if config.graphql_gateway.enabled {
        Section::on(Gateway {
            pool: pools.map(|p| GatewayPool::from(&*p.graphql)),
            results_refused_total: results_refused_total(ResultGateway::Graphql),
            write_invalidations_total: write_invalidations(GatewayKind::Graphql),
        })
    } else {
        Section::off("[graphql_gateway] enabled = false")
    };
    let mcp = if config.mcp.enabled {
        Section::on(McpGateway {
            pool: pools.map(|p| GatewayPool::from(&*p.mcp)),
            results_truncated_total: crate::mcp::results_truncated_total(),
            write_invalidations_total: write_invalidations(GatewayKind::Mcp),
            tool_cache: tool_cache(state).await,
        })
    } else {
        Section::off("[mcp] enabled = false")
    };
    Gateways { http, graphql, mcp }
}

/// Statements through `gateway` that retired cached reads (C-02); `None`
/// without the query-cache feature.
fn write_invalidations(gateway: crate::gateway_policy::GatewayKind) -> Option<u64> {
    #[cfg(feature = "query-cache")]
    {
        Some(crate::cache::generations::gateway_write_invalidations(
            gateway,
        ))
    }
    #[cfg(not(feature = "query-cache"))]
    {
        let _ = gateway;
        None
    }
}

/// The MCP tool cache's totals, from the `stats_json` `/metrics` reports.
#[cfg(feature = "query-cache")]
async fn tool_cache(state: &Arc<AdminState>) -> Section<ToolCacheData> {
    use serde_json::Value;
    let Some(cache) = state.mcp_cache.read().await.clone() else {
        return Section::off("the MCP tool cache is off ([mcp] tool_cache_enabled = false)");
    };
    let v = cache.stats_json();
    let n = |key: &str| v[key].as_u64().unwrap_or(0);
    let sum = |key: &str| {
        v[key]
            .as_object()
            .map_or(0u64, |m| m.values().filter_map(Value::as_u64).sum())
    };
    Section::on(ToolCache {
        entries: n("entries"),
        bytes: n("retained_bytes"),
        owners: n("owners"),
        hits_total: sum("hits"),
        misses_total: sum("misses"),
        stores_total: n("stores"),
        stale_rejected_total: n("stale_rejected"),
        expired_total: n("expired"),
        fills_raced_total: n("fills_raced"),
        evictions_total: sum("evictions"),
    })
}

#[cfg(not(feature = "query-cache"))]
async fn tool_cache(_state: &Arc<AdminState>) -> Section<ToolCacheData> {
    Section::off("query-cache feature not compiled in")
}

#[cfg(test)]
mod tests;
