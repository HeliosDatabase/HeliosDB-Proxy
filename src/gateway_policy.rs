//! One policy contract for the non-PG-wire gateways (H-06 slice 2).
//!
//! The HTTP SQL, GraphQL and MCP gateways run SQL through their own pooled
//! backend clients. A [`GatewayPolicy`] gives them the PG-wire listener's
//! request policies, built from the same daemon objects:
//!
//! - `[rate_limit]`: one check per gateway request, keyed by the configured
//!   `key_by` (`client_ip` = the HTTP peer, `user` / `database` = the
//!   gateway's backend user / database, `global`); a throttle verdict sleeps
//!   (capped like PG-wire), a denial answers 429 / a JSON-RPC error;
//! - `[query_rewrite]`: the same rules rewrite every statement a gateway
//!   sends;
//! - `[circuit_breaker]`: a gateway fast-fails while its backend's circuit is
//!   open, and records connect outcomes on it.
//!
//! - `[multi_tenancy]`: a gateway configured with a `tenant_header` takes the
//!   tenant from that request header and applies the tenant's row filter to
//!   every statement, like the PG-wire listener does for a session; a request
//!   without a known tenant is refused (403). A gateway without a
//!   `tenant_header` has no per-request identity, and the daemon refuses that
//!   combination under `strict_config` (warns otherwise);
//! - `[multi_tenancy]` active-query caps: a request of a tenant at its
//!   `max_active_queries_per_tenant` cap waits up to `active_query_wait_ms`
//!   for the tenant's permit (the PG-wire path's permit, the same semaphore)
//!   and is refused (429) when it does not get one. Only the SQL-running part
//!   of a request holds the permit. `max_clients_per_tenant` does not apply to
//!   these stateless requests: it counts PG-wire sessions only;
//! - `[cache]` (C-02): every statement a gateway sends that may have written
//!   retires the cached reads of the tables it wrote, or every cached read
//!   when they are unknown, as a write through the PG-wire listener does
//!   ([`GatewayPolicy::note_statement`], Sprinter 6a0c6c34d27a).

use std::borrow::Cow;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

/// Longest sleep a throttle verdict imposes on one request, matching the
/// PG-wire listener (a misconfiguration must not pin a task indefinitely).
pub const RATE_LIMIT_MAX_BACKPRESSURE: Duration = Duration::from_secs(5);

/// Why a gateway request is refused before it reaches the backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDenied {
    /// `[rate_limit]` denied it.
    RateLimited {
        message: String,
        retry_after_ms: u64,
    },
    /// The backend's circuit is open.
    CircuitOpen { node: String },
    /// `[multi_tenancy]` is on and the request names no known tenant.
    Tenant { reason: String },
    /// `[multi_tenancy]` is on and the statement touches a tenant table in a
    /// shape its row filter cannot cover; it is not forwarded.
    TenantStatement { reason: String },
    /// The tenant is at its `max_active_queries_per_tenant` cap and the
    /// request got no permit within `active_query_wait_ms`.
    TenantQueryLimit { message: String },
}

impl PolicyDenied {
    /// HTTP status a gateway answers with.
    pub fn http_status(&self) -> u16 {
        match self {
            PolicyDenied::RateLimited { .. } => 429,
            PolicyDenied::CircuitOpen { .. } => 503,
            PolicyDenied::Tenant { .. } | PolicyDenied::TenantStatement { .. } => 403,
            PolicyDenied::TenantQueryLimit { .. } => 429,
        }
    }
}

impl std::fmt::Display for PolicyDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyDenied::RateLimited {
                message,
                retry_after_ms,
            } => write!(
                f,
                "rate limit exceeded: {message} (retry after {retry_after_ms}ms)"
            ),
            PolicyDenied::CircuitOpen { node } => {
                write!(
                    f,
                    "circuit open for node {node}: backend temporarily unavailable"
                )
            }
            PolicyDenied::Tenant { reason } => write!(f, "tenant required: {reason}"),
            PolicyDenied::TenantStatement { reason } => write!(f, "{reason}"),
            PolicyDenied::TenantQueryLimit { message } => write!(f, "{message}"),
        }
    }
}

/// Who a gateway request is, for the rate-limit key.
#[derive(Debug, Clone, Copy)]
pub struct RequestIdentity<'a> {
    pub peer: Option<IpAddr>,
    pub user: &'a str,
    pub database: Option<&'a str>,
}

/// Which gateway a statement came through (metric label).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum GatewayKind {
    /// The HTTP `/sql` gateway.
    Http,
    /// The MCP agent gateway.
    Mcp,
    /// The GraphQL gateway.
    Graphql,
}

impl GatewayKind {
    /// Number of gateways.
    pub const COUNT: usize = 3;
    /// Every gateway, in label order.
    pub const ALL: [GatewayKind; GatewayKind::COUNT] =
        [GatewayKind::Http, GatewayKind::Mcp, GatewayKind::Graphql];

    /// The metric label (`http`, `mcp`, `graphql`), as the gateway pool
    /// series use.
    pub fn as_str(self) -> &'static str {
        match self {
            GatewayKind::Http => "http",
            GatewayKind::Mcp => "mcp",
            GatewayKind::Graphql => "graphql",
        }
    }

    /// Position in [`Self::ALL`].
    pub fn index(self) -> usize {
        match self {
            GatewayKind::Http => 0,
            GatewayKind::Mcp => 1,
            GatewayKind::Graphql => 2,
        }
    }
}

/// The tenant a gateway request acts for (`[multi_tenancy]`, H-06).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTenant(pub String);

/// `ExecutionContext.metadata` key under which the GraphQL gateway hands the
/// request's tenant to the engine.
pub const TENANT_METADATA_KEY: &str = "helios.gateway_tenant";

/// The policies a gateway applies; empty (everything allowed, nothing
/// rewritten) unless the daemon attached its objects.
#[derive(Default, Clone)]
pub struct GatewayPolicy {
    /// The tenant manager and the request header naming the tenant.
    #[cfg(feature = "multi-tenancy")]
    tenancy: Option<(Arc<crate::multi_tenancy::TenantManager>, String)>,
    /// The daemon's tenant budgets (the PG-wire active-query permits) and the
    /// latency histograms their waits are recorded into.
    #[cfg(feature = "multi-tenancy")]
    tenant_slots: Option<Arc<crate::tenant_slots::TenantSlots>>,
    #[cfg(feature = "multi-tenancy")]
    tenant_waits: Option<Arc<crate::metrics_hist::ProxyLatency>>,
    #[cfg(feature = "rate-limiting")]
    rate_limiter: Option<(
        Arc<crate::rate_limit::RateLimiter>,
        crate::config::RateLimitKeyBy,
    )>,
    #[cfg(feature = "query-rewriting")]
    rewriter: Option<Arc<crate::rewriter::QueryRewriter>>,
    #[cfg(feature = "circuit-breaker")]
    circuit: Option<Arc<crate::circuit_breaker::CircuitBreakerManager>>,
    /// Where a statement that may have written retires cached reads (C-02),
    /// when a cache runs.
    #[cfg(feature = "query-cache")]
    cache_writes: Option<crate::cache::GatewayWrites>,
}

impl GatewayPolicy {
    #[cfg(feature = "rate-limiting")]
    pub fn with_rate_limiter(
        mut self,
        limiter: Arc<crate::rate_limit::RateLimiter>,
        key_by: crate::config::RateLimitKeyBy,
    ) -> Self {
        self.rate_limiter = Some((limiter, key_by));
        self
    }

    #[cfg(feature = "query-rewriting")]
    pub fn with_rewriter(mut self, rewriter: Arc<crate::rewriter::QueryRewriter>) -> Self {
        self.rewriter = Some(rewriter);
        self
    }

    #[cfg(feature = "circuit-breaker")]
    pub fn with_circuit_breaker(
        mut self,
        circuit: Arc<crate::circuit_breaker::CircuitBreakerManager>,
    ) -> Self {
        self.circuit = Some(circuit);
        self
    }

    /// Retire cached reads through `writes` after every statement that may
    /// have written ([`Self::note_statement`]).
    #[cfg(feature = "query-cache")]
    pub fn with_cache_writes(mut self, writes: crate::cache::GatewayWrites) -> Self {
        self.cache_writes = Some(writes);
        self
    }

    /// Note one statement `gateway` sent to the backend (the text as sent,
    /// after the rewrite and the tenant filter), once the backend answered,
    /// failed or timed out, whatever the outcome: a statement that timed out
    /// may still commit. When a cache runs, a statement that may have
    /// written retires the cached reads of the tables it wrote, or every
    /// cached read when they are unknown (C-02, Sprinter 6a0c6c34d27a); a
    /// read changes nothing. A no-op without a cache.
    pub async fn note_statement(&self, sql: &str, gateway: GatewayKind) {
        #[cfg(feature = "query-cache")]
        if let Some(writes) = &self.cache_writes {
            writes.note_statement(sql, gateway).await;
        }
        let _ = (sql, gateway);
    }

    /// Take the tenant of every request from header `header` and filter its
    /// statements for that tenant (see [`Self::request_tenant`]).
    #[cfg(feature = "multi-tenancy")]
    pub fn with_tenancy(
        mut self,
        manager: Arc<crate::multi_tenancy::TenantManager>,
        header: String,
    ) -> Self {
        self.tenancy = Some((manager, header));
        self
    }

    /// Share the daemon's tenant budgets, so a request of a tenant with an
    /// active-query cap takes that tenant's permit (see
    /// [`Self::admit_tenant_query`]). `waits` is the histogram the PG-wire
    /// path records its active-query waits into.
    #[cfg(feature = "multi-tenancy")]
    pub fn with_tenant_slots(
        mut self,
        slots: Arc<crate::tenant_slots::TenantSlots>,
        waits: Arc<crate::metrics_hist::ProxyLatency>,
    ) -> Self {
        self.tenant_slots = Some(slots);
        self.tenant_waits = Some(waits);
        self
    }

    /// The active-query permit a request of `tenant` holds while it runs SQL.
    ///
    /// `Ok(None)` when the tenant has no active-query cap (or the request acts
    /// for no tenant). Otherwise the request waits FIFO for the tenant's permit
    /// up to `active_query_wait_ms` (the same wait as the PG-wire path) and is
    /// refused with [`PolicyDenied::TenantQueryLimit`] when it gets none. The
    /// permit is an RAII guard: dropping it, on every path, returns it.
    pub async fn admit_tenant_query(
        &self,
        tenant: Option<&GatewayTenant>,
    ) -> Result<Option<tokio::sync::SemaphorePermit<'_>>, PolicyDenied> {
        #[cfg(feature = "multi-tenancy")]
        if let (Some(slots), Some(waits), Some(t)) =
            (&self.tenant_slots, &self.tenant_waits, tenant)
        {
            if let Some(budget) = slots.budget(&t.0).filter(|b| b.has_query_cap()) {
                return match budget.wait_query(&waits.tenant_query_wait).await {
                    Ok((permit, _waited)) => {
                        budget.note_query_admitted();
                        Ok(Some(permit))
                    }
                    Err(refusal) => Err(PolicyDenied::TenantQueryLimit {
                        message: refusal.to_string(),
                    }),
                };
            }
        }
        let _ = tenant;
        Ok(None)
    }

    /// The tenant a request acts for, read with `header` (a lookup by header
    /// name). `Ok(None)` when this gateway applies no tenancy. Refused when
    /// the header is missing or names no known tenant: a request the proxy
    /// cannot attribute must not run unfiltered.
    pub fn request_tenant<'h>(
        &self,
        header: impl FnOnce(&str) -> Option<&'h str>,
    ) -> Result<Option<GatewayTenant>, PolicyDenied> {
        #[cfg(feature = "multi-tenancy")]
        if let Some((manager, name)) = &self.tenancy {
            let Some(value) = header(name).map(str::trim).filter(|v| !v.is_empty()) else {
                return Err(PolicyDenied::Tenant {
                    reason: format!("missing {name} header"),
                });
            };
            if !manager.has_tenant(&crate::multi_tenancy::TenantId::new(value)) {
                return Err(PolicyDenied::Tenant {
                    reason: format!("unknown tenant in {name} header"),
                });
            }
            return Ok(Some(GatewayTenant(value.to_string())));
        }
        let _ = header;
        Ok(None)
    }

    /// The tenant's row filter on one statement (apply after
    /// [`Self::rewrite`], as the PG-wire listener does).
    ///
    /// `Err` when the statement touches a tenant table in a shape the filter
    /// cannot cover: the caller must not run it.
    pub fn tenant_sql<'a>(
        &self,
        sql: &'a str,
        tenant: Option<&GatewayTenant>,
    ) -> Result<Cow<'a, str>, PolicyDenied> {
        #[cfg(feature = "multi-tenancy")]
        if let (Some((manager, _)), Some(t)) = (&self.tenancy, tenant) {
            let res =
                manager.transform_query(sql, &crate::multi_tenancy::TenantId::new(t.0.as_str()));
            if let Some(reason) = res.refusal {
                return Err(PolicyDenied::TenantStatement { reason });
            }
            if res.transformed {
                tracing::debug!(target: "helios::tenant", tenant = %t.0, "gateway tenant filter injected");
                return Ok(Cow::Owned(res.query));
            }
        }
        let _ = tenant;
        Ok(Cow::Borrowed(sql))
    }

    /// The rate-limit check for one gateway request. A throttle verdict
    /// waits (capped at [`RATE_LIMIT_MAX_BACKPRESSURE`]) and then allows.
    pub async fn admit_request(&self, who: RequestIdentity<'_>) -> Result<(), PolicyDenied> {
        #[cfg(feature = "rate-limiting")]
        if let Some((limiter, key_by)) = &self.rate_limiter {
            use crate::config::RateLimitKeyBy;
            use crate::rate_limit::{LimiterKey, RateLimitResult};
            let key = match key_by {
                RateLimitKeyBy::Global => LimiterKey::Global,
                RateLimitKeyBy::ClientIp => match who.peer {
                    Some(ip) => LimiterKey::ClientIp(ip),
                    None => LimiterKey::Global,
                },
                RateLimitKeyBy::User => LimiterKey::User(who.user.to_string()),
                RateLimitKeyBy::Database => {
                    LimiterKey::Database(who.database.unwrap_or(who.user).to_string())
                }
            };
            match limiter.check(&key, 1) {
                RateLimitResult::Allowed => {}
                RateLimitResult::Warned(msg) => {
                    tracing::warn!(reason = %msg, "gateway rate limit warning")
                }
                RateLimitResult::Throttled(d) | RateLimitResult::Queued(d) => {
                    tokio::time::sleep(d.min(RATE_LIMIT_MAX_BACKPRESSURE)).await
                }
                RateLimitResult::Denied(exc) => {
                    return Err(PolicyDenied::RateLimited {
                        message: exc.message.clone(),
                        retry_after_ms: exc.retry_after.as_millis() as u64,
                    })
                }
            }
        }
        let _ = who;
        Ok(())
    }

    /// Fast-fail while `node`'s circuit is open.
    pub fn check_node(&self, node: &str) -> Result<(), PolicyDenied> {
        #[cfg(feature = "circuit-breaker")]
        if let Some(cb) = &self.circuit {
            if cb.get_breaker(node).get_state() == crate::circuit_breaker::CircuitState::Open {
                return Err(PolicyDenied::CircuitOpen {
                    node: node.to_string(),
                });
            }
        }
        let _ = node;
        Ok(())
    }

    /// Apply `[query_rewrite]` to a statement.
    pub fn rewrite<'a>(&self, sql: &'a str) -> Cow<'a, str> {
        #[cfg(feature = "query-rewriting")]
        if let Some(rw) = &self.rewriter {
            if let Ok(res) = rw.rewrite(sql) {
                if res.was_rewritten() {
                    tracing::debug!(target: "helios::rewrite", rules = ?res.rules_applied, "gateway query rewritten");
                    return Cow::Owned(res.query().to_string());
                }
            }
        }
        Cow::Borrowed(sql)
    }

    /// Record whether a gateway reached `node` (connect + query round trip).
    /// Only reachability counts: an SQL error is the backend answering.
    pub fn record(&self, node: &str, reached: bool, err: &str) {
        #[cfg(feature = "circuit-breaker")]
        if let Some(cb) = &self.circuit {
            let breaker = cb.get_breaker(node);
            if reached {
                breaker.record_success();
            } else {
                breaker.record_failure(err);
            }
        }
        let _ = (node, reached, err);
    }
}

impl std::fmt::Debug for GatewayPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("GatewayPolicy");
        #[cfg(feature = "multi-tenancy")]
        d.field(
            "tenant_header",
            &self.tenancy.as_ref().map(|(_, header)| header),
        );
        #[cfg(feature = "rate-limiting")]
        d.field(
            "rate_limiter",
            &self.rate_limiter.as_ref().map(|(_, key_by)| key_by),
        );
        #[cfg(feature = "query-rewriting")]
        d.field("rewriter", &self.rewriter.is_some());
        #[cfg(feature = "circuit-breaker")]
        d.field("circuit_breaker", &self.circuit.is_some());
        #[cfg(feature = "query-cache")]
        d.field("cache_writes", &self.cache_writes.is_some());
        d.finish()
    }
}

/// A policy shared by a gateway's tasks.
pub type SharedGatewayPolicy = Arc<GatewayPolicy>;

/// The gateways whose results `max_result_rows` / `max_result_bytes` refuse
/// (`heliosdb_proxy_gateway_results_refused_total{gateway=...}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultGateway {
    Http,
    Graphql,
}

impl ResultGateway {
    /// The `gateway` label value.
    pub fn label(self) -> &'static str {
        match self {
            ResultGateway::Http => "http",
            ResultGateway::Graphql => "graphql",
        }
    }
}

/// Requests refused because their result passed a result cap, per gateway
/// (index = `ResultGateway as usize`).
static RESULTS_REFUSED: [std::sync::atomic::AtomicU64; 2] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

/// Count one result refused by a result cap.
pub fn note_result_refused(gateway: ResultGateway) {
    RESULTS_REFUSED[gateway as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

/// Results refused by a result cap since the process started.
pub fn results_refused_total(gateway: ResultGateway) -> u64 {
    RESULTS_REFUSED[gateway as usize].load(std::sync::atomic::Ordering::Relaxed)
}

/// The refusal message for a result the `section` cap cut (`http_gateway` or
/// `graphql_gateway`): names the key that was passed, with its value.
pub fn result_cap_message(
    section: &str,
    truncation: crate::backend::client::Truncation,
    max_rows: usize,
    max_bytes: usize,
) -> String {
    if truncation.row_limit_hit {
        format!("result exceeds {section}.max_result_rows ({max_rows})")
    } else {
        format!("result exceeds {section}.max_result_bytes ({max_bytes})")
    }
}

/// A policy for the gateway tests: tenants `acme` and `globex` behind
/// `header`, each with an active-query cap of 1 that refuses at once (no
/// wait), and `free` with no cap.
#[cfg(all(test, feature = "multi-tenancy"))]
pub(crate) fn capped_tenancy_policy(header: &str) -> GatewayPolicy {
    use crate::multi_tenancy::{
        IsolationStrategy, MultiTenancyConfig, TenantConfig, TenantId, TenantManagerBuilder,
    };
    let mut config = crate::config::ProxyConfig::default();
    config.multi_tenancy.enabled = true;
    config.multi_tenancy.tenants = vec!["acme".into(), "globex".into(), "free".into()];
    config.multi_tenancy.max_active_queries_per_tenant = 1;
    config
        .multi_tenancy
        .tenant_max_active_queries
        .insert("free".into(), 0);
    config.multi_tenancy.active_query_wait_ms = 0;
    let tm = TenantManagerBuilder::new()
        .config(MultiTenancyConfig {
            enabled: true,
            ..Default::default()
        })
        .build();
    for t in ["acme", "globex", "free"] {
        tm.register_tenant(TenantConfig::new(
            TenantId::new(t),
            IsolationStrategy::row("public", "tid"),
        ));
    }
    GatewayPolicy::default()
        .with_tenancy(Arc::new(tm), header.to_string())
        .with_tenant_slots(
            Arc::new(crate::tenant_slots::TenantSlots::from_config(&config)),
            Arc::new(crate::metrics_hist::ProxyLatency::new(&[1.0, 10.0])),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_empty_policy_allows_and_rewrites_nothing() {
        let p = GatewayPolicy::default();
        let who = RequestIdentity {
            peer: None,
            user: "u",
            database: None,
        };
        assert_eq!(p.admit_request(who).await, Ok(()));
        assert_eq!(p.check_node("n:5432"), Ok(()));
        assert!(matches!(p.rewrite("SELECT 1"), Cow::Borrowed("SELECT 1")));
        p.record("n:5432", false, "x");
    }

    #[cfg(feature = "circuit-breaker")]
    #[test]
    fn an_open_circuit_fast_fails_the_gateway() {
        let cb = Arc::new(crate::circuit_breaker::CircuitBreakerManager::new(
            crate::circuit_breaker::ManagerConfig::new(
                crate::circuit_breaker::CircuitBreakerConfig {
                    failure_threshold: 3,
                    ..Default::default()
                },
            ),
        ));
        let p = GatewayPolicy::default().with_circuit_breaker(cb.clone());
        for _ in 0..100 {
            p.record("db:5432", false, "connection refused");
        }
        let denied = p.check_node("db:5432").unwrap_err();
        assert_eq!(denied.http_status(), 503);
        assert!(denied.to_string().contains("circuit open for node db:5432"));
        assert_eq!(p.check_node("other:5432"), Ok(()));
    }

    #[cfg(feature = "rate-limiting")]
    #[tokio::test]
    async fn the_rate_limiter_denies_the_gateway_like_pg_wire() {
        let limiter = Arc::new(crate::rate_limit::RateLimiter::new(
            crate::rate_limit::RateLimitConfig {
                enabled: true,
                default_qps: 1,
                default_burst: 1,
                ..Default::default()
            },
        ));
        let p = GatewayPolicy::default()
            .with_rate_limiter(limiter, crate::config::RateLimitKeyBy::User);
        let who = RequestIdentity {
            peer: None,
            user: "gw",
            database: None,
        };
        let mut denied = None;
        for _ in 0..20 {
            if let Err(e) = p.admit_request(who).await {
                denied = Some(e);
                break;
            }
        }
        let denied = denied.expect("a burst of 1 must deny within 20 requests");
        assert_eq!(denied.http_status(), 429);
    }

    #[test]
    fn without_tenancy_no_tenant_is_required_or_applied() {
        let p = GatewayPolicy::default();
        assert_eq!(p.request_tenant(|_| None), Ok(None));
        assert!(matches!(
            p.tenant_sql("SELECT * FROM t", Some(&GatewayTenant("acme".into()))),
            Ok(Cow::Borrowed(_))
        ));
    }

    #[cfg(feature = "multi-tenancy")]
    #[test]
    fn a_tenant_header_names_the_tenant_and_filters_its_statements() {
        use crate::multi_tenancy::{
            IsolationStrategy, MultiTenancyConfig, TenantConfig, TenantId, TenantManagerBuilder,
            TenantQueryTransformer,
        };
        let tm = TenantManagerBuilder::new()
            .config(MultiTenancyConfig {
                enabled: true,
                ..Default::default()
            })
            .query_transformer(TenantQueryTransformer::new().register_tables(&["orders"], "tid"))
            .build();
        tm.register_tenant(TenantConfig::new(
            TenantId::new("acme"),
            IsolationStrategy::row("public", "tid"),
        ));
        let p = GatewayPolicy::default().with_tenancy(Arc::new(tm), "x-helios-tenant".into());
        let missing = p.request_tenant(|_| None).unwrap_err();
        assert_eq!(missing.http_status(), 403);
        assert!(missing
            .to_string()
            .contains("missing x-helios-tenant header"));
        let unknown = p.request_tenant(|_| Some("globex")).unwrap_err();
        assert!(unknown.to_string().contains("unknown tenant"));
        let acme = p
            .request_tenant(|h| (h == "x-helios-tenant").then_some(" acme "))
            .unwrap()
            .expect("tenant");
        assert_eq!(acme, GatewayTenant("acme".into()));
        let filtered = p.tenant_sql("SELECT * FROM orders", Some(&acme)).unwrap();
        assert!(
            filtered.contains("tid") && filtered.contains("acme"),
            "{filtered}"
        );
        // Tables outside the tenant set pass through.
        assert_eq!(p.tenant_sql("SELECT 1", Some(&acme)).unwrap(), "SELECT 1");
        // A shape the filter cannot cover is refused, not forwarded.
        let refused = p
            .tenant_sql(
                "WITH o AS (SELECT * FROM orders) SELECT * FROM o",
                Some(&acme),
            )
            .unwrap_err();
        assert_eq!(refused.http_status(), 403);
        assert!(refused
            .to_string()
            .contains("refused under row-level tenant isolation"));
    }

    #[cfg(feature = "query-rewriting")]
    #[test]
    fn rewrite_rules_apply_to_gateway_sql() {
        use crate::rewriter::{
            QueryPattern, QueryRewriter, RewriteRule, RewriterConfig, Transformation,
        };
        let rw = QueryRewriter::new(RewriterConfig {
            enabled: true,
            ..Default::default()
        });
        rw.add_rule(
            RewriteRule::build("legacy")
                .pattern(QueryPattern::Table("old_t".into()))
                .transform(Transformation::ReplaceTable {
                    from: "old_t".into(),
                    to: "new_t".into(),
                })
                .build(),
        );
        let p = GatewayPolicy::default().with_rewriter(Arc::new(rw));
        assert_eq!(p.rewrite("SELECT * FROM old_t"), "SELECT * FROM new_t");
    }

    #[cfg(feature = "multi-tenancy")]
    #[tokio::test]
    async fn a_tenant_at_its_query_cap_is_refused_until_its_permit_returns() {
        let p = capped_tenancy_policy("x-helios-tenant");
        let acme = GatewayTenant("acme".into());
        let globex = GatewayTenant("globex".into());
        let free = GatewayTenant("free".into());
        // The permit an in-flight request holds.
        let held = p.admit_tenant_query(Some(&acme)).await.unwrap();
        assert!(held.is_some());
        let refused = p.admit_tenant_query(Some(&acme)).await.unwrap_err();
        assert_eq!(refused.http_status(), 429);
        assert!(
            refused
                .to_string()
                .contains("too many active queries for tenant \"acme\" (limit 1)"),
            "{refused}"
        );
        // Another tenant is unaffected, and so is an uncapped tenant.
        assert!(p.admit_tenant_query(Some(&globex)).await.unwrap().is_some());
        assert!(p.admit_tenant_query(Some(&free)).await.unwrap().is_none());
        // No tenant (no tenant_header on the gateway): no permit.
        assert!(p.admit_tenant_query(None).await.unwrap().is_none());
        // Returned on drop: the tenant's next request is admitted.
        drop(held);
        assert!(p.admit_tenant_query(Some(&acme)).await.unwrap().is_some());
    }

    #[cfg(feature = "multi-tenancy")]
    #[tokio::test]
    async fn a_policy_without_tenant_slots_admits_every_request() {
        let p = GatewayPolicy::default();
        let acme = GatewayTenant("acme".into());
        assert!(p.admit_tenant_query(Some(&acme)).await.unwrap().is_none());
    }
}
