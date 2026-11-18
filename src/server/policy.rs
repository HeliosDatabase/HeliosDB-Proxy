use super::*;

/// The refusal message for a statement naming a tenant table from a session
/// with no identified tenant (`[multi_tenancy] unknown_tenant = "refuse"`).
#[cfg(feature = "multi-tenancy")]
const UNIDENTIFIED_TENANT_REFUSAL: &str =
    "statement refused: this session has no identified tenant and the statement references a tenant table";

/// The outcome of [`ProxyServer::tenant_statement`].
#[cfg(feature = "multi-tenancy")]
pub(super) enum TenantStatement {
    /// Forwarded as it is.
    Unchanged,
    /// Forwarded with the tenant's row filter: this SQL.
    Filtered(String),
    /// Never forwarded: answered SQLSTATE 42501 with this reason.
    Refused(std::borrow::Cow<'static, str>),
}

/// The longest a rate-limit backpressure sleep lasts (a `Throttled` or
/// `Queued` verdict, or the pacing of a message that continues an open cycle),
/// so a misconfiguration can't pin a connection task indefinitely.
#[cfg(feature = "rate-limiting")]
const RATE_LIMIT_MAX_SLEEP: Duration = Duration::from_secs(5);

impl ProxyServer {
    /// Derive the rate-limit bucket key for a session per the configured
    /// keying dimension, memoizing it on the session.
    ///
    /// Every input is fixed for the life of a connection: `key_by` comes from
    /// the config snapshot this connection was accepted under, the client
    /// address never changes, and `user`/`database` are set once from the
    /// startup packet. So the key (and its rendered metrics string) is built
    /// once and every later query borrows it — no `LimiterKey` allocation, no
    /// `variables` read lock, and no `format!` on the per-query gate.
    ///
    /// The one case that is *not* memoized is a key derived from a startup
    /// parameter that is not present yet: that would freeze a placeholder for
    /// the rest of the session, so it is recomputed (exactly as before) until
    /// the parameter appears.
    #[cfg(feature = "rate-limiting")]
    pub(super) async fn rate_limit_key<'a>(
        session: &'a Arc<ClientSession>,
        config: &ProxyConfig,
    ) -> std::borrow::Cow<'a, crate::rate_limit::CachedLimiterKey> {
        use crate::config::RateLimitKeyBy;
        use crate::rate_limit::{CachedLimiterKey, LimiterKey};
        use std::borrow::Cow;

        if let Some(cached) = session.rate_limit_key.get() {
            return Cow::Borrowed(cached);
        }

        // `stable` is false only when the key had to fall back to a default
        // because the startup parameter it keys on is not populated yet.
        let (key, stable) = match config.rate_limit.key_by {
            RateLimitKeyBy::Global => (LimiterKey::Global, true),
            RateLimitKeyBy::ClientIp => (LimiterKey::ClientIp(session.client_addr.ip()), true),
            RateLimitKeyBy::Database => {
                let vars = session.variables.read().await;
                match vars.get("database") {
                    Some(db) => (LimiterKey::Database(db.clone()), true),
                    None => (LimiterKey::Database(String::new()), false),
                }
            }
            RateLimitKeyBy::User => {
                let vars = session.variables.read().await;
                match vars.get("user") {
                    Some(user) => (LimiterKey::User(user.clone()), true),
                    None => (LimiterKey::User(String::new()), false),
                }
            }
        };

        let resolved = CachedLimiterKey::new(key);
        if !stable {
            return Cow::Owned(resolved);
        }

        // A concurrent racer may win the init; either way the stored value is
        // the same key, so borrow whatever landed.
        Cow::Borrowed(session.rate_limit_key.get_or_init(|| resolved))
    }

    /// Check rate limits before a query is forwarded. Returns `Some(bytes)` —
    /// a PG `ErrorResponse` WITHOUT a trailing `ReadyForQuery` (the caller
    /// appends one as the protocol requires) — when the query is denied; `None`
    /// when it may proceed. A throttle/queue verdict is honored by sleeping for
    /// the engine-supplied delay (real backpressure, capped) and then allowing.
    ///
    /// This is the request boundary, so it also takes the request's in-flight
    /// slot of its bucket when `[rate_limit] max_concurrent` is set (see
    /// [`ClientSession::rate_permit`]). `exempt` names the statements that run
    /// past the cap (transaction ends); it is only evaluated when the cap is
    /// enforced. A request over the cap is refused before its QPS token is
    /// spent, with the same 53400 refusal as the QPS denial.
    #[cfg(feature = "rate-limiting")]
    pub(super) async fn rate_limit_check(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        exempt: impl FnOnce() -> bool,
    ) -> Option<Vec<u8>> {
        use crate::rate_limit::RateLimitResult;
        let limiter = state.rate_limiter.as_ref()?;
        let key = Self::rate_limit_key(session, config).await;
        let key = key.as_ref();
        let mut took_slot = false;
        if limiter.in_flight_enforced() && !session.holds_rate_permit() && !exempt() {
            match limiter.try_acquire_in_flight(key) {
                Ok(permit) => {
                    took_slot = permit.holds_slot();
                    *session.rate_permit.lock() = Some(permit);
                }
                Err(refusal) => {
                    state
                        .metrics
                        .rate_limit_concurrency_refused
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::info!(key = %key, limit = refusal.limit, "rate limit concurrency exceeded");
                    return Some(Self::create_error_response(
                        "53400",
                        &format!(
                            "rate limit exceeded: too many concurrent queries for {} (max_concurrent {}); retry when a query completes",
                            key, refusal.limit
                        ),
                    ));
                }
            }
        }
        match limiter.check_cached(key, 1) {
            RateLimitResult::Allowed => None,
            RateLimitResult::Warned(msg) => {
                tracing::warn!(key = %key, reason = %msg, "rate limit warning");
                None
            }
            RateLimitResult::Throttled(d) | RateLimitResult::Queued(d) => {
                tokio::time::sleep(d.min(RATE_LIMIT_MAX_SLEEP)).await;
                None
            }
            RateLimitResult::Denied(exc) => {
                if took_slot {
                    session.release_rate_permit();
                }
                tracing::info!(key = %key, "rate limit exceeded");
                let msg = format!(
                    "rate limit exceeded: {} (retry after {}ms)",
                    exc.message,
                    exc.retry_after.as_millis()
                );
                Some(Self::create_error_response("53400", &msg))
            }
        }
    }

    /// Charge a message that continues a cycle a backend connection holds
    /// open and runs statements of its own (`OpenCycle::runs`: an `Execute`
    /// after a `Flush`, a `Query`). It belongs to a request admitted already,
    /// so it is never refused: an error there would leave the cycle open on
    /// the backend, to be committed by the next `Sync` behind the client's
    /// back. It still takes its token like any request: with the bucket empty
    /// it waits until one refills (the limiter's `retry_after`, at most
    /// `RATE_LIMIT_MAX_SLEEP`), takes it unless another request of the bucket
    /// took it first, and runs. A client that pipelines `Execute`s behind
    /// `Flush`es and never sends a `Sync` runs at the bucket's rate.
    #[cfg(feature = "rate-limiting")]
    pub(super) async fn rate_limit_pace(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) {
        use crate::rate_limit::RateLimitResult;
        let Some(limiter) = state.rate_limiter.as_ref() else {
            return;
        };
        let key = Self::rate_limit_key(session, config).await;
        let key = key.as_ref();
        match limiter.check_cached(key, 1) {
            RateLimitResult::Allowed => {}
            RateLimitResult::Warned(msg) => {
                tracing::warn!(key = %key, reason = %msg, "rate limit warning");
            }
            RateLimitResult::Throttled(d) | RateLimitResult::Queued(d) => {
                tokio::time::sleep(d.min(RATE_LIMIT_MAX_SLEEP)).await;
            }
            RateLimitResult::Denied(exc) => {
                tracing::debug!(
                    key = %key,
                    "rate limit exceeded by a message continuing an open cycle; pacing it"
                );
                tokio::time::sleep(exc.retry_after.min(RATE_LIMIT_MAX_SLEEP)).await;
                let _ = limiter.check_cached(key, 1);
            }
        }
    }

    /// In-band failure feedback. When a query fails against a backend, demote
    /// that node's health *immediately* — a copy-on-write update of the shared
    /// health snapshot, the same structure the periodic health checker
    /// maintains — so routing stops sending work to a dead node within one
    /// query instead of waiting up to a full health-check interval (the
    /// ~`check_interval` blind window). The periodic checker restores the node
    /// on its next successful probe, so this only ever *accelerates* detection.
    ///
    /// True when `err` is evidence the backend itself is unhealthy — and so
    /// should demote it in-band (and trip its circuit breaker) — as opposed to a
    /// client-side problem or a merely slow but healthy query.
    ///
    /// Excluded (return `false`, no penalty):
    /// * `Client …` — a failed or timed-out client write is the client's fault.
    /// * `Backend read timeout` — a backend that emits no bytes within the
    ///   streaming read window is indistinguishable from a legitimately slow but
    ///   healthy query (large sort/aggregate, lock wait, bulk DML). Demoting the
    ///   whole node — cluster-wide, for every session, bypassing the configured
    ///   `failure_threshold` — over one slow query is a false positive; a
    ///   genuinely unresponsive-but-connected backend is still caught by the
    ///   periodic protocol-level health probe.
    ///
    /// Still faults (return `true`): a backend read/write *error* (reset, EOF,
    /// broken pipe), a backend *write* timeout (the backend is not draining its
    /// socket), and any connect-time failure.
    pub(super) fn is_backend_fault(err: &str) -> bool {
        !err.contains("Client")
            && !err.contains("Backend read timeout")
            // H-05: this proxy's own per-node cap refused a connection; the
            // node itself is fine.
            && !err.contains(crate::node_slots::REFUSAL_MARKER)
    }

    /// Errors that do not demote a backend are filtered via `is_backend_fault`:
    /// a client disconnecting mid-query, or one merely-slow query, must never
    /// take a healthy backend out of rotation for every session.
    pub(super) fn note_backend_failure(state: &Arc<ServerState>, addr: &str, err: &str) {
        if !Self::is_backend_fault(err) {
            return;
        }
        // Serialize the read-modify-write of the shared health snapshot. ArcSwap
        // makes only the final pointer swap atomic; without this lock two
        // concurrent writers — in-band demotions for different nodes, or an
        // in-band demotion racing the periodic checker's full-map rebuild — can
        // each load the same snapshot and clobber the other's update (a lost
        // update that resurrects a demoted node, or evicts a recovered one,
        // until the next probe). The lock serializes writers only; every routing
        // read stays lock-free on the ArcSwap.
        let _writers = state.health_write.lock();
        let snapshot = state.health.load_full();
        // Only act (and pay the clone) when the node is currently marked
        // healthy — avoids churning the snapshot on an already-down node.
        if snapshot.get(addr).map(|h| h.healthy).unwrap_or(false) {
            let mut next = (*snapshot).clone();
            if let Some(nh) = next.get_mut(addr) {
                nh.healthy = false;
                nh.failure_count = nh.failure_count.saturating_add(1);
                nh.last_error = Some(format!("in-band failure: {}", err));
                tracing::warn!(
                    node = %addr,
                    error = %err,
                    "in-band failure — node marked unhealthy for fast failover"
                );
            }
            state.health.store(Arc::new(next));
            Self::publish_routing(state);
        }
    }

    /// Record a backend forward failure: demote the node's health in-band AND
    /// (when the feature is on) trip its circuit breaker — the single place the
    /// data path reports "this backend just failed". Both signals consult the
    /// same `is_backend_fault` classifier, so they can never drift apart: a
    /// client-side error or a slow-query read timeout penalizes neither.
    pub(super) fn record_backend_failure(state: &Arc<ServerState>, node: &str, err: &str) {
        Self::note_backend_failure(state, node, err);
        #[cfg(feature = "circuit-breaker")]
        if Self::is_backend_fault(err) {
            Self::circuit_record(state, node, false, err);
        }
    }

    /// True when `node`'s circuit would turn a request away now, so reads
    /// prefer other nodes: open and still in its cooldown, or half-open with
    /// every probe slot taken. An open circuit whose cooldown has passed
    /// admits a probe ([`Self::circuit_admit`]).
    #[cfg(feature = "circuit-breaker")]
    pub(super) fn circuit_is_open(state: &Arc<ServerState>, node: &str) -> bool {
        state
            .circuit_breaker
            .as_ref()
            .map(|cb| !cb.get_breaker(node).admits_now())
            .unwrap_or(false)
    }

    /// Record the outcome of a forward to `node` on its circuit breaker.
    #[cfg(feature = "circuit-breaker")]
    pub(super) fn circuit_record(state: &Arc<ServerState>, node: &str, success: bool, err: &str) {
        if let Some(cb) = state.circuit_breaker.as_ref() {
            let breaker = cb.get_breaker(node);
            if success {
                breaker.record_success();
            } else {
                breaker.record_failure(err);
            }
        }
    }

    /// Admit a request to `node` through its circuit breaker. `Ok(None)`:
    /// closed. `Ok(Some(permit))`: the circuit is half-open (its cooldown
    /// passed) and this request is one of its probes; hold the permit until
    /// the request is done, its outcome recorded as usual
    /// ([`Self::circuit_record`]). `Err`: the fast-fail `ErrorResponse`
    /// (without a trailing `ReadyForQuery`, which the caller appends): open,
    /// or every probe slot taken.
    #[cfg(feature = "circuit-breaker")]
    pub(super) fn circuit_admit(
        state: &Arc<ServerState>,
        node: &str,
    ) -> std::result::Result<Option<crate::circuit_breaker::ProbePermit>, Vec<u8>> {
        let Some(cb) = state.circuit_breaker.as_ref() else {
            return Ok(None);
        };
        match cb.get_breaker(node).admit() {
            crate::circuit_breaker::Admission::Allowed => Ok(None),
            crate::circuit_breaker::Admission::Probe(permit) => {
                tracing::info!(node = %node, "circuit half-open — probing the node");
                Ok(Some(permit))
            }
            crate::circuit_breaker::Admission::Rejected { .. } => {
                tracing::info!(node = %node, "circuit open — fast-failing");
                Err(Self::create_error_response(
                    "08006",
                    &format!("circuit open for node {node}: backend temporarily unavailable"),
                ))
            }
        }
    }

    /// Read-your-writes decision: should reads be pinned to the primary given
    /// the session's last write and the configured window? Pure for testing.
    #[cfg(feature = "lag-routing")]
    pub(super) fn ryw_pins_primary(last_write: Option<std::time::Instant>, window_ms: u64) -> bool {
        window_ms > 0
            && last_write
                .map(|t| t.elapsed() < Duration::from_millis(window_ms))
                .unwrap_or(false)
    }

    /// Lag-exclusion decision: should a standby be dropped from read routing
    /// given its measured lag and the configured ceiling? `max=0` disables
    /// exclusion; unknown lag (None) never excludes. Pure for testing.
    #[cfg(feature = "lag-routing")]
    pub(super) fn lag_excludes_standby(
        lag_bytes: Option<u64>,
        max_lag_bytes: u64,
        require_known: bool,
    ) -> bool {
        match lag_bytes {
            Some(lag) => max_lag_bytes > 0 && lag > max_lag_bytes,
            // Unknown lag is allowed by default (historical behaviour); a
            // strict policy refuses it rather than treating `None` as fresh
            // (H-04).
            None => require_known,
        }
    }

    /// Pure predicate: is `sql` a plain, deterministic, SINGLE-statement
    /// SELECT safe to cache? (Not WITH/locking/volatile/multi-statement/
    /// SELECT INTO.) Uses the built-in volatile list
    /// ([`crate::sql_text::VolatileScan::builtin`]); a result cache that
    /// configures `[cache] extra_volatile_functions` calls
    /// [`Self::is_cacheable_read_sql_with`] with its own scan. Transaction
    /// state is checked separately. Shared by the query-cache and edge-cache
    /// read gates.
    #[cfg(all(test, any(feature = "query-cache", feature = "edge-proxy")))]
    pub(super) fn is_cacheable_read_sql(sql: &str) -> bool {
        Self::is_cacheable_read_sql_with(sql, crate::sql_text::VolatileScan::builtin())
    }

    /// [`Self::is_cacheable_read_sql`] against an explicit `volatile` scan
    /// (the built-in list plus `[cache] extra_volatile_functions`). Shared by
    /// every result cache: the query cache, the edge cache and the MCP tool
    /// cache.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(crate) fn is_cacheable_read_sql_with(
        sql: &str,
        volatile: &crate::sql_text::VolatileScan,
    ) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        if !starts_with_ci(t, "SELECT") {
            return false;
        }
        // Multi-statement simple-query strings (`SELECT ...; UPDATE ...`)
        // must never be cached: replaying the capture would fabricate the
        // trailing statements' results while executing nothing. One
        // trailing `;` is fine; a `;` inside a string literal only costs
        // cacheability — safe.
        let core = t.trim_end();
        let core = core.strip_suffix(';').map(str::trim_end).unwrap_or(core);
        if core.contains(';') {
            return false;
        }
        // `SELECT ... INTO t` CREATES a table (CREATE TABLE AS synonym);
        // replaying it from cache would silently skip the DDL. Word-boundary
        // match, so newline/tab-delimited INTO is caught while a column
        // named `into_x` is not (a literal containing the word merely skips
        // caching).
        if Self::contains_word_ci(core, "into") {
            return false;
        }
        // Row-locking clauses, non-deterministic reads (clock, random,
        // sequences, transaction ids, per-connection values) and side-effecting
        // calls (`set_config` emits ParameterStatus and mutates GUCs; replaying
        // it would suppress the side effect) are all one ASCII-case-insensitive
        // Aho-Corasick pass inside `volatile`. Allocation-free.
        !volatile.refuses(t)
    }

    /// Whether a result cache would look up a cacheable read of `session`
    /// outside an explicit transaction: the query cache runs, or the edge
    /// cache is on and the session is still eligible for it. The relay's
    /// refusal scans ([`Self::cache_refuses`]) and their counters run only
    /// then.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) fn result_cache_consulted(
        session: &ClientSession,
        state: &ServerState,
        config: &ProxyConfig,
    ) -> bool {
        #[cfg(feature = "query-cache")]
        let query_cache = state.query_cache.is_some();
        #[cfg(not(feature = "query-cache"))]
        let query_cache = {
            let _ = state;
            false
        };
        #[cfg(feature = "edge-proxy")]
        let edge = config.edge.enabled
            && !session
                .edge_ineligible
                .load(std::sync::atomic::Ordering::Relaxed);
        #[cfg(not(feature = "edge-proxy"))]
        let edge = {
            let _ = (session, config);
            false
        };
        query_cache || edge
    }

    /// Whether a result cache may use a backend execution-scope proof for `session`
    /// outside an explicit transaction: the query cache runs, or the edge
    /// cache is on and the session is still eligible for it. The relay's
    /// refusal scans ([`Self::cache_refuses`]) and their counters run only
    /// then.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) fn cache_scope_eligible(
        session: &ClientSession,
        state: &ServerState,
        config: &ProxyConfig,
    ) -> bool {
        #[cfg(feature = "query-cache")]
        let query_cache = state.query_cache.is_some()
            && !session
                .cache_ineligible
                .load(std::sync::atomic::Ordering::Relaxed);
        #[cfg(not(feature = "query-cache"))]
        let query_cache = {
            let _ = state;
            false
        };
        #[cfg(feature = "edge-proxy")]
        let edge = config.edge.enabled
            && !session
                .edge_ineligible
                .load(std::sync::atomic::Ordering::Relaxed);
        #[cfg(not(feature = "edge-proxy"))]
        let edge = {
            let _ = (session, config);
            false
        };
        query_cache || edge
    }

    /// Whether neither result cache may look up, capture or store the
    /// cacheable read `sql` (the forwarded text), in any tier (L1, L2, L3,
    /// `[edge]`), counting why: it names a system catalog relation
    /// (`sql_text::names_system_catalog`: session-dependent, changed by DDL
    /// no write announces, answered by raw-text matching on HeliosDB-Nano), a
    /// comment of it holds a time-travel clause (the keys leave comments
    /// out, HeliosDB-Nano and HeliosDB-Lite read it), its text is
    /// backend-ambiguous (PostgreSQL and HeliosDB-Nano/Lite may read
    /// different statements: `sql_text::uncacheable_text`), or it names a
    /// table of `[cache] exclude_tables`. The caller runs it only for a read
    /// a cache would look up.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) fn cache_refuses(state: &ServerState, sql: &str) -> bool {
        use crate::sql_text::CacheRefusal;
        let counter = match crate::sql_text::cache_refusal(sql, state.table_exclusions.as_deref()) {
            None => return false,
            Some(CacheRefusal::SystemCatalog) => &state.metrics.cache_catalog_skipped,
            Some(CacheRefusal::TimeTravelComment) => &state.metrics.cache_time_travel_skipped,
            Some(CacheRefusal::AmbiguousText) => &state.metrics.cache_ambiguous_skipped,
            Some(CacheRefusal::ExcludedTable) => &state.metrics.cache_excluded,
        };
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    }

    /// Unknown functions, writes (including triggers), and context-control
    /// statements cannot preserve a shared result cache's execution identity.
    /// Do not trust the separate operator-extended replay allowlist here.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) fn cache_context_may_change(sql: &str) -> bool {
        // A name such as abs/count does not prove which catalog function or
        // overload the backend resolved. Until that identity is verified,
        // function-call-shaped reads (and EXPLAIN) must execute natively and
        // make the session ineligible for later cached results too.
        static POLICY: std::sync::LazyLock<TrReadPolicy> =
            std::sync::LazyLock::new(TrReadPolicy::default);
        let core = crate::protocol::skip_leading_comments(sql);
        Self::tr_routing_classify(core, &POLICY) != StmtKind::Read
            || !Self::tr_read_eligible(core, &POLICY)
    }

    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) async fn cache_startup_namespace(session: &ClientSession) -> &[u8; 32] {
        if let Some(namespace) = session.cache_startup_namespace.get() {
            return namespace;
        }
        use sha2::{Digest, Sha256};
        let vars = session.variables.read().await;
        if vars.is_empty() {
            return session.cache_startup_namespace.get_or_init(|| [0; 32]);
        }
        let mut sorted: Vec<_> = vars.iter().collect();
        sorted.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let mut hash = Sha256::new();
        hash.update(b"helios-query-cache-startup-v1");
        for (key, value) in sorted {
            for part in [key, value] {
                hash.update((part.len() as u64).to_be_bytes());
                hash.update(part.as_bytes());
            }
        }
        session
            .cache_startup_namespace
            .get_or_init(|| hash.finalize().into())
    }

    /// L1 uses raw SQL within a client connection; retire it before changing
    /// the effective backend namespace (a route, lease or settings change).
    #[cfg(feature = "query-cache")]
    pub(super) fn cache_use_namespace(
        session: &ClientSession,
        cache: &crate::cache::QueryCache,
        namespace: [u8; 32],
    ) {
        let mut previous = session
            .cache_effective_namespace
            .lock()
            .expect("cache namespace mutex poisoned");
        if previous.is_some_and(|old| old != namespace) {
            cache.remove_l1_cache(session.id.as_u64_pair().0);
        }
        *previous = Some(namespace);
    }

    /// Decide whether a read query is safe to serve from / store in the cache,
    /// and build its `CacheContext`. Returns `None` for anything not a plain,
    /// deterministic, non-transactional SELECT.
    ///
    /// `is_cacheable_read` is `StmtFacts::is_cacheable_read` for the statement
    /// (i.e. `is_cacheable_read_sql`, already computed once for this message).
    #[cfg(feature = "query-cache")]
    pub(super) async fn cacheable_read_ctx(
        session: &Arc<ClientSession>,
        is_cacheable_read: bool,
    ) -> Option<crate::cache::CacheContext> {
        if !is_cacheable_read
            || session
                .cache_ineligible
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return None;
        }
        // Never cache mid-transaction (visibility would be wrong).
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return None;
        }
        let (user, database) = {
            let vars = session.variables.read().await;
            (
                vars.get("user").cloned(),
                vars.get("database")
                    .cloned()
                    .unwrap_or_else(|| "default".to_string()),
            )
        };
        Some(crate::cache::CacheContext {
            database,
            user,
            branch: None,
            connection_id: Some(session.id.as_u64_pair().0),
        })
    }

    /// What `[multi_tenancy]` row isolation does to one statement of
    /// `session`, on either protocol: the session's tenant is resolved and
    /// its row filter injected, or the statement is refused (a shape the
    /// filter cannot cover; a tenant table named by a session with no
    /// identified tenant under `unknown_tenant = "refuse"`).
    #[cfg(feature = "multi-tenancy")]
    pub(super) async fn tenant_statement(
        tm: &crate::multi_tenancy::TenantManager,
        sql: &str,
        session: &Arc<ClientSession>,
    ) -> TenantStatement {
        let ctx = Self::tenant_request_ctx(session).await;
        match tm.identify_tenant(&ctx) {
            Some(tenant) => {
                let res = tm.transform_query(sql, &tenant);
                if let Some(reason) = res.refusal {
                    // Never forwarded: a shape the rewrite cannot filter.
                    TenantStatement::Refused(reason.into())
                } else if res.transformed {
                    tracing::debug!(target: "helios::tenant", tenant = %tenant.0, "tenant filter injected");
                    TenantStatement::Filtered(res.query)
                } else {
                    TenantStatement::Unchanged
                }
            }
            // No tenant to filter by: `unknown_tenant = "refuse"` stops a
            // statement on a tenant table here.
            None if tm.refuses_unknown_tenant_query(sql) => {
                TenantStatement::Refused(UNIDENTIFIED_TENANT_REFUSAL.into())
            }
            None => TenantStatement::Unchanged,
        }
    }

    /// Build a multi-tenancy `RequestContext` from the session's startup
    /// parameters (user, database, application_name, ...) so the configured
    /// identifier can resolve the tenant.
    #[cfg(feature = "multi-tenancy")]
    pub(super) async fn tenant_request_ctx(
        session: &Arc<ClientSession>,
    ) -> crate::multi_tenancy::RequestContext {
        let vars = session.variables.read().await;
        crate::multi_tenancy::RequestContext {
            headers: vars.clone(),
            username: vars.get("user").cloned(),
            database: vars.get("database").cloned(),
            auth_token: None,
            sql_context: HashMap::new(),
            client_ip: Some(session.client_addr.ip().to_string()),
            connection_id: Some(session.id.as_u64_pair().0),
        }
    }

    // ---- TR-07 recovery-journal capture hooks --------------------------
}
