use super::*;

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
    #[cfg(feature = "rate-limiting")]
    pub(super) async fn rate_limit_check(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Option<Vec<u8>> {
        use crate::rate_limit::RateLimitResult;
        let limiter = state.rate_limiter.as_ref()?;
        let key = Self::rate_limit_key(session, config).await;
        let key = key.as_ref();
        match limiter.check_cached(key, 1) {
            RateLimitResult::Allowed => None,
            RateLimitResult::Warned(msg) => {
                tracing::warn!(key = %key, reason = %msg, "rate limit warning");
                None
            }
            RateLimitResult::Throttled(d) | RateLimitResult::Queued(d) => {
                // Cap the backpressure sleep so a misconfiguration can't pin a
                // connection task indefinitely.
                tokio::time::sleep(d.min(Duration::from_secs(5))).await;
                None
            }
            RateLimitResult::Denied(exc) => {
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
        !err.contains("Client") && !err.contains("Backend read timeout")
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

    /// True when `node`'s circuit is open (avoid it / fast-fail). A half-open
    /// circuit returns false so a probe query is admitted.
    #[cfg(feature = "circuit-breaker")]
    pub(super) fn circuit_is_open(state: &Arc<ServerState>, node: &str) -> bool {
        state
            .circuit_breaker
            .as_ref()
            .map(|cb| {
                cb.get_breaker(node).get_state() == crate::circuit_breaker::CircuitState::Open
            })
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

    /// If `node`'s circuit is open, build the fast-fail `ErrorResponse` (without
    /// a trailing `ReadyForQuery` — the caller appends one). `None` when the
    /// circuit is closed or half-open and the request may proceed.
    #[cfg(feature = "circuit-breaker")]
    pub(super) fn circuit_fast_fail(state: &Arc<ServerState>, node: &str) -> Option<Vec<u8>> {
        if Self::circuit_is_open(state, node) {
            tracing::info!(node = %node, "circuit open — fast-failing");
            Some(Self::create_error_response(
                "08006",
                &format!("circuit open for node {node}: backend temporarily unavailable"),
            ))
        } else {
            None
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
    /// SELECT INTO.) Transaction state is checked separately. Shared by the
    /// query-cache and edge-cache read gates.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) fn is_cacheable_read_sql(sql: &str) -> bool {
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
        // The remaining checks are all "does `t` contain one of these
        // needles, case-insensitively" — instead of the 13 separate
        // windowed case-insensitive scans (FOR UPDATE, FOR SHARE, 11
        // VOLATILE tokens) that used to each walk `t` byte-by-byte,
        // lowercase `t` ONCE into a scratch buffer and do plain `str::contains`
        // checks against it (Two-Way substring search — no SIMD, but a single
        // linear scan replacing 13 windowed case-insensitive ones is still a
        // clear win). ASCII-only lowercasing (`to_ascii_lowercase`) leaves any
        // non-ASCII bytes untouched, matching `contains_ci`'s byte-for-byte
        // semantics for non-ASCII input.
        let lower = t.to_ascii_lowercase();
        if lower.contains("for update") || lower.contains("for share") {
            return false;
        }
        // Non-deterministic or side-effectful reads must not be reused
        // (set_config emits ParameterStatus and mutates GUCs — replaying it
        // would suppress the side effect).
        const VOLATILE: [&str; 11] = [
            "now(",
            "current_timestamp",
            "current_date",
            "current_time",
            "clock_timestamp",
            "statement_timestamp",
            "random(",
            "nextval(",
            "uuid_generate",
            "gen_random_uuid",
            "set_config(",
        ];
        !VOLATILE.iter().any(|v| lower.contains(v))
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
        if !is_cacheable_read {
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
