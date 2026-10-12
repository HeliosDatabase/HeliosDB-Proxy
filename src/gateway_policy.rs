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
//! `[multi_tenancy]` has no per-request identity on the gateways; the daemon
//! refuses that combination under `strict_config` and warns otherwise.

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
}

impl PolicyDenied {
    /// HTTP status a gateway answers with.
    pub fn http_status(&self) -> u16 {
        match self {
            PolicyDenied::RateLimited { .. } => 429,
            PolicyDenied::CircuitOpen { .. } => 503,
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

/// The policies a gateway applies; empty (everything allowed, nothing
/// rewritten) unless the daemon attached its objects.
#[derive(Default, Clone)]
pub struct GatewayPolicy {
    #[cfg(feature = "rate-limiting")]
    rate_limiter: Option<(
        Arc<crate::rate_limit::RateLimiter>,
        crate::config::RateLimitKeyBy,
    )>,
    #[cfg(feature = "query-rewriting")]
    rewriter: Option<Arc<crate::rewriter::QueryRewriter>>,
    #[cfg(feature = "circuit-breaker")]
    circuit: Option<Arc<crate::circuit_breaker::CircuitBreakerManager>>,
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

/// A policy shared by a gateway's tasks.
pub type SharedGatewayPolicy = Arc<GatewayPolicy>;

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
}
