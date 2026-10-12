use super::*;

impl ProxyServer {
    /// The request policies the HTTP, GraphQL and MCP gateways apply (H-06):
    /// the daemon's own rate limiter (with `[rate_limit] key_by`), query
    /// rewriter and circuit breaker, when those are enabled.
    pub(super) fn gateway_policy(&self) -> crate::gateway_policy::GatewayPolicy {
        let policy = crate::gateway_policy::GatewayPolicy::default();
        #[cfg(feature = "rate-limiting")]
        let policy = match self.state.rate_limiter.as_ref() {
            Some(limiter) => {
                policy.with_rate_limiter(limiter.clone(), self.config.rate_limit.key_by)
            }
            None => policy,
        };
        #[cfg(feature = "query-rewriting")]
        let policy = match self.state.rewriter.as_ref() {
            Some(rw) => policy.with_rewriter(rw.clone()),
            None => policy,
        };
        #[cfg(feature = "circuit-breaker")]
        let policy = match self.state.circuit_breaker.as_ref() {
            Some(cb) => policy.with_circuit_breaker(cb.clone()),
            None => policy,
        };
        policy
    }

    /// `[multi_tenancy]` has no per-request tenant identity on the HTTP,
    /// GraphQL and MCP gateways, so their requests are not tenant-filtered.
    /// Refused under `strict_config`; a loud warning otherwise.
    pub(super) fn check_gateway_tenancy(config: &ProxyConfig) -> Result<()> {
        let gateways: Vec<&str> = [
            ("http_gateway", config.http_gateway.enabled),
            ("graphql_gateway", config.graphql_gateway.enabled),
            ("mcp", config.mcp.enabled),
        ]
        .into_iter()
        .filter_map(|(name, on)| on.then_some(name))
        .collect();
        if !config.multi_tenancy.enabled || gateways.is_empty() {
            return Ok(());
        }
        let msg = format!(
            "[multi_tenancy] is enabled together with {}: those gateways have no per-request \
             tenant identity, so their queries are NOT tenant-filtered (H-06)",
            gateways.join(", ")
        );
        if config.strict_config {
            return Err(ProxyError::Config(format!(
                "{msg}; disable the gateways or [multi_tenancy], or set strict_config = false \
                 to accept it"
            )));
        }
        tracing::warn!("{msg}");
        Ok(())
    }
}
