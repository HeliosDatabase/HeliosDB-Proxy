use super::*;

impl ProxyServer {
    /// The request policies the HTTP, GraphQL and MCP gateways apply (H-06):
    /// the daemon's own rate limiter (with `[rate_limit] key_by`), query
    /// rewriter and circuit breaker, when those are enabled, and, when a
    /// cache runs, the write generations their writes move (C-02,
    /// Sprinter 6a0c6c34d27a).
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
        // `[multi_tenancy]` active-query caps: the same tenant budgets the
        // PG-wire path takes its permits from (empty unless configured).
        #[cfg(feature = "multi-tenancy")]
        let policy =
            policy.with_tenant_slots(self.state.tenant_slots.clone(), self.state.latency.clone());
        #[cfg(feature = "query-cache")]
        let policy = match self.state.table_generations.as_ref() {
            Some(generations) => policy.with_cache_writes(crate::cache::GatewayWrites::new(
                generations.clone(),
                self.state.query_cache.clone(),
            )),
            None => policy,
        };
        policy
    }

    /// `[multi_tenancy]` with an HTTP, GraphQL or MCP gateway that has no
    /// `tenant_header`: that gateway has no per-request tenant identity, so
    /// its requests are not tenant-filtered. Refused under `strict_config`; a
    /// loud warning otherwise. A gateway with a `tenant_header` filters every
    /// request for the tenant it names (H-06).
    pub(super) fn check_gateway_tenancy(config: &ProxyConfig) -> Result<()> {
        let gateways: Vec<&str> = [
            (
                "http_gateway",
                config.http_gateway.enabled && config.http_gateway.tenant_header.is_empty(),
            ),
            (
                "graphql_gateway",
                config.graphql_gateway.enabled && config.graphql_gateway.tenant_header.is_empty(),
            ),
            (
                "mcp",
                config.mcp.enabled && config.mcp.tenant_header.is_empty(),
            ),
        ]
        .into_iter()
        .filter_map(|(name, unattributed)| unattributed.then_some(name))
        .collect();
        if !config.multi_tenancy.enabled || gateways.is_empty() {
            return Ok(());
        }
        let msg = format!(
            "[multi_tenancy] is enabled together with {} without a tenant_header: those \
             gateways have no per-request tenant identity, so their queries are NOT \
             tenant-filtered (H-06)",
            gateways.join(", ")
        );
        if config.strict_config {
            return Err(ProxyError::Config(format!(
                "{msg}; set tenant_header on each gateway, disable the gateways or \
                 [multi_tenancy], or set strict_config = false to accept it"
            )));
        }
        tracing::warn!("{msg}");
        Ok(())
    }
}
