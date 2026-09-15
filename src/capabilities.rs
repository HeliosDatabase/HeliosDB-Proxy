//! Capabilities manifest and opt-in strict configuration (D-05).
//!
//! Every advertised subsystem should be able to answer three questions:
//! is it **compiled** into this binary, is it **enabled** by `proxy.toml`, and
//! is it actually **wired** (a runtime call path exists)? Settings that promise
//! behavior the binary cannot deliver (e.g. `[cache] enabled = true` on a build
//! without the `query-cache` feature) are silent today; `strict_config = true`
//! turns them into startup errors and `GET /capabilities` makes the state
//! inspectable.

use serde::Serialize;

use crate::config::{AuthMode, ProxyConfig, TopologyProviderKind};

/// One subsystem's build/runtime state.
#[derive(Debug, Clone, Serialize)]
pub struct Capability {
    /// Feature/subsystem name (matches the cargo feature where one exists).
    pub name: &'static str,
    /// Compiled into this binary.
    pub compiled: bool,
    /// Switched on by the current configuration.
    pub enabled: bool,
    /// A runtime call path exists for it (library-only modules are `false`).
    pub wired: bool,
    /// Optional caveat, e.g. a deprecated no-op.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<&'static str>,
}

/// Build the capabilities manifest for `config`.
pub fn manifest(config: &ProxyConfig) -> Vec<Capability> {
    let c = |name: &'static str,
             compiled: bool,
             enabled: bool,
             wired: bool,
             note: Option<&'static str>| Capability {
        name,
        compiled,
        enabled,
        wired,
        note,
    };

    vec![
        c(
            "pool-modes",
            cfg!(feature = "pool-modes"),
            cfg!(feature = "pool-modes"),
            cfg!(feature = "pool-modes"),
            None,
        ),
        c(
            "query-cache",
            cfg!(feature = "query-cache"),
            config.cache.enabled,
            cfg!(feature = "query-cache"),
            None,
        ),
        c(
            "routing-hints",
            cfg!(feature = "routing-hints"),
            config.routing_hints.enabled,
            cfg!(feature = "routing-hints"),
            None,
        ),
        c(
            "lag-routing",
            cfg!(feature = "lag-routing"),
            config.lag_routing.enabled,
            cfg!(feature = "lag-routing"),
            None,
        ),
        c(
            "rate-limiting",
            cfg!(feature = "rate-limiting"),
            config.rate_limit.enabled,
            cfg!(feature = "rate-limiting"),
            None,
        ),
        c(
            "circuit-breaker",
            cfg!(feature = "circuit-breaker"),
            config.circuit_breaker.enabled,
            cfg!(feature = "circuit-breaker"),
            None,
        ),
        c(
            "query-analytics",
            cfg!(feature = "query-analytics"),
            config.analytics.enabled,
            cfg!(feature = "query-analytics"),
            None,
        ),
        c(
            "anomaly-detection",
            cfg!(feature = "anomaly-detection"),
            cfg!(feature = "anomaly-detection"),
            cfg!(feature = "anomaly-detection"),
            None,
        ),
        c(
            "multi-tenancy",
            cfg!(feature = "multi-tenancy"),
            config.multi_tenancy.enabled,
            cfg!(feature = "multi-tenancy"),
            None,
        ),
        c(
            "auth-proxy",
            cfg!(feature = "auth-proxy"),
            !matches!(config.auth.mode, AuthMode::Passthrough),
            cfg!(feature = "auth-proxy"),
            None,
        ),
        c(
            "ldap-auth",
            cfg!(feature = "ldap-auth"),
            false,
            cfg!(feature = "ldap-auth"),
            Some("no daemon config selector yet; library surface only"),
        ),
        c(
            "query-rewriting",
            cfg!(feature = "query-rewriting"),
            config.query_rewrite.enabled,
            cfg!(feature = "query-rewriting"),
            None,
        ),
        c(
            "wasm-plugins",
            cfg!(feature = "wasm-plugins"),
            config.plugins.enabled,
            cfg!(feature = "wasm-plugins"),
            None,
        ),
        c(
            "graphql-gateway",
            cfg!(feature = "graphql-gateway"),
            config.graphql_gateway.enabled,
            cfg!(feature = "graphql-gateway"),
            None,
        ),
        c(
            "schema-routing",
            cfg!(feature = "schema-routing"),
            config.schema_routing.enabled,
            cfg!(feature = "schema-routing"),
            None,
        ),
        c(
            "edge-proxy",
            cfg!(feature = "edge-proxy"),
            config.edge.enabled,
            cfg!(feature = "edge-proxy"),
            None,
        ),
        c(
            "distribcache",
            cfg!(feature = "distribcache"),
            false,
            false,
            Some("library-only; not wired into the daemon"),
        ),
        c(
            "postgres-topology",
            cfg!(feature = "postgres-topology"),
            config.topology.provider == TopologyProviderKind::Postgres,
            cfg!(feature = "postgres-topology"),
            None,
        ),
        c(
            "heliosdb-topology",
            cfg!(feature = "heliosdb-topology"),
            false,
            cfg!(feature = "heliosdb-topology"),
            Some("bridge only; no daemon config selector yet"),
        ),
        c("mcp", true, config.mcp.enabled, true, None),
        c(
            "http-gateway",
            true,
            config.http_gateway.enabled,
            true,
            None,
        ),
        c(
            "observability",
            cfg!(feature = "observability"),
            false,
            false,
            Some("reserved no-op; /metrics is always served by the admin API"),
        ),
        c(
            "ha-tr",
            cfg!(feature = "ha-tr"),
            true,
            true,
            Some("deprecated no-op; Transaction Replay ships in the default build"),
        ),
    ]
}

/// Enabled-but-not-compiled subsystems (D-05 strict mode). Each entry names the
/// config key path whose promise the binary cannot keep.
pub fn unavailable_enabled(config: &ProxyConfig) -> Vec<&'static str> {
    // (config key path, enabled now, compiled into this build)
    let checks: [(&'static str, bool, bool); 16] = [
        (
            "cache.enabled",
            config.cache.enabled,
            cfg!(feature = "query-cache"),
        ),
        (
            "routing_hints.enabled",
            config.routing_hints.enabled,
            cfg!(feature = "routing-hints"),
        ),
        (
            "lag_routing.enabled",
            config.lag_routing.enabled,
            cfg!(feature = "lag-routing"),
        ),
        (
            "rate_limit.enabled",
            config.rate_limit.enabled,
            cfg!(feature = "rate-limiting"),
        ),
        (
            "circuit_breaker.enabled",
            config.circuit_breaker.enabled,
            cfg!(feature = "circuit-breaker"),
        ),
        (
            "analytics.enabled",
            config.analytics.enabled,
            cfg!(feature = "query-analytics"),
        ),
        (
            "multi_tenancy.enabled",
            config.multi_tenancy.enabled,
            cfg!(feature = "multi-tenancy"),
        ),
        (
            "auth.mode",
            !matches!(config.auth.mode, AuthMode::Passthrough),
            cfg!(feature = "auth-proxy"),
        ),
        (
            "query_rewrite.enabled",
            config.query_rewrite.enabled,
            cfg!(feature = "query-rewriting"),
        ),
        (
            "plugins.enabled",
            config.plugins.enabled,
            cfg!(feature = "wasm-plugins"),
        ),
        (
            "graphql_gateway.enabled",
            config.graphql_gateway.enabled,
            cfg!(feature = "graphql-gateway"),
        ),
        (
            "schema_routing.enabled",
            config.schema_routing.enabled,
            cfg!(feature = "schema-routing"),
        ),
        (
            "edge.enabled",
            config.edge.enabled,
            cfg!(feature = "edge-proxy"),
        ),
        (
            "topology.provider",
            config.topology.provider == TopologyProviderKind::Postgres,
            cfg!(feature = "postgres-topology"),
        ),
        ("ldap_auth", false, cfg!(feature = "ldap-auth")),
        ("observability", false, cfg!(feature = "observability")),
    ];
    checks
        .iter()
        .filter(|(_, enabled, compiled)| *enabled && !*compiled)
        .map(|(key, _, _)| *key)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_matches_the_build_and_config() {
        let config = ProxyConfig::default();
        let m = manifest(&config);

        let get = |name: &str| m.iter().find(|c| c.name == name).expect(name);

        // Compiled flags mirror the cargo features.
        assert_eq!(get("pool-modes").compiled, cfg!(feature = "pool-modes"));
        assert_eq!(get("query-cache").compiled, cfg!(feature = "query-cache"));
        // A no-op feature must never claim to be wired.
        let obs = get("observability");
        assert!(!obs.enabled && !obs.wired);
        assert!(obs.note.unwrap_or("").contains("no-op"));
        // ha-tr is the deprecated TR no-op.
        assert!(get("ha-tr").note.unwrap_or("").contains("deprecated"));
        // Disabled-by-default subsystems report enabled = false.
        assert!(!get("query-cache").enabled);
        assert!(!get("edge-proxy").enabled);
        // Every entry has a unique name.
        let mut names: Vec<&str> = m.iter().map(|c| c.name).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(before, names.len(), "duplicate capability names");
    }

    #[test]
    fn unavailable_enabled_reports_disabled_build_promises() {
        // With `all-features` nothing is unavailable; with a reduced build the
        // enabled flags above would be reported. Assert the invariant that it
        // never reports a compiled feature.
        let mut config = ProxyConfig::default();
        config.cache.enabled = true;
        let unavailable = unavailable_enabled(&config);
        if cfg!(feature = "query-cache") {
            assert!(
                !unavailable.contains(&"cache.enabled"),
                "compiled features must not be reported unavailable"
            );
        } else {
            assert!(unavailable.contains(&"cache.enabled"));
        }
    }
}
