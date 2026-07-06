//! Edge / geo proxy mode (T3.2).
//!
//! A HeliosProxy can run in *edge* mode: it terminates client SQL
//! against a local in-memory cache and only forwards to the *home*
//! proxy on cache miss. Writes always pass through to home, and
//! home broadcasts invalidations back to every registered edge so
//! cached results don't go stale on subsequent reads.
//!
//! ## Coherence model — last-write-wins with TTL
//!
//! - **Reads**: edge looks up `(query_fingerprint, params)` in the
//!   local cache. Hit → serve from cache. Miss → forward to home,
//!   cache the result with the configured TTL, serve.
//! - **Writes**: edge forwards verbatim. On success, home computes
//!   the set of touched-tables (via the analytics fingerprint) and
//!   pushes an `Invalidate { tables, version }` event to every
//!   registered edge. This covers simple-protocol statements,
//!   extended-protocol (Parse/Bind/Execute) batches, and `COPY ...
//!   FROM` loads (invalidated when the COPY drains, i.e. when the
//!   rows become visible).
//! - **Conflict resolution**: each cache entry carries a `version` in
//!   the *invalidation clock's* domain — the home stamps entries from
//!   its own counter; an edge stamps entries with the last home
//!   version it observed over SSE. An invalidation drops every entry
//!   whose `version <= invalidation.version`, so a read that began
//!   before a write is always swept by that write's event. Events
//!   also carry the home's per-boot `epoch`: a restarted home resets
//!   its clock, and edges flush wholesale on an epoch change instead
//!   of comparing incomparable versions.
//!
//! ## Cross-region link — PG-wire data plane + SSE control plane
//!
//! Data plane (PG-wire): the edge lists the *home proxy's client
//! port* as its `[[nodes]]` backend, so cache misses and all writes
//! flow through the ordinary forwarding path to the home — the
//! captured PG-wire response bytes are exactly what the edge caches.
//! No JSON↔PG-wire re-encoding.
//!
//! Control plane (SSE): each edge holds a long-lived
//! `GET /api/edge/subscribe` against the home's *admin* listener
//! (bearer-token auth, same gate as every other admin route). The
//! home pushes `event: invalidate\ndata: {...}\n\n` frames whenever
//! a write commits; the edge drops matching entries as they arrive.
//!
//! No per-region central registrar, no distributed consensus, no
//! vector clocks. Picks "eventual consistency with bounded
//! staleness" as the explicit contract — readers may see stale data
//! on any region after a write to another region, bounded by SSE
//! delivery lag plus the cache TTL.

#[cfg(feature = "edge-proxy")]
pub mod cache;
#[cfg(feature = "edge-proxy")]
pub mod client;
#[cfg(feature = "edge-proxy")]
pub mod fingerprint;
#[cfg(feature = "edge-proxy")]
pub mod registry;

#[cfg(feature = "edge-proxy")]
pub use cache::{CacheEntry, CacheKey, EdgeCache, EdgeCacheStats};
#[cfg(feature = "edge-proxy")]
pub use registry::{EdgeNode, EdgeRegistry, InvalidationEvent};

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Edge-mode runtime config (`[edge]` in proxy.toml). Every field is
/// serde-defaulted so a partial — or absent — section parses on any
/// build; the machinery only runs when `enabled` *and* the
/// `edge-proxy` feature is compiled in (`validate()` rejects the
/// former without the latter).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeConfig {
    /// Master switch. Off (default) = the edge cache/registry wiring
    /// is skipped entirely and this section is inert.
    #[serde(default)]
    pub enabled: bool,

    /// "edge" or "home" mode. Edges forward writes + cache reads;
    /// home is authoritative.
    #[serde(default = "default_role")]
    pub role: EdgeRole,

    /// For edge: home proxy admin URL (e.g. "https://proxy.home.svc:9090").
    /// Empty when role = home.
    #[serde(default)]
    pub home_url: String,

    /// For edge: bearer token presented to home's admin API for the
    /// invalidation subscription. Empty when role = home. When set,
    /// `home_url` must be https:// (or `allow_insecure_home_url`
    /// opted into) — the token is the home's ADMIN bearer and must
    /// not cross an untrusted network in cleartext.
    #[serde(default)]
    pub auth_token: String,

    /// For edge: allow presenting `auth_token` to a plain-http
    /// `home_url`. Only for provably private links (VPN/WireGuard/
    /// service mesh, loopback dev setups) — mirrors
    /// `admin_allow_insecure` on the home side.
    #[serde(default)]
    pub allow_insecure_home_url: bool,

    /// Default TTL in seconds applied to cache entries when the home
    /// doesn't supply one explicitly. Reads expire after this elapses.
    /// (Integer seconds, like every other `*_secs` duration knob.)
    #[serde(default = "default_cache_ttl_secs")]
    pub default_ttl_secs: u64,

    /// Maximum cache entries before LRU eviction kicks in.
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,

    /// For home: maximum simultaneous registered edge nodes.
    #[serde(default = "default_max_edges")]
    pub max_edges: usize,

    /// For home: edges not seen within this window are pruned from
    /// the registry by the GC sweep (a pruned edge just re-registers
    /// on its next reconnect). Liveness is refreshed by successful
    /// SSE heartbeat writes (every 15s), so keep this comfortably
    /// above ~45s (3 heartbeats) or healthy-but-idle edges will churn.
    #[serde(default = "default_liveness_window_secs")]
    pub liveness_window_secs: u64,

    /// For home: cadence of the registry GC sweep.
    #[serde(default = "default_subscribe_gc_secs")]
    pub subscribe_gc_secs: u64,

    /// For edge: region label reported when subscribing to the home
    /// (shows up in the home's `/api/edge` listing).
    #[serde(default)]
    pub region: String,

    /// For edge: stable id this proxy registers under. Empty
    /// (default) = the runtime falls back to "edge-<pid>".
    #[serde(default)]
    pub edge_id: String,
}

fn default_role() -> EdgeRole {
    EdgeRole::Home
}

fn default_cache_ttl_secs() -> u64 {
    60
}

fn default_max_entries() -> usize {
    10_000
}

fn default_max_edges() -> usize {
    32
}

fn default_liveness_window_secs() -> u64 {
    120
}

fn default_subscribe_gc_secs() -> u64 {
    30
}

impl EdgeConfig {
    /// The entry TTL as a `Duration` (config carries integer seconds,
    /// matching every other `*_secs` knob).
    pub fn default_ttl(&self) -> Duration {
        Duration::from_secs(self.default_ttl_secs)
    }
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            role: default_role(),
            home_url: String::new(),
            auth_token: String::new(),
            allow_insecure_home_url: false,
            default_ttl_secs: default_cache_ttl_secs(),
            max_entries: default_max_entries(),
            max_edges: default_max_edges(),
            liveness_window_secs: default_liveness_window_secs(),
            subscribe_gc_secs: default_subscribe_gc_secs(),
            region: String::new(),
            edge_id: String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EdgeRole {
    /// Authoritative proxy. Routes writes to backends, broadcasts
    /// invalidations to every registered edge.
    Home,
    /// Cache-first proxy. Forwards writes to home, cache reads,
    /// listens for invalidations.
    Edge,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_role_is_home() {
        let cfg = EdgeConfig::default();
        assert_eq!(cfg.role, EdgeRole::Home);
        assert!(!cfg.enabled);
    }

    #[test]
    fn config_round_trips_through_serde() {
        let cfg = EdgeConfig {
            enabled: true,
            role: EdgeRole::Edge,
            home_url: "https://home.svc:9090".into(),
            auth_token: "tkn".into(),
            default_ttl_secs: 30,
            max_entries: 5_000,
            max_edges: 0,
            region: "eu-west".into(),
            edge_id: "edge-a".into(),
            ..EdgeConfig::default()
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: EdgeConfig = serde_json::from_str(&json).unwrap();
        assert!(back.enabled);
        assert_eq!(back.role, EdgeRole::Edge);
        assert_eq!(back.home_url, "https://home.svc:9090");
        assert_eq!(back.default_ttl_secs, 30);
        assert_eq!(back.default_ttl(), Duration::from_secs(30));
        assert_eq!(back.region, "eu-west");
        assert_eq!(back.edge_id, "edge-a");
    }

    #[test]
    fn empty_section_deserializes_with_defaults() {
        // Every field is serde-defaulted: an empty `[edge]` section
        // (or a config written before a field existed) must parse.
        let cfg: EdgeConfig = serde_json::from_str("{}").unwrap();
        assert!(!cfg.enabled);
        assert_eq!(cfg.role, EdgeRole::Home);
        assert_eq!(cfg.home_url, "");
        assert_eq!(cfg.auth_token, "");
        assert!(!cfg.allow_insecure_home_url);
        assert_eq!(cfg.default_ttl_secs, 60);
        assert_eq!(cfg.default_ttl(), Duration::from_secs(60));
        assert_eq!(cfg.max_entries, 10_000);
        assert_eq!(cfg.max_edges, 32);
        assert_eq!(cfg.liveness_window_secs, 120);
        assert_eq!(cfg.subscribe_gc_secs, 30);
        assert_eq!(cfg.region, "");
        assert_eq!(cfg.edge_id, "");
    }

    #[test]
    fn partial_toml_section_parses() {
        let cfg: EdgeConfig = toml::from_str(
            r#"
            enabled = true
            role = "edge"
            home_url = "http://home:9090"
            region = "us-east"
            default_ttl_secs = 45
            "#,
        )
        .unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.role, EdgeRole::Edge);
        assert_eq!(cfg.home_url, "http://home:9090");
        assert_eq!(cfg.region, "us-east");
        // Durations are plain integer seconds, like every other
        // *_secs knob in proxy.toml (locks the TOML shape in).
        assert_eq!(cfg.default_ttl(), Duration::from_secs(45));
        // Unspecified fields fall back to defaults.
        assert_eq!(cfg.liveness_window_secs, 120);
        assert_eq!(cfg.subscribe_gc_secs, 30);
        assert_eq!(cfg.max_entries, 10_000);
    }
}
