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
//!   registered edge.
//! - **Conflict resolution**: each cache entry carries a monotonic
//!   `version` (logical wall-clock). An invalidation drops every
//!   entry whose `version <= invalidation.version`. Late writes
//!   (clock skew across regions) cannot resurrect stale data.
//!
//! ## Cross-region link — pull-on-miss + invalidation push
//!
//! Edge → Home: HTTP/1.1 with bearer-token auth. Each edge starts
//! up by registering with home (`POST /api/edge/register`) and
//! holding the response stream open for invalidation events
//! (chunked-transfer Server-Sent Events).
//!
//! Home → Edge: same connection — home pushes
//! `event: invalidate\ndata: {...}\n\n` whenever a write commits.
//!
//! No per-region central registrar, no distributed consensus, no
//! vector clocks. Picks "eventual consistency with bounded
//! staleness" as the explicit contract — readers may see TTL-window
//! stale data on any region after a write to another region.

pub mod cache;
pub mod registry;

pub use cache::{CacheEntry, CacheKey, EdgeCache, EdgeCacheStats};
pub use registry::{EdgeNode, EdgeRegistry, InvalidationEvent};

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Edge-mode runtime config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeConfig {
    /// "edge" or "home" mode. Edges forward writes + cache reads;
    /// home is authoritative.
    pub role: EdgeRole,

    /// For edge: home proxy admin URL (e.g. "https://proxy.home.svc:9090").
    /// Empty when role = home.
    #[serde(default)]
    pub home_url: String,

    /// For edge: bearer token presented to home for register +
    /// pull-on-miss. Empty when role = home.
    #[serde(default)]
    pub auth_token: String,

    /// Default TTL applied to cache entries when the home doesn't
    /// supply one explicitly. Reads expire after this elapses.
    pub default_ttl: Duration,

    /// Maximum cache entries before LRU eviction kicks in.
    pub max_entries: usize,

    /// For home: maximum simultaneous registered edge nodes.
    pub max_edges: usize,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            role: EdgeRole::Home,
            home_url: String::new(),
            auth_token: String::new(),
            default_ttl: Duration::from_secs(60),
            max_entries: 10_000,
            max_edges: 32,
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
    }

    #[test]
    fn config_round_trips_through_serde() {
        let cfg = EdgeConfig {
            role: EdgeRole::Edge,
            home_url: "https://home.svc:9090".into(),
            auth_token: "tkn".into(),
            default_ttl: Duration::from_secs(30),
            max_entries: 5_000,
            max_edges: 0,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: EdgeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.role, EdgeRole::Edge);
        assert_eq!(back.home_url, "https://home.svc:9090");
        assert_eq!(back.default_ttl, Duration::from_secs(30));
    }
}
