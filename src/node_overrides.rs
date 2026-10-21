//! Operator overrides on node eligibility (Sprinter b251750f1d54).
//!
//! `POST /nodes/{addr}/disable` and `POST /api/chaos force_unhealthy` take a
//! backend out of routing on this proxy instance. An overridden node publishes
//! as unhealthy and the routing snapshot treats it as `enabled = false`: it is
//! never a write target or a read candidate, and under an authoritative
//! topology provider an overridden leader makes writes wait (fail closed)
//! until the override is cleared or the provider names another leader. The
//! proxy never promotes on an override; `[auto_promote]` probes the databases
//! itself and is not affected.
//!
//! The map is owned by the server, written through [`NodeControl`] by the
//! admin API, and re-applied by every health publication so a passing probe
//! cannot re-admit an overridden node. Overrides live in memory only: they
//! are per proxy instance and do not survive a restart.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::Serialize;

use crate::Result;

/// Why a node is held out of routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OverrideKind {
    /// `POST /nodes/{addr}/disable`: operational drain of one backend.
    Disabled,
    /// `POST /api/chaos force_unhealthy`: a failover drill.
    ForceUnhealthy,
}

impl OverrideKind {
    /// The wire name (`disabled` / `force_unhealthy`).
    pub fn as_str(self) -> &'static str {
        match self {
            OverrideKind::Disabled => "disabled",
            OverrideKind::ForceUnhealthy => "force_unhealthy",
        }
    }
}

/// One node's override.
#[derive(Debug, Clone, Serialize)]
pub struct NodeOverride {
    pub kind: OverrideKind,
    /// When the override was applied.
    pub since: chrono::DateTime<chrono::Utc>,
    /// Free-form description shown by `/nodes` and the admin UI.
    pub note: String,
}

impl NodeOverride {
    /// An override of `kind` applied now.
    pub fn now(kind: OverrideKind, note: impl Into<String>) -> Self {
        Self {
            kind,
            since: chrono::Utc::now(),
            note: note.into(),
        }
    }
}

/// The per-node override map. Readers take a short read lock; the health
/// checker consults it once per publication, never per query.
#[derive(Default)]
pub struct NodeOverrides {
    inner: RwLock<HashMap<String, NodeOverride>>,
}

impl NodeOverrides {
    pub fn new() -> Self {
        Self::default()
    }

    /// The override on `addr`, if any.
    pub fn get(&self, addr: &str) -> Option<NodeOverride> {
        self.inner.read().get(addr).cloned()
    }

    /// Whether `addr` is held out of routing.
    pub fn is_overridden(&self, addr: &str) -> bool {
        self.inner.read().contains_key(addr)
    }

    /// Apply `override_` to `addr`, replacing any previous one (returned).
    pub fn set(&self, addr: &str, override_: NodeOverride) -> Option<NodeOverride> {
        self.inner.write().insert(addr.to_string(), override_)
    }

    /// Clear the override on `addr` (returned, if there was one).
    pub fn clear(&self, addr: &str) -> Option<NodeOverride> {
        self.inner.write().remove(addr)
    }

    /// Clear every override of `kind`; returns the addresses cleared, in
    /// address order.
    pub fn clear_kind(&self, kind: OverrideKind) -> Vec<String> {
        let mut map = self.inner.write();
        let mut cleared: Vec<String> = map
            .iter()
            .filter(|(_, o)| o.kind == kind)
            .map(|(a, _)| a.clone())
            .collect();
        for addr in &cleared {
            map.remove(addr);
        }
        cleared.sort();
        cleared
    }

    /// A copy of every override.
    pub fn snapshot(&self) -> HashMap<String, NodeOverride> {
        self.inner.read().clone()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

/// How the admin API changes node eligibility on the running server. The
/// daemon attaches its implementation at startup; without one (library use,
/// unit tests) the admin API only updates its own display copy.
pub trait NodeControl: Send + Sync {
    /// Apply `override_` to `addr`, or clear its override with `None`, and
    /// republish health and routing before returning, so the change is
    /// visible to the next query and the next admin read. Returns the node's
    /// effective `healthy` flag afterwards. Fails when `addr` is not a
    /// configured node.
    fn set_override(&self, addr: &str, override_: Option<NodeOverride>) -> Result<bool>;

    /// The current overrides.
    fn overrides(&self) -> HashMap<String, NodeOverride>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_clear_round_trip() {
        let map = NodeOverrides::new();
        assert!(map.is_empty());
        assert!(!map.is_overridden("a:5432"));
        assert!(map
            .set("a:5432", NodeOverride::now(OverrideKind::Disabled, "drain"))
            .is_none());
        assert!(map.is_overridden("a:5432"));
        let got = map.get("a:5432").expect("set");
        assert_eq!(got.kind, OverrideKind::Disabled);
        assert_eq!(got.note, "drain");
        // Replacing returns the previous override.
        let prev = map
            .set(
                "a:5432",
                NodeOverride::now(OverrideKind::ForceUnhealthy, "drill"),
            )
            .expect("previous");
        assert_eq!(prev.kind, OverrideKind::Disabled);
        assert_eq!(
            map.get("a:5432").unwrap().kind,
            OverrideKind::ForceUnhealthy
        );
        assert!(map.clear("a:5432").is_some());
        assert!(map.clear("a:5432").is_none());
        assert!(map.is_empty());
    }

    #[test]
    fn clear_kind_leaves_other_kinds_alone() {
        let map = NodeOverrides::new();
        map.set(
            "b:5432",
            NodeOverride::now(OverrideKind::ForceUnhealthy, ""),
        );
        map.set(
            "a:5432",
            NodeOverride::now(OverrideKind::ForceUnhealthy, ""),
        );
        map.set("c:5432", NodeOverride::now(OverrideKind::Disabled, ""));
        assert_eq!(
            map.clear_kind(OverrideKind::ForceUnhealthy),
            vec!["a:5432".to_string(), "b:5432".to_string()]
        );
        assert!(
            map.is_overridden("c:5432"),
            "an operational disable survives a chaos reset"
        );
        assert!(map.clear_kind(OverrideKind::ForceUnhealthy).is_empty());
        assert_eq!(map.snapshot().len(), 1);
    }

    #[test]
    fn kinds_serialize_with_their_wire_names() {
        let o = NodeOverride::now(OverrideKind::ForceUnhealthy, "x");
        let v = serde_json::to_value(&o).unwrap();
        assert_eq!(v["kind"], "force_unhealthy");
        assert_eq!(OverrideKind::Disabled.as_str(), "disabled");
        assert!(v["since"].as_str().is_some());
    }
}
