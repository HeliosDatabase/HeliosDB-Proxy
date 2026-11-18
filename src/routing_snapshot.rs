//! One routing view for writes, reads, pool eviction and `/topology` (H-01
//! step 3).
//!
//! The proxy used to answer "where do writes go" from the topology tracker,
//! "where do reads go" from configured roles plus the health map, and
//! `/topology` from a third derivation. After a provider-driven promotion
//! those answers disagreed: writes followed the new leader while reads kept
//! treating the configured roles as truth. A [`RoutingSnapshot`] is built in
//! one place from the configured nodes, their health and (when a topology
//! provider is authoritative) the tracker's leader, and published through a
//! [`RoutingView`]; every consumer reads the same published value.
//!
//! The builder is pure ([`RoutingSnapshot::build`]) so the routing rules are
//! unit-tested without a server.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;

use crate::config::NodeRole;

/// One configured node as the builder sees it.
#[derive(Debug, Clone, Copy)]
pub struct NodeFacts<'a> {
    pub address: &'a str,
    pub role: NodeRole,
    pub enabled: bool,
    pub healthy: bool,
    pub weight: u32,
    pub latency_ms: f64,
    pub lag_bytes: Option<u64>,
}

/// Who decides the write target.
#[derive(Debug, Clone, Copy)]
pub enum Authority<'a> {
    /// `[topology] provider = "static"`: configured roles and health decide.
    Static,
    /// A topology provider decides. `leader` is its leader while its authority
    /// is valid (`None` otherwise); `lease` is how much longer that authority
    /// lasts without a refresh, `None` for an authority that does not expire.
    Provider {
        leader: Option<&'a str>,
        epoch: u64,
        lease: Option<Duration>,
    },
}

/// A node eligible to serve reads.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaView {
    pub address: String,
    pub weight: u32,
    pub latency_ms: f64,
    pub lag_bytes: Option<u64>,
}

/// The routing view at one instant.
#[derive(Debug, Clone)]
pub struct RoutingSnapshot {
    /// Static topology: increments on every change of `leader` since boot.
    /// Provider topology: the tracker's authority epoch.
    pub epoch: u64,
    /// Whether a topology provider decides the leader.
    pub authoritative: bool,
    /// The write target, if any.
    pub leader: Option<String>,
    /// Provider topology: the leader stops being a write target at this
    /// instant unless a newer snapshot extends it. `None` means no expiry.
    pub leader_valid_until: Option<Instant>,
    /// Read candidates, in configuration order.
    pub replicas: Vec<ReplicaView>,
    pub taken_at: Instant,
}

impl RoutingSnapshot {
    /// The snapshot published before the first build: no leader, no replicas.
    pub fn empty(authoritative: bool) -> Self {
        Self {
            epoch: 0,
            authoritative,
            leader: None,
            leader_valid_until: None,
            replicas: Vec::new(),
            taken_at: Instant::now(),
        }
    }

    /// Build a snapshot from the configured nodes (in configuration order).
    ///
    /// - Write target. Static: the first enabled, healthy `role = "primary"`
    ///   node. Provider: the provider's leader if it is an enabled configured
    ///   node and its authority is valid; health does not veto it (the
    ///   provider is the authority, and the write path reports a dead leader
    ///   by failing the write).
    /// - Read candidates: enabled, healthy nodes. Static, or a provider with
    ///   no valid leader: only `role = "standby"` / `"replica"` nodes,
    ///   as before. Provider with a leader: every node except the leader,
    ///   whatever its configured role, so a demoted former primary serves
    ///   reads and a promoted former standby stops being treated as one.
    pub fn build(nodes: &[NodeFacts<'_>], authority: Authority<'_>, now: Instant) -> Self {
        let configured_replica =
            |n: &NodeFacts<'_>| matches!(n.role, NodeRole::Standby | NodeRole::ReadReplica);
        let (authoritative, epoch, leader, leader_valid_until) = match authority {
            Authority::Static => {
                let leader = nodes
                    .iter()
                    .find(|n| n.enabled && n.healthy && n.role == NodeRole::Primary)
                    .map(|n| n.address.to_string());
                (false, 0, leader, None)
            }
            Authority::Provider {
                leader,
                epoch,
                lease,
            } => {
                let leader = leader
                    .filter(|l| nodes.iter().any(|n| n.enabled && n.address == *l))
                    .map(str::to_string);
                let valid_until = leader.as_ref().and(lease).map(|d| now + d);
                (true, epoch, leader, valid_until)
            }
        };
        let replicas = nodes
            .iter()
            .filter(|n| n.enabled && n.healthy)
            .filter(|n| match (authoritative, leader.as_deref()) {
                (true, Some(l)) => n.address != l,
                _ => configured_replica(n),
            })
            .map(|n| ReplicaView {
                address: n.address.to_string(),
                weight: n.weight,
                latency_ms: n.latency_ms,
                lag_bytes: n.lag_bytes,
            })
            .collect();
        Self {
            epoch,
            authoritative,
            leader,
            leader_valid_until,
            replicas,
            taken_at: now,
        }
    }

    /// The write target at `now`: the leader unless its provider authority
    /// has lapsed since this snapshot was taken (writes then wait, failing
    /// closed, until a newer snapshot names a leader).
    pub fn write_leader(&self, now: Instant) -> Option<&str> {
        match self.leader_valid_until {
            Some(until) if now >= until => None,
            _ => self.leader.as_deref(),
        }
    }
}

/// The published [`RoutingSnapshot`] plus the counters `/topology` reports.
pub struct RoutingView {
    current: ArcSwap<RoutingSnapshot>,
    /// Serializes builders so a snapshot computed from older inputs can never
    /// be stored after one computed from newer inputs. Holds the last leader
    /// any published snapshot named, so a leader that is lost and later
    /// replaced by another node is a move even when snapshots without a
    /// leader were published in between.
    publish: parking_lot::Mutex<Option<String>>,
    leader_changes: AtomicU64,
    /// Wall-clock time of the last move, Unix milliseconds; 0 = none since boot.
    last_leader_change_ms: AtomicI64,
    evicted_idle: AtomicU64,
}

impl RoutingView {
    pub fn new(authoritative: bool) -> Self {
        Self {
            current: ArcSwap::from_pointee(RoutingSnapshot::empty(authoritative)),
            publish: parking_lot::Mutex::new(None),
            leader_changes: AtomicU64::new(0),
            last_leader_change_ms: AtomicI64::new(0),
            evicted_idle: AtomicU64::new(0),
        }
    }

    /// The current snapshot: one lock-free atomic load.
    pub fn load(&self) -> Arc<RoutingSnapshot> {
        self.current.load_full()
    }

    /// Build and publish a snapshot. `build` runs under the publish lock, so
    /// it must not block. Returns the previous leader when the leader moved
    /// from one node to a different node, including through snapshots with
    /// no leader in between; connections to that node are the caller's to
    /// retire. A leader that merely disappears (lost lease, conflict, outage)
    /// returns `None`: that can be transient, and the node may still be
    /// serving.
    pub fn publish_with(&self, build: impl FnOnce() -> RoutingSnapshot) -> Option<String> {
        let mut last_leader = self.publish.lock();
        let mut next = build();
        let prev = self.current.load();
        if !next.authoritative {
            next.epoch = prev.epoch + u64::from(prev.leader != next.leader);
        }
        let moved = match (last_leader.as_ref(), next.leader.as_ref()) {
            (Some(old), Some(new)) if old != new => Some(old.clone()),
            _ => None,
        };
        if let Some(new) = &next.leader {
            if last_leader.as_ref() != Some(new) {
                *last_leader = Some(new.clone());
            }
        }
        if moved.is_some() {
            self.leader_changes.fetch_add(1, Ordering::Relaxed);
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
            self.last_leader_change_ms.store(now_ms, Ordering::Relaxed);
        }
        self.current.store(Arc::new(next));
        moved
    }

    /// When the leader last moved from one node to another (wall clock), or
    /// `None` when it has not since boot.
    pub fn last_leader_change(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        match self.last_leader_change_ms.load(Ordering::Relaxed) {
            0 => None,
            ms => chrono::DateTime::from_timestamp_millis(ms),
        }
    }

    /// Record idle pooled connections closed because their node stopped
    /// being the leader.
    pub fn record_evicted(&self, n: usize) {
        self.evicted_idle.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// Leader moves from one node to another since boot.
    pub fn leader_changes_total(&self) -> u64 {
        self.leader_changes.load(Ordering::Relaxed)
    }

    /// Idle pooled connections closed on leader moves since boot.
    pub fn evicted_idle_total(&self) -> u64 {
        self.evicted_idle.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(address: &'static str, role: NodeRole, healthy: bool) -> NodeFacts<'static> {
        NodeFacts {
            address,
            role,
            enabled: true,
            healthy,
            weight: 1,
            latency_ms: 0.5,
            lag_bytes: None,
        }
    }

    fn addrs(s: &RoutingSnapshot) -> Vec<&str> {
        s.replicas.iter().map(|r| r.address.as_str()).collect()
    }

    fn cluster() -> Vec<NodeFacts<'static>> {
        vec![
            node("a:5432", NodeRole::Primary, true),
            node("b:5432", NodeRole::Standby, true),
            node("c:5432", NodeRole::ReadReplica, true),
        ]
    }

    #[test]
    fn static_topology_uses_configured_roles_and_health() {
        let now = Instant::now();
        let s = RoutingSnapshot::build(&cluster(), Authority::Static, now);
        assert!(!s.authoritative);
        assert_eq!(s.write_leader(now), Some("a:5432"));
        assert_eq!(addrs(&s), ["b:5432", "c:5432"]);

        let mut nodes = cluster();
        nodes[0].healthy = false;
        nodes[2].enabled = false;
        let s = RoutingSnapshot::build(&nodes, Authority::Static, now);
        assert_eq!(s.leader, None, "an unhealthy primary is not a write target");
        assert_eq!(addrs(&s), ["b:5432"]);
    }

    #[test]
    fn provider_leader_moves_reads_off_the_promoted_node() {
        let now = Instant::now();
        let promoted = Authority::Provider {
            leader: Some("b:5432"),
            epoch: 7,
            lease: Some(Duration::from_secs(10)),
        };
        let s = RoutingSnapshot::build(&cluster(), promoted, now);
        assert_eq!(s.epoch, 7);
        assert_eq!(s.write_leader(now), Some("b:5432"));
        // The configured primary was demoted: it serves reads now; the
        // promoted standby does not.
        assert_eq!(addrs(&s), ["a:5432", "c:5432"]);
    }

    #[test]
    fn provider_leader_is_not_vetoed_by_health_but_must_be_configured() {
        let now = Instant::now();
        let mut nodes = cluster();
        nodes[1].healthy = false;
        let lease = Some(Duration::from_secs(10));
        let s = RoutingSnapshot::build(
            &nodes,
            Authority::Provider {
                leader: Some("b:5432"),
                epoch: 1,
                lease,
            },
            now,
        );
        assert_eq!(s.write_leader(now), Some("b:5432"));
        assert_eq!(addrs(&s), ["a:5432", "c:5432"]);

        let unknown = Authority::Provider {
            leader: Some("z:5432"),
            epoch: 1,
            lease,
        };
        assert_eq!(RoutingSnapshot::build(&nodes, unknown, now).leader, None);

        nodes[1].enabled = false;
        let disabled = Authority::Provider {
            leader: Some("b:5432"),
            epoch: 1,
            lease,
        };
        assert_eq!(RoutingSnapshot::build(&nodes, disabled, now).leader, None);
    }

    #[test]
    fn provider_without_leader_falls_back_to_configured_replicas() {
        let now = Instant::now();
        let none = Authority::Provider {
            leader: None,
            epoch: 3,
            lease: None,
        };
        let s = RoutingSnapshot::build(&cluster(), none, now);
        assert_eq!(s.write_leader(now), None);
        assert_eq!(addrs(&s), ["b:5432", "c:5432"]);
    }

    #[test]
    fn write_leader_lapses_with_the_lease() {
        let now = Instant::now();
        let lease = Duration::from_millis(500);
        let s = RoutingSnapshot::build(
            &cluster(),
            Authority::Provider {
                leader: Some("a:5432"),
                epoch: 1,
                lease: Some(lease),
            },
            now,
        );
        assert_eq!(s.write_leader(now + lease / 2), Some("a:5432"));
        assert_eq!(s.write_leader(now + lease), None);

        let manual = Authority::Provider {
            leader: Some("a:5432"),
            epoch: 1,
            lease: None,
        };
        let s = RoutingSnapshot::build(&cluster(), manual, now);
        assert_eq!(
            s.write_leader(now + Duration::from_secs(3600)),
            Some("a:5432"),
            "a manual authority does not expire"
        );
    }

    #[test]
    fn publish_reports_only_moves_between_nodes() {
        let view = RoutingView::new(true);
        let at = |leader: Option<&'static str>, epoch| {
            let now = Instant::now();
            move || {
                RoutingSnapshot::build(
                    &cluster(),
                    Authority::Provider {
                        leader,
                        epoch,
                        lease: None,
                    },
                    now,
                )
            }
        };
        assert_eq!(view.publish_with(at(Some("a:5432"), 1)), None);
        assert_eq!(view.publish_with(at(Some("a:5432"), 1)), None);
        assert_eq!(
            view.publish_with(at(None, 1)),
            None,
            "a lost leader is not a move"
        );
        assert_eq!(view.publish_with(at(Some("a:5432"), 1)), None);
        assert_eq!(
            view.publish_with(at(Some("b:5432"), 2)),
            Some("a:5432".to_string())
        );
        assert_eq!(view.leader_changes_total(), 1);
        assert_eq!(view.load().epoch, 2, "provider epochs are taken as given");
    }

    #[test]
    fn a_leader_replaced_after_a_gap_is_a_move_and_is_timestamped() {
        let view = RoutingView::new(true);
        let at = |leader: Option<&'static str>| {
            move || {
                RoutingSnapshot::build(
                    &cluster(),
                    Authority::Provider {
                        leader,
                        epoch: 1,
                        lease: None,
                    },
                    Instant::now(),
                )
            }
        };
        assert_eq!(view.publish_with(at(Some("a:5432"))), None);
        assert_eq!(view.last_leader_change(), None, "no move since boot yet");
        assert_eq!(view.publish_with(at(None)), None);
        assert_eq!(view.publish_with(at(None)), None);
        let before = chrono::Utc::now() - chrono::Duration::seconds(1);
        assert_eq!(
            view.publish_with(at(Some("b:5432"))),
            Some("a:5432".to_string()),
            "a lost leader replaced by another node is a move"
        );
        assert_eq!(view.leader_changes_total(), 1);
        let at_time = view.last_leader_change().expect("timestamped");
        assert!(at_time >= before && at_time <= chrono::Utc::now());
        assert_eq!(
            view.publish_with(at(Some("b:5432"))),
            None,
            "the same leader again is not a move"
        );
        assert_eq!(view.leader_changes_total(), 1);
    }

    #[test]
    fn static_epoch_counts_leader_changes() {
        let view = RoutingView::new(false);
        let mut nodes = cluster();
        let publish = |view: &RoutingView, nodes: &[NodeFacts<'static>]| {
            view.publish_with(|| RoutingSnapshot::build(nodes, Authority::Static, Instant::now()))
        };
        publish(&view, &nodes);
        assert_eq!(view.load().epoch, 1);
        publish(&view, &nodes);
        assert_eq!(view.load().epoch, 1, "an unchanged leader keeps the epoch");
        nodes[0].healthy = false;
        publish(&view, &nodes);
        assert_eq!(view.load().epoch, 2);
        assert_eq!(view.load().leader, None);
    }
}
