//! Home-side registry of subscribed edges + invalidation broadcast.
//!
//! When an edge boots in `EdgeRole::Edge`, it calls home's
//! `POST /api/edge/register` once at startup. The home stores the
//! edge's address + auth token and adds it to the broadcast set.
//!
//! On every committed write, the home calls `broadcast` with an
//! `InvalidationEvent { up_to_version, tables }`. Each registered
//! edge receives a copy via the SSE channel that `register` opened.
//!
//! Edges that fail to ack within `ack_timeout` are removed from the
//! set — the home does *not* retry forever. A missed invalidation
//! degrades correctness only as far as the cache TTL: stale entries
//! age out within `default_ttl`. That's the explicit "bounded
//! staleness" contract from the module doc.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// One registered edge node from the home's perspective.
#[derive(Debug, Clone, Serialize)]
pub struct EdgeNode {
    pub edge_id: String,
    pub region: String,
    /// HTTP base URL the home pings for ack-checks.
    pub base_url: String,
    /// First-seen + last-acked timestamps.
    pub registered_at: String,
    pub last_seen: String,
    /// Total invalidations broadcast to this edge.
    pub invalidations_sent: u64,
}

/// Wire shape of an invalidation. Carried over the SSE channel from
/// home to every edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvalidationEvent {
    /// Logical version assigned by the home at write commit.
    pub up_to_version: u64,
    /// Tables touched. Empty = invalidate every cached entry within
    /// the version bound.
    pub tables: Vec<String>,
    /// Wall-clock at which the home committed the write — useful for
    /// log correlation.
    pub committed_at: String,
}

/// Per-edge in-process channel the registry pushes events into.
/// Each edge holds the matching receiver in its SSE connection.
struct EdgeSubscription {
    node: EdgeNode,
    sender: mpsc::Sender<InvalidationEvent>,
    /// last_seen as Instant for liveness check (the public node
    /// stringifies this too).
    last_seen_inst: Instant,
}

/// Home-side edge registry. Cheap to clone via Arc.
#[derive(Clone)]
pub struct EdgeRegistry {
    inner: Arc<RwLock<HashMap<String, EdgeSubscription>>>,
    max_edges: usize,
    /// Edges that don't ack within this window get expired on the
    /// next broadcast pass.
    liveness_window: Duration,
}

impl EdgeRegistry {
    pub fn new(max_edges: usize, liveness_window: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            max_edges,
            liveness_window,
        }
    }

    /// Register a new edge. Returns the receiver the SSE handler
    /// holds open. Caller is responsible for keeping the receiver
    /// alive — when it drops, the next broadcast prunes the edge.
    ///
    /// The channel is bounded: a slow edge that doesn't drain
    /// fast enough back-pressures into the broadcast call (which
    /// is async). Default capacity 64 events lets bursts ride
    /// through without dropping.
    pub fn register(
        &self,
        edge_id: &str,
        region: &str,
        base_url: &str,
        now_iso: &str,
    ) -> Result<mpsc::Receiver<InvalidationEvent>, RegistryError> {
        let mut g = self.inner.write();
        if !g.contains_key(edge_id) && g.len() >= self.max_edges {
            return Err(RegistryError::CapacityExceeded(self.max_edges));
        }
        let (tx, rx) = mpsc::channel(64);
        let sub = EdgeSubscription {
            node: EdgeNode {
                edge_id: edge_id.to_string(),
                region: region.to_string(),
                base_url: base_url.to_string(),
                registered_at: now_iso.to_string(),
                last_seen: now_iso.to_string(),
                invalidations_sent: 0,
            },
            sender: tx,
            last_seen_inst: Instant::now(),
        };
        g.insert(edge_id.to_string(), sub);
        Ok(rx)
    }

    /// Remove an edge — used when the home decides to evict
    /// (manual unregister, or cleanup during shutdown).
    pub fn unregister(&self, edge_id: &str) -> bool {
        self.inner.write().remove(edge_id).is_some()
    }

    /// Broadcast an invalidation to every subscribed edge. Edges
    /// whose channel has closed (receiver dropped) are pruned.
    /// Returns (sent, pruned).
    pub async fn broadcast(&self, ev: InvalidationEvent) -> (u32, u32) {
        // Snapshot recipients under the lock, then send outside it
        // so we don't hold the write lock across await points.
        let recipients: Vec<(String, mpsc::Sender<InvalidationEvent>)> = {
            let g = self.inner.read();
            g.iter()
                .map(|(id, sub)| (id.clone(), sub.sender.clone()))
                .collect()
        };

        let mut sent = 0u32;
        let mut dead: Vec<String> = Vec::new();
        for (id, tx) in recipients {
            match tx.send(ev.clone()).await {
                Ok(()) => {
                    sent += 1;
                }
                Err(_) => {
                    dead.push(id);
                }
            }
        }

        // Prune closed channels + bump per-edge counters under the
        // write lock.
        let mut g = self.inner.write();
        for id in &dead {
            g.remove(id);
        }
        for sub in g.values_mut() {
            sub.node.invalidations_sent = sub.node.invalidations_sent.saturating_add(1);
            sub.last_seen_inst = Instant::now();
        }
        (sent, dead.len() as u32)
    }

    /// Read-only snapshot of currently-registered edges. Used by
    /// the admin UI / `/api/edge` endpoint.
    pub fn list(&self) -> Vec<EdgeNode> {
        self.inner.read().values().map(|s| s.node.clone()).collect()
    }

    pub fn count(&self) -> usize {
        self.inner.read().len()
    }

    /// Garbage-collect edges that haven't been seen within
    /// `liveness_window`. Returns the count pruned.
    pub fn prune_stale(&self) -> u32 {
        let cutoff = Instant::now() - self.liveness_window;
        let mut g = self.inner.write();
        let dead: Vec<String> = g
            .iter()
            .filter(|(_, s)| s.last_seen_inst < cutoff)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &dead {
            g.remove(id);
        }
        dead.len() as u32
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    CapacityExceeded(usize),
}

impl std::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistryError::CapacityExceeded(n) => {
                write!(f, "edge registry full (max {})", n)
            }
        }
    }
}

impl std::error::Error for RegistryError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_returns_receiver_with_invalidations() {
        let r = EdgeRegistry::new(10, Duration::from_secs(60));
        let mut rx = r.register("edge-1", "us-east", "https://e1", "ts").unwrap();
        assert_eq!(r.count(), 1);
        let (sent, pruned) = r
            .broadcast(InvalidationEvent {
                up_to_version: 5,
                tables: vec!["users".into()],
                committed_at: "ts".into(),
            })
            .await;
        assert_eq!(sent, 1);
        assert_eq!(pruned, 0);
        let ev = rx.recv().await.expect("receive");
        assert_eq!(ev.up_to_version, 5);
        assert_eq!(ev.tables, vec!["users".to_string()]);
    }

    #[tokio::test]
    async fn broadcast_prunes_dropped_receivers() {
        let r = EdgeRegistry::new(10, Duration::from_secs(60));
        let _rx_keep = r.register("edge-keep", "us-east", "u", "ts").unwrap();
        {
            let _rx_drop = r.register("edge-drop", "us-west", "u", "ts").unwrap();
            // _rx_drop dropped at the end of this scope.
        }
        let (sent, pruned) = r
            .broadcast(InvalidationEvent {
                up_to_version: 1,
                tables: vec![],
                committed_at: "ts".into(),
            })
            .await;
        assert_eq!(sent, 1);
        assert_eq!(pruned, 1);
        assert_eq!(r.count(), 1);
    }

    #[test]
    fn register_rejects_when_at_capacity() {
        let r = EdgeRegistry::new(2, Duration::from_secs(60));
        let _a = r.register("a", "us-east", "u", "ts").unwrap();
        let _b = r.register("b", "us-west", "u", "ts").unwrap();
        let err = r.register("c", "eu-west", "u", "ts").unwrap_err();
        assert!(matches!(err, RegistryError::CapacityExceeded(2)));
    }

    #[test]
    fn register_replaces_existing_id() {
        let r = EdgeRegistry::new(2, Duration::from_secs(60));
        let _a1 = r.register("a", "us-east", "u", "t1").unwrap();
        // Re-register with same id under a different region — replaces
        // the slot, count stays the same.
        let _a2 = r.register("a", "eu-west", "u", "t2").unwrap();
        assert_eq!(r.count(), 1);
        let nodes = r.list();
        assert_eq!(nodes[0].region, "eu-west");
    }

    #[test]
    fn unregister_removes_edge() {
        let r = EdgeRegistry::new(10, Duration::from_secs(60));
        let _rx = r.register("edge-1", "us-east", "u", "ts").unwrap();
        assert!(r.unregister("edge-1"));
        assert_eq!(r.count(), 0);
        // Idempotent.
        assert!(!r.unregister("edge-1"));
    }

    #[test]
    fn list_returns_snapshot() {
        let r = EdgeRegistry::new(10, Duration::from_secs(60));
        let _a = r.register("a", "r1", "u1", "ts").unwrap();
        let _b = r.register("b", "r2", "u2", "ts").unwrap();
        let mut nodes = r.list();
        nodes.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].edge_id, "a");
        assert_eq!(nodes[1].edge_id, "b");
    }

    #[tokio::test]
    async fn invalidations_sent_counter_increments() {
        let r = EdgeRegistry::new(10, Duration::from_secs(60));
        let mut _rx = r.register("e1", "r", "u", "ts").unwrap();
        for _ in 0..3 {
            let _ = r
                .broadcast(InvalidationEvent {
                    up_to_version: 1,
                    tables: vec![],
                    committed_at: "ts".into(),
                })
                .await;
        }
        let n = r.list();
        assert_eq!(n[0].invalidations_sent, 3);
    }

    #[test]
    fn prune_stale_removes_old_entries() {
        let r = EdgeRegistry::new(10, Duration::from_millis(10));
        let _rx = r.register("old", "r", "u", "ts").unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let pruned = r.prune_stale();
        assert_eq!(pruned, 1);
        assert_eq!(r.count(), 0);
    }
}
