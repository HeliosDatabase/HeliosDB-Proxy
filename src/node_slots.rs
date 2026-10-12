//! Per-node backend connection caps (H-05 slice 1).
//!
//! `[[nodes]] max_connections` bounds the backend connections this proxy
//! keeps to a node — in use and parked idle alike — with a FIFO semaphore per
//! capped node. A connection carries its slot (an owned permit) for its whole
//! life, including while it sits in the idle pool; dropping the connection
//! releases the slot. Uncapped nodes have no entry, so acquiring for them is
//! one map lookup.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ProxyConfig;

/// A backend connection's hold on its node's cap; `None` for uncapped nodes.
pub type Slot = Option<OwnedSemaphorePermit>;

/// Phrase every slot refusal carries, so the in-band health logic can tell a
/// proxy-side cap (the node is fine) from a failing backend.
pub const REFUSAL_MARKER: &str = "proxy connection cap";

struct NodeSlot {
    limit: usize,
    sem: Arc<Semaphore>,
    waits: AtomicU64,
    refusals: AtomicU64,
}

/// What `/metrics` reports for one capped node.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct NodeSlotStats {
    pub node: String,
    pub limit: usize,
    pub in_use: usize,
    pub waits: u64,
    pub refusals: u64,
}

/// No slot came free within the wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotTimeout {
    pub node: String,
    pub limit: usize,
}

impl std::fmt::Display for SlotTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node {} is at its max_connections ({}): {REFUSAL_MARKER} reached",
            self.node, self.limit
        )
    }
}

/// The caps of every node with `max_connections > 0`. Built once at startup.
#[derive(Default)]
pub struct NodeSlots {
    nodes: HashMap<String, NodeSlot>,
}

impl NodeSlots {
    pub fn from_config(config: &ProxyConfig) -> Self {
        let nodes = config
            .nodes
            .iter()
            .filter(|n| n.max_connections > 0)
            .map(|n| {
                (
                    n.address().to_string(),
                    NodeSlot {
                        limit: n.max_connections,
                        sem: Arc::new(Semaphore::new(n.max_connections)),
                        waits: AtomicU64::new(0),
                        refusals: AtomicU64::new(0),
                    },
                )
            })
            .collect();
        Self { nodes }
    }

    /// Take a slot on `node` for a new backend connection. Uncapped: `None`
    /// at once. Full: call `free_idle` once (it should close one idle pooled
    /// connection to `node`, returning whether it did) and retry, then wait
    /// up to `wait` in FIFO order. Returns the wait in `waited` when there was
    /// one.
    pub async fn acquire(
        &self,
        node: &str,
        wait: Duration,
        free_idle: impl FnOnce() -> bool,
    ) -> Result<(Slot, Option<Duration>), SlotTimeout> {
        let Some(slot) = self.nodes.get(node) else {
            return Ok((None, None));
        };
        if let Ok(p) = slot.sem.clone().try_acquire_owned() {
            return Ok((Some(p), None));
        }
        if free_idle() {
            if let Ok(p) = slot.sem.clone().try_acquire_owned() {
                return Ok((Some(p), None));
            }
        }
        slot.waits.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        match tokio::time::timeout(wait, slot.sem.clone().acquire_owned()).await {
            Ok(Ok(p)) => Ok((Some(p), Some(started.elapsed()))),
            _ => {
                slot.refusals.fetch_add(1, Ordering::Relaxed);
                Err(SlotTimeout {
                    node: node.to_string(),
                    limit: slot.limit,
                })
            }
        }
    }

    /// Per capped node: limit, slots in use, waits and refusals since start.
    pub fn stats(&self) -> Vec<NodeSlotStats> {
        let mut out: Vec<NodeSlotStats> = self
            .nodes
            .iter()
            .map(|(node, s)| NodeSlotStats {
                node: node.clone(),
                limit: s.limit,
                in_use: s.limit - s.sem.available_permits().min(s.limit),
                waits: s.waits.load(Ordering::Relaxed),
                refusals: s.refusals.load(Ordering::Relaxed),
            })
            .collect();
        out.sort_by(|a, b| a.node.cmp(&b.node));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capped(limit: usize) -> NodeSlots {
        let mut config = ProxyConfig::default();
        config.add_node("db:5432", "primary").unwrap();
        config.add_node("other:5432", "standby").unwrap();
        config.nodes[0].max_connections = limit;
        NodeSlots::from_config(&config)
    }

    #[tokio::test]
    async fn uncapped_nodes_need_no_slot() {
        let slots = capped(1);
        let (slot, waited) = slots
            .acquire("other:5432", Duration::from_millis(1), || false)
            .await
            .unwrap();
        assert!(slot.is_none() && waited.is_none());
        assert_eq!(slots.stats().len(), 1);
    }

    #[tokio::test]
    async fn a_full_node_frees_an_idle_connection_then_waits_then_refuses() {
        let slots = capped(1);
        let held = slots
            .acquire("db:5432", Duration::from_millis(1), || false)
            .await
            .unwrap()
            .0;
        assert_eq!(slots.stats()[0].in_use, 1);

        // Nothing idle to free and nobody releases: refused after the wait.
        let err = slots
            .acquire("db:5432", Duration::from_millis(20), || false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("max_connections (1)"));
        assert_eq!(slots.stats()[0].refusals, 1);

        // Freeing an idle connection (dropping its slot) hands it over at once.
        let mut holder = Some(held);
        let (slot, waited) = slots
            .acquire("db:5432", Duration::from_millis(20), || {
                holder.take();
                true
            })
            .await
            .unwrap();
        assert!(slot.is_some() && waited.is_none());

        // A waiter is admitted when a slot is released within the wait.
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(slot);
        });
        let (slot, waited) = slots
            .acquire("db:5432", Duration::from_secs(5), || false)
            .await
            .unwrap();
        releaser.await.unwrap();
        assert!(slot.is_some() && waited.is_some());
        assert_eq!(slots.stats()[0].waits, 2);
    }
}
