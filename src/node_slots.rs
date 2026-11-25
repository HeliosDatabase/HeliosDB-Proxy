//! Per-node backend connection caps (H-05 slice 1) and counts.
//!
//! A node's cap is its `[[nodes]] max_connections`, else `[pool]
//! max_connections` (`0` = unlimited). It bounds the backend connections this
//! proxy keeps to the node — in use and parked idle alike — with a FIFO
//! semaphore per node. A connection carries its slot (an owned permit) for its
//! whole life, including while it sits in the idle pool; dropping the
//! connection releases the slot. An uncapped node's semaphore holds
//! `Semaphore::MAX_PERMITS`, so it never waits: its permits count the open
//! connections (`/pools`, `[pool] min_connections`). The caps are read once at
//! startup; a node first seen later (added by a SIGHUP reload) is uncapped.
//!
//! `[limits] max_concurrent_reconnects_per_node` bounds, per node, how many
//! backend connections the proxy is opening at once on a session's behalf
//! (a transaction-replay re-home, a reconnect after a lost backend, a standby
//! connection), from TCP connect through authentication. It meters the
//! reconnect wave a failover sends to the new primary. Default 16; `0`
//! takes no permit at all.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ProxyConfig;

/// A backend connection's hold on its node's slot (`None` only for a
/// connection opened outside the node accounting, e.g. in tests).
pub type Slot = Option<OwnedSemaphorePermit>;

/// Phrase every slot refusal carries, so the in-band health logic can tell a
/// proxy-side cap (the node is fine) from a failing backend.
pub const REFUSAL_MARKER: &str = "proxy connection cap";

struct NodeSlot {
    /// The cap; `None` = unlimited (the semaphore then only counts).
    limit: Option<usize>,
    sem: Arc<Semaphore>,
    /// Sessions waiting for a slot right now.
    waiting: AtomicUsize,
    waits: AtomicU64,
    refusals: AtomicU64,
    /// Connections opened to the node since startup.
    opened: AtomicU64,
}

impl NodeSlot {
    fn new(limit: usize) -> Self {
        let limit = (limit > 0).then_some(limit);
        Self {
            limit,
            sem: Arc::new(Semaphore::new(limit.unwrap_or(Semaphore::MAX_PERMITS))),
            waiting: AtomicUsize::new(0),
            waits: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
            opened: AtomicU64::new(0),
        }
    }

    /// Connections holding a slot now (in use and parked).
    fn open(&self) -> usize {
        let total = self.limit.unwrap_or(Semaphore::MAX_PERMITS);
        total - self.sem.available_permits().min(total)
    }

    fn take(&self, permit: OwnedSemaphorePermit) -> Slot {
        self.opened.fetch_add(1, Ordering::Relaxed);
        Some(permit)
    }
}

/// Counts one waiter while alive, so a wait dropped mid-way (a request's
/// `[pool_mode] acquire_timeout_secs` deadline, a client gone) never leaves
/// the gauge raised.
struct Waiting<'a>(&'a AtomicUsize);

impl<'a> Waiting<'a> {
    fn new(gauge: &'a AtomicUsize) -> Self {
        gauge.fetch_add(1, Ordering::Relaxed);
        Self(gauge)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
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

/// What `/pools` reports for one node, capped or not.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct NodeConnStats {
    pub node: String,
    /// The cap; `None` = unlimited.
    pub limit: Option<usize>,
    /// Backend connections open now, in use and parked.
    pub open: usize,
    /// Sessions waiting for a slot right now.
    pub waiting: usize,
    /// Connections opened since startup.
    pub opened: u64,
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

/// The slots of every node: the configured ones built at startup with their
/// caps, any other added uncapped when first dialed.
#[derive(Default)]
pub struct NodeSlots {
    nodes: dashmap::DashMap<String, Arc<NodeSlot>>,
    /// Some configured node is capped: only then can a session wait for a
    /// slot ([`Self::has_waiters`]).
    any_capped: bool,
    /// `[limits] max_concurrent_reconnects_per_node`; 0 = unlimited.
    reconnect_limit: usize,
    /// One FIFO semaphore per node being dialed (created on first use, so a
    /// node added by a reload or a topology provider is covered too).
    reconnecting: dashmap::DashMap<String, Arc<Semaphore>>,
    reconnect_waits: AtomicU64,
    reconnect_refusals: AtomicU64,
    /// Most reconnects one node had in progress at once, as each dial got its
    /// permit.
    reconnect_peak: AtomicUsize,
}

/// What `/metrics` reports for the reconnect limit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ReconnectStats {
    pub limit_per_node: usize,
    pub in_progress: usize,
    /// Most connections being opened at once to one node since startup.
    pub peak_per_node: usize,
    pub waits: u64,
    pub refusals: u64,
}

/// No reconnect permit came free within the wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconnectTimeout {
    pub node: String,
    pub limit: usize,
}

impl std::fmt::Display for ReconnectTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node {} already has max_concurrent_reconnects_per_node ({}) connections being opened: {REFUSAL_MARKER} reached",
            self.node, self.limit
        )
    }
}

impl NodeSlots {
    pub fn from_config(config: &ProxyConfig) -> Self {
        let nodes: dashmap::DashMap<String, Arc<NodeSlot>> = config
            .nodes
            .iter()
            .map(|n| {
                (
                    n.address().to_string(),
                    Arc::new(NodeSlot::new(config.node_connection_cap(n))),
                )
            })
            .collect();
        let any_capped = nodes.iter().any(|e| e.value().limit.is_some());
        Self {
            nodes,
            any_capped,
            reconnect_limit: config.limits.max_concurrent_reconnects_per_node,
            ..Self::default()
        }
    }

    /// A session is waiting for a slot of `node` now. Without a capped node
    /// nobody can wait, and this is one field read.
    pub fn has_waiters(&self, node: &str) -> bool {
        self.any_capped
            && self
                .nodes
                .get(node)
                .is_some_and(|s| s.waiting.load(Ordering::Relaxed) > 0)
    }

    /// `node`'s slot, an uncapped one created for a node first seen now.
    fn slot(&self, node: &str) -> Arc<NodeSlot> {
        if let Some(slot) = self.nodes.get(node) {
            return Arc::clone(&slot);
        }
        Arc::clone(
            &self
                .nodes
                .entry(node.to_string())
                .or_insert_with(|| Arc::new(NodeSlot::new(0))),
        )
    }

    /// The cap of `node`; `None` = unlimited.
    pub fn limit(&self, node: &str) -> Option<usize> {
        self.nodes.get(node).and_then(|s| s.limit)
    }

    /// Backend connections open to `node` now, in use and parked.
    pub fn open(&self, node: &str) -> usize {
        self.nodes.get(node).map_or(0, |s| s.open())
    }

    /// A slot on `node` without waiting and without closing a parked
    /// connection: `None` when the node is full (the idle reaper's top-ups,
    /// which never compete with sessions).
    pub fn try_acquire(&self, node: &str) -> Option<Slot> {
        let slot = self.slot(node);
        let permit = slot.sem.clone().try_acquire_owned().ok()?;
        Some(slot.take(permit))
    }

    /// A permit to open a connection to `node` on a session's behalf, held
    /// from TCP connect through authentication. `None` at once when the limit
    /// is off; otherwise waits up to `wait` in FIFO order. Returns the wait
    /// when there was one.
    pub async fn acquire_reconnect(
        &self,
        node: &str,
        wait: Duration,
    ) -> Result<(Option<OwnedSemaphorePermit>, Option<Duration>), ReconnectTimeout> {
        if self.reconnect_limit == 0 {
            return Ok((None, None));
        }
        let sem = match self.reconnecting.get(node) {
            Some(s) => s.clone(),
            None => self
                .reconnecting
                .entry(node.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(self.reconnect_limit)))
                .clone(),
        };
        if let Ok(p) = sem.clone().try_acquire_owned() {
            self.note_reconnect_peak(&sem);
            return Ok((Some(p), None));
        }
        self.reconnect_waits.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        match tokio::time::timeout(wait, sem.clone().acquire_owned()).await {
            Ok(Ok(p)) => {
                self.note_reconnect_peak(&sem);
                Ok((Some(p), Some(started.elapsed())))
            }
            _ => {
                self.reconnect_refusals.fetch_add(1, Ordering::Relaxed);
                Err(ReconnectTimeout {
                    node: node.to_string(),
                    limit: self.reconnect_limit,
                })
            }
        }
    }

    /// A reconnect permit for `node` without waiting (the idle reaper's
    /// top-ups): `None` when the node's reconnects are all in progress;
    /// `Some(None)` when the limit is off.
    pub fn try_acquire_reconnect(&self, node: &str) -> Option<Option<OwnedSemaphorePermit>> {
        if self.reconnect_limit == 0 {
            return Some(None);
        }
        let sem = self
            .reconnecting
            .entry(node.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(self.reconnect_limit)))
            .clone();
        let permit = sem.clone().try_acquire_owned().ok()?;
        self.note_reconnect_peak(&sem);
        Some(Some(permit))
    }

    fn note_reconnect_peak(&self, sem: &Semaphore) {
        let in_progress = self.reconnect_limit - sem.available_permits().min(self.reconnect_limit);
        self.reconnect_peak
            .fetch_max(in_progress, Ordering::Relaxed);
    }

    /// The reconnect limit's state; `None` when it is off.
    pub fn reconnect_stats(&self) -> Option<ReconnectStats> {
        (self.reconnect_limit > 0).then(|| ReconnectStats {
            limit_per_node: self.reconnect_limit,
            in_progress: self
                .reconnecting
                .iter()
                .map(|e| {
                    self.reconnect_limit - e.value().available_permits().min(self.reconnect_limit)
                })
                .sum(),
            peak_per_node: self.reconnect_peak.load(Ordering::Relaxed),
            waits: self.reconnect_waits.load(Ordering::Relaxed),
            refusals: self.reconnect_refusals.load(Ordering::Relaxed),
        })
    }

    /// Take a slot on `node` for a new backend connection. Uncapped: at
    /// once. Full: call `free_idle` once (it should close one idle pooled
    /// connection to `node`, returning whether it did) and retry, then wait
    /// up to `wait` in FIFO order. Returns the wait in `waited` when there was
    /// one.
    pub async fn acquire(
        &self,
        node: &str,
        wait: Duration,
        free_idle: impl FnOnce() -> bool,
    ) -> Result<(Slot, Option<Duration>), SlotTimeout> {
        let slot = self.slot(node);
        if let Ok(p) = slot.sem.clone().try_acquire_owned() {
            return Ok((slot.take(p), None));
        }
        // Counted as waiting before looking for an idle connection to close:
        // a session parking one while this runs either finds the waiter
        // (`has_waiters`) and closes it, or parks it before `free_idle`
        // looks, which then closes it. No gap leaves a parked connection
        // holding the slot this request waits for.
        let waiting = Waiting::new(&slot.waiting);
        if free_idle() {
            if let Ok(p) = slot.sem.clone().try_acquire_owned() {
                return Ok((slot.take(p), None));
            }
        }
        slot.waits.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let acquired = tokio::time::timeout(wait, slot.sem.clone().acquire_owned()).await;
        drop(waiting);
        match acquired {
            Ok(Ok(p)) => Ok((slot.take(p), Some(started.elapsed()))),
            _ => {
                slot.refusals.fetch_add(1, Ordering::Relaxed);
                Err(SlotTimeout {
                    node: node.to_string(),
                    limit: slot.limit.unwrap_or(Semaphore::MAX_PERMITS),
                })
            }
        }
    }

    /// Per capped node: limit, slots in use, waits and refusals since start.
    pub fn stats(&self) -> Vec<NodeSlotStats> {
        let mut out: Vec<NodeSlotStats> = self
            .nodes
            .iter()
            .filter_map(|e| {
                let s = e.value();
                s.limit.map(|limit| NodeSlotStats {
                    node: e.key().clone(),
                    limit,
                    in_use: s.open(),
                    waits: s.waits.load(Ordering::Relaxed),
                    refusals: s.refusals.load(Ordering::Relaxed),
                })
            })
            .collect();
        out.sort_by(|a, b| a.node.cmp(&b.node));
        out
    }

    /// Per node, capped or not: what `/pools` reports.
    pub fn conn_stats(&self) -> Vec<NodeConnStats> {
        let mut out: Vec<NodeConnStats> = self
            .nodes
            .iter()
            .map(|e| {
                let s = e.value();
                NodeConnStats {
                    node: e.key().clone(),
                    limit: s.limit,
                    open: s.open(),
                    waiting: s.waiting.load(Ordering::Relaxed),
                    opened: s.opened.load(Ordering::Relaxed),
                    waits: s.waits.load(Ordering::Relaxed),
                    refusals: s.refusals.load(Ordering::Relaxed),
                }
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
    async fn reconnects_are_bounded_per_node_when_configured() {
        let mut config = ProxyConfig::default();
        config.add_node("db:5432", "primary").unwrap();
        assert_eq!(config.limits.max_concurrent_reconnects_per_node, 16);
        assert_eq!(
            NodeSlots::from_config(&config)
                .reconnect_stats()
                .map(|r| r.limit_per_node),
            Some(16),
            "on by default"
        );
        config.limits.max_concurrent_reconnects_per_node = 0;
        let off = NodeSlots::from_config(&config);
        let (p, w) = off
            .acquire_reconnect("db:5432", Duration::from_millis(1))
            .await
            .unwrap();
        assert!(p.is_none() && w.is_none());
        assert_eq!(off.reconnect_stats(), None);

        config.limits.max_concurrent_reconnects_per_node = 2;
        let slots = NodeSlots::from_config(&config);
        let wait = Duration::from_millis(20);
        let a = slots.acquire_reconnect("db:5432", wait).await.unwrap().0;
        let b = slots.acquire_reconnect("db:5432", wait).await.unwrap().0;
        assert!(a.is_some() && b.is_some());
        assert_eq!(slots.reconnect_stats().unwrap().peak_per_node, 2);
        // Another node (e.g. one a reload added) has its own permits.
        assert!(slots
            .acquire_reconnect("new:5432", wait)
            .await
            .unwrap()
            .0
            .is_some());
        // The third dial to db waits, then is refused with the marker.
        let err = slots.acquire_reconnect("db:5432", wait).await.unwrap_err();
        assert!(err
            .to_string()
            .contains("max_concurrent_reconnects_per_node (2)"));
        assert!(err.to_string().contains(REFUSAL_MARKER));
        // A finished dial admits a waiter.
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(a);
        });
        let (c, waited) = slots
            .acquire_reconnect("db:5432", Duration::from_secs(5))
            .await
            .unwrap();
        release.await.unwrap();
        assert!(c.is_some() && waited.is_some());
        assert_eq!(
            slots.reconnect_stats(),
            Some(ReconnectStats {
                limit_per_node: 2,
                in_progress: 2,
                peak_per_node: 2,
                waits: 2,
                refusals: 1,
            })
        );
        drop((b, c));
        let idle = slots.reconnect_stats().unwrap();
        assert_eq!((idle.in_progress, idle.peak_per_node), (0, 2));
    }

    /// An uncapped node never waits; its slots only count its connections.
    #[tokio::test]
    async fn uncapped_nodes_never_wait_and_are_counted() {
        let slots = capped(1);
        let mut held = Vec::new();
        for _ in 0..3 {
            let (slot, waited) = slots
                .acquire("other:5432", Duration::from_millis(1), || false)
                .await
                .unwrap();
            assert!(slot.is_some() && waited.is_none());
            held.push(slot);
        }
        assert_eq!(slots.stats().len(), 1, "/metrics lists capped nodes only");
        assert_eq!(slots.open("other:5432"), 3);
        assert_eq!(slots.limit("other:5432"), None);
        held.pop();
        assert_eq!(slots.open("other:5432"), 2);
        let other = slots
            .conn_stats()
            .into_iter()
            .find(|s| s.node == "other:5432")
            .unwrap();
        assert_eq!((other.limit, other.open, other.opened), (None, 2, 3));
        // A node first seen now (added by a reload) is counted, uncapped.
        let late = slots.try_acquire("late:5432").expect("uncapped");
        assert!(late.is_some());
        assert_eq!(slots.open("late:5432"), 1);
    }

    /// `[pool] max_connections` caps every node; a `[[nodes]]
    /// max_connections` overrides it; 0 is unlimited.
    #[test]
    fn the_pool_cap_applies_to_every_node_unless_overridden() {
        let mut config = ProxyConfig::default();
        config.add_node("a:5432", "primary").unwrap();
        config.add_node("b:5432", "standby").unwrap();
        config.pool.max_connections = 8;
        config.nodes[1].max_connections = 3;
        let slots = NodeSlots::from_config(&config);
        assert_eq!(slots.limit("a:5432"), Some(8));
        assert_eq!(slots.limit("b:5432"), Some(3));
        assert_eq!(
            slots
                .stats()
                .iter()
                .map(|s| (s.node.as_str(), s.limit))
                .collect::<Vec<_>>(),
            [("a:5432", 8), ("b:5432", 3)]
        );
        config.pool.max_connections = 0;
        let slots = NodeSlots::from_config(&config);
        assert_eq!(slots.limit("a:5432"), None);
        assert_eq!(slots.limit("b:5432"), Some(3));
    }

    /// A session waiting for a full node's slot is seen (the release path
    /// then closes a connection instead of parking it); an uncapped node
    /// never has one.
    #[tokio::test]
    async fn a_waiter_for_a_full_node_is_seen() {
        let slots = Arc::new(capped(1));
        assert!(!slots.has_waiters("db:5432"));
        let held = slots.try_acquire("db:5432").expect("free");
        let waiter = {
            let slots = Arc::clone(&slots);
            tokio::spawn(async move {
                slots
                    .acquire("db:5432", Duration::from_secs(5), || false)
                    .await
                    .map(|(slot, _)| slot.is_some())
            })
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !slots.has_waiters("db:5432") {
            assert!(Instant::now() < deadline, "the waiter never showed");
            tokio::task::yield_now().await;
        }
        assert!(!slots.has_waiters("other:5432"));
        drop(held);
        assert_eq!(waiter.await.unwrap(), Ok(true));
        assert!(!slots.has_waiters("db:5432"));
        // A wait abandoned by its caller (an outer deadline) is no waiter.
        let held = slots.try_acquire("db:5432").expect("free");
        let abandoned = tokio::time::timeout(
            Duration::from_millis(20),
            slots.acquire("db:5432", Duration::from_secs(5), || false),
        )
        .await;
        assert!(abandoned.is_err());
        assert!(!slots.has_waiters("db:5432"));
        assert_eq!(slots.stats()[0].refusals, 0, "not refused by the node cap");
        drop(held);
        let uncapped = NodeSlots::from_config(&{
            let mut config = ProxyConfig::default();
            config.add_node("db:5432", "primary").unwrap();
            config
        });
        assert!(!uncapped.any_capped);
    }

    /// The top-ups' slot: never waits, never frees an idle connection.
    #[tokio::test]
    async fn try_acquire_refuses_a_full_node_at_once() {
        let slots = capped(1);
        let first = slots.try_acquire("db:5432").expect("free");
        assert!(first.is_some());
        assert!(slots.try_acquire("db:5432").is_none());
        drop(first);
        assert!(slots.try_acquire("db:5432").is_some());
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
