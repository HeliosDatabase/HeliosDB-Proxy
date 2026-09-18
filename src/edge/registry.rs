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
//! Delivery is best-effort by design: a full per-edge buffer means
//! that edge just misses the event (it is *not* pruned for being
//! slow — only closed channels and liveness-window expiry prune).
//! A missed invalidation degrades correctness only as far as the
//! cache TTL: stale entries age out within `default_ttl`. That's the
//! explicit "bounded staleness" contract from the module doc.

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// How many recent invalidations the home keeps for warm resumption
/// (C-03). Bounded so replay memory is a fixed ~`cap` events per home,
/// never proportional to write rate.
const REGISTRY_HISTORY_CAP: usize = 256;

/// Parse a `boot:seq` resumption cursor.
fn parse_cursor(cursor: &str) -> Option<(u64, u64)> {
    let (boot, seq) = cursor.split_once(':')?;
    Some((boot.parse().ok()?, seq.parse().ok()?))
}

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
    /// The home's per-boot epoch. A restarted home resets its version
    /// clock, so version bounds are only comparable within one epoch;
    /// an edge that observes a NEW epoch flushes its cache and
    /// re-syncs its clock. `0` (the serde default, for events from
    /// homes that predate this field) disables epoch handling.
    #[serde(default)]
    pub epoch: u64,
    /// Monotonic stream offset assigned by the home when the event is
    /// broadcast (C-03). `0` (the serde default) means unsequenced —
    /// an event from a home that predates cursor resumption.
    #[serde(default)]
    pub seq: u64,
}

/// Outcome of a cursor-aware (re)subscribe (C-03).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    /// The home replayed everything the edge missed. The cache is warm;
    /// only the queued/streamed events need applying.
    Warm {
        /// Number of history events replayed into the channel.
        replayed: usize,
        /// Highest stream offset the edge is caught up through.
        upto: u64,
    },
    /// The cursor was absent, came from a different home boot, or is
    /// older than the bounded history. The edge must flush its cache.
    Gap {
        /// Highest stream offset represented by the forced flush.
        upto: u64,
    },
}

/// Bounded ring of recent invalidations with monotonic offsets, so a
/// reconnecting edge can replay what it missed (C-03).
struct InvalidationLog {
    next_seq: u64,
    history: VecDeque<InvalidationEvent>,
    cap: usize,
}

impl InvalidationLog {
    fn new(cap: usize) -> Self {
        Self {
            next_seq: 0,
            history: VecDeque::new(),
            cap,
        }
    }

    /// Assign the next offset, record a copy (trimming the oldest beyond
    /// `cap`), and return the offset.
    fn record(&mut self, ev: &mut InvalidationEvent) -> u64 {
        ev.seq = self.next_seq;
        self.next_seq += 1;
        self.history.push_back(ev.clone());
        while self.history.len() > self.cap {
            self.history.pop_front();
        }
        ev.seq
    }

    /// Queue history after `boot:seq` into `tx`. Bounded by the channel
    /// capacity: more to replay than the channel can hold means a flush
    /// is safer than a silently truncated history.
    fn resume(
        &self,
        home_boot: u64,
        cursor_boot: u64,
        cursor: u64,
        tx: &mpsc::Sender<InvalidationEvent>,
    ) -> Resume {
        let upto = self.next_seq.saturating_sub(1);
        if cursor_boot != home_boot || self.next_seq == 0 {
            return Resume::Gap { upto }; // different boot, or nothing streamed yet
        }
        let last = self.next_seq - 1;
        if cursor >= last {
            return Resume::Warm {
                replayed: 0,
                upto: last,
            }; // already caught up
        }
        match self.history.front() {
            Some(first) if first.seq <= cursor + 1 => {
                let replay: Vec<&InvalidationEvent> =
                    self.history.iter().filter(|e| e.seq > cursor).collect();
                if replay.len() >= 60 {
                    return Resume::Gap { upto };
                }
                let mut replayed = 0usize;
                for ev in replay {
                    if tx.try_send(ev.clone()).is_err() {
                        return Resume::Gap { upto };
                    }
                    replayed += 1;
                }
                Resume::Warm { replayed, upto }
            }
            _ => Resume::Gap { upto },
        }
    }
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
    /// Per-boot identity mixed into resumption cursors: a cursor from a
    /// previous home process (or a different home) can never be mistaken
    /// for a position in this boot's stream.
    boot_id: u64,
    /// Bounded replay history (C-03).
    log: Arc<Mutex<InvalidationLog>>,
    /// Edges that don't ack within this window get expired on the
    /// next broadcast pass.
    liveness_window: Duration,
}

impl EdgeRegistry {
    pub fn new(max_edges: usize, liveness_window: Duration) -> Self {
        let boot_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(1);
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            max_edges,
            boot_id,
            log: Arc::new(Mutex::new(InvalidationLog::new(REGISTRY_HISTORY_CAP))),
            liveness_window,
        }
    }

    /// Register a new edge. Returns the receiver the SSE handler
    /// holds open. Caller is responsible for keeping the receiver
    /// alive — when it drops, the next broadcast prunes the edge.
    ///
    /// The channel is bounded: a slow edge that doesn't drain fast
    /// enough starts *missing* events (`broadcast` uses `try_send`,
    /// never blocking the fan-out on one consumer). Capacity 64
    /// lets bursts ride through without dropping.
    pub fn register(
        &self,
        edge_id: &str,
        region: &str,
        base_url: &str,
        now_iso: &str,
    ) -> Result<mpsc::Receiver<InvalidationEvent>, RegistryError> {
        self.register_at(edge_id, region, base_url, now_iso, None)
            .map(|(rx, _)| rx)
    }

    /// The home's per-boot stream identity (C-03 cursors).
    pub fn boot_id(&self) -> u64 {
        self.boot_id
    }

    /// Cursor-aware registration (C-03): when the edge presents a valid
    /// `boot:seq` cursor still inside the bounded history, the missed
    /// events are replayed into the returned channel before the
    /// subscription goes live, and the edge may keep its warm cache.
    pub fn register_at(
        &self,
        edge_id: &str,
        region: &str,
        base_url: &str,
        now_iso: &str,
        cursor: Option<&str>,
    ) -> Result<(mpsc::Receiver<InvalidationEvent>, Resume), RegistryError> {
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
            sender: tx.clone(),
            last_seen_inst: Instant::now(),
        };
        let resume = match (self.boot_id, cursor.and_then(parse_cursor)) {
            (boot, Some((c_boot, c_seq))) => self.log.lock().resume(boot, c_boot, c_seq, &tx),
            _ => Resume::Gap { upto: 0 },
        };
        g.insert(edge_id.to_string(), sub);
        Ok((rx, resume))
    }

    /// Remove an edge — used when the home decides to evict
    /// (manual unregister, or cleanup during shutdown).
    pub fn unregister(&self, edge_id: &str) -> bool {
        self.inner.write().remove(edge_id).is_some()
    }

    /// Broadcast an invalidation to every subscribed edge. Edges
    /// whose channel has closed (receiver dropped) are pruned.
    /// Returns (sent, pruned).
    ///
    /// Per-edge `try_send` so one slow edge can never stall the whole
    /// fan-out (this runs on the write path). A full buffer counts as
    /// not-sent — the edge keeps its slot (only closed channels are
    /// pruned here; a persistently-stuck edge stops getting `last_seen`
    /// bumps and ages out via `prune_stale` instead) and the missed
    /// event is covered by the bounded-staleness TTL contract.
    pub async fn broadcast(&self, mut ev: InvalidationEvent) -> (u32, u32) {
        use tokio::sync::mpsc::error::TrySendError;

        // try_send never awaits, so doing the whole pass under the
        // write lock is safe (no lock held across an await point).
        let mut g = self.inner.write();
        self.log.lock().record(&mut ev);
        let mut sent = 0u32;
        let mut dead: Vec<String> = Vec::new();
        for (id, sub) in g.iter_mut() {
            match sub.sender.try_send(ev.clone()) {
                Ok(()) => {
                    sent += 1;
                    sub.node.invalidations_sent = sub.node.invalidations_sent.saturating_add(1);
                    sub.last_seen_inst = Instant::now();
                    // Keep the surfaced timestamp honest too (the event
                    // already carries a wall-clock string — reuse it).
                    sub.node.last_seen = ev.committed_at.clone();
                }
                Err(TrySendError::Full(_)) => {
                    tracing::warn!(
                        edge_id = %id,
                        up_to_version = ev.up_to_version,
                        "edge invalidation buffer full — event dropped for this edge \
                         (stale entries age out within the cache TTL)"
                    );
                }
                Err(TrySendError::Closed(_)) => {
                    dead.push(id.clone());
                }
            }
        }
        for id in &dead {
            g.remove(id);
        }
        (sent, dead.len() as u32)
    }

    /// Refresh an edge's liveness. Called by the SSE handler after
    /// every successfully-written heartbeat, so a healthy but
    /// write-idle home never GC-prunes its live subscribers —
    /// `prune_stale` is thereby demoted to a backstop for wedged or
    /// dead connections whose heartbeat writes stop succeeding.
    /// Returns false when the edge_id is not registered (e.g. a
    /// concurrent re-register replaced then dropped the slot).
    pub fn touch(&self, edge_id: &str, now_iso: &str) -> bool {
        let mut g = self.inner.write();
        match g.get_mut(edge_id) {
            Some(sub) => {
                sub.last_seen_inst = Instant::now();
                sub.node.last_seen = now_iso.to_string();
                true
            }
            None => false,
        }
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
        // `Instant - Duration` panics on underflow, and the monotonic
        // clock can start near zero (e.g. shortly after host boot). If
        // the process hasn't even been up for one liveness window,
        // nothing can be stale yet — prune nothing this pass.
        let Some(cutoff) = Instant::now().checked_sub(self.liveness_window) else {
            return 0;
        };
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
                seq: 0,
                up_to_version: 5,
                tables: vec!["users".into()],
                committed_at: "ts".into(),
                epoch: 0,
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
                seq: 0,
                up_to_version: 1,
                tables: vec![],
                committed_at: "ts".into(),
                epoch: 0,
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

    #[tokio::test]
    async fn reregister_same_id_replaces_sender_and_closes_old_receiver() {
        // Edge reconnect: after a network drop the edge re-subscribes
        // under the SAME edge_id while its stale SSE connection may
        // linger up to a heartbeat interval. The re-register must
        // replace the slot's sender — the old receiver then reads None
        // so the stale subscribe loop exits — and must succeed even at
        // max_edges capacity (no double-counted slot).
        let r = EdgeRegistry::new(1, Duration::from_secs(60));
        let mut rx_old = r.register("a", "us-east", "u", "t1").unwrap();
        let mut rx_new = r.register("a", "us-east", "u", "t2").unwrap();
        assert_eq!(r.count(), 1);

        // Old channel's sender was dropped by the replacement → closed.
        assert!(
            rx_old.recv().await.is_none(),
            "stale receiver must observe closure"
        );

        // Broadcasts reach only the new subscription.
        let (sent, pruned) = r
            .broadcast(InvalidationEvent {
                seq: 0,
                up_to_version: 9,
                tables: vec![],
                committed_at: "ts".into(),
                epoch: 0,
            })
            .await;
        assert_eq!((sent, pruned), (1, 0));
        let ev = rx_new.recv().await.expect("new receiver is live");
        assert_eq!(ev.up_to_version, 9);
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
                    seq: 0,
                    up_to_version: 1,
                    tables: vec![],
                    committed_at: "ts".into(),
                    epoch: 0,
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

    #[test]
    fn touch_keeps_edge_alive_across_prune() {
        // Heartbeat-touch semantics: a healthy idle edge whose SSE
        // heartbeats keep succeeding must never be GC-pruned; once the
        // touches stop, the liveness window reaps it.
        let r = EdgeRegistry::new(10, Duration::from_millis(50));
        let _rx = r.register("e1", "r", "u", "t0").unwrap();
        std::thread::sleep(Duration::from_millis(60));
        assert!(r.touch("e1", "t1"));
        assert_eq!(r.prune_stale(), 0, "touched edge survives the sweep");
        assert_eq!(r.count(), 1);
        assert_eq!(r.list()[0].last_seen, "t1", "surfaced timestamp advances");
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(r.prune_stale(), 1, "untouched edge ages out");
        assert_eq!(r.count(), 0);
        // Touching an unknown id is a no-op.
        assert!(!r.touch("e1", "t2"));
    }

    #[test]
    fn prune_stale_survives_near_boot_underflow() {
        // Regression: `Instant::now() - liveness_window` used to panic
        // when the monotonic clock was younger than the window. A window
        // larger than any possible uptime forces the checked_sub(None)
        // path deterministically: prune nothing, keep every edge.
        let r = EdgeRegistry::new(10, Duration::from_secs(u64::MAX));
        let _rx = r.register("young", "r", "u", "ts").unwrap();
        let pruned = r.prune_stale();
        assert_eq!(pruned, 0);
        assert_eq!(r.count(), 1);
    }

    #[tokio::test]
    async fn broadcast_full_buffer_drops_event_but_keeps_edge() {
        let r = EdgeRegistry::new(10, Duration::from_secs(60));
        let mut rx = r.register("slow", "r", "u", "ts").unwrap();

        // Fill the bounded channel (capacity 64) without draining.
        for i in 0..64 {
            let (sent, pruned) = r
                .broadcast(InvalidationEvent {
                    seq: 0,
                    up_to_version: i,
                    tables: vec![],
                    committed_at: "ts".into(),
                    epoch: 0,
                })
                .await;
            assert_eq!((sent, pruned), (1, 0));
        }

        // Buffer full: the event is dropped for this edge (not sent),
        // but the edge is NOT pruned — slowness isn't death.
        let (sent, pruned) = r
            .broadcast(InvalidationEvent {
                seq: 0,
                up_to_version: 64,
                tables: vec![],
                committed_at: "ts".into(),
                epoch: 0,
            })
            .await;
        assert_eq!(sent, 0);
        assert_eq!(pruned, 0);
        assert_eq!(r.count(), 1);
        // invalidations_sent counts only actual deliveries.
        assert_eq!(r.list()[0].invalidations_sent, 64);

        // Once the edge drains, delivery resumes.
        let first = rx.recv().await.expect("receive");
        assert_eq!(first.up_to_version, 0);
        let (sent, _) = r
            .broadcast(InvalidationEvent {
                seq: 0,
                up_to_version: 65,
                tables: vec![],
                committed_at: "ts".into(),
                epoch: 0,
            })
            .await;
        assert_eq!(sent, 1);
    }

    fn ev(v: u64) -> InvalidationEvent {
        InvalidationEvent {
            seq: 0,
            up_to_version: v,
            tables: vec!["t".into()],
            committed_at: "ts".into(),
            epoch: 0,
        }
    }

    #[tokio::test]
    async fn resume_replays_missed_tail_warm() {
        let r = EdgeRegistry::new(10, Duration::from_secs(60));
        let mut rx = r.register("e1", "us", "u", "ts").unwrap();
        r.broadcast(ev(1)).await;
        r.broadcast(ev(2)).await;
        let (seq1, ev1) = {
            let e = rx.recv().await.unwrap();
            (e.seq, e)
        };
        assert_eq!(seq1, 0);
        assert_eq!(ev1.up_to_version, 1);
        drop(rx); // network drop

        let cursor = format!("{}:{}", r.boot_id, seq1);
        let (mut rx2, resume) = r
            .register_at("e1", "us", "u", "ts2", Some(&cursor))
            .unwrap();
        assert_eq!(
            resume,
            Resume::Warm {
                replayed: 1,
                upto: 1
            }
        );
        let replayed = rx2.recv().await.unwrap();
        assert_eq!(replayed.seq, 1);
        assert_eq!(replayed.up_to_version, 2);
        assert!(rx2.try_recv().is_err(), "nothing else queued");
    }

    #[tokio::test]
    async fn resume_gap_when_cursor_foreign_stale_or_missing() {
        let r = EdgeRegistry::new(10, Duration::from_secs(60));
        // Nothing streamed yet.
        let (_rx, resume) = r.register_at("e1", "us", "u", "ts", Some("123:0")).unwrap();
        assert_eq!(resume, Resume::Gap { upto: 0 });
        // Foreign boot id.
        r.broadcast(ev(1)).await;
        let (_rx2, resume) = r
            .register_at("e1", "us", "u", "ts", Some("999999:0"))
            .unwrap();
        assert_eq!(resume, Resume::Gap { upto: 0 });
        // Malformed / absent cursor.
        let (_rx3, resume) = r.register_at("e1", "us", "u", "ts", Some("junk")).unwrap();
        assert_eq!(resume, Resume::Gap { upto: 0 });
        let (_rx4, resume) = r.register_at("e1", "us", "u", "ts", None).unwrap();
        assert_eq!(resume, Resume::Gap { upto: 0 });
        // Caught-up cursor resumes warm with no replay.
        let cursor = format!("{}:{}", r.boot_id, 0);
        let (_rx5, resume) = r.register_at("e1", "us", "u", "ts", Some(&cursor)).unwrap();
        assert_eq!(
            resume,
            Resume::Warm {
                replayed: 0,
                upto: 0
            }
        );
    }
}
