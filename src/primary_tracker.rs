//! Primary Tracker - Tracks current primary node for query routing
//!
//! Monitors cluster topology and maintains the current primary node
//! information. During switchover, updates are received from the
//! switchover coordinator to ensure queries are routed correctly.
//!
//! # Topology Providers
//!
//! The primary tracker uses a `TopologyProvider` trait to abstract over
//! different topology sources:
//!
//! - **HeliosDB**: Uses the internal `TopologyManager` from the replication
//!   subsystem (feature-gated behind `heliosdb-topology`).
//! - **PostgreSQL**: Polls `pg_stat_replication` / `pg_is_in_recovery()`
//!   to detect primary changes (feature-gated behind `postgres-topology`).
//! - **Manual/Standalone**: Programmatic set/clear via API calls.

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use uuid::Uuid;

// ── Topology provider trait ─────────────────────────────────────────

/// Information about a node in the cluster topology.
#[derive(Debug, Clone)]
pub struct TopologyNodeInfo {
    /// Node UUID
    pub node_id: Uuid,
    /// Client-facing address (host:port)
    pub client_addr: String,
    /// Whether the node is currently healthy
    pub is_healthy: bool,
}

/// Events emitted by a topology provider.
#[derive(Debug, Clone)]
pub enum TopologyEvent {
    /// The primary node changed.
    PrimaryChanged {
        old_primary: Option<Uuid>,
        new_primary: Uuid,
    },
    /// A node left the cluster.
    NodeLeft { node_id: Uuid },
    /// A node's health status changed.
    HealthChanged { node_id: Uuid, is_healthy: bool },
}

/// Trait abstracting topology discovery.
///
/// Implement this for any database backend (HeliosDB, PostgreSQL, etc.)
/// to enable automatic primary tracking.
pub trait TopologyProvider: Send + Sync + 'static {
    /// Subscribe to topology change events.
    fn subscribe(&self) -> broadcast::Receiver<TopologyEvent>;

    /// Get the current primary node, if one exists.
    fn get_primary(&self) -> Option<TopologyNodeInfo>;

    /// Look up a node by its UUID.
    fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo>;

    /// Whether the provider currently sees conflicting authority: more than
    /// one node claims to be the writable primary and the provider cannot
    /// decide between them. The tracker then drops its leader at once, so
    /// writes fail closed, instead of waiting for the lease to run out.
    fn authority_conflict(&self) -> bool {
        false
    }

    /// Polls that found more than one node claiming write authority since
    /// the provider started, resolved or not. Exported as
    /// `heliosdb_proxy_topology_conflicting_primaries_total`.
    fn conflicts_total(&self) -> u64 {
        0
    }

    /// The database timeline of the current leader, for a provider that
    /// reports one (Patroni). `None` without a leader or when unknown.
    fn leader_timeline(&self) -> Option<u64> {
        None
    }
}

/// Pick the primary among nodes that each report themselves writable
/// (H-01): the one on the strictly highest timeline — a promotion always
/// starts a new timeline, so an old primary that kept running is behind.
/// `None` when any timeline is unknown or the highest is shared: the
/// conflict cannot be resolved and nothing may be authorized.
pub fn choose_by_timeline(candidates: &[(usize, Option<u64>)]) -> Option<usize> {
    let mut best: Option<(usize, u64)> = None;
    let mut tied = false;
    for &(idx, timeline) in candidates {
        let t = timeline?;
        match best {
            None => best = Some((idx, t)),
            Some((_, b)) if t > b => {
                best = Some((idx, t));
                tied = false;
            }
            Some((_, b)) if t == b => tied = true,
            Some(_) => {}
        }
    }
    if tied {
        None
    } else {
        best.map(|(idx, _)| idx)
    }
}

// ── PostgreSQL topology provider ────────────────────────────────────

/// PostgreSQL-based topology provider.
///
/// Discovers the primary by polling `pg_is_in_recovery()` on each
/// configured node. Detects primary changes by comparing results
/// across polling intervals.
#[cfg(feature = "postgres-topology")]
pub struct PostgresTopologyProvider {
    /// Nodes to poll
    nodes: Vec<PostgresNode>,
    /// Event broadcaster
    event_tx: broadcast::Sender<TopologyEvent>,
    /// Current primary (cached)
    current_primary: RwLock<Option<TopologyNodeInfo>>,
    /// Polling interval
    poll_interval: Duration,
    /// Shared rustls client config for TLS negotiation. Built once at
    /// construction time from the Mozilla root set.
    tls_config: std::sync::Arc<rustls::ClientConfig>,
    /// TLS policy applied to every probe connection.
    tls_mode: crate::backend::TlsMode,
    /// More than one node is writable and the timelines do not decide it.
    conflict: std::sync::atomic::AtomicBool,
    /// Polls that found conflicting writable nodes (resolved or not).
    conflicts_total: AtomicU64,
}

#[cfg(feature = "postgres-topology")]
#[derive(Debug, Clone)]
pub struct PostgresNode {
    pub node_id: Uuid,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
}

#[cfg(feature = "postgres-topology")]
impl PostgresTopologyProvider {
    /// Create a new PostgreSQL topology provider.
    pub fn new(nodes: Vec<PostgresNode>) -> Self {
        let (event_tx, _) = broadcast::channel(16);
        Self {
            nodes,
            event_tx,
            current_primary: RwLock::new(None),
            poll_interval: Duration::from_secs(2),
            tls_config: crate::backend::tls::default_client_config(),
            tls_mode: crate::backend::TlsMode::Prefer,
            conflict: std::sync::atomic::AtomicBool::new(false),
            conflicts_total: AtomicU64::new(0),
        }
    }

    /// Polls that found more than one writable node.
    pub fn conflicts_total(&self) -> u64 {
        self.conflicts_total.load(Ordering::Relaxed)
    }

    /// Set polling interval.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set the TLS policy used when opening probe connections.
    pub fn with_tls_mode(mut self, mode: crate::backend::TlsMode) -> Self {
        self.tls_mode = mode;
        self
    }

    /// Start polling in the background.
    pub async fn start(&self) {
        let mut interval = tokio::time::interval(self.poll_interval);

        loop {
            interval.tick().await;
            self.poll_nodes().await;
        }
    }

    /// Poll all nodes and detect primary.
    async fn poll_nodes(&self) {
        let mut writable: Vec<usize> = Vec::new();

        for (idx, node) in self.nodes.iter().enumerate() {
            match self.probe_recovery(node).await {
                Ok(in_recovery) => {
                    // A node reporting `pg_is_in_recovery() = false` is
                    // writable. More than one (an old primary that kept
                    // running after a promotion) is resolved by timeline
                    // below, never by probe order.
                    if !in_recovery {
                        writable.push(idx);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        node = %node.host,
                        port = node.port,
                        error = %e,
                        "topology probe failed"
                    );
                    let _ = self.event_tx.send(TopologyEvent::HealthChanged {
                        node_id: node.node_id,
                        is_healthy: false,
                    });
                }
            }
        }

        let chosen = match writable.as_slice() {
            [] => None,
            [only] => Some(*only),
            many => {
                self.conflicts_total.fetch_add(1, Ordering::Relaxed);
                let mut candidates = Vec::with_capacity(many.len());
                for &idx in many {
                    let timeline = self.probe_timeline(&self.nodes[idx]).await.ok();
                    candidates.push((idx, timeline));
                }
                let pick = choose_by_timeline(&candidates);
                tracing::warn!(
                    writable = ?many
                        .iter()
                        .map(|&i| format!("{}:{}", self.nodes[i].host, self.nodes[i].port))
                        .collect::<Vec<_>>(),
                    timelines = ?candidates.iter().map(|c| c.1).collect::<Vec<_>>(),
                    resolved = pick.is_some(),
                    "topology: more than one writable node"
                );
                pick
            }
        };
        self.conflict
            .store(writable.len() > 1 && chosen.is_none(), Ordering::Relaxed);
        let next_primary = chosen.map(|idx| {
            let node = &self.nodes[idx];
            TopologyNodeInfo {
                node_id: node.node_id,
                client_addr: format!("{}:{}", node.host, node.port),
                is_healthy: true,
            }
        });

        let old_primary_id = self.current_primary.read().as_ref().map(|p| p.node_id);
        let new_primary_id = next_primary.as_ref().map(|p| p.node_id);
        if old_primary_id != new_primary_id {
            *self.current_primary.write() = next_primary;
            if let Some(new_id) = new_primary_id {
                let _ = self.event_tx.send(TopologyEvent::PrimaryChanged {
                    old_primary: old_primary_id,
                    new_primary: new_id,
                });
            }
        }
    }

    /// The node's current timeline (`pg_control_checkpoint().timeline_id`).
    /// Needs a role allowed to call the function (superuser, or granted
    /// EXECUTE); when it cannot be read the conflict stays unresolved.
    async fn probe_timeline(&self, node: &PostgresNode) -> crate::backend::BackendResult<u64> {
        let mut client = crate::backend::BackendClient::connect(&self.probe_config(node)).await?;
        let value = client
            .query_scalar("SELECT timeline_id::bigint FROM pg_control_checkpoint()")
            .await?;
        client.close().await;
        Ok(value.as_i64("timeline_id")?.unwrap_or(0).max(0) as u64)
    }

    fn probe_config(&self, node: &PostgresNode) -> crate::backend::BackendConfig {
        crate::backend::BackendConfig {
            host: node.host.clone(),
            port: node.port,
            user: node.user.clone(),
            password: node.password.clone(),
            database: Some(node.database.clone()),
            application_name: Some("helios-topology".into()),
            tls_mode: self.tls_mode,
            connect_timeout: self.poll_interval.min(Duration::from_secs(5)),
            query_timeout: self.poll_interval,
            tls_config: self.tls_config.clone(),
        }
    }

    /// Connect to a single node and run `SELECT pg_is_in_recovery()`.
    ///
    /// Returns `Ok(true)` if the node is a standby, `Ok(false)` for a
    /// primary. Errors propagate as `BackendError`.
    async fn probe_recovery(&self, node: &PostgresNode) -> crate::backend::BackendResult<bool> {
        let mut client = crate::backend::BackendClient::connect(&self.probe_config(node)).await?;
        let value = client.query_scalar("SELECT pg_is_in_recovery()").await?;
        client.close().await;
        Ok(value.as_bool("pg_is_in_recovery")?.unwrap_or(false))
    }
}

#[cfg(feature = "postgres-topology")]
impl TopologyProvider for PostgresTopologyProvider {
    fn subscribe(&self) -> broadcast::Receiver<TopologyEvent> {
        self.event_tx.subscribe()
    }

    fn get_primary(&self) -> Option<TopologyNodeInfo> {
        self.current_primary.read().clone()
    }

    fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo> {
        self.nodes
            .iter()
            .find(|n| n.node_id == id)
            .map(|n| TopologyNodeInfo {
                node_id: n.node_id,
                client_addr: format!("{}:{}", n.host, n.port),
                is_healthy: true, // Would be checked via actual connection
            })
    }

    fn authority_conflict(&self) -> bool {
        self.conflict.load(Ordering::Relaxed)
    }

    fn conflicts_total(&self) -> u64 {
        self.conflicts_total.load(Ordering::Relaxed)
    }
}

// ── Patroni topology provider ───────────────────────────────────────

/// A configured node the Patroni provider may resolve a leader to.
#[derive(Debug, Clone)]
pub struct PatroniNode {
    pub node_id: Uuid,
    /// `host:port` exactly as configured in `[[nodes]]`.
    pub address: String,
}

/// What one `GET /cluster` answer says about write authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatroniView {
    /// The configured node the running leader maps to.
    pub leader: Option<(Uuid, String)>,
    /// The leader's timeline (the authority epoch Patroni reports).
    pub timeline: Option<u64>,
    /// More than one member claims to be the running leader.
    pub conflict: bool,
    /// Why there is no leader, when there is none.
    pub reason: Option<String>,
}

/// Decide the writable leader from a Patroni `GET /cluster` body (H-01).
///
/// Only a member whose role is `leader` (or the pre-3.0 `master`) and whose
/// state is `running` counts; a `standby_leader` leads a standby cluster and
/// is never writable. The leader must be one of the configured nodes (by
/// `host:port`, host compared case-insensitively) — an unknown leader is not
/// authorized. Two running leaders are a conflict: nothing is authorized.
pub fn parse_patroni_cluster(body: &serde_json::Value, nodes: &[PatroniNode]) -> PatroniView {
    let none = |reason: String, conflict: bool| PatroniView {
        leader: None,
        timeline: None,
        conflict,
        reason: Some(reason),
    };
    let Some(members) = body.get("members").and_then(|m| m.as_array()) else {
        return none("response has no members array".into(), false);
    };
    let leaders: Vec<&serde_json::Value> = members
        .iter()
        .filter(|m| {
            let role = m.get("role").and_then(|r| r.as_str()).unwrap_or("");
            let state = m.get("state").and_then(|s| s.as_str()).unwrap_or("");
            (role == "leader" || role == "master") && state == "running"
        })
        .collect();
    let leader = match leaders.as_slice() {
        [] => return none("no running leader".into(), false),
        [one] => *one,
        many => {
            return none(
                format!("{} members claim to be the running leader", many.len()),
                true,
            )
        }
    };
    let host = leader.get("host").and_then(|h| h.as_str()).unwrap_or("");
    let port = leader.get("port").and_then(|p| p.as_u64()).unwrap_or(5432);
    let address = format!("{host}:{port}");
    let timeline = leader.get("timeline").and_then(|t| t.as_u64());
    match nodes
        .iter()
        .find(|n| n.address.eq_ignore_ascii_case(&address))
    {
        Some(n) => PatroniView {
            leader: Some((n.node_id, n.address.clone())),
            timeline,
            conflict: false,
            reason: None,
        },
        None => PatroniView {
            leader: None,
            timeline,
            conflict: false,
            reason: Some(format!(
                "leader {address} is not a configured [[nodes]] address"
            )),
        },
    }
}

/// Patroni-based topology provider (H-01): the cluster's running leader, as
/// Patroni's DCS sees it, is the write primary. Polls `GET /cluster` on the
/// configured REST endpoints in order and uses the first answer. When no
/// endpoint answers, it reports no primary, so the tracker's lease runs out
/// and writes fail closed.
#[cfg(feature = "postgres-topology")]
pub struct PatroniTopologyProvider {
    endpoints: Vec<String>,
    nodes: Vec<PatroniNode>,
    client: reqwest::Client,
    poll_interval: Duration,
    current: RwLock<Option<TopologyNodeInfo>>,
    timeline: AtomicU64,
    conflict: std::sync::atomic::AtomicBool,
    conflicts_total: AtomicU64,
    event_tx: broadcast::Sender<TopologyEvent>,
}

#[cfg(feature = "postgres-topology")]
impl PatroniTopologyProvider {
    /// `endpoints`: REST base URLs; `request_timeout` bounds each request.
    pub fn new(endpoints: Vec<String>, nodes: Vec<PatroniNode>, request_timeout: Duration) -> Self {
        let (event_tx, _) = broadcast::channel(16);
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .connect_timeout(request_timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            endpoints,
            nodes,
            client,
            poll_interval: Duration::from_secs(2),
            current: RwLock::new(None),
            timeline: AtomicU64::new(0),
            conflict: std::sync::atomic::AtomicBool::new(false),
            conflicts_total: AtomicU64::new(0),
            event_tx,
        }
    }

    /// Set polling interval.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// The leader's timeline from the last successful poll (0 = none yet).
    pub fn timeline(&self) -> u64 {
        self.timeline.load(Ordering::Relaxed)
    }

    /// Polls whose answer named more than one running leader.
    pub fn conflicts_total(&self) -> u64 {
        self.conflicts_total.load(Ordering::Relaxed)
    }

    /// Poll forever.
    pub async fn start(&self) {
        let mut interval = tokio::time::interval(self.poll_interval);
        loop {
            interval.tick().await;
            self.poll().await;
        }
    }

    async fn fetch(&self, endpoint: &str) -> Result<serde_json::Value, String> {
        let url = format!("{}/cluster", endpoint.trim_end_matches('/'));
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("{url}: HTTP {}", resp.status()));
        }
        resp.json::<serde_json::Value>()
            .await
            .map_err(|e| format!("{url}: {e}"))
    }

    async fn poll(&self) {
        let mut view = None;
        for ep in &self.endpoints {
            match self.fetch(ep).await {
                Ok(body) => {
                    view = Some(parse_patroni_cluster(&body, &self.nodes));
                    break;
                }
                Err(e) => tracing::warn!(error = %e, "patroni topology: endpoint unavailable"),
            }
        }
        // No endpoint answered: authority is unknown. Report no primary so
        // the tracker's lease expires rather than being refreshed.
        let view = view.unwrap_or(PatroniView {
            leader: None,
            timeline: None,
            conflict: false,
            reason: Some("no Patroni endpoint answered".into()),
        });
        self.apply(view);
    }

    fn apply(&self, view: PatroniView) {
        if view.conflict {
            self.conflicts_total.fetch_add(1, Ordering::Relaxed);
        }
        self.conflict.store(view.conflict, Ordering::Relaxed);
        if let Some(t) = view.timeline {
            self.timeline.store(t, Ordering::Relaxed);
        }
        if let Some(reason) = &view.reason {
            tracing::warn!(%reason, "patroni topology: no writable leader");
        }
        let next = view.leader.map(|(node_id, address)| TopologyNodeInfo {
            node_id,
            client_addr: address,
            is_healthy: true,
        });
        let old = self.current.read().as_ref().map(|p| p.node_id);
        let new = next.as_ref().map(|p| p.node_id);
        *self.current.write() = next;
        if old != new {
            if let Some(new_primary) = new {
                let _ = self.event_tx.send(TopologyEvent::PrimaryChanged {
                    old_primary: old,
                    new_primary,
                });
            }
        }
    }
}

#[cfg(feature = "postgres-topology")]
impl TopologyProvider for PatroniTopologyProvider {
    fn subscribe(&self) -> broadcast::Receiver<TopologyEvent> {
        self.event_tx.subscribe()
    }

    fn get_primary(&self) -> Option<TopologyNodeInfo> {
        self.current.read().clone()
    }

    fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo> {
        let n = self.nodes.iter().find(|n| n.node_id == id)?;
        Some(TopologyNodeInfo {
            node_id: n.node_id,
            client_addr: n.address.clone(),
            is_healthy: true,
        })
    }

    fn authority_conflict(&self) -> bool {
        self.conflict.load(Ordering::Relaxed)
    }

    fn conflicts_total(&self) -> u64 {
        self.conflicts_total.load(Ordering::Relaxed)
    }

    fn leader_timeline(&self) -> Option<u64> {
        self.current.read().as_ref()?;
        let t = self.timeline.load(Ordering::Relaxed);
        (t > 0).then_some(t)
    }
}

// ── HeliosDB topology provider (bridges to internal TopologyManager) ─

#[cfg(feature = "heliosdb-topology")]
pub mod heliosdb_provider {
    //! Bridge to the HeliosDB-Lite internal `TopologyManager`.
    //!
    //! This module is only compiled when HeliosProxy is built as part of
    //! the HeliosDB-Lite workspace (feature `heliosdb-topology`).
    //! It wraps the internal replication types behind the generic
    //! `TopologyProvider` trait so that `PrimaryTracker` can use them
    //! without a hard dependency.

    use super::*;

    /// Wrapper that adapts the HeliosDB `TopologyManager` to the
    /// `TopologyProvider` trait.
    ///
    /// Consumers pass this struct to `PrimaryTracker::with_provider()`.
    pub struct HeliosTopologyProvider<T: HeliosTopologyBridge> {
        inner: Arc<T>,
    }

    /// Trait that the HeliosDB replication crate must implement to
    /// bridge into the proxy topology system.
    ///
    /// This avoids a direct `use crate::replication::topology` import
    /// and allows the standalone proxy to compile without the
    /// replication crate.
    pub trait HeliosTopologyBridge: Send + Sync + 'static {
        fn subscribe(&self) -> broadcast::Receiver<TopologyEvent>;
        fn get_primary(&self) -> Option<TopologyNodeInfo>;
        fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo>;
    }

    impl<T: HeliosTopologyBridge> HeliosTopologyProvider<T> {
        pub fn new(inner: Arc<T>) -> Self {
            Self { inner }
        }
    }

    impl<T: HeliosTopologyBridge> TopologyProvider for HeliosTopologyProvider<T> {
        fn subscribe(&self) -> broadcast::Receiver<TopologyEvent> {
            self.inner.subscribe()
        }

        fn get_primary(&self) -> Option<TopologyNodeInfo> {
            self.inner.get_primary()
        }

        fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo> {
            self.inner.get_node(id)
        }
    }
}

// ── Primary info & events ───────────────────────────────────────────

/// Primary node information
#[derive(Debug, Clone)]
pub struct PrimaryInfo {
    /// Node ID
    pub node_id: Uuid,
    /// Client address (host:port)
    pub address: String,
    /// Time when this node became primary
    pub became_primary_at: Instant,
    /// Whether this is confirmed (vs pending switchover)
    pub is_confirmed: bool,
    /// Authority epoch: increments on every observed leader change since
    /// boot. A fenced consumer can refuse to write under an older epoch
    /// (H-01; enforcement is H-02).
    pub epoch: u64,
}

/// Primary change event
#[derive(Debug, Clone)]
pub enum PrimaryChangeEvent {
    /// Primary changed to new node
    Changed {
        old: Option<Uuid>,
        new: Uuid,
        address: String,
    },
    /// Primary lost (no healthy primary)
    Lost { old: Uuid },
    /// Primary confirmed (after switchover completes)
    Confirmed { node_id: Uuid },
}

// ── Primary Tracker ─────────────────────────────────────────────────

/// Primary Tracker
///
/// Can be used in three modes:
/// 1. **With a TopologyProvider** – automatic tracking via `with_provider()`.
/// 2. **Standalone** – manual `set_primary()` / `clear_primary()` calls.
/// 3. **PostgreSQL** – pass a `PostgresTopologyProvider` (feature `postgres-topology`).
pub struct PrimaryTracker {
    /// Optional topology provider (Box<dyn> for either HeliosDB or PostgreSQL)
    provider: Option<Arc<dyn TopologyProvider>>,
    /// Current primary info
    current_primary: RwLock<Option<PrimaryInfo>>,
    /// Event broadcaster
    event_tx: broadcast::Sender<PrimaryChangeEvent>,
    /// Tracking interval
    tracking_interval: Duration,
    /// Authority epoch counter (H-01).
    epoch: AtomicU64,
    /// How long a provider observation stays authoritative without a refresh
    /// (H-02). Provider-backed trackers only: a manual/standalone authority
    /// does not expire.
    lease_timeout: Duration,
    /// Last time the provider confirmed the current leader (or the primary was
    /// set manually). `None` before the first observation.
    last_refresh: RwLock<Option<Instant>>,
}

impl PrimaryTracker {
    /// Create a standalone primary tracker (manual set/clear).
    pub fn new_standalone() -> Self {
        let (event_tx, _) = broadcast::channel(16);
        Self {
            provider: None,
            current_primary: RwLock::new(None),
            event_tx,
            tracking_interval: Duration::from_millis(500),
            epoch: AtomicU64::new(0),
            lease_timeout: Duration::from_secs(10),
            last_refresh: RwLock::new(None),
        }
    }

    /// Create a primary tracker backed by a topology provider.
    pub fn with_provider(provider: Arc<dyn TopologyProvider>) -> Self {
        let (event_tx, _) = broadcast::channel(16);
        Self {
            provider: Some(provider),
            current_primary: RwLock::new(None),
            event_tx,
            tracking_interval: Duration::from_millis(500),
            epoch: AtomicU64::new(0),
            lease_timeout: Duration::from_secs(10),
            last_refresh: RwLock::new(None),
        }
    }

    /// Set tracking interval.
    pub fn with_tracking_interval(mut self, interval: Duration) -> Self {
        self.tracking_interval = interval;
        self
    }

    /// Set how long a provider observation stays authoritative without a
    /// refresh (H-02). Must be non-zero; the caller's config validation
    /// rejects 0.
    pub fn with_lease_timeout(mut self, timeout: Duration) -> Self {
        self.lease_timeout = timeout;
        self
    }

    /// Whether a topology provider backs this tracker (any non-static
    /// `[topology] provider`).
    pub fn has_provider(&self) -> bool {
        self.provider.is_some()
    }

    /// The provider's count of polls that saw conflicting write authority
    /// (0 for a standalone tracker).
    pub fn provider_conflicts_total(&self) -> u64 {
        self.provider.as_ref().map_or(0, |p| p.conflicts_total())
    }

    /// The provider's leader timeline, when it reports one (Patroni).
    pub fn provider_leader_timeline(&self) -> Option<u64> {
        self.provider.as_ref()?.leader_timeline()
    }

    /// Subscribe to primary change events.
    pub fn subscribe(&self) -> broadcast::Receiver<PrimaryChangeEvent> {
        self.event_tx.subscribe()
    }

    /// Get current primary info.
    pub fn get_primary(&self) -> Option<PrimaryInfo> {
        self.current_primary.read().clone()
    }

    /// Get current primary node ID.
    pub fn get_primary_id(&self) -> Option<Uuid> {
        self.current_primary.read().as_ref().map(|p| p.node_id)
    }

    /// Get current primary address.
    pub fn get_primary_address(&self) -> Option<String> {
        self.current_primary
            .read()
            .as_ref()
            .map(|p| p.address.clone())
    }

    /// Check if we have a healthy primary.
    pub fn has_primary(&self) -> bool {
        self.current_primary.read().is_some()
    }

    /// Current authority epoch (H-01): increments on every observed leader
    /// change since boot. Zero means no leader has been observed yet.
    pub fn get_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Whether the current authority is still valid (H-02). A provider-backed
    /// tracker's authority expires `lease_timeout` after the last successful
    /// provider observation, so a lost provider (or a partitioned control
    /// path) fails closed instead of authorizing writes on stale knowledge.
    /// A standalone/manual authority never expires.
    pub fn authority_valid(&self) -> bool {
        if self.current_primary.read().is_none() {
            return false;
        }
        if self.provider.is_none() {
            return true;
        }
        self.lease_remaining().is_some()
    }

    /// Remaining lease, or `None` when the authority has expired or was never
    /// observed. Standalone/manual trackers have no lease and return `None`.
    pub fn lease_remaining(&self) -> Option<Duration> {
        self.provider.as_ref()?;
        let last = *self.last_refresh.read();
        let last = last?;
        self.lease_timeout.checked_sub(last.elapsed())
    }

    /// Set primary manually (or called during switchover).
    pub fn set_primary(&self, node_id: Uuid, address: String) {
        let old_primary = self.current_primary.read().as_ref().map(|p| p.node_id);
        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;

        let new_info = PrimaryInfo {
            node_id,
            address: address.clone(),
            became_primary_at: Instant::now(),
            is_confirmed: false,
            epoch,
        };

        *self.current_primary.write() = Some(new_info);
        *self.last_refresh.write() = Some(Instant::now());

        let _ = self.event_tx.send(PrimaryChangeEvent::Changed {
            old: old_primary,
            new: node_id,
            address,
        });

        tracing::info!(
            "Primary tracker: set primary to {} (pending confirmation)",
            node_id
        );
    }

    /// Confirm the current primary (called after switchover completes).
    pub fn confirm_primary(&self) {
        let mut guard = self.current_primary.write();
        if let Some(ref mut info) = *guard {
            info.is_confirmed = true;
            let node_id = info.node_id;
            drop(guard);
            *self.last_refresh.write() = Some(Instant::now());

            let _ = self
                .event_tx
                .send(PrimaryChangeEvent::Confirmed { node_id });
            tracing::info!("Primary tracker: confirmed primary {}", node_id);
        }
    }

    /// Clear primary (called when primary is lost).
    pub fn clear_primary(&self) {
        let old_primary = self.current_primary.write().take();
        *self.last_refresh.write() = None;

        if let Some(info) = old_primary {
            let _ = self
                .event_tx
                .send(PrimaryChangeEvent::Lost { old: info.node_id });
            tracing::warn!("Primary tracker: lost primary {}", info.node_id);
        }
    }

    /// Run the primary tracker loop (requires a topology provider).
    ///
    /// If no provider is set, this returns immediately — use manual
    /// `set_primary()` / `clear_primary()` instead.
    pub async fn run(&self) {
        let provider = match &self.provider {
            Some(p) => Arc::clone(p),
            None => {
                tracing::info!("Primary tracker: no topology provider, running in standalone mode");
                return;
            }
        };

        let mut topology_rx = provider.subscribe();
        let mut interval = tokio::time::interval(self.tracking_interval);

        // Initial detection
        self.detect_primary_from_provider(&*provider);

        loop {
            tokio::select! {
                event = topology_rx.recv() => {
                    match event {
                        Ok(TopologyEvent::PrimaryChanged { old_primary, new_primary }) => {
                            self.handle_primary_changed(&*provider, old_primary, new_primary);
                        }
                        Ok(TopologyEvent::NodeLeft { node_id }) => {
                            self.handle_node_left(node_id);
                        }
                        Ok(TopologyEvent::HealthChanged { node_id, is_healthy }) => {
                            self.handle_health_changed(node_id, is_healthy);
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::warn!("Primary tracker lagged {} events", n);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    self.periodic_check(&*provider);
                }
            }
        }
    }

    // ── Internal helpers ────────────────────────────────────────────

    fn detect_primary_from_provider(&self, provider: &dyn TopologyProvider) {
        if let Some(primary) = provider.get_primary() {
            // A heartbeat on the same address refreshes the lease; only an
            // address change advances the authority epoch.
            *self.last_refresh.write() = Some(Instant::now());
            let same = self
                .current_primary
                .read()
                .as_ref()
                .map(|p| p.address == primary.client_addr)
                .unwrap_or(false);
            if same {
                return;
            }
            let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
            let info = PrimaryInfo {
                node_id: primary.node_id,
                address: primary.client_addr.clone(),
                became_primary_at: Instant::now(),
                is_confirmed: true,
                epoch,
            };

            *self.current_primary.write() = Some(info);
            tracing::info!("Primary tracker: detected primary {}", primary.node_id);
        }
    }

    fn handle_primary_changed(
        &self,
        provider: &dyn TopologyProvider,
        old: Option<Uuid>,
        new: Uuid,
    ) {
        // Already following `new` (the periodic check reconciled first, or a
        // duplicate event): refresh the lease, do not start a new epoch.
        if self.current_primary.read().as_ref().map(|p| p.node_id) == Some(new) {
            *self.last_refresh.write() = Some(Instant::now());
            return;
        }
        let address = provider
            .get_node(new)
            .map(|n| n.client_addr)
            .unwrap_or_else(|| format!("{}:5432", new));

        let epoch = self.epoch.fetch_add(1, Ordering::Relaxed) + 1;
        let info = PrimaryInfo {
            node_id: new,
            address: address.clone(),
            became_primary_at: Instant::now(),
            is_confirmed: true,
            epoch,
        };

        *self.current_primary.write() = Some(info);
        *self.last_refresh.write() = Some(Instant::now());

        let _ = self
            .event_tx
            .send(PrimaryChangeEvent::Changed { old, new, address });

        tracing::info!("Primary tracker: primary changed from {:?} to {}", old, new);
    }

    fn handle_node_left(&self, node_id: Uuid) {
        let current = self.current_primary.read().as_ref().map(|p| p.node_id);
        if current == Some(node_id) {
            self.clear_primary();
        }
    }

    fn handle_health_changed(&self, node_id: Uuid, is_healthy: bool) {
        if !is_healthy {
            let current = self.current_primary.read().as_ref().map(|p| p.node_id);
            if current == Some(node_id) {
                tracing::warn!("Primary {} became unhealthy", node_id);
            }
        }
    }

    fn periodic_check(&self, provider: &dyn TopologyProvider) {
        // Conflicting authority (two writable primaries the provider cannot
        // order): drop the leader now so writes fail closed, rather than
        // routing to either until the lease runs out.
        if provider.authority_conflict() {
            if self.current_primary.read().is_some() {
                tracing::warn!("Primary tracker: provider reports conflicting primaries; clearing");
                self.clear_primary();
            }
            return;
        }
        let current_id = self.current_primary.read().as_ref().map(|p| p.node_id);

        if let Some(id) = current_id {
            let provider_primary = provider.get_primary();
            let provider_id = provider_primary.as_ref().map(|p| p.node_id);
            if provider_id == Some(id) {
                // Heartbeat: the provider still reports this leader, so the
                // authority lease is refreshed (H-02).
                *self.last_refresh.write() = Some(Instant::now());
            } else if let Some(new) = provider_id {
                // The provider names a different leader: follow it. Its change
                // event normally gets here first; this reconciles a missed or
                // lagged event, which would otherwise keep the old leader until
                // its lease ran out and then stall writes for good.
                self.handle_primary_changed(provider, Some(id), new);
                return;
            } else if let Some(node) = provider.get_node(id) {
                if !node.is_healthy {
                    tracing::warn!("Primary {} is unhealthy in periodic check", id);
                }
            } else {
                self.clear_primary();
            }

            // Loss of authority: the provider reports no primary and the last
            // observation has expired. Drop the stale leader so the write path
            // fails closed instead of authorizing on stale knowledge (H-02).
            if provider_primary.is_none() && !self.authority_valid() {
                tracing::warn!(
                    "Primary tracker: authority lease expired with no provider primary; clearing"
                );
                self.clear_primary();
            }
        } else {
            self.detect_primary_from_provider(provider);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_decides_between_writable_nodes() {
        // One strictly highest timeline wins, wherever it sits.
        assert_eq!(choose_by_timeline(&[(0, Some(3)), (1, Some(4))]), Some(1));
        assert_eq!(choose_by_timeline(&[(0, Some(5)), (1, Some(4))]), Some(0));
        assert_eq!(choose_by_timeline(&[(2, Some(7))]), Some(2));
        // A tie or an unknown timeline cannot be resolved.
        assert_eq!(choose_by_timeline(&[(0, Some(4)), (1, Some(4))]), None);
        assert_eq!(choose_by_timeline(&[(0, Some(9)), (1, None)]), None);
        // A tie below the maximum does not matter.
        assert_eq!(
            choose_by_timeline(&[(0, Some(2)), (1, Some(2)), (2, Some(3))]),
            Some(2)
        );
    }

    fn patroni_nodes() -> (Vec<PatroniNode>, Uuid, Uuid) {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        (
            vec![
                PatroniNode {
                    node_id: a,
                    address: "pg-a:5432".into(),
                },
                PatroniNode {
                    node_id: b,
                    address: "pg-b:5432".into(),
                },
            ],
            a,
            b,
        )
    }

    fn member(host: &str, role: &str, state: &str, timeline: u64) -> serde_json::Value {
        serde_json::json!({
            "name": host, "host": host, "port": 5432,
            "role": role, "state": state, "timeline": timeline
        })
    }

    #[test]
    fn patroni_cluster_resolves_the_running_leader() {
        let (nodes, a, b) = patroni_nodes();
        let body = serde_json::json!({ "members": [
            member("pg-a", "replica", "streaming", 6),
            member("PG-B", "leader", "running", 6),
        ]});
        let v = parse_patroni_cluster(&body, &nodes);
        assert_eq!(v.leader, Some((b, "pg-b:5432".to_string())));
        assert_eq!(v.timeline, Some(6));
        assert!(!v.conflict);

        // Pre-3.0 Patroni calls the leader "master".
        let body = serde_json::json!({ "members": [member("pg-a", "master", "running", 2)] });
        assert_eq!(
            parse_patroni_cluster(&body, &nodes).leader.map(|l| l.0),
            Some(a)
        );
    }

    #[test]
    fn patroni_cluster_authorizes_nothing_when_unsure() {
        let (nodes, _, _) = patroni_nodes();
        let cases = [
            // A standby cluster's leader is not writable.
            (
                serde_json::json!({ "members": [member("pg-a", "standby_leader", "running", 3)] }),
                false,
            ),
            // A leader that is not running.
            (
                serde_json::json!({ "members": [member("pg-a", "leader", "stopped", 3)] }),
                false,
            ),
            // A leader outside the configured nodes.
            (
                serde_json::json!({ "members": [member("pg-z", "leader", "running", 3)] }),
                false,
            ),
            // No members at all / malformed body.
            (serde_json::json!({ "members": [] }), false),
            (serde_json::json!({ "error": "x" }), false),
            // Two running leaders: a conflict.
            (
                serde_json::json!({ "members": [
                    member("pg-a", "leader", "running", 3),
                    member("pg-b", "leader", "running", 4),
                ]}),
                true,
            ),
        ];
        for (body, conflict) in cases {
            let v = parse_patroni_cluster(&body, &nodes);
            assert!(v.leader.is_none(), "{body}");
            assert!(v.reason.is_some(), "{body}");
            assert_eq!(v.conflict, conflict, "{body}");
        }
    }

    /// A provider reporting conflicting authority makes the tracker drop its
    /// leader on the next check, without waiting for the lease.
    #[test]
    fn tracker_drops_the_leader_on_conflicting_authority() {
        struct Conflicted {
            conflict: std::sync::atomic::AtomicBool,
            leader: RwLock<Option<TopologyNodeInfo>>,
            tx: broadcast::Sender<TopologyEvent>,
        }
        impl TopologyProvider for Conflicted {
            fn subscribe(&self) -> broadcast::Receiver<TopologyEvent> {
                self.tx.subscribe()
            }
            fn get_primary(&self) -> Option<TopologyNodeInfo> {
                self.leader.read().clone()
            }
            fn get_node(&self, _id: Uuid) -> Option<TopologyNodeInfo> {
                None
            }
            fn authority_conflict(&self) -> bool {
                self.conflict.load(Ordering::Relaxed)
            }
        }
        let id = Uuid::new_v4();
        let p = Arc::new(Conflicted {
            conflict: std::sync::atomic::AtomicBool::new(false),
            leader: RwLock::new(Some(TopologyNodeInfo {
                node_id: id,
                client_addr: "pg-a:5432".into(),
                is_healthy: true,
            })),
            tx: broadcast::channel(4).0,
        });
        let tracker =
            PrimaryTracker::with_provider(p.clone()).with_lease_timeout(Duration::from_secs(3600));
        tracker.periodic_check(p.as_ref());
        assert!(tracker.has_primary(), "leader adopted");

        p.conflict.store(true, Ordering::Relaxed);
        *p.leader.write() = None;
        tracker.periodic_check(p.as_ref());
        assert!(
            !tracker.has_primary(),
            "conflict clears at once, lease notwithstanding"
        );
        tracker.periodic_check(p.as_ref());
        assert!(
            !tracker.has_primary(),
            "and stays cleared while the conflict lasts"
        );
    }

    #[test]
    fn test_authority_epoch_increments_on_primary_change() {
        let tracker = PrimaryTracker::new_standalone();
        assert_eq!(tracker.get_epoch(), 0);
        assert!(tracker.get_primary().is_none());

        tracker.set_primary(Uuid::new_v4(), "a:5432".to_string());
        assert_eq!(tracker.get_epoch(), 1);
        assert_eq!(tracker.get_primary().unwrap().epoch, 1);

        tracker.set_primary(Uuid::new_v4(), "b:5432".to_string());
        assert_eq!(tracker.get_epoch(), 2);
        let info = tracker.get_primary().unwrap();
        assert_eq!(info.epoch, 2);
        assert_eq!(info.address, "b:5432");
    }

    #[test]
    fn test_authority_lease_expires_and_heartbeats_refresh_it() {
        let topo = Arc::new(MockTopology::new());
        topo.set_primary(Uuid::new_v4(), "primary-a:5432");
        let tracker = PrimaryTracker::with_provider(topo.clone())
            .with_lease_timeout(Duration::from_millis(120));

        assert!(
            !tracker.authority_valid(),
            "no provider observation yet: invalid"
        );

        tracker.detect_primary_from_provider(&*topo);
        assert!(tracker.authority_valid());
        assert!(tracker.lease_remaining().is_some());

        // A heartbeat within the lease refreshes it.
        std::thread::sleep(Duration::from_millis(80));
        tracker.detect_primary_from_provider(&*topo);
        std::thread::sleep(Duration::from_millis(80));
        assert!(
            tracker.authority_valid(),
            "same-address heartbeat must refresh the lease"
        );

        // Stop refreshing: the lease expires and authority fails closed.
        std::thread::sleep(Duration::from_millis(140));
        assert!(!tracker.authority_valid());
        assert!(tracker.lease_remaining().is_none());
    }

    #[test]
    fn test_expired_authority_clears_when_provider_loses_primary() {
        let topo = Arc::new(MockTopology::new());
        topo.set_primary(Uuid::new_v4(), "primary-a:5432");
        let tracker = PrimaryTracker::with_provider(topo.clone())
            .with_lease_timeout(Duration::from_millis(60));
        tracker.detect_primary_from_provider(&*topo);
        assert!(tracker.has_primary());

        // Provider loses the primary; within the lease the tracker keeps it
        // (a brief probe gap must not flap the write path).
        topo.lose_primary();
        tracker.periodic_check(&*topo);
        assert!(tracker.has_primary(), "within lease: not dropped yet");

        // Past the lease, the stale leader is dropped so writes fail closed.
        std::thread::sleep(Duration::from_millis(90));
        tracker.periodic_check(&*topo);
        assert!(
            !tracker.has_primary(),
            "expired lease clears the stale leader"
        );
        assert!(!tracker.authority_valid());
    }

    #[test]
    fn test_standalone_primary_tracker() {
        let tracker = PrimaryTracker::new_standalone();

        assert!(!tracker.has_primary());

        let node_id = Uuid::new_v4();
        tracker.set_primary(node_id, "localhost:5432".to_string());

        assert!(tracker.has_primary());
        assert_eq!(tracker.get_primary_id(), Some(node_id));
        assert_eq!(
            tracker.get_primary_address(),
            Some("localhost:5432".to_string())
        );
        // Manual/standalone authority does not expire and has no lease.
        assert!(tracker.authority_valid());
        assert!(tracker.lease_remaining().is_none());

        // Not confirmed yet
        let info = tracker.get_primary().unwrap();
        assert!(!info.is_confirmed);

        // Confirm
        tracker.confirm_primary();
        let info = tracker.get_primary().unwrap();
        assert!(info.is_confirmed);

        // Clear
        tracker.clear_primary();
        assert!(!tracker.has_primary());
        assert!(!tracker.authority_valid());
    }

    /// Minimal mock topology provider for testing.
    struct MockTopology {
        event_tx: broadcast::Sender<TopologyEvent>,
        primary: RwLock<Option<TopologyNodeInfo>>,
        /// Known nodes, kept even after they stop being primary so `get_node`
        /// mirrors the postgres provider (which knows its configured nodes).
        nodes: RwLock<std::collections::HashMap<Uuid, TopologyNodeInfo>>,
    }

    impl MockTopology {
        fn new() -> Self {
            let (event_tx, _) = broadcast::channel(16);
            Self {
                event_tx,
                primary: RwLock::new(None),
                nodes: RwLock::new(std::collections::HashMap::new()),
            }
        }

        fn set_primary(&self, node_id: Uuid, addr: &str) {
            let info = TopologyNodeInfo {
                node_id,
                client_addr: addr.to_string(),
                is_healthy: true,
            };
            self.nodes.write().insert(node_id, info.clone());
            *self.primary.write() = Some(info);
        }

        /// Simulate the provider losing track of the primary while the node
        /// itself remains known (the unreachable/quorum-lost case).
        fn lose_primary(&self) {
            *self.primary.write() = None;
        }
    }

    impl TopologyProvider for MockTopology {
        fn subscribe(&self) -> broadcast::Receiver<TopologyEvent> {
            self.event_tx.subscribe()
        }

        fn get_primary(&self) -> Option<TopologyNodeInfo> {
            self.primary.read().clone()
        }

        fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo> {
            self.nodes.read().get(&id).cloned()
        }
    }

    #[test]
    fn test_provider_backed_tracker() {
        let topo = Arc::new(MockTopology::new());
        let node_id = Uuid::new_v4();
        topo.set_primary(node_id, "primary:5432");

        let tracker = PrimaryTracker::with_provider(topo.clone());
        tracker.detect_primary_from_provider(topo.as_ref());

        assert!(tracker.has_primary());
        assert_eq!(tracker.get_primary_id(), Some(node_id));
    }

    /// Simulate a PostgreSQL 3-node cluster (primary + sync + async standby)
    /// where the primary fails and a standby is promoted.
    #[test]
    fn test_postgresql_failover_scenario() {
        let topo = Arc::new(MockTopology::new());

        // Initial state: pg-primary is the primary
        let pg_primary = Uuid::new_v4();
        let pg_sync = Uuid::new_v4();
        let _pg_async = Uuid::new_v4();

        topo.set_primary(pg_primary, "pg-primary:5432");

        let tracker = PrimaryTracker::with_provider(topo.clone());
        tracker.detect_primary_from_provider(topo.as_ref());

        assert!(tracker.has_primary());
        assert_eq!(
            tracker.get_primary_address(),
            Some("pg-primary:5432".to_string())
        );

        // Subscribe to events
        let mut rx = tracker.subscribe();

        // Simulate failover: primary goes down, sync standby promoted
        tracker.clear_primary();
        assert!(!tracker.has_primary());

        // Check Lost event was emitted
        let event = rx.try_recv().unwrap();
        assert!(matches!(event, PrimaryChangeEvent::Lost { old } if old == pg_primary));

        // New primary detected (sync standby promoted)
        tracker.set_primary(pg_sync, "pg-sync:5432".to_string());
        assert!(tracker.has_primary());
        assert_eq!(
            tracker.get_primary_address(),
            Some("pg-sync:5432".to_string())
        );
        assert!(!tracker.get_primary().unwrap().is_confirmed);

        // Confirm after pg_basebackup / replication catchup
        tracker.confirm_primary();
        assert!(tracker.get_primary().unwrap().is_confirmed);

        // Check Changed event
        let event = rx.try_recv().unwrap();
        assert!(matches!(event, PrimaryChangeEvent::Changed { new, .. } if new == pg_sync));
    }

    /// Verify the topology provider trait can be used with custom
    /// implementations (e.g. Patroni, pg_auto_failover, Stolon).
    #[test]
    fn test_custom_topology_provider() {
        struct PatroniProvider {
            leader: RwLock<Option<TopologyNodeInfo>>,
            event_tx: broadcast::Sender<TopologyEvent>,
        }

        impl PatroniProvider {
            fn new() -> Self {
                let (tx, _) = broadcast::channel(16);
                Self {
                    leader: RwLock::new(None),
                    event_tx: tx,
                }
            }
            fn set_leader(&self, id: Uuid, addr: &str) {
                *self.leader.write() = Some(TopologyNodeInfo {
                    node_id: id,
                    client_addr: addr.to_string(),
                    is_healthy: true,
                });
            }
        }

        impl TopologyProvider for PatroniProvider {
            fn subscribe(&self) -> broadcast::Receiver<TopologyEvent> {
                self.event_tx.subscribe()
            }
            fn get_primary(&self) -> Option<TopologyNodeInfo> {
                self.leader.read().clone()
            }
            fn get_node(&self, id: Uuid) -> Option<TopologyNodeInfo> {
                self.leader
                    .read()
                    .as_ref()
                    .filter(|n| n.node_id == id)
                    .cloned()
            }
        }

        let patroni = Arc::new(PatroniProvider::new());
        let leader_id = Uuid::new_v4();
        patroni.set_leader(leader_id, "patroni-leader.svc:5432");

        let tracker = PrimaryTracker::with_provider(patroni.clone());
        tracker.detect_primary_from_provider(patroni.as_ref());

        assert!(tracker.has_primary());
        assert_eq!(
            tracker.get_primary_address(),
            Some("patroni-leader.svc:5432".to_string())
        );
    }

    /// Probing unreachable nodes must not crash the poller; it must
    /// leave `current_primary` as `None` and emit `HealthChanged`
    /// events for each failed probe. Exercises the real `probe_recovery`
    /// path without a live PG.
    #[cfg(feature = "postgres-topology")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_poll_nodes_all_unreachable_sets_no_primary() {
        let nodes = vec![
            PostgresNode {
                node_id: Uuid::new_v4(),
                host: "127.0.0.1".into(),
                port: 1, // no daemon
                user: "postgres".into(),
                password: None,
                database: "postgres".into(),
            },
            PostgresNode {
                node_id: Uuid::new_v4(),
                host: "127.0.0.1".into(),
                port: 2,
                user: "postgres".into(),
                password: None,
                database: "postgres".into(),
            },
        ];

        let provider =
            PostgresTopologyProvider::new(nodes).with_poll_interval(Duration::from_millis(200));
        let mut rx = provider.event_tx.subscribe();

        // Run exactly one poll round.
        provider.poll_nodes().await;

        // No primary detected.
        assert!(provider.get_primary().is_none());

        // Collect health-change events. Use try_recv in a loop with a
        // small yield budget rather than blocking, so the test is
        // deterministic.
        let mut health_events = 0;
        for _ in 0..10 {
            match rx.try_recv() {
                Ok(TopologyEvent::HealthChanged {
                    is_healthy: false, ..
                }) => {
                    health_events += 1;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        assert!(
            health_events >= 1,
            "expected at least one HealthChanged event"
        );
    }

    /// End to end over HTTP (H-01): the provider takes the first endpoint
    /// that answers, the tracker follows Patroni's leader across a switchover
    /// (with its timeline) from the periodic check alone, a late change event
    /// does not start a second epoch, and a two-leader answer clears the
    /// tracker's leader at once.
    #[cfg(feature = "postgres-topology")]
    #[tokio::test]
    async fn patroni_provider_follows_the_leader_over_http() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let body = Arc::new(std::sync::Mutex::new(String::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let served = body.clone();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut req = [0u8; 2048];
                let _ = sock.read(&mut req).await;
                let b = served.lock().unwrap().clone();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{b}",
                    b.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            }
        });
        let member = |host: &str, role: &str, timeline: u64| {
            serde_json::json!({
                "name": host, "host": host, "port": 5432,
                "role": role, "state": "running", "timeline": timeline
            })
        };
        let set = |members: Vec<serde_json::Value>| {
            *body.lock().unwrap() = serde_json::json!({ "members": members }).to_string();
        };

        let (a_id, b_id) = (Uuid::new_v4(), Uuid::new_v4());
        let nodes = vec![
            PatroniNode {
                node_id: a_id,
                address: "pg-a:5432".into(),
            },
            PatroniNode {
                node_id: b_id,
                address: "pg-b:5432".into(),
            },
        ];
        // The first endpoint refuses connections; the second answers.
        let p = Arc::new(PatroniTopologyProvider::new(
            vec!["http://127.0.0.1:1".into(), format!("http://{addr}")],
            nodes,
            Duration::from_secs(2),
        ));
        let tracker =
            PrimaryTracker::with_provider(p.clone()).with_lease_timeout(Duration::from_secs(3600));

        set(vec![
            member("pg-a", "leader", 3),
            member("pg-b", "replica", 3),
        ]);
        p.poll().await;
        tracker.periodic_check(p.as_ref());
        assert_eq!(tracker.get_primary_address().as_deref(), Some("pg-a:5432"));
        assert_eq!(tracker.provider_leader_timeline(), Some(3));

        // Switchover: pg-b is promoted onto timeline 4.
        set(vec![
            member("pg-a", "replica", 4),
            member("pg-b", "leader", 4),
        ]);
        p.poll().await;
        tracker.periodic_check(p.as_ref());
        // Followed by the periodic check alone (no change event consumed).
        assert_eq!(tracker.get_primary_address().as_deref(), Some("pg-b:5432"));
        assert_eq!(tracker.provider_leader_timeline(), Some(4));
        assert_eq!(tracker.provider_conflicts_total(), 0);
        assert_eq!(tracker.get_epoch(), 2);
        // The change event arriving after the reconciliation is a no-op.
        tracker.handle_primary_changed(p.as_ref(), Some(a_id), b_id);
        assert_eq!(tracker.get_epoch(), 2);

        // Split brain: two running leaders authorize nothing.
        set(vec![
            member("pg-a", "leader", 4),
            member("pg-b", "leader", 4),
        ]);
        p.poll().await;
        tracker.periodic_check(p.as_ref());
        assert!(!tracker.has_primary());
        assert_eq!(tracker.provider_leader_timeline(), None);
        assert_eq!(tracker.provider_conflicts_total(), 1);
    }
}
