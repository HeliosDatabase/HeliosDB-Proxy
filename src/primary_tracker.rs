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

use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::RwLock;
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
        }
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
        let mut next_primary: Option<TopologyNodeInfo> = None;

        for node in &self.nodes {
            match self.probe_recovery(node).await {
                Ok(in_recovery) => {
                    // The node reporting `pg_is_in_recovery() = false` is
                    // the primary. In a healthy cluster there is exactly
                    // one; we take the first we encounter so split-brain
                    // (briefly possible during failover) still yields a
                    // deterministic choice.
                    if !in_recovery && next_primary.is_none() {
                        next_primary = Some(TopologyNodeInfo {
                            node_id: node.node_id,
                            client_addr: format!("{}:{}", node.host, node.port),
                            is_healthy: true,
                        });
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

    /// Connect to a single node and run `SELECT pg_is_in_recovery()`.
    ///
    /// Returns `Ok(true)` if the node is a standby, `Ok(false)` for a
    /// primary. Errors propagate as `BackendError`.
    async fn probe_recovery(
        &self,
        node: &PostgresNode,
    ) -> crate::backend::BackendResult<bool> {
        use crate::backend::{BackendClient, BackendConfig};

        let cfg = BackendConfig {
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
        };

        let mut client = BackendClient::connect(&cfg).await?;
        let value = client
            .query_scalar("SELECT pg_is_in_recovery()")
            .await?;
        client.close().await;
        Ok(value
            .as_bool("pg_is_in_recovery")?
            .unwrap_or(false))
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
        self.nodes.iter().find(|n| n.node_id == id).map(|n| TopologyNodeInfo {
            node_id: n.node_id,
            client_addr: format!("{}:{}", n.host, n.port),
            is_healthy: true, // Would be checked via actual connection
        })
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
        }
    }

    /// Set tracking interval.
    pub fn with_tracking_interval(mut self, interval: Duration) -> Self {
        self.tracking_interval = interval;
        self
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
        self.current_primary.read().as_ref().map(|p| p.address.clone())
    }

    /// Check if we have a healthy primary.
    pub fn has_primary(&self) -> bool {
        self.current_primary.read().is_some()
    }

    /// Set primary manually (or called during switchover).
    pub fn set_primary(&self, node_id: Uuid, address: String) {
        let old_primary = self.current_primary.read().as_ref().map(|p| p.node_id);

        let new_info = PrimaryInfo {
            node_id,
            address: address.clone(),
            became_primary_at: Instant::now(),
            is_confirmed: false,
        };

        *self.current_primary.write() = Some(new_info);

        let _ = self.event_tx.send(PrimaryChangeEvent::Changed {
            old: old_primary,
            new: node_id,
            address,
        });

        tracing::info!("Primary tracker: set primary to {} (pending confirmation)", node_id);
    }

    /// Confirm the current primary (called after switchover completes).
    pub fn confirm_primary(&self) {
        let mut guard = self.current_primary.write();
        if let Some(ref mut info) = *guard {
            info.is_confirmed = true;
            let node_id = info.node_id;
            drop(guard);

            let _ = self.event_tx.send(PrimaryChangeEvent::Confirmed { node_id });
            tracing::info!("Primary tracker: confirmed primary {}", node_id);
        }
    }

    /// Clear primary (called when primary is lost).
    pub fn clear_primary(&self) {
        let old_primary = self.current_primary.write().take();

        if let Some(info) = old_primary {
            let _ = self.event_tx.send(PrimaryChangeEvent::Lost { old: info.node_id });
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
            let info = PrimaryInfo {
                node_id: primary.node_id,
                address: primary.client_addr.clone(),
                became_primary_at: Instant::now(),
                is_confirmed: true,
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
        let address = provider
            .get_node(new)
            .map(|n| n.client_addr)
            .unwrap_or_else(|| format!("{}:5432", new));

        let info = PrimaryInfo {
            node_id: new,
            address: address.clone(),
            became_primary_at: Instant::now(),
            is_confirmed: true,
        };

        *self.current_primary.write() = Some(info);

        let _ = self.event_tx.send(PrimaryChangeEvent::Changed {
            old,
            new,
            address,
        });

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
        let current_id = self.current_primary.read().as_ref().map(|p| p.node_id);

        if let Some(id) = current_id {
            if let Some(node) = provider.get_node(id) {
                if !node.is_healthy {
                    tracing::warn!("Primary {} is unhealthy in periodic check", id);
                }
            } else {
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
    fn test_standalone_primary_tracker() {
        let tracker = PrimaryTracker::new_standalone();

        assert!(!tracker.has_primary());

        let node_id = Uuid::new_v4();
        tracker.set_primary(node_id, "localhost:5432".to_string());

        assert!(tracker.has_primary());
        assert_eq!(tracker.get_primary_id(), Some(node_id));
        assert_eq!(tracker.get_primary_address(), Some("localhost:5432".to_string()));

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
    }

    /// Minimal mock topology provider for testing.
    struct MockTopology {
        event_tx: broadcast::Sender<TopologyEvent>,
        primary: RwLock<Option<TopologyNodeInfo>>,
    }

    impl MockTopology {
        fn new() -> Self {
            let (event_tx, _) = broadcast::channel(16);
            Self {
                event_tx,
                primary: RwLock::new(None),
            }
        }

        fn set_primary(&self, node_id: Uuid, addr: &str) {
            *self.primary.write() = Some(TopologyNodeInfo {
                node_id,
                client_addr: addr.to_string(),
                is_healthy: true,
            });
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
            let p = self.primary.read();
            p.as_ref().filter(|n| n.node_id == id).cloned()
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
        assert_eq!(tracker.get_primary_address(), Some("pg-primary:5432".to_string()));

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
        assert_eq!(tracker.get_primary_address(), Some("pg-sync:5432".to_string()));
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
                Self { leader: RwLock::new(None), event_tx: tx }
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
                self.leader.read().as_ref().filter(|n| n.node_id == id).cloned()
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

        let provider = PostgresTopologyProvider::new(nodes)
            .with_poll_interval(Duration::from_millis(200));
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
                Ok(TopologyEvent::HealthChanged { is_healthy: false, .. }) => {
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
}
