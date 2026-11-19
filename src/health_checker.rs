//! Health Checker - HeliosProxy
//!
//! Continuous node health monitoring with configurable checks,
//! failure detection, and automatic recovery.
//!
//! Every check opens a real backend connection, so the checker needs a
//! backend template ([`HealthChecker::with_backend_template`]). Without one,
//! [`HealthChecker::start`] and [`HealthChecker::force_check`] fail with
//! [`ProxyError::HealthCheck`] and a forced check marks the node unhealthy:
//! a node that could not be checked is never reported as passing a check.

use super::{NodeEndpoint, NodeId, ProxyError, Result};
use crate::backend::{BackendClient, BackendConfig};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch, RwLock};
use tokio::task::{JoinHandle, JoinSet};

/// Health checker configuration
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Interval between health checks
    pub check_interval: Duration,
    /// Timeout for health check
    pub check_timeout: Duration,
    /// Number of consecutive failures before marking unhealthy
    pub failure_threshold: u32,
    /// Number of consecutive successes before marking healthy
    pub success_threshold: u32,
    /// Enable detailed health checks (query execution)
    pub detailed_checks: bool,
    /// Health check query (if detailed_checks enabled)
    pub check_query: String,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(5),
            check_timeout: Duration::from_secs(3),
            failure_threshold: 3,
            success_threshold: 2,
            detailed_checks: false,
            check_query: "SELECT 1".to_string(),
        }
    }
}

/// Node health status
#[derive(Debug, Clone)]
pub struct NodeHealth {
    /// Node ID
    pub node_id: NodeId,
    /// Is node healthy
    pub healthy: bool,
    /// Last check timestamp
    pub last_check: Option<chrono::DateTime<chrono::Utc>>,
    /// Last successful check
    pub last_success: Option<chrono::DateTime<chrono::Utc>>,
    /// Consecutive failures
    pub consecutive_failures: u32,
    /// Consecutive successes
    pub consecutive_successes: u32,
    /// Last error message
    pub last_error: Option<String>,
    /// Average response time (ms)
    pub avg_response_ms: f64,
    /// Total checks performed
    pub total_checks: u64,
    /// Total failures
    pub total_failures: u64,
    /// Ticks skipped for this node because the previous probe was still
    /// running (the per-node share of the `health_probe_skipped_inflight`
    /// counter).
    pub probes_skipped_inflight: u64,
}

impl NodeHealth {
    fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            healthy: true, // Assume healthy until proven otherwise
            last_check: None,
            last_success: None,
            consecutive_failures: 0,
            consecutive_successes: 0,
            last_error: None,
            avg_response_ms: 0.0,
            total_checks: 0,
            total_failures: 0,
            probes_skipped_inflight: 0,
        }
    }
}

/// RAII marker for "a probe for this node is currently running".
///
/// Acquired synchronously on the checker loop *before* the probe task is
/// spawned and moved into that task, so the slot is released when the probe
/// finishes, panics, or is aborted by [`HealthChecker::stop`] (abort drops the
/// task's locals, which runs this `Drop`).
struct InFlightGuard {
    node_id: NodeId,
    set: Arc<Mutex<HashSet<NodeId>>>,
}

impl InFlightGuard {
    /// Returns `None` when a probe for `node_id` is already in flight.
    fn try_acquire(set: &Arc<Mutex<HashSet<NodeId>>>, node_id: NodeId) -> Option<Self> {
        if set.lock().insert(node_id) {
            Some(Self {
                node_id,
                set: set.clone(),
            })
        } else {
            None
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.set.lock().remove(&self.node_id);
    }
}

/// Why a health check did not succeed.
#[derive(Debug)]
enum CheckFailure {
    /// The check could not run at all (no backend template or no
    /// registered endpoint). Marks the node unhealthy at once, regardless
    /// of `failure_threshold`: there is nothing transient to wait out.
    Unconfigured(String),
    /// The check ran and failed (connect error, query error, timeout).
    /// Counted against `failure_threshold`.
    Failed(String),
}

/// Health check event
#[derive(Debug, Clone)]
pub enum HealthEvent {
    /// Node became healthy
    NodeHealthy { node_id: NodeId },
    /// Node became unhealthy
    NodeUnhealthy { node_id: NodeId, reason: String },
    /// Health check completed
    CheckCompleted { node_id: NodeId, latency_ms: f64 },
    /// Health check failed
    CheckFailed { node_id: NodeId, error: String },
}

/// Health Checker
pub struct HealthChecker {
    /// Configuration
    config: HealthConfig,
    /// Node endpoints
    nodes: Arc<RwLock<HashMap<NodeId, NodeEndpoint>>>,
    /// Node health states
    health: Arc<RwLock<HashMap<NodeId, NodeHealth>>>,
    /// Event channel sender
    event_tx: mpsc::Sender<HealthEvent>,
    /// Event channel receiver
    event_rx: Option<mpsc::Receiver<HealthEvent>>,
    /// Running flag
    running: Arc<RwLock<bool>>,
    /// Backend-connection template. Host/port are overridden per-node at
    /// check time; auth, TLS, and timeouts are shared. When `None`, no
    /// check can run: `start` and `force_check` fail, and a forced check
    /// marks the node unhealthy.
    backend_template: Option<BackendConfig>,
    /// Nodes with a probe still running. A tick that finds a node here skips
    /// it instead of stacking a second probe: with
    /// `check_timeout >= check_interval` (both configurable) a hung backend
    /// would otherwise collect one extra detached task — and one extra fresh
    /// connection — per tick, i.e. a reconnection storm against a backend that
    /// is already struggling.
    in_flight: Arc<Mutex<HashSet<NodeId>>>,
    /// `health_probe_skipped_inflight`: total ticks skipped by the guard
    /// above. Exposed via [`HealthChecker::health_probe_skipped_inflight`] and,
    /// per node, via [`NodeHealth::probes_skipped_inflight`].
    skipped_inflight: Arc<AtomicU64>,
    /// Shutdown signal for the probe loop. Dropping the sender (i.e. dropping
    /// the checker) also stops the loop.
    shutdown_tx: Mutex<Option<watch::Sender<bool>>>,
    /// Handle to the probe loop task. The loop owns the `JoinSet` of in-flight
    /// probes, so aborting this handle aborts every probe with it.
    probe_loop: Mutex<Option<JoinHandle<()>>>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(config: HealthConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);

        Self {
            config,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            health: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            event_rx: Some(event_rx),
            running: Arc::new(RwLock::new(false)),
            backend_template: None,
            in_flight: Arc::new(Mutex::new(HashSet::new())),
            skipped_inflight: Arc::new(AtomicU64::new(0)),
            shutdown_tx: Mutex::new(None),
            probe_loop: Mutex::new(None),
        }
    }

    /// Attach a backend-connection template. Required for health checks:
    /// the checker clones it, swaps host/port for each node, opens a
    /// connection, runs `config.check_query`, and reports success/failure.
    /// Without a template no check can run (see [`Self::start`] and
    /// [`Self::force_check`]).
    pub fn with_backend_template(mut self, template: BackendConfig) -> Self {
        self.backend_template = Some(template);
        self
    }

    /// Add a node to monitor
    pub fn add_node(&mut self, endpoint: NodeEndpoint) {
        let node_id = endpoint.id;
        let nodes = self.nodes.clone();
        let health = self.health.clone();

        tokio::spawn(async move {
            nodes.write().await.insert(node_id, endpoint);
            health
                .write()
                .await
                .insert(node_id, NodeHealth::new(node_id));
        });
    }

    /// Remove a node from monitoring
    pub fn remove_node(&mut self, node_id: &NodeId) {
        let id = *node_id;
        let nodes = self.nodes.clone();
        let health = self.health.clone();

        tokio::spawn(async move {
            nodes.write().await.remove(&id);
            health.write().await.remove(&id);
        });
    }

    /// Start health checking.
    ///
    /// Fails with [`ProxyError::HealthCheck`] when no backend template is
    /// attached: the checker could not run a single check, so it does not
    /// pretend to monitor anything.
    pub async fn start(&self) -> Result<()> {
        if self.backend_template.is_none() {
            return Err(ProxyError::HealthCheck(Self::no_template_reason()));
        }
        {
            let mut running = self.running.write().await;
            if *running {
                return Ok(()); // Already running
            }
            *running = true;
        }

        let config = self.config.clone();
        let nodes = self.nodes.clone();
        let health = self.health.clone();
        let event_tx = self.event_tx.clone();
        let running = self.running.clone();
        let backend_template = self.backend_template.clone();
        let in_flight = self.in_flight.clone();
        let skipped_inflight = self.skipped_inflight.clone();

        // A timeout at least as long as the interval means a hung node cannot
        // be re-probed every tick; those ticks are skipped (and counted as
        // `health_probe_skipped_inflight`) instead. Warn rather than reject --
        // existing deployments may legitimately run this way.
        if config.check_timeout >= config.check_interval {
            tracing::warn!(
                check_timeout_ms = config.check_timeout.as_millis() as u64,
                check_interval_ms = config.check_interval.as_millis() as u64,
                "health check_timeout >= check_interval: probes for a slow node will skip ticks (health_probe_skipped_inflight) instead of stacking"
            );
        }

        // Fresh shutdown channel per start() so a previously stopped checker
        // can be restarted without inheriting a stale signal.
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        *self.shutdown_tx.lock() = Some(shutdown_tx);

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);
            // Owning the probe tasks (instead of detaching them) is what makes
            // stop() able to abort probes that are still in flight: dropping
            // this JoinSet -- which happens when this task is aborted -- aborts
            // every probe it holds.
            let mut probes: JoinSet<()> = JoinSet::new();

            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    // stop() fired, or the checker (and with it the sender) was
                    // dropped. Either way, leave now rather than at the next
                    // tick -- with a long check_interval that could be minutes.
                    _ = shutdown_rx.changed() => break,
                }

                if !*running.read().await {
                    break;
                }

                // Reap finished probes so the JoinSet does not grow over a long
                // uptime (their in-flight slots are already released by Drop).
                while probes.try_join_next().is_some() {}

                // Snapshot (node_id, endpoint) pairs under a short read
                // lock so the spawned tasks don't race on the map.
                let snapshot: Vec<(NodeId, NodeEndpoint)> = nodes
                    .read()
                    .await
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .collect();

                for (node_id, endpoint) in snapshot {
                    // Acquire the per-node slot synchronously, before spawning,
                    // so the decision is exact for this tick.
                    let guard = match InFlightGuard::try_acquire(&in_flight, node_id) {
                        Some(guard) => guard,
                        None => {
                            // Previous probe still running: skip this tick for
                            // this node. Opening another connection to a node
                            // that is already not answering only adds to the
                            // pile-up.
                            skipped_inflight.fetch_add(1, Ordering::Relaxed);
                            if let Some(node_health) = health.write().await.get_mut(&node_id) {
                                node_health.probes_skipped_inflight += 1;
                            }
                            tracing::debug!(
                                node_id = ?node_id,
                                "health_probe_skipped_inflight: probe still in flight, skipping tick"
                            );
                            continue;
                        }
                    };

                    let config = config.clone();
                    let health = health.clone();
                    let event_tx = event_tx.clone();
                    let template = backend_template.clone();

                    probes.spawn(async move {
                        // Released when this probe ends -- including on abort,
                        // which drops the task's locals.
                        let _in_flight = guard;
                        Self::check_node_health(
                            node_id,
                            Some(endpoint),
                            template,
                            &config,
                            &health,
                            &event_tx,
                        )
                        .await;
                    });
                }
            }

            // Cooperative exit: abort and reap whatever is still probing.
            probes.shutdown().await;
            tracing::info!("Health checker stopped");
        });

        *self.probe_loop.lock() = Some(handle);

        tracing::info!("Health checker started");
        Ok(())
    }

    /// Stop health checking.
    ///
    /// Takes effect immediately. The loop is woken through its shutdown
    /// channel, and its task handle is aborted so that in-flight probes (owned
    /// by the loop's `JoinSet`) are aborted with it. Previously this only
    /// cleared a flag that the loop noticed on its *next* `interval.tick()`,
    /// leaving the loop and any hung probe alive for up to a full interval.
    pub async fn stop(&self) -> Result<()> {
        *self.running.write().await = false;
        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(true);
        }
        // The handle is left in place (not taken) so a repeated stop() is a
        // harmless no-op and callers can still observe completion.
        if let Some(handle) = self.probe_loop.lock().as_ref() {
            handle.abort();
        }
        tracing::info!("Health checker stopped");
        Ok(())
    }

    /// `health_probe_skipped_inflight` -- total health-check ticks skipped
    /// because a probe for that node was still running. A steadily rising
    /// value means at least one backend is answering slower than
    /// `check_interval`. Per-node counts live in
    /// [`NodeHealth::probes_skipped_inflight`], which is part of the
    /// `get_health` / `all_health` output.
    pub fn health_probe_skipped_inflight(&self) -> u64 {
        self.skipped_inflight.load(Ordering::Relaxed)
    }

    /// Number of nodes with a health probe currently in flight.
    pub fn in_flight_probes(&self) -> usize {
        self.in_flight.lock().len()
    }

    fn no_template_reason() -> String {
        "health checker has no backend template, so it cannot connect to any node; \
         attach one with HealthChecker::with_backend_template"
            .to_string()
    }

    /// Check a single node's health
    async fn check_node_health(
        node_id: NodeId,
        endpoint: Option<NodeEndpoint>,
        backend_template: Option<BackendConfig>,
        config: &HealthConfig,
        health: &Arc<RwLock<HashMap<NodeId, NodeHealth>>>,
        event_tx: &mpsc::Sender<HealthEvent>,
    ) {
        let start = std::time::Instant::now();
        let check_result =
            Self::perform_check(endpoint.as_ref(), backend_template.as_ref(), config).await;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        let mut health_guard = health.write().await;
        if let Some(node_health) = health_guard.get_mut(&node_id) {
            if !matches!(check_result, Err(CheckFailure::Unconfigured(_))) {
                node_health.total_checks += 1;
                node_health.last_check = Some(chrono::Utc::now());

                // Update average response time (exponential moving average)
                let alpha = 0.2;
                node_health.avg_response_ms =
                    alpha * latency_ms + (1.0 - alpha) * node_health.avg_response_ms;
            }

            match check_result {
                Err(CheckFailure::Unconfigured(reason)) => {
                    // No check ran: leave the check counters, timestamps and
                    // latency alone, but never leave the node reported healthy.
                    node_health.consecutive_successes = 0;
                    node_health.last_error = Some(reason.clone());
                    if node_health.healthy {
                        node_health.healthy = false;
                        let _ = event_tx
                            .send(HealthEvent::NodeUnhealthy {
                                node_id,
                                reason: reason.clone(),
                            })
                            .await;
                        tracing::warn!("Node {:?} marked unhealthy: {}", node_id, reason);
                    }
                    let _ = event_tx
                        .send(HealthEvent::CheckFailed {
                            node_id,
                            error: reason,
                        })
                        .await;
                }
                Ok(()) => {
                    node_health.consecutive_failures = 0;
                    node_health.consecutive_successes += 1;
                    node_health.last_success = Some(chrono::Utc::now());
                    node_health.last_error = None;

                    // Check if should mark healthy
                    if !node_health.healthy
                        && node_health.consecutive_successes >= config.success_threshold
                    {
                        node_health.healthy = true;
                        let _ = event_tx.send(HealthEvent::NodeHealthy { node_id }).await;
                        tracing::info!("Node {:?} marked healthy", node_id);
                    }

                    let _ = event_tx
                        .send(HealthEvent::CheckCompleted {
                            node_id,
                            latency_ms,
                        })
                        .await;
                }
                Err(CheckFailure::Failed(error)) => {
                    node_health.consecutive_successes = 0;
                    node_health.consecutive_failures += 1;
                    node_health.total_failures += 1;
                    node_health.last_error = Some(error.clone());

                    // Check if should mark unhealthy
                    if node_health.healthy
                        && node_health.consecutive_failures >= config.failure_threshold
                    {
                        node_health.healthy = false;
                        let _ = event_tx
                            .send(HealthEvent::NodeUnhealthy {
                                node_id,
                                reason: error.clone(),
                            })
                            .await;
                        tracing::warn!("Node {:?} marked unhealthy: {}", node_id, error);
                    }

                    let _ = event_tx
                        .send(HealthEvent::CheckFailed { node_id, error })
                        .await;
                }
            }
        }
    }

    /// Perform the actual health check against a backend.
    ///
    /// Connects using a template `BackendConfig` with host/port swapped
    /// for the node's endpoint, then runs `config.check_query` (default
    /// `SELECT 1`) via a simple scalar query. The whole operation is
    /// timed-out by `config.check_timeout`.
    ///
    /// If `endpoint` or `backend_template` is `None` no check can run and
    /// the result is [`CheckFailure::Unconfigured`] — never success.
    async fn perform_check(
        endpoint: Option<&NodeEndpoint>,
        backend_template: Option<&BackendConfig>,
        config: &HealthConfig,
    ) -> std::result::Result<(), CheckFailure> {
        let template = backend_template
            .ok_or_else(|| CheckFailure::Unconfigured(Self::no_template_reason()))?;
        let endpoint = endpoint.ok_or_else(|| {
            CheckFailure::Unconfigured(
                "node has no registered endpoint, so it cannot be checked".to_string(),
            )
        })?;

        let mut cfg = template.clone();
        cfg.host = endpoint.host.clone();
        cfg.port = endpoint.port;
        cfg.connect_timeout = cfg.connect_timeout.min(config.check_timeout);

        let outcome = tokio::time::timeout(config.check_timeout, async {
            let mut client = BackendClient::connect(&cfg)
                .await
                .map_err(|e| format!("connect: {}", e))?;
            let _scalar = client
                .query_scalar(&config.check_query)
                .await
                .map_err(|e| format!("query: {}", e))?;
            client.close().await;
            Ok::<(), String>(())
        })
        .await;

        match outcome {
            Ok(inner) => inner.map_err(CheckFailure::Failed),
            Err(_) => Err(CheckFailure::Failed(format!(
                "health check exceeded {:?}",
                config.check_timeout
            ))),
        }
    }

    /// Get health status for a node
    pub async fn get_health(&self, node_id: &NodeId) -> Option<NodeHealth> {
        self.health.read().await.get(node_id).cloned()
    }

    /// Get all health statuses
    pub async fn all_health(&self) -> HashMap<NodeId, NodeHealth> {
        self.health.read().await.clone()
    }

    /// Get count of healthy nodes
    pub async fn healthy_count(&self) -> usize {
        self.health
            .read()
            .await
            .values()
            .filter(|h| h.healthy)
            .count()
    }

    /// Get count of unhealthy nodes
    pub async fn unhealthy_count(&self) -> usize {
        self.health
            .read()
            .await
            .values()
            .filter(|h| !h.healthy)
            .count()
    }

    /// Force a health check for a specific node.
    ///
    /// Deliberately bypasses the periodic loop's in-flight guard: this is an
    /// explicit operator/caller-driven probe and it runs inline (awaited by the
    /// caller) rather than being spawned, so it cannot pile up the way detached
    /// per-tick probes could.
    ///
    /// `Ok(())` means the check ran; its outcome is in [`Self::get_health`].
    /// Fails with [`ProxyError::HealthCheck`] when the node is not registered,
    /// or when no backend template is attached — in that case the node is
    /// also marked unhealthy with that reason as its `last_error`.
    pub async fn force_check(&self, node_id: &NodeId) -> Result<()> {
        let config = self.config.clone();
        let health = self.health.clone();
        let event_tx = self.event_tx.clone();
        let id = *node_id;
        let endpoint = self.nodes.read().await.get(&id).cloned();
        if endpoint.is_none() || !health.read().await.contains_key(&id) {
            return Err(ProxyError::HealthCheck(format!(
                "node {:?} is not registered with the health checker",
                id
            )));
        }
        let template = self.backend_template.clone();
        let configured = template.is_some();

        Self::check_node_health(id, endpoint, template, &config, &health, &event_tx).await;
        if configured {
            Ok(())
        } else {
            Err(ProxyError::HealthCheck(Self::no_template_reason()))
        }
    }

    /// Mark a node as unhealthy (manual override)
    pub async fn mark_unhealthy(&self, node_id: &NodeId, reason: &str) {
        if let Some(health) = self.health.write().await.get_mut(node_id) {
            health.healthy = false;
            health.last_error = Some(reason.to_string());

            let _ = self
                .event_tx
                .send(HealthEvent::NodeUnhealthy {
                    node_id: *node_id,
                    reason: reason.to_string(),
                })
                .await;
        }
    }

    /// Mark a node as healthy (manual override)
    pub async fn mark_healthy(&self, node_id: &NodeId) {
        if let Some(health) = self.health.write().await.get_mut(node_id) {
            health.healthy = true;
            health.last_error = None;
            health.consecutive_failures = 0;

            let _ = self
                .event_tx
                .send(HealthEvent::NodeHealthy { node_id: *node_id })
                .await;
        }
    }

    /// Take the event receiver
    pub fn take_event_receiver(&mut self) -> Option<mpsc::Receiver<HealthEvent>> {
        self.event_rx.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = HealthConfig::default();
        assert_eq!(config.check_interval, Duration::from_secs(5));
        assert_eq!(config.failure_threshold, 3);
        assert_eq!(config.success_threshold, 2);
    }

    #[test]
    fn test_node_health_new() {
        let node_id = NodeId::new();
        let health = NodeHealth::new(node_id);

        assert!(health.healthy);
        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.consecutive_successes, 0);
    }

    #[tokio::test]
    async fn test_add_remove_node() {
        let mut checker = HealthChecker::new(HealthConfig::default());
        let endpoint = NodeEndpoint::new("localhost", 5432);
        let node_id = endpoint.id;

        checker.add_node(endpoint);

        // Wait for async task
        tokio::time::sleep(Duration::from_millis(50)).await;

        let health = checker.get_health(&node_id).await;
        assert!(health.is_some());

        checker.remove_node(&node_id);

        // Wait for async task
        tokio::time::sleep(Duration::from_millis(50)).await;

        let health = checker.get_health(&node_id).await;
        assert!(health.is_none());
    }

    #[tokio::test]
    async fn test_mark_unhealthy() {
        let checker = HealthChecker::new(HealthConfig::default());
        let node_id = NodeId::new();

        checker
            .health
            .write()
            .await
            .insert(node_id, NodeHealth::new(node_id));

        checker.mark_unhealthy(&node_id, "Test failure").await;

        let health = checker.get_health(&node_id).await.unwrap();
        assert!(!health.healthy);
        assert_eq!(health.last_error, Some("Test failure".to_string()));
    }

    fn unreachable_template() -> BackendConfig {
        use crate::backend::{tls::default_client_config, TlsMode};
        BackendConfig {
            host: "unused".into(),
            port: 0,
            user: "postgres".into(),
            password: None,
            database: None,
            application_name: Some("helios-health-check".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_millis(200),
            query_timeout: Duration::from_millis(200),
            tls_config: default_client_config(),
        }
    }

    /// Fail closed: without a template or endpoint no check can run, and
    /// `perform_check` says so — it never reports success.
    #[tokio::test]
    async fn test_perform_check_without_template_or_endpoint_is_unconfigured() {
        let config = HealthConfig::default();
        let endpoint = NodeEndpoint::new("127.0.0.1", 1);
        let template = unreachable_template();

        match HealthChecker::perform_check(Some(&endpoint), None, &config).await {
            Err(CheckFailure::Unconfigured(msg)) => {
                assert!(msg.contains("no backend template"), "{msg}")
            }
            other => panic!("expected Unconfigured, got {other:?}"),
        }
        match HealthChecker::perform_check(None, Some(&template), &config).await {
            Err(CheckFailure::Unconfigured(msg)) => assert!(msg.contains("endpoint"), "{msg}"),
            other => panic!("expected Unconfigured, got {other:?}"),
        }
    }

    /// `start` refuses to run a checker that cannot check anything.
    #[tokio::test]
    async fn test_start_without_template_fails() {
        let checker = HealthChecker::new(HealthConfig::default());
        let err = checker
            .start()
            .await
            .expect_err("start without a backend template must fail");
        assert!(matches!(err, ProxyError::HealthCheck(_)), "got {err:?}");
        assert!(!*checker.running.read().await);
        assert!(checker.probe_loop.lock().is_none());
    }

    /// A forced check without a template fails and marks the node
    /// unhealthy at once (no `failure_threshold` grace), without counting a
    /// check that never ran.
    #[tokio::test]
    async fn test_force_check_without_template_marks_node_unhealthy() {
        let mut checker = HealthChecker::new(HealthConfig {
            failure_threshold: 3,
            ..Default::default()
        });
        let mut events = checker.take_event_receiver().expect("receiver");
        let endpoint = NodeEndpoint::new("127.0.0.1", 1);
        let node_id = endpoint.id;
        checker.add_node(endpoint);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let err = checker
            .force_check(&node_id)
            .await
            .expect_err("forced check without a template must fail");
        assert!(matches!(err, ProxyError::HealthCheck(_)), "got {err:?}");

        let health = checker.get_health(&node_id).await.expect("health");
        assert!(!health.healthy, "an unchecked node must not stay healthy");
        assert!(
            health
                .last_error
                .as_deref()
                .is_some_and(|e| e.contains("no backend template")),
            "last_error: {:?}",
            health.last_error
        );
        assert_eq!(health.total_checks, 0, "no check ran");
        assert!(health.last_check.is_none());
        assert_eq!(checker.healthy_count().await, 0);

        match events.try_recv() {
            Ok(HealthEvent::NodeUnhealthy { node_id: id, .. }) => assert_eq!(id, node_id),
            other => panic!("expected NodeUnhealthy, got {other:?}"),
        }
    }

    /// A forced check for a node the checker does not know is an error.
    #[tokio::test]
    async fn test_force_check_unknown_node_errors() {
        let checker = HealthChecker::new(HealthConfig::default())
            .with_backend_template(unreachable_template());
        let err = checker
            .force_check(&NodeId::new())
            .await
            .expect_err("unknown node");
        assert!(matches!(err, ProxyError::HealthCheck(_)), "got {err:?}");
    }

    /// A forced check that runs and fails counts against the threshold
    /// (real-check behaviour is unchanged).
    #[tokio::test]
    async fn test_force_check_failure_counts_against_threshold() {
        let mut checker = HealthChecker::new(HealthConfig {
            check_timeout: Duration::from_millis(300),
            failure_threshold: 2,
            ..Default::default()
        })
        .with_backend_template(unreachable_template());
        let endpoint = NodeEndpoint::new("127.0.0.1", 1);
        let node_id = endpoint.id;
        checker.add_node(endpoint);
        tokio::time::sleep(Duration::from_millis(50)).await;

        checker.force_check(&node_id).await.expect("check ran");
        let health = checker.get_health(&node_id).await.unwrap();
        assert!(health.healthy, "one failure is below the threshold of 2");
        assert_eq!(health.total_checks, 1);
        assert_eq!(health.total_failures, 1);

        checker.force_check(&node_id).await.expect("check ran");
        let health = checker.get_health(&node_id).await.unwrap();
        assert!(!health.healthy);
        assert_eq!(health.total_checks, 2);
    }

    /// When the endpoint + template point at an unreachable address,
    /// `perform_check` surfaces a connect error inside the timeout.
    /// This proves the real-check path is wired end-to-end without
    /// requiring a live PG instance.
    #[tokio::test]
    async fn test_perform_check_returns_connect_error_to_unreachable_endpoint() {
        use crate::backend::{tls::default_client_config, TlsMode};

        let config = HealthConfig {
            check_interval: Duration::from_secs(1),
            // Tight timeout — we want this test to finish in a few
            // hundred ms even when the OS TCP stack stalls.
            check_timeout: Duration::from_millis(300),
            failure_threshold: 1,
            success_threshold: 1,
            detailed_checks: true,
            check_query: "SELECT 1".to_string(),
        };

        // 127.0.0.1:1 — almost always refused (no daemon on port 1).
        let endpoint = NodeEndpoint::new("127.0.0.1", 1);
        let template = BackendConfig {
            host: "unused".into(),
            port: 0,
            user: "postgres".into(),
            password: None,
            database: None,
            application_name: Some("helios-health-check".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_millis(200),
            query_timeout: Duration::from_millis(200),
            tls_config: default_client_config(),
        };

        let result = HealthChecker::perform_check(Some(&endpoint), Some(&template), &config).await;
        // Error should mention either "connect" (refused / unreachable)
        // or the timeout message.
        let msg = match result {
            Err(CheckFailure::Failed(msg)) => msg,
            other => panic!("expected a failed check, got {other:?}"),
        };
        assert!(
            msg.contains("connect") || msg.contains("exceeded"),
            "unexpected error message: {}",
            msg
        );
    }

    // ---------------------------------------------------------------------
    // In-flight probe guard + prompt shutdown
    // ---------------------------------------------------------------------

    fn hanging_template(timeout: Duration) -> BackendConfig {
        use crate::backend::{tls::default_client_config, TlsMode};
        BackendConfig {
            host: "unused".into(),
            port: 0,
            user: "postgres".into(),
            password: None,
            database: None,
            application_name: Some("helios-health-check".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: timeout,
            query_timeout: timeout,
            tls_config: default_client_config(),
        }
    }

    /// Fake backend that accepts connections and never answers the startup
    /// message, so every probe against it blocks for the whole
    /// `check_timeout`. Returns the bound address plus a count of accepted
    /// connections — i.e. of probes actually spawned.
    async fn spawn_hanging_backend() -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>)
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = accepted.clone();
        tokio::spawn(async move {
            // Hold each socket open: closing it would fail the probe fast and
            // release its in-flight slot, defeating the point of the fixture.
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                held.push(sock);
            }
        });
        (addr, accepted)
    }

    /// A probe that is still running must not be spawned again on the next
    /// tick. Before the in-flight guard, `check_timeout >= check_interval`
    /// meant one extra detached task — and one extra backend connection — per
    /// tick against a hung node (a reconnection storm); this test counts the
    /// connections the node actually receives.
    #[tokio::test]
    async fn test_inflight_guard_skips_ticks_instead_of_stacking_probes() {
        let (addr, accepted) = spawn_hanging_backend().await;

        let config = HealthConfig {
            check_interval: Duration::from_millis(50),
            // Deliberately >= interval — the pathological-but-legal config.
            check_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let mut checker = HealthChecker::new(config)
            .with_backend_template(hanging_template(Duration::from_secs(30)));
        let endpoint = NodeEndpoint::new(addr.ip().to_string(), addr.port());
        let node_id = endpoint.id;
        checker.add_node(endpoint);
        tokio::time::sleep(Duration::from_millis(50)).await;

        checker.start().await.unwrap();
        // ~10 ticks at 50ms.
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "hung node must be probed exactly once while that probe is in flight"
        );
        assert_eq!(checker.in_flight_probes(), 1);
        assert!(
            checker.health_probe_skipped_inflight() >= 2,
            "expected skipped ticks to be counted, got {}",
            checker.health_probe_skipped_inflight()
        );

        let health = checker.get_health(&node_id).await.unwrap();
        assert!(
            health.probes_skipped_inflight >= 2,
            "per-node skip counter not surfaced in health output: {}",
            health.probes_skipped_inflight
        );
        assert_eq!(
            health.total_checks, 0,
            "a skipped tick must not be recorded as a completed check"
        );

        checker.stop().await.unwrap();
    }

    /// `stop()` must take effect immediately — both for the loop task and for
    /// probes still in flight. Previously it only cleared a flag that the loop
    /// read on its next tick, so with a 30s interval the loop (and the hung
    /// probe's backend connection) lived on for up to 30s after shutdown.
    #[tokio::test]
    async fn test_stop_aborts_inflight_probe_and_loop_immediately() {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (eof_tx, eof_rx) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            // Never answer. The only way this loop sees EOF is the probe task
            // being dropped, i.e. aborted.
            loop {
                match sock.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            let _ = eof_tx.send(());
        });

        let config = HealthConfig {
            check_interval: Duration::from_secs(30),
            check_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let mut checker = HealthChecker::new(config)
            .with_backend_template(hanging_template(Duration::from_secs(30)));
        checker.add_node(NodeEndpoint::new(addr.ip().to_string(), addr.port()));
        tokio::time::sleep(Duration::from_millis(50)).await;

        checker.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            checker.in_flight_probes(),
            1,
            "probe should still be hanging on the fake backend"
        );

        checker.stop().await.unwrap();

        tokio::time::timeout(Duration::from_secs(2), eof_rx)
            .await
            .expect("stop() did not abort the in-flight probe within 2s")
            .expect("fake backend task died");

        let loop_finished = {
            let guard = checker.probe_loop.lock();
            guard.as_ref().unwrap().is_finished()
        };
        assert!(
            loop_finished,
            "probe loop still alive after stop() (it used to wait for the next tick)"
        );
        assert_eq!(
            checker.in_flight_probes(),
            0,
            "in-flight slot must be released when the probe is aborted"
        );
    }

    /// The guard is per node: a node whose probe is hung must not block probes
    /// for the other nodes.
    #[tokio::test]
    async fn test_inflight_guard_is_per_node() {
        let (slow_addr, slow_accepted) = spawn_hanging_backend().await;

        let config = HealthConfig {
            check_interval: Duration::from_millis(50),
            check_timeout: Duration::from_secs(30),
            ..Default::default()
        };
        let mut checker = HealthChecker::new(config)
            .with_backend_template(hanging_template(Duration::from_secs(30)));

        checker.add_node(NodeEndpoint::new(
            slow_addr.ip().to_string(),
            slow_addr.port(),
        ));
        // 127.0.0.1:1 — refused immediately, so this node's probe always
        // finishes inside a tick and is free to run again next tick.
        let fast = NodeEndpoint::new("127.0.0.1", 1);
        let fast_id = fast.id;
        checker.add_node(fast);
        tokio::time::sleep(Duration::from_millis(50)).await;

        checker.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        checker.stop().await.unwrap();

        assert_eq!(
            slow_accepted.load(Ordering::SeqCst),
            1,
            "hung node must not be re-probed"
        );
        let fast_health = checker.get_health(&fast_id).await.unwrap();
        assert!(
            fast_health.total_checks >= 2,
            "fast node was starved by the hung node: {} checks",
            fast_health.total_checks
        );
        assert_eq!(
            fast_health.probes_skipped_inflight, 0,
            "fast node should never be skipped"
        );
    }

    #[tokio::test]
    async fn test_healthy_count() {
        let checker = HealthChecker::new(HealthConfig::default());

        let node1 = NodeId::new();
        let node2 = NodeId::new();
        let node3 = NodeId::new();

        {
            let mut health = checker.health.write().await;
            health.insert(node1, NodeHealth::new(node1));
            health.insert(node2, NodeHealth::new(node2));

            let mut unhealthy = NodeHealth::new(node3);
            unhealthy.healthy = false;
            health.insert(node3, unhealthy);
        }

        assert_eq!(checker.healthy_count().await, 2);
        assert_eq!(checker.unhealthy_count().await, 1);
    }
}
