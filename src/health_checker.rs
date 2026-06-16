//! Health Checker - HeliosProxy
//!
//! Continuous node health monitoring with configurable checks,
//! failure detection, and automatic recovery.

use super::{NodeEndpoint, NodeId, Result};
use crate::backend::{BackendClient, BackendConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};

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
        }
    }
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
    /// Optional backend-connection template. Host/port are overridden
    /// per-node at check time; auth, TLS, and timeouts are shared. When
    /// `None`, `perform_check` returns Ok(()) without opening a socket
    /// — useful for unit tests and for construction-time scenarios
    /// where the caller does not yet have backend credentials.
    backend_template: Option<BackendConfig>,
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
        }
    }

    /// Attach a backend-connection template. Required for real health
    /// checks: the checker clones it, swaps host/port for each node,
    /// opens a connection, runs `config.check_query`, and reports
    /// success/failure. Without a template the checker runs a no-op
    /// success (retained for tests that construct a checker without
    /// real PG backing).
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

    /// Start health checking
    pub async fn start(&self) -> Result<()> {
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

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);

            loop {
                interval.tick().await;

                if !*running.read().await {
                    break;
                }

                // Snapshot (node_id, endpoint) pairs under a short read
                // lock so the spawned tasks don't race on the map.
                let snapshot: Vec<(NodeId, NodeEndpoint)> = nodes
                    .read()
                    .await
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .collect();

                for (node_id, endpoint) in snapshot {
                    let config = config.clone();
                    let health = health.clone();
                    let event_tx = event_tx.clone();
                    let template = backend_template.clone();

                    tokio::spawn(async move {
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

            tracing::info!("Health checker stopped");
        });

        tracing::info!("Health checker started");
        Ok(())
    }

    /// Stop health checking
    pub async fn stop(&self) -> Result<()> {
        *self.running.write().await = false;
        Ok(())
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
            node_health.total_checks += 1;
            node_health.last_check = Some(chrono::Utc::now());

            // Update average response time (exponential moving average)
            let alpha = 0.2;
            node_health.avg_response_ms =
                alpha * latency_ms + (1.0 - alpha) * node_health.avg_response_ms;

            match check_result {
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
                Err(error) => {
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
    /// If either `endpoint` or `backend_template` is `None`, returns
    /// `Ok(())` immediately — this is the skeleton path used by unit
    /// tests that don't wire a real backend. Production callers are
    /// expected to supply both via
    /// `HealthChecker::with_backend_template` + `add_node(endpoint)`.
    async fn perform_check(
        endpoint: Option<&NodeEndpoint>,
        backend_template: Option<&BackendConfig>,
        config: &HealthConfig,
    ) -> std::result::Result<(), String> {
        let (endpoint, template) = match (endpoint, backend_template) {
            (Some(e), Some(t)) => (e, t),
            _ => return Ok(()), // Skeleton / unit-test path.
        };

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
            Ok(inner) => inner,
            Err(_) => Err(format!("health check exceeded {:?}", config.check_timeout)),
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

    /// Force a health check for a specific node
    pub async fn force_check(&self, node_id: &NodeId) -> Result<()> {
        let config = self.config.clone();
        let health = self.health.clone();
        let event_tx = self.event_tx.clone();
        let id = *node_id;
        let endpoint = self.nodes.read().await.get(&id).cloned();
        let template = self.backend_template.clone();

        Self::check_node_health(id, endpoint, template, &config, &health, &event_tx).await;
        Ok(())
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

    /// Without an endpoint or template, `perform_check` must return
    /// `Ok(())` immediately — preserves the pre-T0-TR3 test-friendly
    /// behaviour for unit tests that don't stand up a real backend.
    #[tokio::test]
    async fn test_perform_check_skeleton_path_returns_ok() {
        let config = HealthConfig::default();
        let result = HealthChecker::perform_check(None, None, &config).await;
        assert!(result.is_ok());
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
            host: "placeholder".into(),
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
        assert!(result.is_err(), "expected failure, got {:?}", result);
        // Error should mention either "connect" (refused / unreachable)
        // or the timeout message.
        let msg = result.unwrap_err();
        assert!(
            msg.contains("connect") || msg.contains("exceeded"),
            "unexpected error message: {}",
            msg
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
