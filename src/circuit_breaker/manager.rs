//! Circuit Breaker Manager
//!
//! Manages circuit breakers for multiple nodes with centralized configuration
//! and monitoring.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use super::breaker::{CircuitBreaker, CircuitOpen, RequestGuard};
use super::config::{CircuitBreakerConfig, NodeOverride};
use super::metrics::{CircuitMetrics, CircuitStats};
use super::state::{CircuitBreakerListener, CircuitState};

/// Configuration for the circuit breaker manager
#[derive(Debug, Clone)]
pub struct ManagerConfig {
    /// Global default configuration
    pub default_config: CircuitBreakerConfig,

    /// Per-node configuration overrides
    pub node_overrides: Vec<NodeOverride>,

    /// Enable manager-level metrics collection
    pub metrics_enabled: bool,

    /// Auto-create breakers for unknown nodes
    pub auto_create: bool,
}

impl Default for ManagerConfig {
    fn default() -> Self {
        Self {
            default_config: CircuitBreakerConfig::default(),
            node_overrides: Vec::new(),
            metrics_enabled: true,
            auto_create: true,
        }
    }
}

impl ManagerConfig {
    /// Create a new manager config
    pub fn new(default_config: CircuitBreakerConfig) -> Self {
        Self {
            default_config,
            ..Default::default()
        }
    }

    /// Add a node override
    pub fn with_node_override(mut self, override_: NodeOverride) -> Self {
        self.node_overrides.push(override_);
        self
    }

    /// Enable or disable metrics
    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.metrics_enabled = enabled;
        self
    }

    /// Get effective config for a node
    pub fn get_node_config(&self, node_id: &str) -> CircuitBreakerConfig {
        for override_ in &self.node_overrides {
            if override_.node_id == node_id {
                return override_.apply_to(&self.default_config);
            }
        }
        self.default_config.clone()
    }
}

/// Circuit Breaker Manager
///
/// Manages multiple circuit breakers for different nodes, providing centralized
/// configuration, monitoring, and node health filtering.
pub struct CircuitBreakerManager {
    /// Circuit breakers per node
    breakers: DashMap<String, CircuitBreaker>,

    /// Configuration
    config: parking_lot::RwLock<ManagerConfig>,

    /// Shared listeners for all breakers
    shared_listeners: parking_lot::RwLock<Vec<Arc<dyn CircuitBreakerListener>>>,

    /// Metrics collector
    metrics: CircuitMetrics,
}

impl CircuitBreakerManager {
    /// Create a new circuit breaker manager
    pub fn new(config: ManagerConfig) -> Self {
        Self {
            breakers: DashMap::new(),
            config: parking_lot::RwLock::new(config),
            shared_listeners: parking_lot::RwLock::new(Vec::new()),
            metrics: CircuitMetrics::new(),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(ManagerConfig::default())
    }

    /// Get or create a circuit breaker for a node
    pub fn get_breaker(&self, node_id: &str) -> CircuitBreaker {
        if let Some(breaker) = self.breakers.get(node_id) {
            return breaker.clone();
        }

        let config = self.config.read();
        if !config.auto_create {
            // Return a permissive breaker if auto-create disabled
            return CircuitBreaker::new(node_id, CircuitBreakerConfig::default());
        }

        let node_config = config.get_node_config(node_id);
        drop(config);

        let breaker = CircuitBreaker::new(node_id, node_config);

        // Add shared listeners
        let listeners = self.shared_listeners.read();
        for listener in listeners.iter() {
            breaker.add_listener(Arc::clone(listener));
        }

        self.breakers.insert(node_id.to_string(), breaker.clone());
        breaker
    }

    /// Try to allow a request to a specific node
    pub fn allow_request(&self, node_id: &str) -> Result<RequestGuard, CircuitOpen> {
        let breaker = self.get_breaker(node_id);
        let result = breaker.allow_request();

        // Record metrics
        let config = self.config.read();
        if config.metrics_enabled {
            drop(config);
            match &result {
                Ok(_) => self.metrics.record_allowed(node_id),
                Err(_) => self.metrics.record_rejected(node_id),
            }
        }

        result
    }

    /// Wrap a function with circuit breaker protection
    pub fn wrap_request<F, T, E>(&self, node_id: &str, f: F) -> Result<T, WrapError<E>>
    where
        F: FnOnce() -> Result<T, E>,
        E: std::fmt::Display,
    {
        let guard = self
            .allow_request(node_id)
            .map_err(WrapError::CircuitOpen)?;

        match f() {
            Ok(result) => {
                guard.success();
                Ok(result)
            }
            Err(e) => {
                guard.failure(&e.to_string());
                Err(WrapError::Inner(e))
            }
        }
    }

    /// Async version of wrap_request
    pub async fn wrap_request_async<F, Fut, T, E>(
        &self,
        node_id: &str,
        f: F,
    ) -> Result<T, WrapError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let guard = self
            .allow_request(node_id)
            .map_err(WrapError::CircuitOpen)?;

        match f().await {
            Ok(result) => {
                guard.success();
                Ok(result)
            }
            Err(e) => {
                guard.failure(&e.to_string());
                Err(WrapError::Inner(e))
            }
        }
    }

    /// Get healthy nodes from a list (filters out nodes with open circuits)
    pub fn get_healthy_nodes<T: HasNodeId + Clone>(&self, nodes: &[T]) -> Vec<T> {
        nodes
            .iter()
            .filter(|node| {
                self.breakers
                    .get(node.node_id())
                    .map(|b| b.get_state() != CircuitState::Open)
                    .unwrap_or(true) // Unknown nodes are considered healthy
            })
            .cloned()
            .collect()
    }

    /// Get all node IDs with open circuits
    pub fn get_open_circuits(&self) -> Vec<String> {
        self.breakers
            .iter()
            .filter(|entry| entry.value().get_state() == CircuitState::Open)
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get all node IDs with unhealthy circuits (open or half-open)
    pub fn get_unhealthy_nodes(&self) -> Vec<String> {
        self.breakers
            .iter()
            .filter(|entry| entry.value().get_state().is_unhealthy())
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get state for all managed nodes
    pub fn get_all_states(&self) -> Vec<(String, CircuitState)> {
        self.breakers
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().get_state()))
            .collect()
    }

    /// Force open circuit for a node
    pub fn force_open(&self, node_id: &str, admin: Option<&str>) {
        let breaker = self.get_breaker(node_id);
        breaker.force_open(admin);
    }

    /// Force close circuit for a node
    pub fn force_close(&self, node_id: &str, admin: Option<&str>) {
        if let Some(breaker) = self.breakers.get(node_id) {
            breaker.force_close(admin);
        }
    }

    /// Reset circuit for a node
    pub fn reset(&self, node_id: &str) {
        if let Some(breaker) = self.breakers.get(node_id) {
            breaker.reset();
        }
    }

    /// Reset all circuits
    pub fn reset_all(&self) {
        for entry in self.breakers.iter() {
            entry.value().reset();
        }
    }

    /// Remove a circuit breaker
    pub fn remove(&self, node_id: &str) -> Option<CircuitBreaker> {
        self.breakers.remove(node_id).map(|(_, b)| b)
    }

    /// Add a shared listener for all circuit breakers
    pub fn add_listener(&self, listener: Arc<dyn CircuitBreakerListener>) {
        // Add to existing breakers
        for entry in self.breakers.iter() {
            entry.value().add_listener(Arc::clone(&listener));
        }

        // Store for future breakers
        self.shared_listeners.write().push(listener);
    }

    /// Update global configuration
    pub fn update_config(&self, config: ManagerConfig) {
        // Update existing breakers with new configs
        for entry in self.breakers.iter() {
            let node_config = config.get_node_config(entry.key());
            entry.value().update_config(node_config);
        }

        *self.config.write() = config;
    }

    /// Get current configuration
    pub fn config(&self) -> ManagerConfig {
        self.config.read().clone()
    }

    /// Get metrics
    pub fn metrics(&self) -> &CircuitMetrics {
        &self.metrics
    }

    /// Get statistics for all circuits
    pub fn get_stats(&self) -> CircuitStats {
        let mut stats = CircuitStats::default();

        for entry in self.breakers.iter() {
            let breaker = entry.value();
            stats.add_node_stats(
                entry.key(),
                breaker.get_state(),
                breaker.failure_count(),
                breaker.open_count(),
                breaker.total_failures(),
                breaker.total_successes(),
            );
        }

        stats
    }

    /// Get number of managed nodes
    pub fn node_count(&self) -> usize {
        self.breakers.len()
    }

    /// Check if a specific node exists
    pub fn has_node(&self, node_id: &str) -> bool {
        self.breakers.contains_key(node_id)
    }
}

/// Error type for wrapped requests
#[derive(Debug)]
pub enum WrapError<E> {
    /// Circuit is open
    CircuitOpen(CircuitOpen),
    /// Inner function error
    Inner(E),
}

impl<E: std::fmt::Display> std::fmt::Display for WrapError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WrapError::CircuitOpen(open) => write!(f, "{}", open),
            WrapError::Inner(e) => write!(f, "{}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for WrapError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WrapError::CircuitOpen(open) => Some(open),
            WrapError::Inner(e) => Some(e),
        }
    }
}

impl<E> WrapError<E> {
    /// Check if this is a circuit open error
    pub fn is_circuit_open(&self) -> bool {
        matches!(self, WrapError::CircuitOpen(_))
    }

    /// Get retry-after duration if circuit is open
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            WrapError::CircuitOpen(open) => Some(open.retry_after),
            WrapError::Inner(_) => None,
        }
    }
}

/// Trait for types that have a node ID
pub trait HasNodeId {
    fn node_id(&self) -> &str;
}

impl HasNodeId for String {
    fn node_id(&self) -> &str {
        self
    }
}

impl HasNodeId for &str {
    fn node_id(&self) -> &str {
        self
    }
}

/// Simple node info for testing
#[derive(Debug, Clone)]
pub struct SimpleNode {
    pub id: String,
}

impl HasNodeId for SimpleNode {
    fn node_id(&self) -> &str {
        &self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manager_creation() {
        let manager = CircuitBreakerManager::with_defaults();
        assert_eq!(manager.node_count(), 0);
    }

    #[test]
    fn test_manager_get_breaker() {
        let manager = CircuitBreakerManager::with_defaults();

        let breaker = manager.get_breaker("node-1");
        assert_eq!(breaker.node_id(), "node-1");
        assert_eq!(breaker.get_state(), CircuitState::Closed);

        assert_eq!(manager.node_count(), 1);
        assert!(manager.has_node("node-1"));
    }

    #[test]
    fn test_manager_allow_request() {
        let manager = CircuitBreakerManager::with_defaults();

        let guard = manager.allow_request("node-1").expect("should allow");
        guard.success();

        let breaker = manager.get_breaker("node-1");
        assert_eq!(breaker.total_successes(), 1);
    }

    #[test]
    fn test_manager_healthy_nodes() {
        let config =
            ManagerConfig::new(CircuitBreakerConfig::builder().failure_threshold(2).build());
        let manager = CircuitBreakerManager::new(config);

        // Create some nodes
        let nodes = vec![
            SimpleNode {
                id: "node-1".to_string(),
            },
            SimpleNode {
                id: "node-2".to_string(),
            },
            SimpleNode {
                id: "node-3".to_string(),
            },
        ];

        // Initially all healthy
        let healthy = manager.get_healthy_nodes(&nodes);
        assert_eq!(healthy.len(), 3);

        // Open circuit for node-2
        manager.force_open("node-2", None);

        let healthy = manager.get_healthy_nodes(&nodes);
        assert_eq!(healthy.len(), 2);
        assert!(healthy.iter().all(|n| n.id != "node-2"));
    }

    #[test]
    fn test_manager_wrap_request() {
        let manager = CircuitBreakerManager::with_defaults();

        let result = manager.wrap_request("node-1", || Ok::<i32, &str>(42));
        assert_eq!(result.unwrap(), 42);

        let result = manager.wrap_request("node-1", || Err::<i32, &str>("error"));
        assert!(result.is_err());
    }

    #[test]
    fn test_manager_node_overrides() {
        let config =
            ManagerConfig::new(CircuitBreakerConfig::builder().failure_threshold(5).build())
                .with_node_override(NodeOverride::new("special-node").with_failure_threshold(10));

        let manager = CircuitBreakerManager::new(config);

        let normal_breaker = manager.get_breaker("normal-node");
        assert_eq!(normal_breaker.config().failure_threshold, 5);

        let special_breaker = manager.get_breaker("special-node");
        assert_eq!(special_breaker.config().failure_threshold, 10);
    }

    #[test]
    fn test_manager_get_open_circuits() {
        let manager = CircuitBreakerManager::with_defaults();

        manager.force_open("node-1", None);
        manager.force_open("node-3", None);
        let _ = manager.get_breaker("node-2"); // Closed

        let open = manager.get_open_circuits();
        assert_eq!(open.len(), 2);
        assert!(open.contains(&"node-1".to_string()));
        assert!(open.contains(&"node-3".to_string()));
    }

    #[test]
    fn test_manager_reset_all() {
        let config =
            ManagerConfig::new(CircuitBreakerConfig::builder().failure_threshold(1).build());
        let manager = CircuitBreakerManager::new(config);

        // Open some circuits
        manager.force_open("node-1", None);
        manager.force_open("node-2", None);

        assert_eq!(manager.get_open_circuits().len(), 2);

        manager.reset_all();
        assert_eq!(manager.get_open_circuits().len(), 0);
    }

    #[tokio::test]
    async fn test_manager_wrap_async() {
        let manager = CircuitBreakerManager::with_defaults();

        let result = manager
            .wrap_request_async("node-1", || async { Ok::<i32, &str>(42) })
            .await;
        assert_eq!(result.unwrap(), 42);
    }
}
