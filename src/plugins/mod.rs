//! WASM Plugin System
//!
//! Feature 11: Extensible plugin system using WebAssembly for safe,
//! sandboxed, high-performance extensibility.
//!
//! # Overview
//!
//! The WASM plugin system enables custom:
//! - Authentication schemes
//! - Query transformations
//! - Caching strategies
//! - Routing decisions
//! - Metrics collection
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │              WASM PLUGIN RUNTIME                 │
//! │  ┌──────────────────────────────────────────┐   │
//! │  │ Plugin Manager                           │   │
//! │  │ - Load/unload plugins                    │   │
//! │  │ - Version management                     │   │
//! │  │ - Health monitoring                      │   │
//! │  └──────────────────────────────────────────┘   │
//! │  ┌─────────────────┬─────────────────┐          │
//! │  │     Plugin A    │     Plugin B    │          │
//! │  │     (.wasm)     │     (.wasm)     │          │
//! │  └─────────────────┴─────────────────┘          │
//! │  ┌──────────────────────────────────────┐      │
//! │  │ Host Functions (Secure API)          │      │
//! │  │ - Query execution                    │      │
//! │  │ - Cache access                        │      │
//! │  │ - Metrics / Logging                  │      │
//! │  └──────────────────────────────────────┘      │
//! └─────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod runtime;
pub mod loader;
pub mod host_functions;
pub mod host_imports;
pub mod sandbox;
pub mod hot_reload;
pub mod metrics;

pub use config::{PluginRuntimeConfig, PluginRuntimeConfigBuilder, PluginConfig};
pub use runtime::{WasmPluginRuntime, LoadedPlugin, PluginState, PluginError};
pub use loader::{PluginLoader, PluginManifest, PluginLoadError};
pub use host_functions::HostFunctionRegistry;
pub use sandbox::{PluginSandbox, SecurityPolicy, Permission, ResourceLimits};
pub use hot_reload::{HotReloader, ReloadEvent, ReloadError};
pub use metrics::{PluginMetrics, PluginStats, HookLatency};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use parking_lot::RwLock;
use dashmap::DashMap;

/// Plugin metadata
#[derive(Debug, Clone)]
pub struct PluginMetadata {
    /// Plugin name
    pub name: String,

    /// Version string
    pub version: String,

    /// Description
    pub description: String,

    /// Author
    pub author: String,

    /// Supported hooks
    pub hooks: Vec<HookType>,

    /// Required permissions
    pub permissions: Vec<Permission>,

    /// Minimum memory requirement
    pub min_memory: usize,

    /// Maximum memory requirement
    pub max_memory: usize,
}

impl Default for PluginMetadata {
    fn default() -> Self {
        Self {
            name: String::new(),
            version: "0.0.0".to_string(),
            description: String::new(),
            author: String::new(),
            hooks: Vec::new(),
            permissions: Vec::new(),
            min_memory: 1024 * 1024,      // 1MB
            max_memory: 64 * 1024 * 1024, // 64MB
        }
    }
}

/// Hook types supported by plugins
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookType {
    /// Before query execution
    PreQuery,

    /// After query execution
    PostQuery,

    /// Authentication
    Authenticate,

    /// Authorization
    Authorize,

    /// Cache lookup
    CacheGet,

    /// Cache store
    CacheSet,

    /// Routing decision
    Route,

    /// Query rewriting
    Rewrite,

    /// Metrics collection
    Metrics,

    /// On connection
    OnConnect,

    /// On disconnect
    OnDisconnect,

    /// Custom hook
    Custom,
}

impl HookType {
    /// Get the export function name for this hook
    pub fn export_name(&self) -> &'static str {
        match self {
            HookType::PreQuery => "pre_query",
            HookType::PostQuery => "post_query",
            HookType::Authenticate => "authenticate",
            HookType::Authorize => "authorize",
            HookType::CacheGet => "cache_get",
            HookType::CacheSet => "cache_set",
            HookType::Route => "route",
            HookType::Rewrite => "rewrite",
            HookType::Metrics => "metrics",
            HookType::OnConnect => "on_connect",
            HookType::OnDisconnect => "on_disconnect",
            HookType::Custom => "custom_hook",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "pre_query" | "prequery" => Some(HookType::PreQuery),
            "post_query" | "postquery" => Some(HookType::PostQuery),
            "authenticate" | "auth" => Some(HookType::Authenticate),
            "authorize" => Some(HookType::Authorize),
            "cache_get" | "cacheget" => Some(HookType::CacheGet),
            "cache_set" | "cacheset" => Some(HookType::CacheSet),
            "route" | "routing" => Some(HookType::Route),
            "rewrite" => Some(HookType::Rewrite),
            "metrics" => Some(HookType::Metrics),
            "on_connect" | "connect" => Some(HookType::OnConnect),
            "on_disconnect" | "disconnect" => Some(HookType::OnDisconnect),
            "custom" => Some(HookType::Custom),
            _ => None,
        }
    }
}

/// Hook context passed to plugins
#[derive(Debug, Clone)]
pub struct HookContext {
    /// Request ID
    pub request_id: String,

    /// Client ID
    pub client_id: Option<String>,

    /// User identity
    pub identity: Option<String>,

    /// Current database
    pub database: Option<String>,

    /// Current branch
    pub branch: Option<String>,

    /// Additional attributes
    pub attributes: HashMap<String, String>,
}

impl Default for HookContext {
    fn default() -> Self {
        Self {
            request_id: uuid::Uuid::new_v4().to_string(),
            client_id: None,
            identity: None,
            database: None,
            branch: None,
            attributes: HashMap::new(),
        }
    }
}

/// Query context for query-related hooks
#[derive(Debug, Clone)]
pub struct QueryContext {
    /// The SQL query
    pub query: String,

    /// Normalized query (for fingerprinting)
    pub normalized: String,

    /// Tables referenced
    pub tables: Vec<String>,

    /// Is read-only query
    pub is_read_only: bool,

    /// Hook context
    pub hook_context: HookContext,
}

/// Result of a pre-query hook
#[derive(Debug, Clone)]
pub enum PreQueryResult {
    /// Continue with query
    Continue,

    /// Rewrite the query
    Rewrite(String),

    /// Block the query
    Block(String),

    /// Return cached result
    Cached(Vec<u8>),
}

/// Outcome passed to post-query hooks.
///
/// Observer-only — post hooks may not change the result that has already
/// gone back to the client. Useful for audit logs, metrics, and async
/// downstream signalling.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PostQueryOutcome {
    /// Whether the query completed successfully
    pub success: bool,

    /// Backend node the query was routed to (if any)
    pub target_node: Option<String>,

    /// Wall-clock execution time in microseconds
    pub elapsed_us: u64,

    /// Response size in bytes (including all protocol framing)
    pub response_bytes: u64,

    /// Error message if the query failed
    pub error: Option<String>,
}

/// Result of authentication hook
#[derive(Debug, Clone)]
pub enum AuthResult {
    /// Authentication successful
    Success(Identity),

    /// Authentication failed
    Denied(String),

    /// Defer to next authenticator
    Defer,
}

/// User identity from authentication
#[derive(Debug, Clone)]
pub struct Identity {
    /// User ID
    pub user_id: String,

    /// Username
    pub username: String,

    /// Roles
    pub roles: Vec<String>,

    /// Tenant ID
    pub tenant_id: Option<String>,

    /// Additional claims
    pub claims: HashMap<String, String>,
}

impl Default for Identity {
    fn default() -> Self {
        Self {
            user_id: String::new(),
            username: String::new(),
            roles: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
        }
    }
}

/// Result of routing hook
#[derive(Debug, Clone)]
pub enum RouteResult {
    /// Use default routing
    Default,

    /// Route to specific node
    Node(String),

    /// Route to primary
    Primary,

    /// Route to any standby
    Standby,

    /// Route to specific branch
    Branch(String),
}

/// Plugin manager for coordinating all plugins
pub struct PluginManager {
    /// Runtime for WASM execution
    runtime: Arc<WasmPluginRuntime>,

    /// Loaded plugins by name
    plugins: DashMap<String, Arc<LoadedPlugin>>,

    /// Hooks registry
    hooks: RwLock<HashMap<HookType, Vec<String>>>,

    /// Configuration
    config: PluginRuntimeConfig,

    /// Hot reloader (if enabled)
    hot_reloader: Option<HotReloader>,

    /// Metrics collector
    metrics: Arc<PluginMetrics>,
}

impl PluginManager {
    /// Create a new plugin manager
    pub fn new(config: PluginRuntimeConfig) -> Result<Self, PluginError> {
        let runtime = Arc::new(WasmPluginRuntime::new(&config)?);
        let metrics = Arc::new(PluginMetrics::new());

        let hot_reloader = if config.hot_reload {
            Some(HotReloader::new(&config.plugin_dir)?)
        } else {
            None
        };

        Ok(Self {
            runtime,
            plugins: DashMap::new(),
            hooks: RwLock::new(HashMap::new()),
            config,
            hot_reloader,
            metrics,
        })
    }

    /// Load a plugin from file
    pub fn load_plugin(&self, path: &std::path::Path) -> Result<(), PluginError> {
        let loader = PluginLoader::new();
        let (manifest, wasm_bytes) = loader.load(path)?;

        let plugin = self.runtime.instantiate(&manifest, &wasm_bytes)?;
        let plugin = Arc::new(plugin);

        // Register hooks
        {
            let mut hooks = self.hooks.write();
            for hook in &manifest.hooks {
                hooks
                    .entry(*hook)
                    .or_insert_with(Vec::new)
                    .push(manifest.name.clone());
            }
        }

        self.plugins.insert(manifest.name.clone(), plugin);

        tracing::info!(
            plugin = %manifest.name,
            version = %manifest.version,
            hooks = ?manifest.hooks,
            "Plugin loaded"
        );

        Ok(())
    }

    /// Unload a plugin
    pub fn unload_plugin(&self, name: &str) -> Result<(), PluginError> {
        if let Some((_, plugin)) = self.plugins.remove(name) {
            // Remove from hooks registry
            let mut hooks = self.hooks.write();
            for hook_plugins in hooks.values_mut() {
                hook_plugins.retain(|p| p != name);
            }

            // Call plugin's on_unload if it exists
            if let Err(e) = self.runtime.call_hook(&plugin, HookType::OnDisconnect, &[]) {
                tracing::warn!(plugin = %name, error = %e, "Error calling on_unload");
            }

            tracing::info!(plugin = %name, "Plugin unloaded");
        }

        Ok(())
    }

    /// Reload a plugin
    pub fn reload_plugin(&self, name: &str) -> Result<(), PluginError> {
        if let Some(plugin) = self.plugins.get(name) {
            let path = plugin.path.clone();
            drop(plugin);

            self.unload_plugin(name)?;
            self.load_plugin(&path)?;
        }

        Ok(())
    }

    /// Execute pre-query hooks
    pub fn execute_pre_query(&self, ctx: &QueryContext) -> PreQueryResult {
        let hooks = self.hooks.read();
        let plugin_names = hooks.get(&HookType::PreQuery).cloned().unwrap_or_default();
        drop(hooks);

        for plugin_name in plugin_names {
            if let Some(plugin) = self.plugins.get(&plugin_name) {
                let start = std::time::Instant::now();

                match self.runtime.call_pre_query(&plugin, ctx) {
                    Ok(result) => {
                        self.metrics.record_hook_call(
                            &plugin_name,
                            HookType::PreQuery,
                            start.elapsed(),
                            true,
                        );

                        match result {
                            PreQueryResult::Continue => continue,
                            other => return other,
                        }
                    }
                    Err(e) => {
                        self.metrics.record_hook_call(
                            &plugin_name,
                            HookType::PreQuery,
                            start.elapsed(),
                            false,
                        );
                        tracing::warn!(
                            plugin = %plugin_name,
                            error = %e,
                            "Pre-query hook failed"
                        );
                    }
                }
            }
        }

        PreQueryResult::Continue
    }

    /// Execute post-query hooks.
    ///
    /// Fan-out notification to every registered PostQuery plugin. Unlike
    /// `execute_pre_query`, no plugin can short-circuit the others — post
    /// hooks are observer-only (logging, metrics, audit). Errors from any
    /// plugin are logged but never block completion.
    pub fn execute_post_query(&self, ctx: &QueryContext, outcome: &PostQueryOutcome) {
        let hooks = self.hooks.read();
        let plugin_names = hooks.get(&HookType::PostQuery).cloned().unwrap_or_default();
        drop(hooks);

        for plugin_name in plugin_names {
            if let Some(plugin) = self.plugins.get(&plugin_name) {
                let start = std::time::Instant::now();

                // Serialise ctx + outcome into a single payload via the generic
                // `call_hook`. Runtime-specific marshalling lives there.
                let payload = match serde_json::to_vec(&(ctx, outcome)) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            plugin = %plugin_name,
                            error = %e,
                            "Post-query serialisation failed"
                        );
                        continue;
                    }
                };

                match self.runtime.call_hook(&plugin, HookType::PostQuery, &payload) {
                    Ok(_) => {
                        self.metrics.record_hook_call(
                            &plugin_name,
                            HookType::PostQuery,
                            start.elapsed(),
                            true,
                        );
                    }
                    Err(e) => {
                        self.metrics.record_hook_call(
                            &plugin_name,
                            HookType::PostQuery,
                            start.elapsed(),
                            false,
                        );
                        tracing::warn!(
                            plugin = %plugin_name,
                            error = %e,
                            "Post-query hook failed"
                        );
                    }
                }
            }
        }
    }

    /// Execute authentication hooks
    pub fn execute_authenticate(&self, request: &AuthRequest) -> AuthResult {
        let hooks = self.hooks.read();
        let plugin_names = hooks.get(&HookType::Authenticate).cloned().unwrap_or_default();
        drop(hooks);

        for plugin_name in plugin_names {
            if let Some(plugin) = self.plugins.get(&plugin_name) {
                let start = std::time::Instant::now();

                match self.runtime.call_authenticate(&plugin, request) {
                    Ok(result) => {
                        self.metrics.record_hook_call(
                            &plugin_name,
                            HookType::Authenticate,
                            start.elapsed(),
                            true,
                        );

                        match result {
                            AuthResult::Defer => continue,
                            other => return other,
                        }
                    }
                    Err(e) => {
                        self.metrics.record_hook_call(
                            &plugin_name,
                            HookType::Authenticate,
                            start.elapsed(),
                            false,
                        );
                        tracing::warn!(
                            plugin = %plugin_name,
                            error = %e,
                            "Authenticate hook failed"
                        );
                    }
                }
            }
        }

        AuthResult::Defer
    }

    /// Execute routing hooks
    pub fn execute_route(&self, ctx: &QueryContext) -> RouteResult {
        let hooks = self.hooks.read();
        let plugin_names = hooks.get(&HookType::Route).cloned().unwrap_or_default();
        drop(hooks);

        for plugin_name in plugin_names {
            if let Some(plugin) = self.plugins.get(&plugin_name) {
                let start = std::time::Instant::now();

                match self.runtime.call_route(&plugin, ctx) {
                    Ok(result) => {
                        self.metrics.record_hook_call(
                            &plugin_name,
                            HookType::Route,
                            start.elapsed(),
                            true,
                        );

                        match result {
                            RouteResult::Default => continue,
                            other => return other,
                        }
                    }
                    Err(e) => {
                        self.metrics.record_hook_call(
                            &plugin_name,
                            HookType::Route,
                            start.elapsed(),
                            false,
                        );
                        tracing::warn!(
                            plugin = %plugin_name,
                            error = %e,
                            "Route hook failed"
                        );
                    }
                }
            }
        }

        RouteResult::Default
    }

    /// List loaded plugins
    pub fn list_plugins(&self) -> Vec<PluginInfo> {
        self.plugins
            .iter()
            .map(|entry| {
                let plugin = entry.value();
                let stats = self.metrics.get_plugin_stats(&plugin.metadata.name);

                PluginInfo {
                    name: plugin.metadata.name.clone(),
                    version: plugin.metadata.version.clone(),
                    description: plugin.metadata.description.clone(),
                    hooks: plugin.metadata.hooks.clone(),
                    state: plugin.state.clone(),
                    stats,
                }
            })
            .collect()
    }

    /// Get plugin metrics
    pub fn get_metrics(&self) -> PluginManagerMetrics {
        PluginManagerMetrics {
            plugins_loaded: self.plugins.len(),
            total_hook_calls: self.metrics.total_calls(),
            total_errors: self.metrics.total_errors(),
            avg_latency: self.metrics.avg_latency(),
            plugins: self.list_plugins(),
        }
    }

    /// Check for hot reload updates
    pub fn check_updates(&self) -> Result<Vec<ReloadEvent>, PluginError> {
        if let Some(ref reloader) = self.hot_reloader {
            let events = reloader.check()?;

            for event in &events {
                match event {
                    ReloadEvent::Modified(name) => {
                        tracing::info!(plugin = %name, "Hot reloading plugin");
                        if let Err(e) = self.reload_plugin(name) {
                            tracing::error!(plugin = %name, error = %e, "Hot reload failed");
                        }
                    }
                    ReloadEvent::Removed(name) => {
                        tracing::info!(plugin = %name, "Plugin file removed, unloading");
                        if let Err(e) = self.unload_plugin(name) {
                            tracing::error!(plugin = %name, error = %e, "Unload failed");
                        }
                    }
                    ReloadEvent::Added(path) => {
                        tracing::info!(path = %path.display(), "New plugin detected, loading");
                        if let Err(e) = self.load_plugin(path) {
                            tracing::error!(path = %path.display(), error = %e, "Load failed");
                        }
                    }
                }
            }

            Ok(events)
        } else {
            Ok(Vec::new())
        }
    }
}

/// Authentication request
#[derive(Debug, Clone)]
pub struct AuthRequest {
    /// HTTP headers
    pub headers: HashMap<String, String>,

    /// Username (if provided)
    pub username: Option<String>,

    /// Password (if provided)
    pub password: Option<String>,

    /// Client IP
    pub client_ip: String,

    /// Target database
    pub database: Option<String>,
}

/// Plugin information for listing
#[derive(Debug, Clone)]
pub struct PluginInfo {
    /// Plugin name
    pub name: String,

    /// Version
    pub version: String,

    /// Description
    pub description: String,

    /// Supported hooks
    pub hooks: Vec<HookType>,

    /// Current state
    pub state: PluginState,

    /// Statistics
    pub stats: PluginStats,
}

/// Plugin manager metrics
#[derive(Debug, Clone)]
pub struct PluginManagerMetrics {
    /// Number of plugins loaded
    pub plugins_loaded: usize,

    /// Total hook calls
    pub total_hook_calls: u64,

    /// Total errors
    pub total_errors: u64,

    /// Average latency
    pub avg_latency: Duration,

    /// Per-plugin info
    pub plugins: Vec<PluginInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hook_type_export_name() {
        assert_eq!(HookType::PreQuery.export_name(), "pre_query");
        assert_eq!(HookType::Authenticate.export_name(), "authenticate");
        assert_eq!(HookType::Route.export_name(), "route");
    }

    #[test]
    fn test_hook_type_from_str() {
        assert_eq!(HookType::from_str("pre_query"), Some(HookType::PreQuery));
        assert_eq!(HookType::from_str("authenticate"), Some(HookType::Authenticate));
        assert_eq!(HookType::from_str("unknown"), None);
    }

    #[test]
    fn test_plugin_metadata_default() {
        let meta = PluginMetadata::default();
        assert!(meta.name.is_empty());
        assert_eq!(meta.version, "0.0.0");
        assert!(meta.hooks.is_empty());
    }

    #[test]
    fn test_hook_context_default() {
        let ctx = HookContext::default();
        assert!(!ctx.request_id.is_empty());
        assert!(ctx.client_id.is_none());
    }

    #[test]
    fn test_pre_query_result() {
        let result = PreQueryResult::Continue;
        assert!(matches!(result, PreQueryResult::Continue));

        let result = PreQueryResult::Block("blocked".to_string());
        assert!(matches!(result, PreQueryResult::Block(_)));
    }

    #[test]
    fn test_auth_result() {
        let result = AuthResult::Denied("invalid".to_string());
        assert!(matches!(result, AuthResult::Denied(_)));

        let result = AuthResult::Defer;
        assert!(matches!(result, AuthResult::Defer));
    }

    #[test]
    fn test_route_result() {
        let result = RouteResult::Default;
        assert!(matches!(result, RouteResult::Default));

        let result = RouteResult::Branch("test".to_string());
        assert!(matches!(result, RouteResult::Branch(_)));
    }

    #[test]
    fn test_identity_default() {
        let identity = Identity::default();
        assert!(identity.user_id.is_empty());
        assert!(identity.roles.is_empty());
        assert!(identity.tenant_id.is_none());
    }

    /// With no plugins registered, `execute_post_query` must be a silent
    /// no-op — the proxy's post-query hook call site fires unconditionally
    /// whenever a plugin manager exists, so "no hooks subscribed" must not
    /// panic or take a lock it shouldn't.
    #[test]
    fn test_execute_post_query_no_plugins_is_noop() {
        let config = PluginRuntimeConfig::default();
        let pm = PluginManager::new(config).expect("construct PluginManager");

        let ctx = QueryContext {
            query: "SELECT 1".to_string(),
            normalized: "SELECT 1".to_string(),
            tables: Vec::new(),
            is_read_only: true,
            hook_context: HookContext::default(),
        };
        let outcome = PostQueryOutcome {
            success: true,
            target_node: Some("primary".to_string()),
            elapsed_us: 42,
            response_bytes: 128,
            error: None,
        };

        // Must not panic; no plugins registered means this is pure no-op.
        pm.execute_post_query(&ctx, &outcome);

        // Metrics should remain empty — no hook was actually invoked.
        let metrics = pm.get_metrics();
        assert_eq!(metrics.plugins_loaded, 0);
        assert_eq!(metrics.total_hook_calls, 0);
    }

    /// Same for `execute_pre_query` — the no-plugins default path must
    /// yield `Continue` so the proxy's main loop forwards normally.
    #[test]
    fn test_execute_pre_query_no_plugins_returns_continue() {
        let pm = PluginManager::new(PluginRuntimeConfig::default())
            .expect("construct PluginManager");
        let ctx = QueryContext {
            query: "SELECT 1".to_string(),
            normalized: "SELECT 1".to_string(),
            tables: Vec::new(),
            is_read_only: true,
            hook_context: HookContext::default(),
        };
        assert!(matches!(pm.execute_pre_query(&ctx), PreQueryResult::Continue));
    }

    /// `PostQueryOutcome` must serialise cleanly — post-hook plugins
    /// receive a JSON representation on the WASM boundary.
    #[test]
    fn test_post_query_outcome_serialisation() {
        let outcome = PostQueryOutcome {
            success: false,
            target_node: None,
            elapsed_us: 1234,
            response_bytes: 0,
            error: Some("backend timeout".to_string()),
        };
        let json = serde_json::to_string(&outcome).expect("serialise");
        assert!(json.contains("\"success\":false"));
        assert!(json.contains("\"elapsed_us\":1234"));
        assert!(json.contains("backend timeout"));
    }
}
