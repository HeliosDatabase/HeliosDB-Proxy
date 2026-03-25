//! WASM Plugin Runtime
//!
//! Core runtime for executing WebAssembly plugins using wasmtime.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use super::config::PluginRuntimeConfig;
use super::host_functions::HostFunctionRegistry;
use super::sandbox::{PluginSandbox, SecurityPolicy, ResourceLimits};
use super::{
    AuthRequest, AuthResult, HookType, PluginMetadata, PreQueryResult,
    QueryContext, RouteResult,
};

/// Error types for plugin operations
#[derive(Debug, Clone)]
pub enum PluginError {
    /// Failed to load plugin
    LoadError(String),

    /// Failed to instantiate plugin
    InstantiationError(String),

    /// Plugin execution failed
    ExecutionError(String),

    /// Plugin timed out
    Timeout(String),

    /// Memory limit exceeded
    MemoryExceeded(String),

    /// Security policy violation
    SecurityViolation(String),

    /// Invalid plugin manifest
    InvalidManifest(String),

    /// Hook not found
    HookNotFound(String),

    /// Internal runtime error
    RuntimeError(String),
}

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginError::LoadError(msg) => write!(f, "Load error: {}", msg),
            PluginError::InstantiationError(msg) => write!(f, "Instantiation error: {}", msg),
            PluginError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            PluginError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            PluginError::MemoryExceeded(msg) => write!(f, "Memory exceeded: {}", msg),
            PluginError::SecurityViolation(msg) => write!(f, "Security violation: {}", msg),
            PluginError::InvalidManifest(msg) => write!(f, "Invalid manifest: {}", msg),
            PluginError::HookNotFound(msg) => write!(f, "Hook not found: {}", msg),
            PluginError::RuntimeError(msg) => write!(f, "Runtime error: {}", msg),
        }
    }
}

impl std::error::Error for PluginError {}

/// Plugin state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginState {
    /// Plugin is loading
    Loading,

    /// Plugin is ready
    Running,

    /// Plugin is paused
    Paused,

    /// Plugin has errored
    Error(String),

    /// Plugin is unloading
    Unloading,
}

/// A loaded and instantiated plugin
pub struct LoadedPlugin {
    /// Plugin metadata
    pub metadata: PluginMetadata,

    /// Current state
    pub state: PluginState,

    /// File path
    pub path: PathBuf,

    /// Compiled module bytes (serialized)
    compiled_module: Vec<u8>,

    /// Security sandbox
    sandbox: PluginSandbox,

    /// Instance data (mock for non-wasmtime builds)
    instance_data: RwLock<PluginInstanceData>,

    /// Creation timestamp
    loaded_at: Instant,

    /// Last invocation timestamp
    last_invoked: RwLock<Option<Instant>>,

    /// Invocation count
    invocation_count: std::sync::atomic::AtomicU64,
}

/// Plugin instance data
struct PluginInstanceData {
    /// Plugin memory usage
    memory_used: usize,

    /// Fuel consumed (if metering enabled)
    fuel_consumed: u64,

    /// Custom state from plugin
    state: HashMap<String, Vec<u8>>,
}

impl LoadedPlugin {
    /// Create a new loaded plugin
    pub fn new(
        metadata: PluginMetadata,
        path: PathBuf,
        compiled_module: Vec<u8>,
        sandbox: PluginSandbox,
    ) -> Self {
        Self {
            metadata,
            state: PluginState::Running,
            path,
            compiled_module,
            sandbox,
            instance_data: RwLock::new(PluginInstanceData {
                memory_used: 0,
                fuel_consumed: 0,
                state: HashMap::new(),
            }),
            loaded_at: Instant::now(),
            last_invoked: RwLock::new(None),
            invocation_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get memory usage
    pub fn memory_used(&self) -> usize {
        self.instance_data.read().memory_used
    }

    /// Get invocation count
    pub fn invocation_count(&self) -> u64 {
        self.invocation_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get uptime
    pub fn uptime(&self) -> Duration {
        self.loaded_at.elapsed()
    }

    /// Get last invoked time
    pub fn last_invoked(&self) -> Option<Instant> {
        *self.last_invoked.read()
    }

    /// Record an invocation
    pub fn record_invocation(&self) {
        self.invocation_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *self.last_invoked.write() = Some(Instant::now());
    }
}

/// WASM plugin runtime
pub struct WasmPluginRuntime {
    /// Runtime configuration
    config: PluginRuntimeConfig,

    /// Host function registry
    host_functions: Arc<HostFunctionRegistry>,

    /// Module cache (path -> compiled bytes)
    module_cache: RwLock<HashMap<PathBuf, Vec<u8>>>,

    /// Default security policy
    default_policy: SecurityPolicy,

    /// Creation timestamp
    created_at: Instant,
}

impl WasmPluginRuntime {
    /// Create a new WASM runtime
    pub fn new(config: &PluginRuntimeConfig) -> Result<Self, PluginError> {
        let host_functions = Arc::new(HostFunctionRegistry::new());

        let default_policy = SecurityPolicy {
            allowed_hosts: vec!["localhost".to_string()],
            allowed_paths: vec![config.plugin_dir.clone()],
            max_memory: config.memory_limit,
            max_execution_time: config.timeout,
            allow_network: false,
            allow_filesystem: false,
        };

        Ok(Self {
            config: config.clone(),
            host_functions,
            module_cache: RwLock::new(HashMap::new()),
            default_policy,
            created_at: Instant::now(),
        })
    }

    /// Instantiate a plugin from manifest and WASM bytes
    pub fn instantiate(
        &self,
        manifest: &super::loader::PluginManifest,
        wasm_bytes: &[u8],
    ) -> Result<LoadedPlugin, PluginError> {
        // Validate WASM module (basic magic number check)
        if wasm_bytes.len() < 8 {
            return Err(PluginError::LoadError("WASM module too small".to_string()));
        }

        // WASM magic number: 0x00 0x61 0x73 0x6d (\\0asm)
        if &wasm_bytes[0..4] != b"\x00asm" {
            return Err(PluginError::LoadError("Invalid WASM magic number".to_string()));
        }

        // Build metadata from manifest
        let metadata = PluginMetadata {
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            description: manifest.description.clone(),
            author: manifest.author.clone(),
            hooks: manifest.hooks.clone(),
            permissions: manifest.permissions.clone(),
            min_memory: manifest.min_memory,
            max_memory: manifest.max_memory.min(self.config.memory_limit),
        };

        // Build sandbox with merged policy
        let resource_limits = ResourceLimits {
            max_memory: metadata.max_memory,
            max_execution_time: self.config.timeout,
            max_fuel: if self.config.fuel_metering {
                Some(self.config.fuel_limit)
            } else {
                None
            },
            max_table_elements: 10000,
            max_instances: 1,
        };

        let sandbox = PluginSandbox::new(
            self.default_policy.clone(),
            resource_limits,
            manifest.permissions.clone(),
        );

        // "Compile" module (in real impl, would use wasmtime::Module::new)
        // For now, store the raw bytes as the "compiled" form
        let compiled_module = wasm_bytes.to_vec();

        // Cache the compiled module
        {
            let mut cache = self.module_cache.write();
            cache.insert(manifest.path.clone(), compiled_module.clone());
        }

        Ok(LoadedPlugin::new(
            metadata,
            manifest.path.clone(),
            compiled_module,
            sandbox,
        ))
    }

    /// Call a hook on a plugin
    pub fn call_hook(
        &self,
        plugin: &LoadedPlugin,
        hook: HookType,
        args: &[u8],
    ) -> Result<Vec<u8>, PluginError> {
        // Check if plugin supports this hook
        if !plugin.metadata.hooks.contains(&hook) {
            return Err(PluginError::HookNotFound(format!(
                "Plugin {} does not support hook {:?}",
                plugin.metadata.name, hook
            )));
        }

        // Check state
        if plugin.state != PluginState::Running {
            return Err(PluginError::ExecutionError(format!(
                "Plugin {} is not running (state: {:?})",
                plugin.metadata.name, plugin.state
            )));
        }

        // Record invocation
        plugin.record_invocation();

        // In a real implementation, this would:
        // 1. Get or create a wasmtime::Instance
        // 2. Look up the exported function for this hook
        // 3. Call the function with args
        // 4. Handle timeout/fuel exhaustion
        // 5. Return the result

        // For now, return empty success
        Ok(Vec::new())
    }

    /// Call pre-query hook
    pub fn call_pre_query(
        &self,
        plugin: &LoadedPlugin,
        ctx: &QueryContext,
    ) -> Result<PreQueryResult, PluginError> {
        // Serialize context
        let args = serde_json::to_vec(ctx).map_err(|e| {
            PluginError::ExecutionError(format!("Failed to serialize context: {}", e))
        })?;

        // Call the hook
        let result = self.call_hook(plugin, HookType::PreQuery, &args)?;

        // Deserialize result (or return default)
        if result.is_empty() {
            return Ok(PreQueryResult::Continue);
        }

        serde_json::from_slice(&result).map_err(|e| {
            PluginError::ExecutionError(format!("Failed to deserialize result: {}", e))
        })
    }

    /// Call authenticate hook
    pub fn call_authenticate(
        &self,
        plugin: &LoadedPlugin,
        request: &AuthRequest,
    ) -> Result<AuthResult, PluginError> {
        // Serialize request
        let args = serde_json::to_vec(request).map_err(|e| {
            PluginError::ExecutionError(format!("Failed to serialize request: {}", e))
        })?;

        // Call the hook
        let result = self.call_hook(plugin, HookType::Authenticate, &args)?;

        // Deserialize result (or return default)
        if result.is_empty() {
            return Ok(AuthResult::Defer);
        }

        serde_json::from_slice(&result).map_err(|e| {
            PluginError::ExecutionError(format!("Failed to deserialize result: {}", e))
        })
    }

    /// Call route hook
    pub fn call_route(
        &self,
        plugin: &LoadedPlugin,
        ctx: &QueryContext,
    ) -> Result<RouteResult, PluginError> {
        // Serialize context
        let args = serde_json::to_vec(ctx).map_err(|e| {
            PluginError::ExecutionError(format!("Failed to serialize context: {}", e))
        })?;

        // Call the hook
        let result = self.call_hook(plugin, HookType::Route, &args)?;

        // Deserialize result (or return default)
        if result.is_empty() {
            return Ok(RouteResult::Default);
        }

        serde_json::from_slice(&result).map_err(|e| {
            PluginError::ExecutionError(format!("Failed to deserialize result: {}", e))
        })
    }

    /// Get runtime statistics
    pub fn stats(&self) -> RuntimeStats {
        RuntimeStats {
            uptime: self.created_at.elapsed(),
            cached_modules: self.module_cache.read().len(),
            fuel_metering_enabled: self.config.fuel_metering,
            memory_limit: self.config.memory_limit,
            timeout: self.config.timeout,
        }
    }
}

/// Runtime statistics
#[derive(Debug, Clone)]
pub struct RuntimeStats {
    /// Uptime
    pub uptime: Duration,

    /// Number of cached modules
    pub cached_modules: usize,

    /// Whether fuel metering is enabled
    pub fuel_metering_enabled: bool,

    /// Memory limit per plugin
    pub memory_limit: usize,

    /// Execution timeout
    pub timeout: Duration,
}

// Serialization support for hook types
impl serde::Serialize for QueryContext {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("QueryContext", 5)?;
        state.serialize_field("query", &self.query)?;
        state.serialize_field("normalized", &self.normalized)?;
        state.serialize_field("tables", &self.tables)?;
        state.serialize_field("is_read_only", &self.is_read_only)?;
        state.end()
    }
}

impl serde::Serialize for AuthRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("AuthRequest", 5)?;
        state.serialize_field("headers", &self.headers)?;
        state.serialize_field("username", &self.username)?;
        state.serialize_field("password", &self.password)?;
        state.serialize_field("client_ip", &self.client_ip)?;
        state.serialize_field("database", &self.database)?;
        state.end()
    }
}

impl<'de> serde::Deserialize<'de> for PreQueryResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Helper {
            action: String,
            #[serde(default)]
            value: Option<String>,
            #[serde(default)]
            data: Option<Vec<u8>>,
        }

        let helper = Helper::deserialize(deserializer)?;
        match helper.action.as_str() {
            "continue" => Ok(PreQueryResult::Continue),
            "rewrite" => Ok(PreQueryResult::Rewrite(
                helper.value.unwrap_or_default(),
            )),
            "block" => Ok(PreQueryResult::Block(
                helper.value.unwrap_or_default(),
            )),
            "cached" => Ok(PreQueryResult::Cached(
                helper.data.unwrap_or_default(),
            )),
            _ => Ok(PreQueryResult::Continue),
        }
    }
}

impl<'de> serde::Deserialize<'de> for AuthResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Helper {
            action: String,
            #[serde(default)]
            identity: Option<IdentityHelper>,
            #[serde(default)]
            message: Option<String>,
        }

        #[derive(serde::Deserialize)]
        struct IdentityHelper {
            user_id: String,
            username: String,
            #[serde(default)]
            roles: Vec<String>,
            #[serde(default)]
            tenant_id: Option<String>,
        }

        let helper = Helper::deserialize(deserializer)?;
        match helper.action.as_str() {
            "success" => {
                let id = helper.identity.unwrap_or(IdentityHelper {
                    user_id: String::new(),
                    username: String::new(),
                    roles: Vec::new(),
                    tenant_id: None,
                });
                Ok(AuthResult::Success(super::Identity {
                    user_id: id.user_id,
                    username: id.username,
                    roles: id.roles,
                    tenant_id: id.tenant_id,
                    claims: std::collections::HashMap::new(),
                }))
            }
            "denied" => Ok(AuthResult::Denied(
                helper.message.unwrap_or_default(),
            )),
            "defer" => Ok(AuthResult::Defer),
            _ => Ok(AuthResult::Defer),
        }
    }
}

impl<'de> serde::Deserialize<'de> for RouteResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Helper {
            action: String,
            #[serde(default)]
            target: Option<String>,
        }

        let helper = Helper::deserialize(deserializer)?;
        match helper.action.as_str() {
            "default" => Ok(RouteResult::Default),
            "node" => Ok(RouteResult::Node(helper.target.unwrap_or_default())),
            "primary" => Ok(RouteResult::Primary),
            "standby" => Ok(RouteResult::Standby),
            "branch" => Ok(RouteResult::Branch(helper.target.unwrap_or_default())),
            _ => Ok(RouteResult::Default),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_error_display() {
        let err = PluginError::LoadError("test".to_string());
        assert!(err.to_string().contains("Load error"));

        let err = PluginError::Timeout("plugin-a".to_string());
        assert!(err.to_string().contains("Timeout"));
    }

    #[test]
    fn test_plugin_state() {
        assert_eq!(PluginState::Running, PluginState::Running);
        assert_ne!(PluginState::Running, PluginState::Paused);
    }

    #[test]
    fn test_runtime_creation() {
        let config = PluginRuntimeConfig::default();
        let runtime = WasmPluginRuntime::new(&config);
        assert!(runtime.is_ok());
    }

    #[test]
    fn test_runtime_stats() {
        let config = PluginRuntimeConfig::default();
        let runtime = WasmPluginRuntime::new(&config).unwrap();
        let stats = runtime.stats();

        assert_eq!(stats.cached_modules, 0);
        assert!(stats.fuel_metering_enabled);
    }

    #[test]
    fn test_loaded_plugin_invocation_count() {
        let metadata = PluginMetadata::default();
        let sandbox = PluginSandbox::default();
        let plugin = LoadedPlugin::new(
            metadata,
            PathBuf::from("/test/plugin.wasm"),
            vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
            sandbox,
        );

        assert_eq!(plugin.invocation_count(), 0);
        plugin.record_invocation();
        assert_eq!(plugin.invocation_count(), 1);
        plugin.record_invocation();
        assert_eq!(plugin.invocation_count(), 2);
    }
}
