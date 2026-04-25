//! WASM Plugin Runtime
//!
//! Real wasmtime-backed plugin executor.
//!
//! ## ABI
//!
//! Plugins export, at minimum:
//!
//! - `memory` (default linear memory).
//! - `alloc(size: i32) -> i32` — host calls this to obtain a slot
//!   in plugin memory it can write input bytes into.
//! - `dealloc(ptr: i32, size: i32)` — host calls this to free either
//!   the input slot (after the call) or the output slot (after the
//!   host has read the result).
//! - One function per declared hook, with one of two signatures:
//!   - **Result-returning hooks** (`pre_query`, `route`,
//!     `authenticate`, `rewrite`): `(ptr: i32, len: i32) -> i64`
//!     where the i64 is `(result_ptr << 32) | result_len`.
//!     `result_ptr == 0 && result_len == 0` is a valid "no result"
//!     reply (host treats it as the default per-hook outcome).
//!   - **Observer hooks** (`post_query`, `metrics`, `on_connect`,
//!     `on_disconnect`): `(ptr: i32, len: i32)` with no return —
//!     the host ignores any output the plugin may have written.
//!
//! The runtime tries the result-returning signature first; if the
//! exported function has the no-return shape it falls back.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use wasmtime::{Engine, Instance, Linker, Module, Store, TypedFunc, Memory};

use super::config::PluginRuntimeConfig;
use super::host_functions::HostFunctionRegistry;
use super::host_imports::{register_kv_imports, KvBackend, StoreCtx};
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

    /// Compiled wasmtime module — cheap to clone (internally Arc'd)
    /// and shared across invocations. Replaces the prior Vec<u8> stub.
    module: Module,

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
        module: Module,
        sandbox: PluginSandbox,
    ) -> Self {
        Self {
            metadata,
            state: PluginState::Running,
            path,
            module,
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

    /// Borrow the compiled module (Arc-cheap clone available via
    /// `plugin.module.clone()` if the caller needs to outlive the
    /// borrow).
    pub(crate) fn module(&self) -> &Module {
        &self.module
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

    /// wasmtime engine — shared across all plugins. Compiles modules
    /// once; modules cheaply share a reference to it.
    engine: Engine,

    /// Host function registry
    host_functions: Arc<HostFunctionRegistry>,

    /// Per-plugin KV backend bridged into wasmtime imports. Survives
    /// across calls so plugins can persist state between hooks.
    kv: KvBackend,

    /// Module cache (path -> compiled module). Avoids re-compiling
    /// the same `.wasm` on every load.
    module_cache: RwLock<HashMap<PathBuf, Module>>,

    /// Default security policy
    default_policy: SecurityPolicy,

    /// Creation timestamp
    created_at: Instant,
}

impl WasmPluginRuntime {
    /// Create a new WASM runtime
    pub fn new(config: &PluginRuntimeConfig) -> Result<Self, PluginError> {
        let host_functions = Arc::new(HostFunctionRegistry::new());

        let mut engine_config = wasmtime::Config::new();
        if config.fuel_metering {
            engine_config.consume_fuel(true);
        }
        // Epoch-based interrupts let us bound execution time without
        // polling fuel from inside the call.
        engine_config.epoch_interruption(true);

        let engine = Engine::new(&engine_config).map_err(|e| {
            PluginError::RuntimeError(format!("wasmtime engine init: {}", e))
        })?;

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
            engine,
            host_functions,
            kv: KvBackend::new(),
            module_cache: RwLock::new(HashMap::new()),
            default_policy,
            created_at: Instant::now(),
        })
    }

    /// Expose the per-plugin KV backend so admin/test code can seed
    /// or inspect a plugin's state without going through WASM.
    pub fn kv(&self) -> &KvBackend {
        &self.kv
    }

    /// Expose the engine so tests + the plugin manager can build new
    /// `Store`s against it.
    pub(crate) fn engine(&self) -> &Engine {
        &self.engine
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

        // Compile via wasmtime — this validates the module and produces
        // an Arc-wrapped Module ready for repeated instantiation.
        let module = Module::from_binary(&self.engine, wasm_bytes).map_err(|e| {
            PluginError::InstantiationError(format!("wasmtime compile: {}", e))
        })?;

        // Cache the compiled Module (cheap clone on hit).
        {
            let mut cache = self.module_cache.write();
            cache.insert(manifest.path.clone(), module.clone());
        }

        Ok(LoadedPlugin::new(
            metadata,
            manifest.path.clone(),
            module,
            sandbox,
        ))
    }

    /// Call a hook on a plugin via wasmtime.
    ///
    /// 1. Build a fresh `Store` (wasmtime stores are not Sync, so each
    ///    invocation is isolated).
    /// 2. Apply fuel metering (per-call fuel cap) when configured.
    /// 3. Instantiate the module against an empty Linker — host
    ///    functions are TODO; plugins that import them will fail at
    ///    instantiation with a clear error message.
    /// 4. Look up `memory`, `alloc`, `dealloc`, and the named hook
    ///    function exports.
    /// 5. Allocate a slot in plugin memory, write `args`, call the
    ///    hook, decode `(result_ptr, result_len)`, copy the result
    ///    bytes out.
    /// 6. Free both input and output slots via `dealloc`.
    /// 7. Drop the store; the plugin's per-call state is gone.
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

        // Fresh per-call store; not Sync, so we never share across calls.
        // The data carries the plugin's identity + a clone of the shared
        // KV backend so host imports can route to the right namespace.
        let store_ctx = StoreCtx {
            plugin_name: plugin.metadata.name.clone(),
            kv: self.kv.clone(),
        };
        let mut store: Store<StoreCtx> = Store::new(&self.engine, store_ctx);
        if self.config.fuel_metering {
            // wasmtime's set_fuel returns Result; cap is per-call.
            store.set_fuel(self.config.fuel_limit).map_err(|e| {
                PluginError::RuntimeError(format!("set_fuel: {}", e))
            })?;
        }
        // Epoch interruption was enabled at engine init. If we don't
        // raise the deadline above the engine's current epoch (0), the
        // store traps as soon as the function is entered. A deadline
        // of `u64::MAX` effectively disables the interrupt for this
        // call — real time enforcement happens via fuel. A future
        // commit can ratchet this down per-config and pump epoch ticks
        // from a background task to enforce wall-clock timeouts.
        store.set_epoch_deadline(u64::MAX);

        // Linker carries the host imports under `env::*`. Plugins that
        // don't import any of them are unaffected; plugins that need
        // KV (or other future imports) get them resolved here.
        let mut linker: Linker<StoreCtx> = Linker::new(&self.engine);
        register_kv_imports(&mut linker)?;
        let instance = linker
            .instantiate(&mut store, &plugin.module)
            .map_err(|e| {
                PluginError::InstantiationError(format!(
                    "instantiate {}: {}",
                    plugin.metadata.name, e
                ))
            })?;

        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
            PluginError::ExecutionError(format!(
                "plugin {} does not export `memory`",
                plugin.metadata.name
            ))
        })?;

        let alloc = get_typed::<i32, i32>(&instance, &mut store, "alloc")?;
        let dealloc = get_typed::<(i32, i32), ()>(&instance, &mut store, "dealloc")?;

        // Allocate input slot inside the plugin's address space and
        // copy `args` in.
        let in_len = args.len() as i32;
        let in_ptr = alloc.call(&mut store, in_len).map_err(|e| {
            PluginError::ExecutionError(format!("alloc({}): {}", in_len, e))
        })?;
        if in_len > 0 {
            write_memory(&memory, &mut store, in_ptr, args)?;
        }

        // Try the result-returning ABI first; if the export has the
        // observer ABI (no return), fall back to that.
        let export_name = hook.export_name();
        let result_bytes = match get_typed::<(i32, i32), i64>(&instance, &mut store, export_name) {
            Ok(hook_fn) => {
                let packed = hook_fn.call(&mut store, (in_ptr, in_len)).map_err(|e| {
                    PluginError::ExecutionError(format!(
                        "hook {} call: {}",
                        export_name, e
                    ))
                })?;
                let out_ptr = (packed >> 32) as i32;
                let out_len = (packed & 0xFFFF_FFFF) as i32;
                if out_len > 0 {
                    let bytes = read_memory(&memory, &store, out_ptr, out_len)?;
                    // Free the plugin-allocated output slot.
                    let _ = dealloc.call(&mut store, (out_ptr, out_len));
                    bytes
                } else {
                    Vec::new()
                }
            }
            Err(_) => {
                // Observer ABI: (i32, i32) → ()
                let observer = get_typed::<(i32, i32), ()>(
                    &instance,
                    &mut store,
                    export_name,
                )?;
                observer.call(&mut store, (in_ptr, in_len)).map_err(|e| {
                    PluginError::ExecutionError(format!(
                        "observer hook {} call: {}",
                        export_name, e
                    ))
                })?;
                Vec::new()
            }
        };

        // Free the input slot. Best-effort; failure here doesn't
        // abort the call (the store is about to be dropped anyway).
        let _ = dealloc.call(&mut store, (in_ptr, in_len));

        // Update per-plugin instance accounting.
        if self.config.fuel_metering {
            if let Ok(remaining) = store.get_fuel() {
                let consumed = self.config.fuel_limit.saturating_sub(remaining);
                plugin.instance_data.write().fuel_consumed = consumed;
            }
        }
        plugin.instance_data.write().memory_used =
            (memory.data_size(&store)) as usize;

        Ok(result_bytes)
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

/// Look up a typed exported function on an instance, with a uniform
/// "missing/wrong-shape" error message.
fn get_typed<T, P, R>(
    instance: &Instance,
    store: &mut Store<T>,
    name: &str,
) -> Result<TypedFunc<P, R>, PluginError>
where
    P: wasmtime::WasmParams,
    R: wasmtime::WasmResults,
{
    instance
        .get_typed_func::<P, R>(store, name)
        .map_err(|e| PluginError::ExecutionError(format!("export `{}`: {}", name, e)))
}

/// Copy `bytes` into the plugin's linear memory at `ptr`. Bounds-
/// checked via wasmtime's safe `Memory::write`.
fn write_memory<T>(
    memory: &Memory,
    store: &mut Store<T>,
    ptr: i32,
    bytes: &[u8],
) -> Result<(), PluginError> {
    memory.write(store, ptr as usize, bytes).map_err(|e| {
        PluginError::ExecutionError(format!("memory.write @ {}: {}", ptr, e))
    })
}

/// Copy `len` bytes out of plugin memory starting at `ptr`.
fn read_memory<T>(
    memory: &Memory,
    store: &Store<T>,
    ptr: i32,
    len: i32,
) -> Result<Vec<u8>, PluginError> {
    if len <= 0 {
        return Ok(Vec::new());
    }
    let mut out = vec![0u8; len as usize];
    memory.read(store, ptr as usize, &mut out).map_err(|e| {
        PluginError::ExecutionError(format!("memory.read @ {}+{}: {}", ptr, len, e))
    })?;
    Ok(out)
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

    /// Build a tiny WAT module for runtime tests against a specific
    /// engine. wasmtime requires Module and instantiating Engine to
    /// match — so this takes the runtime's engine rather than
    /// constructing one locally.
    ///
    /// Exports `memory`, `alloc`, `dealloc`, a `pre_query` hook that
    /// ignores its input and returns a fixed payload at offset 1024,
    /// and a `post_query` observer hook.
    fn build_test_module(engine: &Engine) -> Module {
        const PAYLOAD: &[u8] = b"hello-from-wasm";
        let payload_hex: String = PAYLOAD
            .iter()
            .map(|b| format!("\\{:02x}", b))
            .collect();
        let wat = format!(
            r#"
            (module
              (memory (export "memory") 1)

              ;; Trivial alloc: always returns offset 4096 (test inputs
              ;; are tiny so non-overlapping reuse is fine here). Real
              ;; plugins ship a real allocator; the runtime only cares
              ;; that `alloc` returns a writable address.
              (func (export "alloc") (param $size i32) (result i32)
                (i32.const 4096))

              (func (export "dealloc") (param $ptr i32) (param $size i32)
                (drop (local.get $ptr))
                (drop (local.get $size)))

              ;; Result-returning hook: writes PAYLOAD at offset 1024 and
              ;; returns (1024 << 32) | PAYLOAD.len.
              (func (export "pre_query")
                (param $in_ptr i32) (param $in_len i32) (result i64)
                (i64.or
                  (i64.shl (i64.const 1024) (i64.const 32))
                  (i64.const {payload_len})))

              ;; Observer hook: takes args, returns nothing.
              (func (export "post_query")
                (param $in_ptr i32) (param $in_len i32)
                (drop (local.get $in_ptr)))

              (data (i32.const 1024) "{payload}")
            )
            "#,
            payload = payload_hex,
            payload_len = PAYLOAD.len(),
        );
        let bytes = wat::parse_str(&wat).expect("wat parses");
        Module::from_binary(engine, &bytes).expect("module compiles")
    }

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
        // Re-use the test module — its compiled `Module` is what
        // the LoadedPlugin needs now that the field is wasmtime-typed.
        let engine = Engine::default();
        let module = build_test_module(&engine);
        let metadata = PluginMetadata::default();
        let sandbox = PluginSandbox::default();
        let plugin = LoadedPlugin::new(
            metadata,
            PathBuf::from("/test/plugin.wasm"),
            module,
            sandbox,
        );

        assert_eq!(plugin.invocation_count(), 0);
        plugin.record_invocation();
        assert_eq!(plugin.invocation_count(), 1);
        plugin.record_invocation();
        assert_eq!(plugin.invocation_count(), 2);
    }

    /// End-to-end: load a WAT-built module, call `pre_query`, observe
    /// the plugin's payload comes back through the (ptr, len) ABI.
    /// This is the killer test that proves the wasmtime path is
    /// real, not a stub.
    #[test]
    fn test_call_hook_roundtrips_real_wasm() {
        let mut config = PluginRuntimeConfig::default();
        // Disable fuel metering — the test module is trivial and we
        // don't want to debug fuel exhaustion in unit tests.
        config.fuel_metering = false;
        let runtime = WasmPluginRuntime::new(&config).unwrap();

        let module = build_test_module(runtime.engine());
        let mut metadata = PluginMetadata::default();
        metadata.name = "test-roundtrip".to_string();
        metadata.hooks = vec![HookType::PreQuery, HookType::PostQuery];

        let plugin = LoadedPlugin::new(
            metadata,
            PathBuf::from("/test/roundtrip.wasm"),
            module,
            PluginSandbox::default(),
        );
        // Force into Running state — Loading would block.
        // (LoadedPlugin::new already sets Running by default.)

        let bytes = runtime
            .call_hook(&plugin, HookType::PreQuery, b"ignored input")
            .expect("pre_query call");
        assert_eq!(bytes, b"hello-from-wasm");
        assert_eq!(plugin.invocation_count(), 1);

        // Observer ABI: post_query has no return; should yield empty.
        let out = runtime
            .call_hook(&plugin, HookType::PostQuery, b"some bytes")
            .expect("post_query call");
        assert!(out.is_empty());
        assert_eq!(plugin.invocation_count(), 2);
    }

    /// A plugin that doesn't declare a hook in its metadata cannot
    /// invoke that hook even if the WASM exports the function.
    #[test]
    fn test_call_hook_rejects_undeclared_hook() {
        let runtime = WasmPluginRuntime::new(&PluginRuntimeConfig::default()).unwrap();
        let module = build_test_module(runtime.engine());
        let mut metadata = PluginMetadata::default();
        metadata.hooks = vec![]; // declares nothing
        let plugin = LoadedPlugin::new(
            metadata,
            PathBuf::from("/test/empty.wasm"),
            module,
            PluginSandbox::default(),
        );
        let err = runtime
            .call_hook(&plugin, HookType::PreQuery, &[])
            .unwrap_err();
        assert!(matches!(err, PluginError::HookNotFound(_)));
    }

    /// Calling a hook whose export name is missing surfaces as
    /// `ExecutionError`, not a panic.
    #[test]
    fn test_call_hook_missing_export_returns_error() {
        let runtime = WasmPluginRuntime::new(&PluginRuntimeConfig::default()).unwrap();
        let module = build_test_module(runtime.engine());
        let mut metadata = PluginMetadata::default();
        // Declare a hook the test module doesn't export.
        metadata.hooks = vec![HookType::Authenticate];
        let plugin = LoadedPlugin::new(
            metadata,
            PathBuf::from("/test/missing.wasm"),
            module,
            PluginSandbox::default(),
        );
        let err = runtime
            .call_hook(&plugin, HookType::Authenticate, &[])
            .unwrap_err();
        assert!(matches!(err, PluginError::ExecutionError(_)));
    }

    /// Build a WAT module that imports kv_set + kv_get from `env` and
    /// calls kv_set on `pre_query`. Used to validate the host-import
    /// bridge end-to-end through wasmtime.
    fn build_kv_test_module(engine: &Engine) -> Module {
        // Layout:
        //   offset 100: 3 bytes "key"
        //   offset 200: 5 bytes "value"
        let wat = r#"
            (module
              (import "env" "kv_set"
                (func $kv_set (param i32 i32 i32 i32) (result i32)))
              (memory (export "memory") 1)

              (data (i32.const 100) "key")
              (data (i32.const 200) "value")

              (func (export "alloc") (param i32) (result i32) (i32.const 4096))
              (func (export "dealloc") (param i32 i32))

              ;; pre_query: kv_set("key", "value"); return 0 (no payload).
              (func (export "pre_query")
                (param $in_ptr i32) (param $in_len i32) (result i64)
                (drop (call $kv_set
                  (i32.const 100) (i32.const 3)
                  (i32.const 200) (i32.const 5)))
                (i64.const 0))
            )
        "#;
        let bytes = wat::parse_str(wat).expect("kv-wat parses");
        Module::from_binary(engine, &bytes).expect("kv module compiles")
    }

    /// Calls a WASM `pre_query` hook that invokes the host's kv_set
    /// import. Verifies the value lands in the runtime's KvBackend
    /// under the plugin's namespace and is readable from Rust.
    #[test]
    fn test_host_kv_import_persists_value() {
        let mut config = PluginRuntimeConfig::default();
        config.fuel_metering = false;
        let runtime = WasmPluginRuntime::new(&config).unwrap();

        let module = build_kv_test_module(runtime.engine());
        let mut metadata = PluginMetadata::default();
        metadata.name = "kv-test-plugin".to_string();
        metadata.hooks = vec![HookType::PreQuery];

        let plugin = LoadedPlugin::new(
            metadata,
            PathBuf::from("/test/kv.wasm"),
            module,
            PluginSandbox::default(),
        );

        // Sanity: namespace empty before the call.
        assert_eq!(runtime.kv().get("kv-test-plugin", b"key"), None);

        let _ = runtime
            .call_hook(&plugin, HookType::PreQuery, &[])
            .expect("pre_query call");

        // The plugin called kv_set("key", "value") inside WASM; the
        // host should have stored it under this plugin's namespace.
        assert_eq!(
            runtime.kv().get("kv-test-plugin", b"key"),
            Some(b"value".to_vec())
        );
        // And nowhere else.
        assert_eq!(runtime.kv().get("other-plugin", b"key"), None);
    }
}
