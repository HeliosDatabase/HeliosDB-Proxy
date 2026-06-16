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
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use wasmtime::{Engine, Instance, InstancePre, Linker, Memory, Module, Store, TypedFunc};

use super::config::PluginRuntimeConfig;
use super::host_functions::HostFunctionRegistry;
use super::host_imports::{register_crypto_imports, register_kv_imports, KvBackend, StoreCtx};
use super::sandbox::{PluginSandbox, ResourceLimits, SecurityPolicy};
use super::{
    AuthRequest, AuthResult, HookType, PluginMetadata, PreQueryResult, QueryContext, RouteResult,
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

    /// Pre-resolved instantiation plan (module imports linked against the
    /// runtime's shared `Linker`). Computed once on the first hook call and
    /// reused for every subsequent call, so per-dispatch cost drops to
    /// `Store::new` + `InstancePre::instantiate` — no per-call `Linker`
    /// allocation, host-import re-registration, or import-name resolution.
    instance_pre: OnceLock<InstancePre<StoreCtx>>,

    /// Security sandbox
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
            instance_pre: OnceLock::new(),
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
    #[allow(dead_code)]
    pub(crate) fn module(&self) -> &Module {
        &self.module
    }

    /// Get memory usage
    pub fn memory_used(&self) -> usize {
        self.instance_data.read().memory_used
    }

    /// Get invocation count
    pub fn invocation_count(&self) -> u64 {
        self.invocation_count
            .load(std::sync::atomic::Ordering::Relaxed)
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
        self.invocation_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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

    /// Shared host-import linker, built once with the `env::*` KV and
    /// crypto imports. Reused to pre-resolve every plugin's instantiation
    /// plan instead of allocating a fresh `Linker` and re-registering host
    /// functions on every hook call.
    linker: Linker<StoreCtx>,

    /// Stop flag for the background epoch ticker thread (see `new`).
    epoch_stop: Arc<AtomicBool>,

    /// Host function registry
    #[allow(dead_code)]
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

        let engine = Engine::new(&engine_config)
            .map_err(|e| PluginError::RuntimeError(format!("wasmtime engine init: {}", e)))?;

        // Build the host-import linker once. The KV/crypto imports read
        // their state from each call's `Store` data (Caller<StoreCtx>), so
        // a single shared linker is correct across all plugins and calls.
        let mut linker: Linker<StoreCtx> = Linker::new(&engine);
        register_kv_imports(&mut linker)?;
        register_crypto_imports(&mut linker)?;

        // Background epoch ticker: bumps the engine epoch every ~1ms so
        // that per-call epoch deadlines actually enforce a wall-clock
        // timeout on plugin execution (previously the deadline was set to
        // u64::MAX, so the configured timeout was never enforced). A
        // std::thread is used so enforcement works with or without a tokio
        // runtime; it exits within ~1ms of the runtime being dropped.
        let epoch_stop = Arc::new(AtomicBool::new(false));
        {
            let engine = engine.clone();
            let stop = epoch_stop.clone();
            std::thread::Builder::new()
                .name("wasm-epoch-ticker".into())
                .spawn(move || {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(1));
                        engine.increment_epoch();
                    }
                })
                .ok();
        }

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
            linker,
            epoch_stop,
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

    /// Borrow the shared host-import linker (used by the manager/tests to
    /// pre-resolve modules against the same import set).
    #[allow(dead_code)]
    pub(crate) fn linker(&self) -> &Linker<StoreCtx> {
        &self.linker
    }

    /// Expose the engine so tests + the plugin manager can build new
    /// `Store`s against it.
    #[allow(dead_code)]
    pub(crate) fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Expose the runtime config so the plugin manager can consult
    /// fields it owns (e.g. `trust_root`) without holding a separate
    /// copy.
    pub fn config(&self) -> &PluginRuntimeConfig {
        &self.config
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
            return Err(PluginError::LoadError(
                "Invalid WASM magic number".to_string(),
            ));
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
        let module = Module::from_binary(&self.engine, wasm_bytes)
            .map_err(|e| PluginError::InstantiationError(format!("wasmtime compile: {}", e)))?;

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
            store
                .set_fuel(self.config.fuel_limit)
                .map_err(|e| PluginError::RuntimeError(format!("set_fuel: {}", e)))?;
        }
        // Epoch interruption was enabled at engine init and a background
        // ticker bumps the engine epoch every ~1ms. Set the deadline to
        // `timeout` worth of ticks so a runaway plugin traps at its
        // configured wall-clock timeout instead of blocking the caller
        // indefinitely. (Set after Store::new, before the call.)
        let deadline_ticks = self.config.timeout.as_millis().max(1).min(u64::MAX as u128) as u64;
        store.set_epoch_deadline(deadline_ticks);

        // Instantiate from the plugin's pre-resolved plan, computed once
        // against the runtime's shared host-import linker and cached on the
        // plugin. Per call this is just a linear-memory/table init — no
        // Linker allocation, no host-import re-registration, no import-name
        // resolution.
        let instance_pre = match plugin.instance_pre.get() {
            Some(ip) => ip,
            None => {
                let ip = self.linker.instantiate_pre(&plugin.module).map_err(|e| {
                    PluginError::InstantiationError(format!(
                        "pre-instantiate {}: {}",
                        plugin.metadata.name, e
                    ))
                })?;
                // Race-tolerant: if another thread set it first, keep theirs.
                let _ = plugin.instance_pre.set(ip);
                plugin.instance_pre.get().expect("just set")
            }
        };
        let instance = instance_pre.instantiate(&mut store).map_err(|e| {
            PluginError::InstantiationError(format!("instantiate {}: {}", plugin.metadata.name, e))
        })?;

        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
            PluginError::ExecutionError(format!(
                "plugin {} does not export `memory`",
                plugin.metadata.name
            ))
        })?;

        let alloc = get_typed::<_, i32, i32>(&instance, &mut store, "alloc")?;
        let dealloc = get_typed::<_, (i32, i32), ()>(&instance, &mut store, "dealloc")?;

        // Allocate input slot inside the plugin's address space and
        // copy `args` in.
        let in_len = args.len() as i32;
        let in_ptr = alloc
            .call(&mut store, in_len)
            .map_err(|e| PluginError::ExecutionError(format!("alloc({}): {}", in_len, e)))?;
        if in_len > 0 {
            write_memory(&memory, &mut store, in_ptr, args)?;
        }

        // Try the result-returning ABI first; if the export has the
        // observer ABI (no return), fall back to that.
        let export_name = hook.export_name();
        let result_bytes = match get_typed::<_, (i32, i32), i64>(&instance, &mut store, export_name)
        {
            Ok(hook_fn) => {
                let packed = hook_fn.call(&mut store, (in_ptr, in_len)).map_err(|e| {
                    PluginError::ExecutionError(format!("hook {} call: {}", export_name, e))
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
                let observer = get_typed::<_, (i32, i32), ()>(&instance, &mut store, export_name)?;
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
        plugin.instance_data.write().memory_used = memory.data_size(&store);

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

impl Drop for WasmPluginRuntime {
    fn drop(&mut self) {
        // Signal the epoch ticker thread to exit; it polls the flag every
        // ~1ms and then releases its Engine clone.
        self.epoch_stop
            .store(true, std::sync::atomic::Ordering::Relaxed);
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

// Serialization support for hook types — written by hand because the
// types live in `super` and we can't derive without touching every
// field's type chain. Includes hook_context so plugins can read
// per-request attributes (tenant_id, agent_id, ai_traffic, etc).
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
        state.serialize_field("hook_context", &self.hook_context)?;
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
    memory
        .write(store, ptr as usize, bytes)
        .map_err(|e| PluginError::ExecutionError(format!("memory.write @ {}: {}", ptr, e)))
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
            "rewrite" => Ok(PreQueryResult::Rewrite(helper.value.unwrap_or_default())),
            "block" => Ok(PreQueryResult::Block(helper.value.unwrap_or_default())),
            "cached" => Ok(PreQueryResult::Cached(helper.data.unwrap_or_default())),
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
            "denied" => Ok(AuthResult::Denied(helper.message.unwrap_or_default())),
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
            #[serde(default)]
            reason: Option<String>,
        }

        let helper = Helper::deserialize(deserializer)?;
        match helper.action.as_str() {
            "default" => Ok(RouteResult::Default),
            "node" => Ok(RouteResult::Node(helper.target.unwrap_or_default())),
            "primary" => Ok(RouteResult::Primary),
            "standby" => Ok(RouteResult::Standby),
            "branch" => Ok(RouteResult::Branch(helper.target.unwrap_or_default())),
            // Block carries a human-readable reason in its own field so
            // it doesn't overload `target` (which is a node identifier
            // for the other variants).
            "block" => Ok(RouteResult::Block(
                helper
                    .reason
                    .unwrap_or_else(|| "blocked by plugin".to_string()),
            )),
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
        let payload_hex: String = PAYLOAD.iter().map(|b| format!("\\{:02x}", b)).collect();
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

    /// A module whose `pre_query` hook spins forever — used to prove the
    /// epoch-deadline wall-clock timeout actually interrupts a runaway
    /// plugin instead of blocking the caller indefinitely.
    fn build_spin_module(engine: &Engine) -> Module {
        let wat = r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 4096))
              (func (export "dealloc") (param i32) (param i32))
              (func (export "pre_query") (param i32) (param i32) (result i64)
                (loop $l (br $l))
                (i64.const 0)))
        "#;
        let bytes = wat::parse_str(wat).expect("wat parses");
        Module::from_binary(engine, &bytes).expect("module compiles")
    }

    /// A runaway plugin must trap at its configured timeout (enforced by
    /// the background epoch ticker), not hang the caller. Guarded by a 5s
    /// join so a regression fails fast instead of hanging the test binary.
    #[test]
    fn test_call_hook_enforces_timeout() {
        let mut config = PluginRuntimeConfig::default();
        config.fuel_metering = false; // isolate epoch enforcement from fuel
        config.timeout = Duration::from_millis(100);
        let runtime = Arc::new(WasmPluginRuntime::new(&config).unwrap());

        let module = build_spin_module(runtime.engine());
        let mut metadata = PluginMetadata::default();
        metadata.name = "spin".to_string();
        metadata.hooks = vec![HookType::PreQuery];
        let plugin = Arc::new(LoadedPlugin::new(
            metadata,
            PathBuf::from("/test/spin.wasm"),
            module,
            PluginSandbox::default(),
        ));

        let (tx, rx) = std::sync::mpsc::channel();
        {
            let r = runtime.clone();
            let p = plugin.clone();
            std::thread::spawn(move || {
                let res = r.call_hook(&p, HookType::PreQuery, b"{}");
                let _ = tx.send(res.is_err());
            });
        }
        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(is_err) => assert!(is_err, "runaway plugin should trap with an error"),
            Err(_) => panic!("call_hook did not return within 5s — epoch timeout not enforced"),
        }
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

    /// Build a WAT module that imports `env.sha256_hex` and exposes a
    /// `pre_query` hook which:
    ///   1. computes sha256_hex over an embedded "abc" payload at
    ///      offset 100 (3 bytes)
    ///   2. writes the 64-byte hex digest at offset 200
    ///   3. returns the packed (200 << 32) | 64
    /// so the host can read the digest out of plugin memory.
    fn build_sha256_test_module(engine: &Engine) -> Module {
        let wat = r#"
            (module
              (import "env" "sha256_hex"
                (func $sha256_hex (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)

              (data (i32.const 100) "abc")

              (func (export "alloc") (param i32) (result i32) (i32.const 4096))
              (func (export "dealloc") (param i32 i32))

              (func (export "pre_query")
                (param $in_ptr i32) (param $in_len i32) (result i64)
                (drop (call $sha256_hex
                  (i32.const 100) (i32.const 3)
                  (i32.const 200)))
                (i64.or
                  (i64.shl (i64.const 200) (i64.const 32))
                  (i64.const 64)))
            )
        "#;
        let bytes = wat::parse_str(wat).expect("sha256-wat parses");
        Module::from_binary(engine, &bytes).expect("sha256 module compiles")
    }

    /// RouteResult deserialiser handles the new Block variant via a
    /// `reason` field separate from `target` (which the other variants
    /// use as a node identifier).
    #[test]
    fn test_route_result_deserialises_block_with_reason() {
        let json = r#"{"action":"block","reason":"cross-region read forbidden"}"#;
        let r: RouteResult = serde_json::from_str(json).expect("block deserialises");
        match r {
            RouteResult::Block(reason) => {
                assert_eq!(reason, "cross-region read forbidden");
            }
            other => panic!("expected Block, got {:?}", other),
        }
    }

    /// Block without a reason field falls back to a generic message —
    /// keeps the deserialiser permissive when plugins forget the field.
    #[test]
    fn test_route_result_block_defaults_reason_when_missing() {
        let json = r#"{"action":"block"}"#;
        let r: RouteResult = serde_json::from_str(json).expect("block deserialises");
        match r {
            RouteResult::Block(reason) => {
                assert!(!reason.is_empty(), "default reason should not be empty");
            }
            other => panic!("expected Block, got {:?}", other),
        }
    }

    /// SHA-256 of "abc" is the canonical RFC 6234 test vector. Verifies
    /// the host-import bridge produces real cryptographic output, not
    /// the FNV-flavoured placeholder that audit-chain ships today.
    #[test]
    fn test_host_sha256_import_matches_rfc_6234_vector() {
        const SHA256_OF_ABC: &[u8; 64] =
            b"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

        let mut config = PluginRuntimeConfig::default();
        config.fuel_metering = false;
        let runtime = WasmPluginRuntime::new(&config).unwrap();

        let module = build_sha256_test_module(runtime.engine());
        let mut metadata = PluginMetadata::default();
        metadata.name = "sha256-test-plugin".to_string();
        metadata.hooks = vec![HookType::PreQuery];

        let plugin = LoadedPlugin::new(
            metadata,
            PathBuf::from("/test/sha256.wasm"),
            module,
            PluginSandbox::default(),
        );

        let out = runtime
            .call_hook(&plugin, HookType::PreQuery, &[])
            .expect("pre_query call");
        assert_eq!(out.len(), 64);
        assert_eq!(&out[..], SHA256_OF_ABC);
    }
}
