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
pub mod host_functions;
pub mod host_imports;
pub mod hot_reload;
pub mod loader;
pub mod metrics;
pub mod runtime;
pub mod sandbox;

pub use config::{PluginConfig, PluginRuntimeConfig, PluginRuntimeConfigBuilder};
pub use host_functions::HostFunctionRegistry;
pub use hot_reload::{
    DirScan, DirWatch, Fingerprint, HotReloadWatcher, HotReloader, ReloadError, ReloadEvent,
};
pub use loader::{
    PluginLoadError, PluginLoader, PluginManifest, SignatureVerifier, VerifiedPlugin,
};
pub use metrics::{HookLatency, PluginMetrics, PluginStats};
pub use runtime::{LoadedPlugin, PluginError, PluginState, WasmPluginRuntime};
pub use sandbox::{Permission, PluginSandbox, ResourceLimits, SecurityPolicy};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::plugin_artefact::ArtefactKind;

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

    /// Whether the proxy calls this hook: `pre_query`, `route` and
    /// `post_query` for statements, `authenticate` at login. A plugin
    /// declaring any other hook is refused at load, since it would
    /// silently never run.
    pub fn is_dispatched(&self) -> bool {
        matches!(
            self,
            HookType::PreQuery | HookType::PostQuery | HookType::Route | HookType::Authenticate
        )
    }

    /// Parse from string
    #[allow(clippy::should_implement_trait)]
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
#[derive(Debug, Clone, serde::Serialize)]
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
#[derive(Debug, Clone, Default)]
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

    /// Reject the query with the given reason. Reaches the client as a
    /// PostgreSQL ErrorResponse (severity ERROR, SQLSTATE 42000)
    /// followed by ReadyForQuery — same wire shape as
    /// `PreQueryResult::Block` so clients see one consistent error
    /// path regardless of which hook rejected.
    Block(String),
}

/// Plugins of each hook, in dispatch order. Published as one snapshot, so a
/// hook call takes no lock and never holds a map entry across a WASM call.
#[derive(Default)]
struct HookTable {
    by_hook: HashMap<HookType, Arc<[Arc<LoadedPlugin>]>>,
}

/// The last load attempt of one plugin file.
#[derive(Debug, Clone)]
struct Attempt {
    /// The file version attempted.
    fingerprint: Fingerprint,
    /// The plugin name it declares, or why it could not be read or
    /// verified.
    outcome: Result<String, String>,
    /// Why that version, read and verified, is not loaded.
    load_error: Option<LoadFailure>,
}

/// Why a verified plugin version is not loaded.
#[derive(Debug, Clone)]
enum LoadFailure {
    /// `max_plugins` plugins are loaded: retried on every rescan.
    Capacity(String),
    /// The module was refused: retried when the file changes.
    Refused(String),
}

impl LoadFailure {
    fn message(&self) -> &str {
        match self {
            LoadFailure::Capacity(m) | LoadFailure::Refused(m) => m,
        }
    }
}

/// Where a plugin loaded from the plugin directory came from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Source {
    path: PathBuf,
    fingerprint: Fingerprint,
}

/// The plugin directory's reconcile state, guarded by the manager's change
/// lock (every load, unload and rescan takes it).
struct DirState {
    watch: DirWatch,
    attempts: BTreeMap<PathBuf, Attempt>,
    sources: HashMap<String, Source>,
    /// The trust-root version the attempts were verified under.
    trust_applied: Option<Fingerprint>,
    /// Files whose latest change is still within the debounce period.
    pending: Vec<PathBuf>,
}

/// What a [`PluginManager::rescan`] changed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReloadReport {
    /// Plugins loaded that were not loaded before.
    pub loaded: Vec<String>,
    /// Plugins replaced by a new version (or the same version from another
    /// file).
    pub updated: Vec<String>,
    /// Plugins unloaded: their file is gone, declares another name now, or
    /// no longer verifies against a changed trust root.
    pub unloaded: Vec<String>,
    /// Files that could not be read, verified or loaded this round. A
    /// plugin whose new version failed keeps running its previous one.
    pub failed: Vec<(PathBuf, String)>,
    /// Files whose change is still within the debounce period.
    pub pending: Vec<PathBuf>,
}

impl ReloadReport {
    /// Whether nothing changed and nothing failed.
    pub fn is_quiet(&self) -> bool {
        self.loaded.is_empty()
            && self.updated.is_empty()
            && self.unloaded.is_empty()
            && self.failed.is_empty()
    }
}

/// The state of one file in the plugin directory ([`PluginManager::files`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginFileState {
    /// Its current version is the loaded plugin `name`.
    Loaded {
        /// The plugin.
        name: String,
    },
    /// It changed and the new version failed; plugin `name` keeps running
    /// the version loaded before.
    Stale {
        /// The plugin.
        name: String,
        /// Why the new version failed.
        error: String,
    },
    /// It declares a plugin another file provides (a `.tar.gz` is preferred
    /// over a bare `.wasm`, then the first file name in sort order).
    Shadowed {
        /// The plugin.
        name: String,
        /// The file that provides it.
        by: PathBuf,
    },
    /// No plugin is loaded from it.
    Failed {
        /// Why.
        error: String,
    },
    /// It changed within the debounce period; not acted on yet.
    Pending,
}

/// One file of the plugin directory and its state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginFileStatus {
    /// The file.
    pub path: PathBuf,
    /// Its state.
    pub state: PluginFileState,
}

/// Plugin manager for coordinating all plugins
pub struct PluginManager {
    /// Runtime for WASM execution
    runtime: Arc<WasmPluginRuntime>,

    /// Loaded plugins by name
    plugins: DashMap<String, Arc<LoadedPlugin>>,

    /// Per-hook dispatch lists, republished on every change.
    hooks: ArcSwap<HookTable>,

    /// The change lock and the plugin directory's reconcile state.
    dir: Mutex<DirState>,

    /// Metrics collector
    metrics: Arc<PluginMetrics>,
}

/// Dispatch order: by file name (so `10-guard.tar.gz` runs before
/// `20-route.tar.gz`), then by plugin name.
fn dispatch_key(plugin: &LoadedPlugin) -> (std::ffi::OsString, &str) {
    (
        plugin.path.file_name().unwrap_or_default().to_os_string(),
        plugin.metadata.name.as_str(),
    )
}

/// Which of two files declaring the same plugin provides it: a packed
/// artefact over a bare module, then the first path in sort order.
fn provides_before(a: &Path, b: &Path) -> bool {
    let packed = |p: &Path| ArtefactKind::of(p) == Some(ArtefactKind::TarGz);
    (!packed(a), a) < (!packed(b), b)
}

impl PluginManager {
    /// Create a new plugin manager. It loads nothing: call
    /// [`rescan`](Self::rescan) to load the plugin directory, or
    /// [`load_plugin`](Self::load_plugin) for single files.
    pub fn new(config: PluginRuntimeConfig) -> Result<Self, PluginError> {
        let runtime = Arc::new(WasmPluginRuntime::new(&config)?);
        let trust = config
            .trust_root
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned());
        let watch = DirWatch::new(
            &config.plugin_dir,
            trust.as_deref(),
            config.hot_reload_debounce,
        );
        Ok(Self {
            runtime,
            plugins: DashMap::new(),
            hooks: ArcSwap::from_pointee(HookTable::default()),
            dir: Mutex::new(DirState {
                watch,
                attempts: BTreeMap::new(),
                sources: HashMap::new(),
                trust_applied: None,
                pending: Vec::new(),
            }),
            metrics: Arc::new(PluginMetrics::new()),
        })
    }

    fn config(&self) -> &PluginRuntimeConfig {
        self.runtime.config()
    }

    /// How often the daemon should [`rescan`](Self::rescan) the plugin
    /// directory: `Some` when `hot_reload` is on.
    pub fn hot_reload_interval(&self) -> Option<Duration> {
        self.config()
            .hot_reload
            .then_some(self.config().hot_reload_interval)
    }

    /// A loader for this manager's policy: the size bound, the trust root
    /// (read once per call, so one rescan verifies every file against the
    /// same keys) and the signed-manifest requirement.
    fn loader(&self) -> Result<PluginLoader, PluginError> {
        let config = self.config();
        let mut loader = PluginLoader::new()
            .with_max_bytes(config.max_artefact_bytes)
            .require_signed_manifest(config.require_signed_manifest);
        if let Some(root) = &config.trust_root {
            let verifier = SignatureVerifier::from_trust_root(root).map_err(|e| {
                PluginError::LoadError(format!("trust root {}: {e}", root.display()))
            })?;
            loader = loader.with_signature_verifier(verifier);
        }
        Ok(loader)
    }

    /// Read and verify a plugin file.
    fn read(loader: &PluginLoader, path: &Path) -> Result<VerifiedPlugin, PluginError> {
        let verified = loader.load_verified(path)?;
        if !verified.unknown_manifest_keys.is_empty() {
            tracing::warn!(
                path = %path.display(),
                keys = ?verified.unknown_manifest_keys,
                "plugin manifest has keys this proxy does not know; they are ignored"
            );
        }
        Ok(verified)
    }

    /// Compile and link a verified plugin, refusing one that would never
    /// run: no hooks, or a hook this proxy does not call.
    fn instantiate(&self, verified: &VerifiedPlugin) -> Result<LoadedPlugin, PluginError> {
        let manifest = &verified.manifest;
        if manifest.hooks.is_empty() {
            return Err(PluginError::InvalidManifest(format!(
                "plugin {} declares no hooks, so it would never run",
                manifest.name
            )));
        }
        if let Some(hook) = manifest.hooks.iter().find(|h| !h.is_dispatched()) {
            return Err(PluginError::InvalidManifest(format!(
                "plugin {} declares the hook {}, which this proxy never calls \
                 (it calls pre_query, route, post_query and authenticate)",
                manifest.name,
                hook.export_name()
            )));
        }
        self.runtime.instantiate(manifest, &verified.wasm)
    }

    /// Refuse a plugin `name` that is not loaded yet when `max_plugins` are.
    fn check_capacity(&self, name: &str) -> Result<(), String> {
        let max = self.config().max_plugins;
        if !self.plugins.contains_key(name) && self.plugins.len() >= max {
            return Err(format!(
                "plugin {name} not loaded: max_plugins ({max}) plugins are loaded"
            ));
        }
        Ok(())
    }

    /// Put `plugin` in place (replacing any plugin of the same name) and
    /// republish the dispatch lists. Returns whether one was replaced.
    fn install(&self, plugin: LoadedPlugin, verified: &VerifiedPlugin) -> bool {
        let name = plugin.metadata.name.clone();
        tracing::info!(
            plugin = %name,
            version = %plugin.metadata.version,
            hooks = ?plugin.metadata.hooks,
            path = %plugin.path.display(),
            sha256 = %plugin.wasm_sha256(),
            signed_by = verified.signed_by.as_deref().unwrap_or("-"),
            manifest_signed_by = verified.manifest_signed_by.as_deref().unwrap_or("-"),
            "Plugin loaded"
        );
        let replaced = self.plugins.insert(name, Arc::new(plugin)).is_some();
        self.publish();
        replaced
    }

    /// Rebuild the per-hook dispatch lists from the loaded plugins, and drop
    /// compiled modules no plugin uses.
    fn publish(&self) {
        let mut plugins: Vec<Arc<LoadedPlugin>> =
            self.plugins.iter().map(|e| Arc::clone(e.value())).collect();
        plugins.sort_by(|a, b| dispatch_key(a).cmp(&dispatch_key(b)));
        let mut by_hook: HashMap<HookType, Vec<Arc<LoadedPlugin>>> = HashMap::new();
        for plugin in &plugins {
            for hook in &plugin.metadata.hooks {
                by_hook.entry(*hook).or_default().push(Arc::clone(plugin));
            }
        }
        self.hooks.store(Arc::new(HookTable {
            by_hook: by_hook
                .into_iter()
                .map(|(hook, list)| (hook, Arc::from(list)))
                .collect(),
        }));
        self.runtime
            .retain_modules(plugins.iter().map(|p| p.wasm_sha256()));
    }

    /// Load a plugin file (a bare `.wasm` or a `.tar.gz` artefact), or
    /// replace the loaded version of the same plugin from the same file.
    /// The new version is compiled and verified before it replaces the old
    /// one, so a failure leaves the running version untouched.
    ///
    /// A plugin whose name is already loaded from another file is refused
    /// (unload it first), and so is a new plugin once `max_plugins` are
    /// loaded. With a trust root configured, the file must be signed by one
    /// of its keys.
    pub fn load_plugin(&self, path: &Path) -> Result<(), PluginError> {
        let mut dir = self.dir.lock();
        let verified = Self::read(&self.loader()?, path)?;
        let name = verified.manifest.name.clone();
        if let Some(loaded) = self.plugins.get(&name) {
            if loaded.path != path {
                return Err(PluginError::LoadError(format!(
                    "a plugin named {name} is already loaded from {}; unload it first",
                    loaded.path.display()
                )));
            }
        }
        self.check_capacity(&name).map_err(PluginError::LoadError)?;
        let plugin = self.instantiate(&verified)?;
        self.install(plugin, &verified);
        // A file of the plugin directory: the reconcile now knows this
        // version is loaded.
        if path.parent() == Some(dir.watch.dir()) {
            let fingerprint = Fingerprint::of_plugin(path);
            dir.attempts.insert(
                path.to_path_buf(),
                Attempt {
                    fingerprint: fingerprint.clone(),
                    outcome: Ok(name.clone()),
                    load_error: None,
                },
            );
            dir.sources.insert(
                name,
                Source {
                    path: path.to_path_buf(),
                    fingerprint,
                },
            );
        }
        Ok(())
    }

    /// Unload a plugin. With hot reload (or the next rescan), a plugin whose
    /// file is still in the plugin directory is loaded again: the directory
    /// is the source of truth.
    pub fn unload_plugin(&self, name: &str) -> Result<(), PluginError> {
        let mut dir = self.dir.lock();
        if self.plugins.remove(name).is_some() {
            dir.sources.remove(name);
            self.publish();
            tracing::info!(plugin = %name, "Plugin unloaded");
        }
        Ok(())
    }

    /// Reload a plugin from its file. The new version replaces the running
    /// one only once it has loaded; on failure the running one stays.
    pub fn reload_plugin(&self, name: &str) -> Result<(), PluginError> {
        let path = match self.plugins.get(name) {
            Some(plugin) => plugin.path.clone(),
            None => return Ok(()),
        };
        self.load_plugin(&path)
    }

    /// Reconcile the loaded plugins with the plugin directory.
    ///
    /// Every `.wasm` / `.tar.gz` file whose version (with its sidecars) has
    /// been stable for the debounce period — every file, with `force` — is
    /// read and verified once per version; a failed file is retried when it
    /// (or a sidecar, or the trust root) changes. Each plugin name is then
    /// provided by one file (a `.tar.gz` before a bare `.wasm`, then the
    /// first in sort order): a new or changed version replaces the running
    /// one once it has loaded, and a plugin whose new version fails keeps
    /// running the previous one. A plugin is unloaded when its file is gone
    /// or now declares another name, and — after a trust-root change — when
    /// it no longer verifies. A trust root that fails to load changes
    /// nothing until it is fixed. A missing directory holds no plugins.
    pub fn rescan(&self, force: bool) -> ReloadReport {
        let mut guard = self.dir.lock();
        let dir = &mut *guard;
        let mut report = ReloadReport::default();
        let scan = dir.watch.scan(Instant::now(), force);
        if let Some(reason) = &scan.unreadable {
            tracing::warn!(%reason, "plugin directory not readable; keeping the loaded plugins");
            report
                .failed
                .push((dir.watch.dir().to_path_buf(), reason.clone()));
            return report;
        }
        let loader = match self.loader() {
            Ok(loader) => loader,
            Err(e) => {
                // Nothing can be verified: change nothing but removals,
                // and try every file again once the trust root loads.
                tracing::error!(error = %e, "plugin trust root unusable; plugin files are not (re)loaded");
                report.failed.push((
                    self.config().trust_root.clone().unwrap_or_default(),
                    e.to_string(),
                ));
                for path in &scan.removed {
                    dir.attempts.remove(path);
                }
                self.unload_orphans(dir, &BTreeMap::new(), false, &mut report);
                return report;
            }
        };
        let trust_changed = match &scan.trust {
            Some(trust) if dir.trust_applied.as_ref() != Some(trust) => {
                let first = dir.trust_applied.is_none();
                dir.trust_applied = Some(trust.clone());
                !first
            }
            _ => false,
        };

        // 1. Read and verify every settled file version not tried yet.
        let mut fresh: HashMap<PathBuf, VerifiedPlugin> = HashMap::new();
        dir.pending.clear();
        for file in &scan.files {
            if !file.settled {
                dir.pending.push(file.path.clone());
                continue;
            }
            let tried = dir
                .attempts
                .get(&file.path)
                .is_some_and(|a| a.fingerprint == file.fingerprint);
            if tried && !trust_changed {
                continue;
            }
            let outcome = match Self::read(&loader, &file.path) {
                Ok(verified) => {
                    let name = verified.manifest.name.clone();
                    fresh.insert(file.path.clone(), verified);
                    Ok(name)
                }
                Err(e) => {
                    tracing::warn!(path = %file.path.display(), error = %e, "plugin file not loaded");
                    report.failed.push((file.path.clone(), e.to_string()));
                    Err(e.to_string())
                }
            };
            dir.attempts.insert(
                file.path.clone(),
                Attempt {
                    fingerprint: file.fingerprint.clone(),
                    outcome,
                    load_error: None,
                },
            );
        }
        for path in &scan.removed {
            dir.attempts.remove(path);
        }
        report.pending = dir.pending.clone();

        // 2. Which file provides each plugin.
        let mut owners: BTreeMap<String, PathBuf> = BTreeMap::new();
        for (path, attempt) in &dir.attempts {
            if let Ok(name) = &attempt.outcome {
                let better = owners
                    .get(name)
                    .is_none_or(|current| provides_before(path, current));
                if better {
                    owners.insert(name.clone(), path.clone());
                }
            }
        }

        // 3. Unload plugins no file provides any more, first: the slots they
        //    free count toward `max_plugins` for the loads below.
        self.unload_orphans(dir, &owners, trust_changed, &mut report);

        // 4. Load what is new or changed.
        for (name, path) in &owners {
            let attempt = dir
                .attempts
                .get(path)
                .expect("owner has an attempt")
                .clone();
            let wanted = Source {
                path: path.clone(),
                fingerprint: attempt.fingerprint.clone(),
            };
            let loaded = self.plugins.contains_key(name);
            if loaded && dir.sources.get(name) == Some(&wanted) {
                continue;
            }
            // A version refused before is retried when its file changes
            // (a fresh attempt has no load error).
            if matches!(attempt.load_error, Some(LoadFailure::Refused(_))) {
                continue;
            }
            let result = match self.check_capacity(name) {
                Err(full) => Err(LoadFailure::Capacity(full)),
                Ok(()) => fresh
                    .remove(path)
                    .map_or_else(|| Self::read(&loader, path), Ok)
                    .and_then(|verified| {
                        self.instantiate(&verified).map(|plugin| (plugin, verified))
                    })
                    .map_err(|e| LoadFailure::Refused(e.to_string())),
            };
            match result {
                Ok((plugin, verified)) => {
                    let replaced = self.install(plugin, &verified);
                    if replaced {
                        report.updated.push(name.clone());
                    } else {
                        report.loaded.push(name.clone());
                    }
                    dir.sources.insert(name.clone(), wanted);
                    if let Some(a) = dir.attempts.get_mut(path) {
                        a.load_error = None;
                    }
                }
                Err(failure) => {
                    // Capacity is re-checked on every rescan: report it once.
                    let repeated = matches!(
                        (&attempt.load_error, &failure),
                        (Some(LoadFailure::Capacity(_)), LoadFailure::Capacity(_))
                    );
                    if let Some(a) = dir.attempts.get_mut(path) {
                        a.load_error = Some(failure.clone());
                    }
                    if repeated {
                        continue;
                    }
                    tracing::warn!(
                        plugin = %name,
                        path = %path.display(),
                        error = %failure.message(),
                        "plugin version not loaded{}",
                        if loaded { "; the running version stays" } else { "" }
                    );
                    report
                        .failed
                        .push((path.clone(), failure.message().to_string()));
                }
            }
        }

        if !report.is_quiet() {
            tracing::info!(
                loaded = ?report.loaded,
                updated = ?report.updated,
                unloaded = ?report.unloaded,
                failed = report.failed.len(),
                "plugin directory reconciled"
            );
        }
        report
    }

    /// Unload the directory plugins `owners` does not provide: their file
    /// is gone or declares another plugin, or its new version failed after
    /// a trust-root change (a failed new version otherwise keeps the
    /// running one).
    fn unload_orphans(
        &self,
        dir: &mut DirState,
        owners: &BTreeMap<String, PathBuf>,
        trust_changed: bool,
        report: &mut ReloadReport,
    ) {
        let orphans: Vec<(String, Source)> = dir
            .sources
            .iter()
            .filter(|(name, _)| !owners.contains_key(*name))
            .map(|(name, source)| (name.clone(), source.clone()))
            .collect();
        let mut unloaded = false;
        for (name, source) in orphans {
            let keep = match dir.attempts.get(&source.path).map(|a| &a.outcome) {
                Some(Err(_)) => !trust_changed,
                // Still declared by its file: only a broken trust root
                // left it without an owner; keep it.
                Some(Ok(declared)) => *declared == name,
                None => false,
            };
            if keep {
                continue;
            }
            dir.sources.remove(&name);
            if self.plugins.remove(&name).is_some() {
                tracing::info!(plugin = %name, path = %source.path.display(), "Plugin unloaded");
                report.unloaded.push(name);
                unloaded = true;
            }
        }
        if unloaded {
            self.publish();
        }
    }

    /// The state of every file in the plugin directory, as of the latest
    /// rescan.
    pub fn files(&self) -> Vec<PluginFileStatus> {
        let dir = self.dir.lock();
        let mut owners: HashMap<&str, &Path> = HashMap::new();
        for (path, attempt) in &dir.attempts {
            if let Ok(name) = &attempt.outcome {
                let better = owners
                    .get(name.as_str())
                    .is_none_or(|current| provides_before(path, current));
                if better {
                    owners.insert(name, path);
                }
            }
        }
        let mut files: Vec<PluginFileStatus> = dir
            .attempts
            .iter()
            .map(|(path, attempt)| {
                let source_of = dir
                    .sources
                    .iter()
                    .find(|(_, s)| &s.path == path)
                    .map(|(name, s)| (name.clone(), s.fingerprint == attempt.fingerprint));
                let state = match (&attempt.outcome, source_of) {
                    (Ok(name), Some((loaded, true))) if *name == loaded => {
                        PluginFileState::Loaded { name: loaded }
                    }
                    (Err(error), Some((name, _))) => PluginFileState::Stale {
                        name,
                        error: error.clone(),
                    },
                    (Ok(name), Some((loaded, false))) if *name == loaded => {
                        PluginFileState::Stale {
                            name: loaded,
                            error: attempt
                                .load_error
                                .as_ref()
                                .map_or("not loaded yet", LoadFailure::message)
                                .to_string(),
                        }
                    }
                    (Ok(name), _) if owners.get(name.as_str()).is_some_and(|o| *o != path) => {
                        PluginFileState::Shadowed {
                            name: name.clone(),
                            by: owners[name.as_str()].to_path_buf(),
                        }
                    }
                    (Ok(_), _) => PluginFileState::Failed {
                        error: attempt
                            .load_error
                            .as_ref()
                            .map_or("not loaded yet", LoadFailure::message)
                            .to_string(),
                    },
                    (Err(error), None) => PluginFileState::Failed {
                        error: error.clone(),
                    },
                };
                PluginFileStatus {
                    path: path.clone(),
                    state,
                }
            })
            .collect();
        for path in &dir.pending {
            if let Some(status) = files.iter_mut().find(|f| &f.path == path) {
                status.state = PluginFileState::Pending;
            } else {
                files.push(PluginFileStatus {
                    path: path.clone(),
                    state: PluginFileState::Pending,
                });
            }
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        files
    }

    /// Borrow the shared per-plugin KV backend. Backs the
    /// `/admin/kv/<plugin>/<key>` endpoints, which read and mutate the
    /// same store plugins see through their `kv_get`/`kv_set` imports.
    pub fn kv(&self) -> &host_imports::KvBackend {
        self.runtime.kv()
    }

    /// A loaded plugin by name.
    pub fn plugin(&self, name: &str) -> Option<Arc<LoadedPlugin>> {
        self.plugins.get(name).map(|p| Arc::clone(p.value()))
    }

    /// The plugins registered for `hook`, in dispatch order.
    fn hook_plugins(&self, hook: HookType) -> Option<Arc<[Arc<LoadedPlugin>]>> {
        self.hooks.load().by_hook.get(&hook).cloned()
    }

    /// Cheap check whether any loaded plugin registered the given hook.
    /// Lets the server's hook wrappers keep the no-plugin path free of
    /// payload clones, SQL parsing, and context construction.
    pub fn has_hook(&self, hook: HookType) -> bool {
        self.hooks
            .load()
            .by_hook
            .get(&hook)
            .is_some_and(|plugins| !plugins.is_empty())
    }

    /// Execute pre-query hooks
    pub fn execute_pre_query(&self, ctx: &QueryContext) -> PreQueryResult {
        let Some(plugins) = self.hook_plugins(HookType::PreQuery) else {
            return PreQueryResult::Continue;
        };
        for plugin in plugins.iter() {
            let name = plugin.metadata.name.as_str();
            let start = std::time::Instant::now();
            match self.runtime.call_pre_query(plugin, ctx) {
                Ok(result) => {
                    self.metrics
                        .record_hook_call(name, HookType::PreQuery, start.elapsed(), true);
                    match result {
                        PreQueryResult::Continue => continue,
                        other => return other,
                    }
                }
                Err(e) => {
                    self.metrics
                        .record_hook_call(name, HookType::PreQuery, start.elapsed(), false);
                    tracing::warn!(plugin = %name, error = %e, "Pre-query hook failed");
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
        let Some(plugins) = self.hook_plugins(HookType::PostQuery) else {
            return;
        };
        if plugins.is_empty() {
            return;
        }

        // Serialise (ctx, outcome) once and share the payload across every
        // PostQuery plugin in the fan-out — it is identical for all of them.
        let payload = match serde_json::to_vec(&(ctx, outcome)) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "Post-query serialisation failed");
                return;
            }
        };

        for plugin in plugins.iter() {
            let name = plugin.metadata.name.as_str();
            let start = std::time::Instant::now();
            match self
                .runtime
                .call_hook(plugin, HookType::PostQuery, &payload)
            {
                Ok(_) => {
                    self.metrics
                        .record_hook_call(name, HookType::PostQuery, start.elapsed(), true);
                }
                Err(e) => {
                    self.metrics.record_hook_call(
                        name,
                        HookType::PostQuery,
                        start.elapsed(),
                        false,
                    );
                    tracing::warn!(plugin = %name, error = %e, "Post-query hook failed");
                }
            }
        }
    }

    /// Execute authentication hooks
    pub fn execute_authenticate(&self, request: &AuthRequest) -> AuthResult {
        let Some(plugins) = self.hook_plugins(HookType::Authenticate) else {
            return AuthResult::Defer;
        };
        for plugin in plugins.iter() {
            let name = plugin.metadata.name.as_str();
            let start = std::time::Instant::now();
            match self.runtime.call_authenticate(plugin, request) {
                Ok(result) => {
                    self.metrics.record_hook_call(
                        name,
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
                        name,
                        HookType::Authenticate,
                        start.elapsed(),
                        false,
                    );
                    tracing::warn!(plugin = %name, error = %e, "Authenticate hook failed");
                }
            }
        }
        AuthResult::Defer
    }

    /// Execute routing hooks
    pub fn execute_route(&self, ctx: &QueryContext) -> RouteResult {
        let Some(plugins) = self.hook_plugins(HookType::Route) else {
            return RouteResult::Default;
        };
        for plugin in plugins.iter() {
            let name = plugin.metadata.name.as_str();
            let start = std::time::Instant::now();
            match self.runtime.call_route(plugin, ctx) {
                Ok(result) => {
                    self.metrics
                        .record_hook_call(name, HookType::Route, start.elapsed(), true);
                    match result {
                        RouteResult::Default => continue,
                        other => return other,
                    }
                }
                Err(e) => {
                    self.metrics
                        .record_hook_call(name, HookType::Route, start.elapsed(), false);
                    tracing::warn!(plugin = %name, error = %e, "Route hook failed");
                }
            }
        }
        RouteResult::Default
    }

    /// List loaded plugins, in dispatch order.
    pub fn list_plugins(&self) -> Vec<PluginInfo> {
        let mut plugins: Vec<Arc<LoadedPlugin>> =
            self.plugins.iter().map(|e| Arc::clone(e.value())).collect();
        plugins.sort_by(|a, b| dispatch_key(a).cmp(&dispatch_key(b)));
        plugins
            .iter()
            .map(|plugin| PluginInfo {
                name: plugin.metadata.name.clone(),
                version: plugin.metadata.version.clone(),
                description: plugin.metadata.description.clone(),
                hooks: plugin.metadata.hooks.clone(),
                state: plugin.state.clone(),
                stats: self.metrics.get_plugin_stats(&plugin.metadata.name),
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

    /// Rescan the plugin directory ([`rescan`](Self::rescan) without
    /// forcing) and report what changed as events: a plugin loaded from a
    /// new file is `Added(path)`, a replaced version `Modified(name)`, an
    /// unloaded plugin `Removed(name)`.
    pub fn check_updates(&self) -> Result<Vec<ReloadEvent>, PluginError> {
        let report = self.rescan(false);
        let path_of = |name: &String| {
            self.plugins
                .get(name)
                .map(|p| p.path.clone())
                .unwrap_or_default()
        };
        let mut events: Vec<ReloadEvent> = report
            .loaded
            .iter()
            .map(|name| ReloadEvent::Added(path_of(name)))
            .collect();
        events.extend(report.updated.into_iter().map(ReloadEvent::Modified));
        events.extend(report.unloaded.into_iter().map(ReloadEvent::Removed));
        Ok(events)
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
        assert_eq!(
            HookType::from_str("authenticate"),
            Some(HookType::Authenticate)
        );
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
        let pm =
            PluginManager::new(PluginRuntimeConfig::default()).expect("construct PluginManager");
        let ctx = QueryContext {
            query: "SELECT 1".to_string(),
            normalized: "SELECT 1".to_string(),
            tables: Vec::new(),
            is_read_only: true,
            hook_context: HookContext::default(),
        };
        assert!(matches!(
            pm.execute_pre_query(&ctx),
            PreQueryResult::Continue
        ));
    }

    // -----------------------------------------------------------------
    // Plugin directory reconcile (startup scan, rescans, hot reload).
    // -----------------------------------------------------------------

    /// A module whose `pre_query` / `route` / `authenticate` answer
    /// `answer` and whose `post_query` observes.
    fn module(hooks: &[&str], answer: &str) -> Vec<u8> {
        let escaped: String = answer.bytes().map(|b| format!("\\{b:02x}")).collect();
        let funcs: String = hooks
            .iter()
            .map(|h| {
                if *h == "post_query" {
                    format!(r#"(func (export "{h}") (param i32 i32))"#)
                } else {
                    format!(
                        r#"(func (export "{h}") (param i32 i32) (result i64)
                             (i64.or (i64.shl (i64.const 1024) (i64.const 32)) (i64.const {len})))"#,
                        len = answer.len()
                    )
                }
            })
            .collect();
        wat::parse_str(format!(
            r#"(module (memory (export "memory") 1)
                 (func (export "alloc") (param i32) (result i32) (i32.const 4096))
                 (func (export "dealloc") (param i32 i32))
                 {funcs}
                 (data (i32.const 1024) "{escaped}"))"#
        ))
        .expect("wat parses")
    }

    /// `<stem>.wasm` with a `<stem>.json` manifest naming `name`.
    fn write_wasm(dir: &Path, stem: &str, name: &str, hooks: &[&str], answer: &str) -> PathBuf {
        let path = dir.join(format!("{stem}.wasm"));
        std::fs::write(&path, module(hooks, answer)).unwrap();
        std::fs::write(
            dir.join(format!("{stem}.json")),
            serde_json::json!({ "name": name, "version": "1.0.0", "hooks": hooks }).to_string(),
        )
        .unwrap();
        path
    }

    /// A packed artefact `<file>` providing `name`.
    fn write_tar(
        dir: &Path,
        file: &str,
        name: &str,
        hooks: &[&str],
        answer: &str,
        key: Option<&ed25519_dalek::SigningKey>,
    ) -> PathBuf {
        let input = crate::plugin_artefact::PackInput {
            name: name.into(),
            version: "1.0.0".into(),
            description: String::new(),
            license: String::new(),
            hooks: hooks.iter().map(|h| h.to_string()).collect(),
            wasm: module(hooks, answer),
            wasm_signature: None,
        };
        let path = dir.join(file);
        std::fs::write(&path, crate::plugin_artefact::pack(&input, key).unwrap().1).unwrap();
        path
    }

    fn manager(dir: &Path, tune: impl FnOnce(&mut PluginRuntimeConfig)) -> PluginManager {
        let mut config = PluginRuntimeConfig {
            plugin_dir: dir.to_path_buf(),
            fuel_metering: false,
            hot_reload_debounce: Duration::ZERO,
            ..Default::default()
        };
        tune(&mut config);
        PluginManager::new(config).unwrap()
    }

    fn ctx() -> QueryContext {
        QueryContext {
            query: "SELECT 1".to_string(),
            normalized: "SELECT ?".to_string(),
            tables: Vec::new(),
            is_read_only: true,
            hook_context: HookContext::default(),
        }
    }

    fn blocked_by(pm: &PluginManager) -> Option<String> {
        match pm.execute_pre_query(&ctx()) {
            PreQueryResult::Block(reason) => Some(reason),
            _ => None,
        }
    }

    fn names(pm: &PluginManager) -> Vec<String> {
        pm.list_plugins().into_iter().map(|p| p.name).collect()
    }

    fn state_of(pm: &PluginManager, path: &Path) -> PluginFileState {
        pm.files()
            .into_iter()
            .find(|f| f.path == path)
            .map(|f| f.state)
            .unwrap_or_else(|| panic!("{} is not tracked", path.display()))
    }

    const BLOCK_A: &str = r#"{"kind": "block", "reason": "a"}"#;
    const BLOCK_B: &str = r#"{"kind": "block", "reason": "b"}"#;

    /// The startup scan loads every plugin file once, in file-name order,
    /// and a second scan with nothing changed does nothing (no hook is
    /// registered twice).
    #[test]
    fn a_rescan_loads_each_plugin_once_in_file_name_order() {
        let dir = tempfile::tempdir().unwrap();
        write_wasm(dir.path(), "20-second", "second", &["pre_query"], BLOCK_B);
        write_tar(
            dir.path(),
            "10-first.tar.gz",
            "first",
            &["pre_query", "post_query"],
            BLOCK_A,
            None,
        );
        let pm = manager(dir.path(), |_| {});
        let report = pm.rescan(true);
        assert_eq!(report.loaded, ["first", "second"]);
        assert!(report.failed.is_empty(), "{:?}", report.failed);
        assert_eq!(names(&pm), ["first", "second"]);
        assert_eq!(blocked_by(&pm).as_deref(), Some("a"), "10-first runs first");
        assert_eq!(pm.hook_plugins(HookType::PreQuery).unwrap().len(), 2);
        assert_eq!(pm.hook_plugins(HookType::PostQuery).unwrap().len(), 1);
        assert!(pm.rescan(true).is_quiet());
        assert_eq!(pm.hook_plugins(HookType::PreQuery).unwrap().len(), 2);
    }

    /// A changed file replaces its plugin once the new version loads; a new
    /// version that fails leaves the running one in place (the file shows
    /// as stale) until a good version arrives.
    #[test]
    fn a_failed_new_version_keeps_the_running_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_wasm(dir.path(), "guard", "guard", &["pre_query"], BLOCK_A);
        let pm = manager(dir.path(), |_| {});
        pm.rescan(true);
        assert_eq!(blocked_by(&pm).as_deref(), Some("a"));

        write_wasm(dir.path(), "guard", "guard", &["pre_query"], BLOCK_B);
        assert_eq!(pm.rescan(true).updated, ["guard"]);
        assert_eq!(blocked_by(&pm).as_deref(), Some("b"));

        std::fs::write(&path, b"\x00asm\x01\x00\x00\x00garbage").unwrap();
        let report = pm.rescan(true);
        assert_eq!(report.failed.len(), 1, "{report:?}");
        assert!(report.unloaded.is_empty());
        assert_eq!(
            blocked_by(&pm).as_deref(),
            Some("b"),
            "the old version still runs"
        );
        assert!(matches!(
            state_of(&pm, &path),
            PluginFileState::Stale { .. }
        ));
        // Nothing changed: the failed version is not retried.
        assert!(pm.rescan(true).is_quiet());

        write_wasm(dir.path(), "guard", "guard", &["pre_query"], BLOCK_A);
        assert_eq!(pm.rescan(true).updated, ["guard"]);
        assert_eq!(
            state_of(&pm, &path),
            PluginFileState::Loaded {
                name: "guard".into()
            }
        );
    }

    /// A removed file unloads its plugin; a file that now declares another
    /// name swaps the plugins.
    #[test]
    fn removed_and_renamed_files_unload_their_plugins() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_wasm(dir.path(), "p", "old-name", &["pre_query"], BLOCK_A);
        let pm = manager(dir.path(), |_| {});
        pm.rescan(true);
        write_wasm(dir.path(), "p", "new-name", &["pre_query"], BLOCK_A);
        let report = pm.rescan(true);
        assert_eq!(report.loaded, ["new-name"]);
        assert_eq!(report.unloaded, ["old-name"]);
        assert_eq!(names(&pm), ["new-name"]);

        std::fs::remove_file(&path).unwrap();
        assert_eq!(pm.rescan(true).unloaded, ["new-name"]);
        assert!(names(&pm).is_empty());
        assert!(!pm.has_hook(HookType::PreQuery));
        assert!(pm.files().is_empty());
    }

    /// One file provides each plugin: a packed artefact before a bare
    /// module of the same name, which takes over when the artefact goes.
    #[test]
    fn an_artefact_shadows_a_bare_module_of_the_same_name() {
        let dir = tempfile::tempdir().unwrap();
        let bare = write_wasm(dir.path(), "x", "x", &["pre_query"], BLOCK_B);
        let packed = write_tar(dir.path(), "x.tar.gz", "x", &["pre_query"], BLOCK_A, None);
        let pm = manager(dir.path(), |_| {});
        assert_eq!(pm.rescan(true).loaded, ["x"]);
        assert_eq!(blocked_by(&pm).as_deref(), Some("a"));
        assert_eq!(
            state_of(&pm, &bare),
            PluginFileState::Shadowed {
                name: "x".into(),
                by: packed.clone()
            }
        );
        std::fs::remove_file(&packed).unwrap();
        assert_eq!(pm.rescan(true).updated, ["x"]);
        assert_eq!(blocked_by(&pm).as_deref(), Some("b"));
    }

    /// With a trust root, an unsigned module fails and is retried when its
    /// signature appears; after the trust root changes, a plugin that no
    /// longer verifies is unloaded.
    #[test]
    fn signatures_are_retried_and_a_trust_root_change_revokes() {
        use ed25519_dalek::Signer;
        let dir = tempfile::tempdir().unwrap();
        let trust = tempfile::tempdir().unwrap();
        let k1 = ed25519_dalek::SigningKey::from_bytes(&[1; 32]);
        let k2 = ed25519_dalek::SigningKey::from_bytes(&[2; 32]);
        let publish = |key: &ed25519_dalek::SigningKey| {
            std::fs::write(
                trust.path().join("publisher.pub"),
                crate::plugin_artefact::public_key_base64(&key.verifying_key()),
            )
            .unwrap()
        };
        publish(&k1);
        let path = write_wasm(dir.path(), "signed", "signed", &["pre_query"], BLOCK_A);
        let pm = manager(dir.path(), |c| {
            c.trust_root = Some(trust.path().to_path_buf())
        });
        let report = pm.rescan(true);
        assert!(report.loaded.is_empty());
        assert!(report.failed[0].1.contains(".sig"), "{report:?}");

        let wasm = std::fs::read(&path).unwrap();
        let sig = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            k1.sign(&wasm).to_bytes(),
        );
        std::fs::write(dir.path().join("signed.sig"), sig).unwrap();
        assert_eq!(pm.rescan(true).loaded, ["signed"]);

        publish(&k2);
        let report = pm.rescan(true);
        assert_eq!(report.unloaded, ["signed"], "{report:?}");
        assert!(!pm.has_hook(HookType::PreQuery));
    }

    /// A trust root that cannot be loaded changes nothing (no plugin is
    /// unloaded because of it) until it is fixed.
    #[test]
    fn a_broken_trust_root_keeps_the_loaded_plugins() {
        let dir = tempfile::tempdir().unwrap();
        let trust = tempfile::tempdir().unwrap();
        let key = ed25519_dalek::SigningKey::from_bytes(&[1; 32]);
        std::fs::write(
            trust.path().join("k.pub"),
            crate::plugin_artefact::public_key_base64(&key.verifying_key()),
        )
        .unwrap();
        write_tar(
            dir.path(),
            "p.tar.gz",
            "p",
            &["pre_query"],
            BLOCK_A,
            Some(&key),
        );
        let pm = manager(dir.path(), |c| {
            c.trust_root = Some(trust.path().to_path_buf())
        });
        assert_eq!(pm.rescan(true).loaded, ["p"]);
        std::fs::write(trust.path().join("broken.pub"), "not a key").unwrap();
        let report = pm.rescan(true);
        assert!(report.unloaded.is_empty());
        assert_eq!(report.failed.len(), 1);
        assert_eq!(names(&pm), ["p"]);
        std::fs::remove_file(trust.path().join("broken.pub")).unwrap();
        assert!(pm.rescan(true).failed.is_empty());
        assert_eq!(names(&pm), ["p"]);
    }

    /// A plugin directory that does not exist yet holds no plugins; once
    /// created, its files load.
    #[test]
    fn a_missing_plugin_directory_is_not_an_error() {
        let base = tempfile::tempdir().unwrap();
        let dir = base.path().join("later");
        let pm = manager(&dir, |_| {});
        assert!(pm.rescan(true).is_quiet());
        std::fs::create_dir(&dir).unwrap();
        write_wasm(&dir, "p", "p", &["route"], r#"{"action": "primary"}"#);
        assert_eq!(pm.rescan(true).loaded, ["p"]);
        assert!(pm.has_hook(HookType::Route));
    }

    /// `max_plugins` bounds the loaded set; a refused plugin loads once a
    /// slot frees, without its file changing.
    #[test]
    fn max_plugins_bounds_the_loaded_set() {
        let dir = tempfile::tempdir().unwrap();
        let a = write_wasm(dir.path(), "a", "a", &["pre_query"], BLOCK_A);
        let b = write_wasm(dir.path(), "b", "b", &["pre_query"], BLOCK_B);
        let pm = manager(dir.path(), |c| c.max_plugins = 1);
        let report = pm.rescan(true);
        assert_eq!(report.loaded, ["a"]);
        assert!(report.failed[0].1.contains("max_plugins"), "{report:?}");
        assert!(matches!(state_of(&pm, &b), PluginFileState::Failed { .. }));
        assert!(
            pm.rescan(true).is_quiet(),
            "a capacity refusal is reported once"
        );
        std::fs::remove_file(&a).unwrap();
        let report = pm.rescan(true);
        assert_eq!(report.unloaded, ["a"]);
        assert_eq!(report.loaded, ["b"]);
    }

    /// A change is acted on only once it has been stable for the debounce
    /// period; an explicit rescan does not wait.
    #[test]
    fn the_debounce_defers_a_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_wasm(dir.path(), "p", "p", &["pre_query"], BLOCK_A);
        let pm = manager(dir.path(), |c| {
            c.hot_reload_debounce = Duration::from_secs(3600)
        });
        let report = pm.rescan(false);
        assert!(report.loaded.is_empty());
        assert_eq!(report.pending, std::slice::from_ref(&path));
        assert_eq!(state_of(&pm, &path), PluginFileState::Pending);
        assert_eq!(pm.rescan(true).loaded, ["p"]);
    }

    /// A plugin that would never run is refused: no hooks, or a hook this
    /// proxy never calls.
    #[test]
    fn plugins_that_would_never_run_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        write_wasm(dir.path(), "connect", "connect", &["on_connect"], BLOCK_A);
        write_wasm(dir.path(), "nothing", "nothing", &[], BLOCK_A);
        let pm = manager(dir.path(), |_| {});
        let report = pm.rescan(true);
        assert!(report.loaded.is_empty());
        let errors: Vec<&str> = report.failed.iter().map(|(_, e)| e.as_str()).collect();
        assert!(
            errors.iter().any(|e| e.contains("never calls")),
            "{errors:?}"
        );
        assert!(errors.iter().any(|e| e.contains("no hooks")), "{errors:?}");
    }

    /// `load_plugin` replaces a plugin loaded from the same file and
    /// refuses one whose name another file provides.
    #[test]
    fn load_plugin_upserts_by_file() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let path = write_wasm(dir.path(), "p", "p", &["pre_query"], BLOCK_A);
        let pm = manager(dir.path(), |_| {});
        pm.load_plugin(&path).unwrap();
        write_wasm(dir.path(), "p", "p", &["pre_query"], BLOCK_B);
        pm.load_plugin(&path).unwrap();
        assert_eq!(blocked_by(&pm).as_deref(), Some("b"));
        assert_eq!(pm.hook_plugins(HookType::PreQuery).unwrap().len(), 1);
        // The directory rescan knows this version is loaded.
        assert!(pm.rescan(true).is_quiet());

        let twin = write_wasm(other.path(), "twin", "p", &["pre_query"], BLOCK_A);
        let err = pm.load_plugin(&twin).unwrap_err();
        assert!(err.to_string().contains("already loaded from"), "{err}");
        pm.unload_plugin("p").unwrap();
        assert!(!pm.has_hook(HookType::PreQuery));
        // The directory is the source of truth: the next rescan reloads it.
        assert_eq!(pm.rescan(true).loaded, ["p"]);
    }

    /// `check_updates` reports a rescan as events.
    #[test]
    fn check_updates_reports_events() {
        let dir = tempfile::tempdir().unwrap();
        let pm = manager(dir.path(), |_| {});
        let path = write_wasm(dir.path(), "p", "p", &["pre_query"], BLOCK_A);
        assert_eq!(
            pm.check_updates().unwrap(),
            [ReloadEvent::Added(path.clone())]
        );
        write_wasm(dir.path(), "p", "p", &["pre_query"], BLOCK_B);
        assert_eq!(
            pm.check_updates().unwrap(),
            [ReloadEvent::Modified("p".into())]
        );
        std::fs::remove_file(&path).unwrap();
        assert_eq!(
            pm.check_updates().unwrap(),
            [ReloadEvent::Removed("p".into())]
        );
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
