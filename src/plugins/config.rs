//! Plugin Configuration
//!
//! Configuration types for the WASM plugin system.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the plugin runtime
#[derive(Debug, Clone)]
pub struct PluginRuntimeConfig {
    /// Enable plugin system
    pub enabled: bool,

    /// Plugin directory
    pub plugin_dir: PathBuf,

    /// Hot reload on file change
    pub hot_reload: bool,

    /// Memory limit per plugin (bytes)
    pub memory_limit: usize,

    /// Execution timeout per call
    pub timeout: Duration,

    /// Maximum plugins loaded
    pub max_plugins: usize,

    /// Enable fuel metering (limits CPU cycles)
    pub fuel_metering: bool,

    /// Fuel limit per call (if fuel_metering enabled)
    pub fuel_limit: u64,

    /// Enable WASM SIMD
    pub enable_simd: bool,

    /// Enable multi-threading
    pub enable_threads: bool,

    /// Cache compiled modules
    pub cache_modules: bool,

    /// Module cache directory
    pub cache_dir: Option<PathBuf>,

    /// Per-plugin configurations
    pub plugins: HashMap<String, PluginConfig>,

    /// Optional Ed25519 trust root: directory of `*.pub` files. When
    /// set, every loaded `.wasm` requires a sidecar `.sig` that
    /// verifies against one of the keys. When `None`, signatures
    /// aren't checked (preserves the dev-loop ergonomic of dropping
    /// unsigned `.wasm` files in the plugin dir).
    pub trust_root: Option<PathBuf>,

    /// Max bytes for a single plugin-KV value (`0` = unlimited).
    /// Applied to the shared `KvBackend` at runtime construction.
    pub kv_max_value_bytes: usize,

    /// Max distinct keys per plugin KV namespace (`0` = unlimited).
    pub kv_max_keys_per_plugin: usize,

    /// Max distinct plugin KV namespaces (`0` = unlimited). Bounds how
    /// many `<plugin>` namespaces the `/admin/kv` endpoint can create.
    pub kv_max_plugins: usize,

    /// Max TOTAL retained bytes across ALL plugin KV namespaces (key +
    /// value + namespace-name bytes; `0` = unlimited). The single cap
    /// that bounds the whole KV footprint regardless of the per-axis
    /// product, so a token-holding `/admin/kv` caller cannot drive the
    /// proxy to an OOM.
    pub kv_max_total_bytes: usize,
}

impl Default for PluginRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            plugin_dir: PathBuf::from("/etc/heliosproxy/plugins"),
            hot_reload: false,
            memory_limit: 64 * 1024 * 1024, // 64MB
            timeout: Duration::from_millis(100),
            max_plugins: 20,
            fuel_metering: true,
            fuel_limit: 1_000_000,
            enable_simd: true,
            enable_threads: false,
            cache_modules: true,
            cache_dir: None,
            plugins: HashMap::new(),
            trust_root: None,
            kv_max_value_bytes: 65536,
            kv_max_keys_per_plugin: 1024,
            kv_max_plugins: 256,
            kv_max_total_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Convert the TOML-shaped `PluginToml` into a live `PluginRuntimeConfig`.
///
/// Values that aren't exposed in TOML (SIMD, threading, module cache)
/// take their runtime defaults.
impl From<&crate::config::PluginToml> for PluginRuntimeConfig {
    fn from(t: &crate::config::PluginToml) -> Self {
        Self {
            enabled: t.enabled,
            plugin_dir: PathBuf::from(&t.plugin_dir),
            hot_reload: t.hot_reload,
            memory_limit: t.memory_limit_mb.saturating_mul(1024 * 1024),
            timeout: Duration::from_millis(t.timeout_ms),
            max_plugins: t.max_plugins,
            fuel_metering: t.fuel_metering,
            fuel_limit: t.fuel_limit,
            enable_simd: true,
            enable_threads: false,
            cache_modules: true,
            cache_dir: None,
            plugins: HashMap::new(),
            trust_root: t.trust_root.as_ref().map(PathBuf::from),
            kv_max_value_bytes: t.kv_max_value_bytes,
            kv_max_keys_per_plugin: t.kv_max_keys_per_plugin,
            kv_max_plugins: t.kv_max_plugins,
            kv_max_total_bytes: t.kv_max_total_bytes,
        }
    }
}

/// Builder for PluginRuntimeConfig
pub struct PluginRuntimeConfigBuilder {
    config: PluginRuntimeConfig,
}

impl PluginRuntimeConfigBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: PluginRuntimeConfig::default(),
        }
    }

    /// Set enabled
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Set plugin directory
    pub fn plugin_dir(mut self, dir: PathBuf) -> Self {
        self.config.plugin_dir = dir;
        self
    }

    /// Set hot reload
    pub fn hot_reload(mut self, enabled: bool) -> Self {
        self.config.hot_reload = enabled;
        self
    }

    /// Set memory limit
    pub fn memory_limit(mut self, limit: usize) -> Self {
        self.config.memory_limit = limit;
        self
    }

    /// Set timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    /// Set max plugins
    pub fn max_plugins(mut self, max: usize) -> Self {
        self.config.max_plugins = max;
        self
    }

    /// Set fuel metering
    pub fn fuel_metering(mut self, enabled: bool) -> Self {
        self.config.fuel_metering = enabled;
        self
    }

    /// Set fuel limit
    pub fn fuel_limit(mut self, limit: u64) -> Self {
        self.config.fuel_limit = limit;
        self
    }

    /// Enable SIMD
    pub fn enable_simd(mut self, enabled: bool) -> Self {
        self.config.enable_simd = enabled;
        self
    }

    /// Enable threads
    pub fn enable_threads(mut self, enabled: bool) -> Self {
        self.config.enable_threads = enabled;
        self
    }

    /// Enable module caching
    pub fn cache_modules(mut self, enabled: bool) -> Self {
        self.config.cache_modules = enabled;
        self
    }

    /// Set cache directory
    pub fn cache_dir(mut self, dir: PathBuf) -> Self {
        self.config.cache_dir = Some(dir);
        self
    }

    /// Add plugin config
    pub fn add_plugin(mut self, name: String, config: PluginConfig) -> Self {
        self.config.plugins.insert(name, config);
        self
    }

    /// Build the config
    pub fn build(self) -> PluginRuntimeConfig {
        self.config
    }
}

impl Default for PluginRuntimeConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-plugin configuration
#[derive(Debug, Clone)]
pub struct PluginConfig {
    /// Enable this plugin
    pub enabled: bool,

    /// Plugin priority (higher = earlier execution)
    pub priority: i32,

    /// Custom config passed to plugin
    pub config: HashMap<String, serde_json::Value>,

    /// Override memory limit
    pub memory_limit: Option<usize>,

    /// Override timeout
    pub timeout: Option<Duration>,

    /// Override fuel limit
    pub fuel_limit: Option<u64>,

    /// Allowed permissions
    pub permissions: Vec<String>,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            priority: 0,
            config: HashMap::new(),
            memory_limit: None,
            timeout: None,
            fuel_limit: None,
            permissions: Vec::new(),
        }
    }
}

/// Builder for PluginConfig
pub struct PluginConfigBuilder {
    config: PluginConfig,
}

impl PluginConfigBuilder {
    /// Create new builder
    pub fn new() -> Self {
        Self {
            config: PluginConfig::default(),
        }
    }

    /// Set enabled
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Set priority
    pub fn priority(mut self, priority: i32) -> Self {
        self.config.priority = priority;
        self
    }

    /// Add config value
    pub fn config_value(mut self, key: &str, value: serde_json::Value) -> Self {
        self.config.config.insert(key.to_string(), value);
        self
    }

    /// Set memory limit
    pub fn memory_limit(mut self, limit: usize) -> Self {
        self.config.memory_limit = Some(limit);
        self
    }

    /// Set timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = Some(timeout);
        self
    }

    /// Set fuel limit
    pub fn fuel_limit(mut self, limit: u64) -> Self {
        self.config.fuel_limit = Some(limit);
        self
    }

    /// Add permission
    pub fn permission(mut self, permission: &str) -> Self {
        self.config.permissions.push(permission.to_string());
        self
    }

    /// Build
    pub fn build(self) -> PluginConfig {
        self.config
    }
}

impl Default for PluginConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_config_default() {
        let config = PluginRuntimeConfig::default();
        assert!(config.enabled);
        assert!(!config.hot_reload);
        assert_eq!(config.memory_limit, 64 * 1024 * 1024);
        assert_eq!(config.max_plugins, 20);
    }

    #[test]
    fn test_runtime_config_builder() {
        let config = PluginRuntimeConfigBuilder::new()
            .enabled(true)
            .hot_reload(true)
            .memory_limit(128 * 1024 * 1024)
            .timeout(Duration::from_millis(200))
            .max_plugins(50)
            .fuel_metering(true)
            .fuel_limit(2_000_000)
            .build();

        assert!(config.enabled);
        assert!(config.hot_reload);
        assert_eq!(config.memory_limit, 128 * 1024 * 1024);
        assert_eq!(config.timeout, Duration::from_millis(200));
        assert_eq!(config.max_plugins, 50);
    }

    #[test]
    fn test_plugin_config_default() {
        let config = PluginConfig::default();
        assert!(config.enabled);
        assert_eq!(config.priority, 0);
        assert!(config.permissions.is_empty());
    }

    #[test]
    fn test_plugin_config_builder() {
        let config = PluginConfigBuilder::new()
            .enabled(true)
            .priority(100)
            .config_value("key", serde_json::json!("value"))
            .memory_limit(32 * 1024 * 1024)
            .permission("http_fetch")
            .permission("cache_read")
            .build();

        assert!(config.enabled);
        assert_eq!(config.priority, 100);
        assert_eq!(config.config.get("key"), Some(&serde_json::json!("value")));
        assert_eq!(config.memory_limit, Some(32 * 1024 * 1024));
        assert_eq!(config.permissions.len(), 2);
    }
}
