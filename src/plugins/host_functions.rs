//! Host Functions
//!
//! Host functions that plugins can call to interact with the proxy.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

/// Host function registry
pub struct HostFunctionRegistry {
    /// Registered functions by namespace
    functions: RwLock<HashMap<String, HashMap<String, HostFunction>>>,

    /// Call statistics
    stats: RwLock<HostFunctionStats>,
}

impl HostFunctionRegistry {
    /// Create a new registry with default functions
    pub fn new() -> Self {
        let registry = Self {
            functions: RwLock::new(HashMap::new()),
            stats: RwLock::new(HostFunctionStats::default()),
        };

        // Register default functions
        registry.register_defaults();
        registry
    }

    /// Register default host functions
    fn register_defaults(&self) {
        // Helios namespace - core functions
        self.register("helios", "log", HostFunction::Log);
        self.register("helios", "metric_inc", HostFunction::MetricInc);
        self.register("helios", "metric_gauge", HostFunction::MetricGauge);
        self.register("helios", "get_config", HostFunction::GetConfig);
        self.register("helios", "get_time", HostFunction::GetTime);

        // Query namespace
        self.register("query", "execute", HostFunction::QueryExecute);
        self.register("query", "prepare", HostFunction::QueryPrepare);
        self.register("query", "get_tables", HostFunction::QueryGetTables);
        self.register("query", "normalize", HostFunction::QueryNormalize);

        // Cache namespace
        self.register("cache", "get", HostFunction::CacheGet);
        self.register("cache", "set", HostFunction::CacheSet);
        self.register("cache", "delete", HostFunction::CacheDelete);
        self.register("cache", "exists", HostFunction::CacheExists);

        // HTTP namespace (requires http_fetch permission)
        self.register("http", "fetch", HostFunction::HttpFetch);
        self.register("http", "post", HostFunction::HttpPost);

        // Crypto namespace
        self.register("crypto", "hash", HostFunction::CryptoHash);
        self.register("crypto", "hmac", HostFunction::CryptoHmac);
        self.register("crypto", "random", HostFunction::CryptoRandom);

        // KV namespace (plugin-local storage)
        self.register("kv", "get", HostFunction::KvGet);
        self.register("kv", "set", HostFunction::KvSet);
        self.register("kv", "delete", HostFunction::KvDelete);
        self.register("kv", "list", HostFunction::KvList);
    }

    /// Register a host function
    pub fn register(&self, namespace: &str, name: &str, function: HostFunction) {
        let mut functions = self.functions.write();
        functions
            .entry(namespace.to_string())
            .or_insert_with(HashMap::new)
            .insert(name.to_string(), function);
    }

    /// Get a host function
    pub fn get(&self, namespace: &str, name: &str) -> Option<HostFunction> {
        let functions = self.functions.read();
        functions
            .get(namespace)
            .and_then(|ns| ns.get(name))
            .cloned()
    }

    /// Check if a function exists
    pub fn exists(&self, namespace: &str, name: &str) -> bool {
        let functions = self.functions.read();
        functions
            .get(namespace)
            .map(|ns| ns.contains_key(name))
            .unwrap_or(false)
    }

    /// List all functions in a namespace
    pub fn list_namespace(&self, namespace: &str) -> Vec<String> {
        let functions = self.functions.read();
        functions
            .get(namespace)
            .map(|ns| ns.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// List all namespaces
    pub fn list_namespaces(&self) -> Vec<String> {
        let functions = self.functions.read();
        functions.keys().cloned().collect()
    }

    /// Record a function call
    pub fn record_call(&self, namespace: &str, name: &str, duration: Duration, success: bool) {
        let mut stats = self.stats.write();
        let key = format!("{}:{}", namespace, name);

        let entry = stats.calls.entry(key).or_insert_with(FunctionCallStats::default);
        entry.total_calls += 1;
        entry.total_duration += duration;

        if success {
            entry.successful_calls += 1;
        } else {
            entry.failed_calls += 1;
        }

        if duration > entry.max_duration {
            entry.max_duration = duration;
        }
    }

    /// Get call statistics
    pub fn get_stats(&self) -> HostFunctionStats {
        self.stats.read().clone()
    }
}

impl Default for HostFunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Host function types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostFunction {
    // Helios core
    Log,
    MetricInc,
    MetricGauge,
    GetConfig,
    GetTime,

    // Query operations
    QueryExecute,
    QueryPrepare,
    QueryGetTables,
    QueryNormalize,

    // Cache operations
    CacheGet,
    CacheSet,
    CacheDelete,
    CacheExists,

    // HTTP operations
    HttpFetch,
    HttpPost,

    // Crypto operations
    CryptoHash,
    CryptoHmac,
    CryptoRandom,

    // KV storage
    KvGet,
    KvSet,
    KvDelete,
    KvList,

    // Custom function
    Custom(String),
}

impl HostFunction {
    /// Get the required permission for this function
    pub fn required_permission(&self) -> Option<super::sandbox::Permission> {
        use super::sandbox::Permission;

        match self {
            // No permission required
            HostFunction::Log => None,
            HostFunction::GetTime => None,
            HostFunction::GetConfig => None,

            // Metrics permission
            HostFunction::MetricInc | HostFunction::MetricGauge => Some(Permission::Metrics),

            // Query permission
            HostFunction::QueryExecute
            | HostFunction::QueryPrepare
            | HostFunction::QueryGetTables
            | HostFunction::QueryNormalize => Some(Permission::QueryExecute),

            // Cache permissions
            HostFunction::CacheGet | HostFunction::CacheExists => Some(Permission::CacheRead),
            HostFunction::CacheSet | HostFunction::CacheDelete => Some(Permission::CacheWrite),

            // HTTP permission
            HostFunction::HttpFetch | HostFunction::HttpPost => Some(Permission::HttpFetch),

            // Crypto permission
            HostFunction::CryptoHash | HostFunction::CryptoHmac | HostFunction::CryptoRandom => {
                Some(Permission::Crypto)
            }

            // KV permissions
            HostFunction::KvGet | HostFunction::KvList => Some(Permission::KvRead),
            HostFunction::KvSet | HostFunction::KvDelete => Some(Permission::KvWrite),

            // Custom functions require custom permission
            HostFunction::Custom(_) => Some(Permission::Custom("custom".to_string())),
        }
    }

    /// Get the function signature (for documentation)
    pub fn signature(&self) -> &'static str {
        match self {
            HostFunction::Log => "log(level: i32, message_ptr: i32, message_len: i32)",
            HostFunction::MetricInc => "metric_inc(name_ptr: i32, name_len: i32, value: f64)",
            HostFunction::MetricGauge => "metric_gauge(name_ptr: i32, name_len: i32, value: f64)",
            HostFunction::GetConfig => "get_config(key_ptr: i32, key_len: i32) -> i32",
            HostFunction::GetTime => "get_time() -> i64",

            HostFunction::QueryExecute => {
                "query_execute(query_ptr: i32, query_len: i32) -> i32"
            }
            HostFunction::QueryPrepare => {
                "query_prepare(query_ptr: i32, query_len: i32) -> i32"
            }
            HostFunction::QueryGetTables => {
                "query_get_tables(query_ptr: i32, query_len: i32) -> i32"
            }
            HostFunction::QueryNormalize => {
                "query_normalize(query_ptr: i32, query_len: i32) -> i32"
            }

            HostFunction::CacheGet => "cache_get(key_ptr: i32, key_len: i32) -> i32",
            HostFunction::CacheSet => {
                "cache_set(key_ptr: i32, key_len: i32, value_ptr: i32, value_len: i32, ttl: i64)"
            }
            HostFunction::CacheDelete => "cache_delete(key_ptr: i32, key_len: i32)",
            HostFunction::CacheExists => "cache_exists(key_ptr: i32, key_len: i32) -> i32",

            HostFunction::HttpFetch => "http_fetch(url_ptr: i32, url_len: i32) -> i32",
            HostFunction::HttpPost => {
                "http_post(url_ptr: i32, url_len: i32, body_ptr: i32, body_len: i32) -> i32"
            }

            HostFunction::CryptoHash => {
                "crypto_hash(algo_ptr: i32, algo_len: i32, data_ptr: i32, data_len: i32) -> i32"
            }
            HostFunction::CryptoHmac => {
                "crypto_hmac(key_ptr: i32, key_len: i32, data_ptr: i32, data_len: i32) -> i32"
            }
            HostFunction::CryptoRandom => "crypto_random(len: i32) -> i32",

            HostFunction::KvGet => "kv_get(key_ptr: i32, key_len: i32) -> i32",
            HostFunction::KvSet => {
                "kv_set(key_ptr: i32, key_len: i32, value_ptr: i32, value_len: i32)"
            }
            HostFunction::KvDelete => "kv_delete(key_ptr: i32, key_len: i32)",
            HostFunction::KvList => "kv_list(prefix_ptr: i32, prefix_len: i32) -> i32",

            HostFunction::Custom(_) => "custom(...)",
        }
    }
}

/// Host function call statistics
#[derive(Debug, Clone, Default)]
pub struct HostFunctionStats {
    /// Per-function statistics
    pub calls: HashMap<String, FunctionCallStats>,
}

/// Per-function call statistics
#[derive(Debug, Clone, Default)]
pub struct FunctionCallStats {
    /// Total calls
    pub total_calls: u64,

    /// Successful calls
    pub successful_calls: u64,

    /// Failed calls
    pub failed_calls: u64,

    /// Total duration
    pub total_duration: Duration,

    /// Maximum duration
    pub max_duration: Duration,
}

impl FunctionCallStats {
    /// Get average duration
    pub fn avg_duration(&self) -> Duration {
        if self.total_calls == 0 {
            Duration::ZERO
        } else {
            self.total_duration / self.total_calls as u32
        }
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_calls == 0 {
            1.0
        } else {
            self.successful_calls as f64 / self.total_calls as f64
        }
    }
}

/// Host function context (passed to function implementations)
pub struct HostFunctionContext {
    /// Plugin name
    pub plugin_name: String,

    /// Request ID
    pub request_id: String,

    /// Plugin memory (for reading/writing)
    pub memory: Arc<RwLock<Vec<u8>>>,

    /// Plugin configuration
    pub config: HashMap<String, serde_json::Value>,

    /// Call start time
    pub start_time: Instant,
}

impl HostFunctionContext {
    /// Read a string from plugin memory
    pub fn read_string(&self, ptr: i32, len: i32) -> Result<String, HostFunctionError> {
        let memory = self.memory.read();
        let start = ptr as usize;
        let end = start + len as usize;

        if end > memory.len() {
            return Err(HostFunctionError::MemoryAccessError(
                "Read out of bounds".to_string(),
            ));
        }

        String::from_utf8(memory[start..end].to_vec())
            .map_err(|e| HostFunctionError::InvalidData(e.to_string()))
    }

    /// Read bytes from plugin memory
    pub fn read_bytes(&self, ptr: i32, len: i32) -> Result<Vec<u8>, HostFunctionError> {
        let memory = self.memory.read();
        let start = ptr as usize;
        let end = start + len as usize;

        if end > memory.len() {
            return Err(HostFunctionError::MemoryAccessError(
                "Read out of bounds".to_string(),
            ));
        }

        Ok(memory[start..end].to_vec())
    }

    /// Write bytes to plugin memory
    pub fn write_bytes(&self, ptr: i32, data: &[u8]) -> Result<(), HostFunctionError> {
        let mut memory = self.memory.write();
        let start = ptr as usize;
        let end = start + data.len();

        if end > memory.len() {
            return Err(HostFunctionError::MemoryAccessError(
                "Write out of bounds".to_string(),
            ));
        }

        memory[start..end].copy_from_slice(data);
        Ok(())
    }

    /// Allocate memory in plugin
    pub fn allocate(&self, size: usize) -> Result<i32, HostFunctionError> {
        let mut memory = self.memory.write();
        let ptr = memory.len() as i32;
        let new_size = memory.len() + size;
        memory.resize(new_size, 0);
        Ok(ptr)
    }

    /// Get elapsed time since call start
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Host function error
#[derive(Debug, Clone)]
pub enum HostFunctionError {
    /// Memory access error
    MemoryAccessError(String),

    /// Invalid data
    InvalidData(String),

    /// Permission denied
    PermissionDenied(String),

    /// Function not found
    FunctionNotFound(String),

    /// Execution error
    ExecutionError(String),

    /// Timeout
    Timeout,
}

impl std::fmt::Display for HostFunctionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostFunctionError::MemoryAccessError(msg) => write!(f, "Memory access error: {}", msg),
            HostFunctionError::InvalidData(msg) => write!(f, "Invalid data: {}", msg),
            HostFunctionError::PermissionDenied(msg) => write!(f, "Permission denied: {}", msg),
            HostFunctionError::FunctionNotFound(msg) => write!(f, "Function not found: {}", msg),
            HostFunctionError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            HostFunctionError::Timeout => write!(f, "Timeout"),
        }
    }
}

impl std::error::Error for HostFunctionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_host_function_registry_new() {
        let registry = HostFunctionRegistry::new();

        // Check default functions are registered
        assert!(registry.exists("helios", "log"));
        assert!(registry.exists("cache", "get"));
        assert!(registry.exists("query", "execute"));
    }

    #[test]
    fn test_host_function_registry_list() {
        let registry = HostFunctionRegistry::new();

        let namespaces = registry.list_namespaces();
        assert!(namespaces.contains(&"helios".to_string()));
        assert!(namespaces.contains(&"cache".to_string()));
        assert!(namespaces.contains(&"query".to_string()));

        let helios_funcs = registry.list_namespace("helios");
        assert!(helios_funcs.contains(&"log".to_string()));
    }

    #[test]
    fn test_host_function_required_permission() {
        assert!(HostFunction::Log.required_permission().is_none());
        assert!(HostFunction::HttpFetch.required_permission().is_some());
        assert!(HostFunction::CacheGet.required_permission().is_some());
    }

    #[test]
    fn test_host_function_signature() {
        let sig = HostFunction::Log.signature();
        assert!(sig.contains("log"));
        assert!(sig.contains("level"));
    }

    #[test]
    fn test_function_call_stats() {
        let mut stats = FunctionCallStats::default();
        stats.total_calls = 10;
        stats.successful_calls = 9;
        stats.failed_calls = 1;
        stats.total_duration = Duration::from_millis(100);

        assert_eq!(stats.avg_duration(), Duration::from_millis(10));
        assert!((stats.success_rate() - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_host_function_context_memory() {
        let ctx = HostFunctionContext {
            plugin_name: "test".to_string(),
            request_id: "req-1".to_string(),
            memory: Arc::new(RwLock::new(vec![0u8; 1024])),
            config: HashMap::new(),
            start_time: Instant::now(),
        };

        // Write and read back
        ctx.write_bytes(0, b"hello").unwrap();
        let read = ctx.read_bytes(0, 5).unwrap();
        assert_eq!(read, b"hello");

        // Read string
        let s = ctx.read_string(0, 5).unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_host_function_context_out_of_bounds() {
        let ctx = HostFunctionContext {
            plugin_name: "test".to_string(),
            request_id: "req-1".to_string(),
            memory: Arc::new(RwLock::new(vec![0u8; 10])),
            config: HashMap::new(),
            start_time: Instant::now(),
        };

        // Try to read beyond memory
        let result = ctx.read_bytes(5, 10);
        assert!(result.is_err());
    }

    #[test]
    fn test_record_call() {
        let registry = HostFunctionRegistry::new();

        registry.record_call("helios", "log", Duration::from_micros(50), true);
        registry.record_call("helios", "log", Duration::from_micros(100), true);
        registry.record_call("helios", "log", Duration::from_micros(75), false);

        let stats = registry.get_stats();
        let log_stats = stats.calls.get("helios:log").unwrap();

        assert_eq!(log_stats.total_calls, 3);
        assert_eq!(log_stats.successful_calls, 2);
        assert_eq!(log_stats.failed_calls, 1);
        assert_eq!(log_stats.max_duration, Duration::from_micros(100));
    }
}
