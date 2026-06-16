//! Security Sandbox
//!
//! Sandboxing and permission system for WASM plugins.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

/// Plugin permissions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Execute queries
    QueryExecute,

    /// Read from cache
    CacheRead,

    /// Write to cache
    CacheWrite,

    /// Make HTTP requests
    HttpFetch,

    /// Access cryptographic functions
    Crypto,

    /// Read from KV store
    KvRead,

    /// Write to KV store
    KvWrite,

    /// Record metrics
    Metrics,

    /// Read configuration
    ConfigRead,

    /// Access network
    Network,

    /// Read filesystem
    FilesystemRead,

    /// Write filesystem
    FilesystemWrite,

    /// Custom permission
    Custom(String),
}

impl Permission {
    /// Parse permission from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "query_execute" | "query" => Some(Permission::QueryExecute),
            "cache_read" => Some(Permission::CacheRead),
            "cache_write" => Some(Permission::CacheWrite),
            "http_fetch" | "http" => Some(Permission::HttpFetch),
            "crypto" | "cryptography" => Some(Permission::Crypto),
            "kv_read" => Some(Permission::KvRead),
            "kv_write" => Some(Permission::KvWrite),
            "metrics" => Some(Permission::Metrics),
            "config_read" | "config" => Some(Permission::ConfigRead),
            "network" => Some(Permission::Network),
            "filesystem_read" | "fs_read" => Some(Permission::FilesystemRead),
            "filesystem_write" | "fs_write" => Some(Permission::FilesystemWrite),
            other => Some(Permission::Custom(other.to_string())),
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &str {
        match self {
            Permission::QueryExecute => "query_execute",
            Permission::CacheRead => "cache_read",
            Permission::CacheWrite => "cache_write",
            Permission::HttpFetch => "http_fetch",
            Permission::Crypto => "crypto",
            Permission::KvRead => "kv_read",
            Permission::KvWrite => "kv_write",
            Permission::Metrics => "metrics",
            Permission::ConfigRead => "config_read",
            Permission::Network => "network",
            Permission::FilesystemRead => "filesystem_read",
            Permission::FilesystemWrite => "filesystem_write",
            Permission::Custom(name) => name,
        }
    }

    /// Check if this is a dangerous permission
    pub fn is_dangerous(&self) -> bool {
        matches!(
            self,
            Permission::Network
                | Permission::FilesystemRead
                | Permission::FilesystemWrite
                | Permission::QueryExecute
        )
    }
}

/// Security policy
#[derive(Debug, Clone)]
pub struct SecurityPolicy {
    /// Allowed hosts for HTTP requests
    pub allowed_hosts: Vec<String>,

    /// Allowed filesystem paths
    pub allowed_paths: Vec<PathBuf>,

    /// Maximum memory
    pub max_memory: usize,

    /// Maximum execution time
    pub max_execution_time: Duration,

    /// Allow network access
    pub allow_network: bool,

    /// Allow filesystem access
    pub allow_filesystem: bool,
}

impl Default for SecurityPolicy {
    fn default() -> Self {
        Self {
            allowed_hosts: Vec::new(),
            allowed_paths: Vec::new(),
            max_memory: 64 * 1024 * 1024, // 64MB
            max_execution_time: Duration::from_millis(100),
            allow_network: false,
            allow_filesystem: false,
        }
    }
}

/// Resource limits
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum memory in bytes
    pub max_memory: usize,

    /// Maximum execution time
    pub max_execution_time: Duration,

    /// Maximum fuel (instruction count)
    pub max_fuel: Option<u64>,

    /// Maximum table elements
    pub max_table_elements: u32,

    /// Maximum instances
    pub max_instances: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory: 64 * 1024 * 1024, // 64MB
            max_execution_time: Duration::from_millis(100),
            max_fuel: Some(1_000_000),
            max_table_elements: 10000,
            max_instances: 1,
        }
    }
}

/// Plugin sandbox
#[derive(Debug, Clone)]
pub struct PluginSandbox {
    /// Security policy
    policy: SecurityPolicy,

    /// Resource limits
    limits: ResourceLimits,

    /// Granted permissions
    permissions: HashSet<Permission>,

    /// Denied hosts
    denied_hosts: HashSet<String>,

    /// Denied paths
    denied_paths: HashSet<PathBuf>,
}

impl PluginSandbox {
    /// Create a new sandbox with policy and permissions
    pub fn new(
        policy: SecurityPolicy,
        limits: ResourceLimits,
        permissions: Vec<Permission>,
    ) -> Self {
        Self {
            policy,
            limits,
            permissions: permissions.into_iter().collect(),
            denied_hosts: HashSet::new(),
            denied_paths: HashSet::new(),
        }
    }

    /// Check if a permission is granted
    pub fn has_permission(&self, permission: &Permission) -> bool {
        self.permissions.contains(permission)
    }

    /// Grant a permission
    pub fn grant_permission(&mut self, permission: Permission) {
        self.permissions.insert(permission);
    }

    /// Revoke a permission
    pub fn revoke_permission(&mut self, permission: &Permission) {
        self.permissions.remove(permission);
    }

    /// Check if a host is allowed
    pub fn is_host_allowed(&self, host: &str) -> bool {
        if self.denied_hosts.contains(host) {
            return false;
        }

        if !self.policy.allow_network && !self.has_permission(&Permission::Network) {
            return false;
        }

        // Check if host matches allowed patterns
        self.policy.allowed_hosts.iter().any(|allowed| {
            if let Some(suffix) = allowed.strip_prefix('*') {
                // Wildcard matching
                host.ends_with(suffix)
            } else {
                host == allowed
            }
        })
    }

    /// Check if a path is allowed
    pub fn is_path_allowed(&self, path: &PathBuf) -> bool {
        if self.denied_paths.contains(path) {
            return false;
        }

        if !self.policy.allow_filesystem
            && !self.has_permission(&Permission::FilesystemRead)
            && !self.has_permission(&Permission::FilesystemWrite)
        {
            return false;
        }

        // Check if path is under allowed directories
        self.policy.allowed_paths.iter().any(|allowed| {
            path.starts_with(allowed)
        })
    }

    /// Deny a host
    pub fn deny_host(&mut self, host: String) {
        self.denied_hosts.insert(host);
    }

    /// Deny a path
    pub fn deny_path(&mut self, path: PathBuf) {
        self.denied_paths.insert(path);
    }

    /// Get resource limits
    pub fn limits(&self) -> &ResourceLimits {
        &self.limits
    }

    /// Get security policy
    pub fn policy(&self) -> &SecurityPolicy {
        &self.policy
    }

    /// Get granted permissions
    pub fn permissions(&self) -> &HashSet<Permission> {
        &self.permissions
    }

    /// Validate a function call
    pub fn validate_call(
        &self,
        function: &super::host_functions::HostFunction,
    ) -> Result<(), SecurityError> {
        // Check if function requires a permission
        if let Some(required) = function.required_permission() {
            if !self.has_permission(&required) {
                return Err(SecurityError::PermissionDenied(format!(
                    "Function {:?} requires permission {:?}",
                    function, required
                )));
            }
        }

        Ok(())
    }

    /// Validate resource usage
    pub fn validate_resources(
        &self,
        memory_used: usize,
        fuel_consumed: Option<u64>,
    ) -> Result<(), SecurityError> {
        // Check memory
        if memory_used > self.limits.max_memory {
            return Err(SecurityError::ResourceExceeded(format!(
                "Memory limit exceeded: {} > {}",
                memory_used, self.limits.max_memory
            )));
        }

        // Check fuel
        if let (Some(consumed), Some(limit)) = (fuel_consumed, self.limits.max_fuel) {
            if consumed > limit {
                return Err(SecurityError::ResourceExceeded(format!(
                    "Fuel limit exceeded: {} > {}",
                    consumed, limit
                )));
            }
        }

        Ok(())
    }
}

impl Default for PluginSandbox {
    fn default() -> Self {
        Self::new(
            SecurityPolicy::default(),
            ResourceLimits::default(),
            Vec::new(),
        )
    }
}

/// Security error
#[derive(Debug, Clone)]
pub enum SecurityError {
    /// Permission denied
    PermissionDenied(String),

    /// Resource exceeded
    ResourceExceeded(String),

    /// Host not allowed
    HostNotAllowed(String),

    /// Path not allowed
    PathNotAllowed(String),

    /// Operation not allowed
    OperationNotAllowed(String),
}

impl std::fmt::Display for SecurityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityError::PermissionDenied(msg) => write!(f, "Permission denied: {}", msg),
            SecurityError::ResourceExceeded(msg) => write!(f, "Resource exceeded: {}", msg),
            SecurityError::HostNotAllowed(msg) => write!(f, "Host not allowed: {}", msg),
            SecurityError::PathNotAllowed(msg) => write!(f, "Path not allowed: {}", msg),
            SecurityError::OperationNotAllowed(msg) => write!(f, "Operation not allowed: {}", msg),
        }
    }
}

impl std::error::Error for SecurityError {}

/// Sandbox builder
pub struct SandboxBuilder {
    sandbox: PluginSandbox,
}

impl SandboxBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            sandbox: PluginSandbox::default(),
        }
    }

    /// Set memory limit
    pub fn memory_limit(mut self, limit: usize) -> Self {
        self.sandbox.limits.max_memory = limit;
        self.sandbox.policy.max_memory = limit;
        self
    }

    /// Set execution timeout
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.sandbox.limits.max_execution_time = timeout;
        self.sandbox.policy.max_execution_time = timeout;
        self
    }

    /// Set fuel limit
    pub fn fuel_limit(mut self, limit: u64) -> Self {
        self.sandbox.limits.max_fuel = Some(limit);
        self
    }

    /// Grant permission
    pub fn grant(mut self, permission: Permission) -> Self {
        self.sandbox.permissions.insert(permission);
        self
    }

    /// Allow host
    pub fn allow_host(mut self, host: String) -> Self {
        self.sandbox.policy.allowed_hosts.push(host);
        self
    }

    /// Allow path
    pub fn allow_path(mut self, path: PathBuf) -> Self {
        self.sandbox.policy.allowed_paths.push(path);
        self
    }

    /// Enable network
    pub fn enable_network(mut self) -> Self {
        self.sandbox.policy.allow_network = true;
        self
    }

    /// Enable filesystem
    pub fn enable_filesystem(mut self) -> Self {
        self.sandbox.policy.allow_filesystem = true;
        self
    }

    /// Build the sandbox
    pub fn build(self) -> PluginSandbox {
        self.sandbox
    }
}

impl Default for SandboxBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_permission_from_str() {
        assert_eq!(Permission::from_str("http_fetch"), Some(Permission::HttpFetch));
        assert_eq!(Permission::from_str("cache_read"), Some(Permission::CacheRead));
        assert_eq!(Permission::from_str("unknown"), Some(Permission::Custom("unknown".to_string())));
    }

    #[test]
    fn test_permission_as_str() {
        assert_eq!(Permission::HttpFetch.as_str(), "http_fetch");
        assert_eq!(Permission::CacheRead.as_str(), "cache_read");
    }

    #[test]
    fn test_permission_is_dangerous() {
        assert!(Permission::Network.is_dangerous());
        assert!(Permission::FilesystemRead.is_dangerous());
        assert!(!Permission::CacheRead.is_dangerous());
        assert!(!Permission::Metrics.is_dangerous());
    }

    #[test]
    fn test_sandbox_default() {
        let sandbox = PluginSandbox::default();
        assert!(sandbox.permissions.is_empty());
        assert_eq!(sandbox.limits.max_memory, 64 * 1024 * 1024);
    }

    #[test]
    fn test_sandbox_permissions() {
        let mut sandbox = PluginSandbox::default();

        assert!(!sandbox.has_permission(&Permission::HttpFetch));

        sandbox.grant_permission(Permission::HttpFetch);
        assert!(sandbox.has_permission(&Permission::HttpFetch));

        sandbox.revoke_permission(&Permission::HttpFetch);
        assert!(!sandbox.has_permission(&Permission::HttpFetch));
    }

    #[test]
    fn test_sandbox_host_check() {
        let sandbox = SandboxBuilder::new()
            .enable_network()
            .grant(Permission::Network)
            .allow_host("api.example.com".to_string())
            .allow_host("*.internal.com".to_string())
            .build();

        assert!(sandbox.is_host_allowed("api.example.com"));
        assert!(sandbox.is_host_allowed("service.internal.com"));
        assert!(!sandbox.is_host_allowed("malicious.com"));
    }

    #[test]
    fn test_sandbox_path_check() {
        let sandbox = SandboxBuilder::new()
            .enable_filesystem()
            .grant(Permission::FilesystemRead)
            .allow_path(PathBuf::from("/tmp/plugins"))
            .build();

        assert!(sandbox.is_path_allowed(&PathBuf::from("/tmp/plugins/data.txt")));
        assert!(!sandbox.is_path_allowed(&PathBuf::from("/etc/passwd")));
    }

    #[test]
    fn test_sandbox_validate_resources() {
        let sandbox = SandboxBuilder::new()
            .memory_limit(1024 * 1024) // 1MB
            .fuel_limit(1000)
            .build();

        // Within limits
        assert!(sandbox.validate_resources(512 * 1024, Some(500)).is_ok());

        // Memory exceeded
        assert!(sandbox.validate_resources(2 * 1024 * 1024, Some(500)).is_err());

        // Fuel exceeded
        assert!(sandbox.validate_resources(512 * 1024, Some(2000)).is_err());
    }

    #[test]
    fn test_sandbox_builder() {
        let sandbox = SandboxBuilder::new()
            .memory_limit(32 * 1024 * 1024)
            .timeout(Duration::from_millis(50))
            .fuel_limit(500_000)
            .grant(Permission::CacheRead)
            .grant(Permission::CacheWrite)
            .allow_host("localhost".to_string())
            .build();

        assert_eq!(sandbox.limits.max_memory, 32 * 1024 * 1024);
        assert_eq!(sandbox.limits.max_execution_time, Duration::from_millis(50));
        assert_eq!(sandbox.limits.max_fuel, Some(500_000));
        assert!(sandbox.has_permission(&Permission::CacheRead));
        assert!(sandbox.has_permission(&Permission::CacheWrite));
    }

    #[test]
    fn test_security_error_display() {
        let err = SecurityError::PermissionDenied("http_fetch".to_string());
        assert!(err.to_string().contains("Permission denied"));

        let err = SecurityError::ResourceExceeded("memory".to_string());
        assert!(err.to_string().contains("Resource exceeded"));
    }

    #[test]
    fn test_denied_hosts_and_paths() {
        let mut sandbox = SandboxBuilder::new()
            .enable_network()
            .grant(Permission::Network)
            .allow_host("*.example.com".to_string())
            .build();

        sandbox.deny_host("bad.example.com".to_string());

        assert!(sandbox.is_host_allowed("good.example.com"));
        assert!(!sandbox.is_host_allowed("bad.example.com"));
    }
}
