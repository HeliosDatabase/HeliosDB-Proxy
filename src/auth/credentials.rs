//! Credential Providers
//!
//! Fetches database credentials from external sources like HashiCorp Vault,
//! AWS Secrets Manager, or environment variables.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use thiserror::Error;

use super::config::CredentialConfig;

/// Credential errors
#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("Credential not found: {0}")]
    NotFound(String),

    #[error("Provider unavailable: {0}")]
    ProviderUnavailable(String),

    #[error("Access denied: {0}")]
    AccessDenied(String),

    #[error("Invalid credential format: {0}")]
    InvalidFormat(String),

    #[error("Credential expired")]
    Expired,

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

/// Database credential
#[derive(Debug, Clone)]
pub struct DatabaseCredential {
    /// Username
    pub username: String,

    /// Password
    pub password: String,

    /// Database name (optional)
    pub database: Option<String>,

    /// Host (optional, for connection routing)
    pub host: Option<String>,

    /// Port (optional)
    pub port: Option<u16>,

    /// Additional connection options
    pub options: HashMap<String, String>,

    /// Credential expiration
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Source provider
    pub source: CredentialSource,
}

impl DatabaseCredential {
    /// Check if credential is expired
    pub fn is_expired(&self) -> bool {
        self.expires_at
            .map(|exp| chrono::Utc::now() > exp)
            .unwrap_or(false)
    }

    /// Get time until expiration
    pub fn time_until_expiration(&self) -> Option<Duration> {
        self.expires_at.and_then(|exp| {
            let now = chrono::Utc::now();
            if exp > now {
                Some((exp - now).to_std().unwrap_or(Duration::ZERO))
            } else {
                None
            }
        })
    }

    /// Build connection string
    pub fn connection_string(&self) -> String {
        let host = self.host.as_deref().unwrap_or("localhost");
        let port = self.port.unwrap_or(5432);
        let database = self.database.as_deref().unwrap_or("postgres");

        format!(
            "postgresql://{}:{}@{}:{}/{}",
            self.username,
            self.password,
            host,
            port,
            database
        )
    }
}

/// Credential source identifier
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialSource {
    /// Static configuration
    Static,

    /// Environment variable
    Environment,

    /// HashiCorp Vault
    Vault,

    /// AWS Secrets Manager
    AwsSecretsManager,

    /// Azure Key Vault
    AzureKeyVault,

    /// GCP Secret Manager
    GcpSecretManager,

    /// Kubernetes secret
    Kubernetes,

    /// Custom provider
    Custom(String),
}

/// Credential provider trait
pub trait CredentialProvider: Send + Sync {
    /// Get credential by key
    fn get_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError>;

    /// Refresh credential
    fn refresh_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError>;

    /// List available credentials
    fn list_credentials(&self) -> Result<Vec<String>, CredentialError>;

    /// Provider name
    fn provider_name(&self) -> &str;
}

/// Credential manager that aggregates multiple providers
pub struct CredentialManager {
    /// Configuration
    config: CredentialConfig,

    /// Credential providers
    providers: Vec<Box<dyn CredentialProvider>>,

    /// Credential cache
    cache: Arc<RwLock<CredentialCache>>,

    /// Default provider index
    default_provider: usize,
}

/// Credential cache
struct CredentialCache {
    entries: HashMap<String, CachedCredential>,
    max_size: usize,
    default_ttl: Duration,
}

struct CachedCredential {
    credential: DatabaseCredential,
    cached_at: Instant,
    ttl: Duration,
}

impl CredentialCache {
    fn new(max_size: usize, default_ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
            default_ttl,
        }
    }

    fn get(&self, key: &str) -> Option<&DatabaseCredential> {
        self.entries.get(key).and_then(|cached| {
            if cached.cached_at.elapsed() < cached.ttl && !cached.credential.is_expired() {
                Some(&cached.credential)
            } else {
                None
            }
        })
    }

    fn insert(&mut self, key: String, credential: DatabaseCredential, ttl: Option<Duration>) {
        if self.entries.len() >= self.max_size {
            self.evict_expired();
        }

        let ttl = ttl.unwrap_or(self.default_ttl);
        self.entries.insert(key, CachedCredential {
            credential,
            cached_at: Instant::now(),
            ttl,
        });
    }

    fn evict_expired(&mut self) {
        self.entries.retain(|_, cached| {
            cached.cached_at.elapsed() < cached.ttl && !cached.credential.is_expired()
        });
    }

    fn invalidate(&mut self, key: &str) {
        self.entries.remove(key);
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

impl CredentialManager {
    /// Create a new credential manager
    pub fn new(config: CredentialConfig) -> Self {
        let cache_ttl = config.cache_ttl;

        Self {
            config,
            providers: Vec::new(),
            cache: Arc::new(RwLock::new(CredentialCache::new(1000, cache_ttl))),
            default_provider: 0,
        }
    }

    /// Create a builder
    pub fn builder() -> CredentialManagerBuilder {
        CredentialManagerBuilder::new()
    }

    /// Add a provider
    pub fn add_provider(&mut self, provider: Box<dyn CredentialProvider>) {
        self.providers.push(provider);
    }

    /// Get credential by key
    pub fn get_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        // Check cache first
        if let Some(cached) = self.cache.read().get(key) {
            return Ok(cached.clone());
        }

        // Try each provider
        for provider in &self.providers {
            match provider.get_credential(key) {
                Ok(credential) => {
                    // Calculate cache TTL based on credential expiration
                    let ttl = credential.time_until_expiration()
                        .map(|d| d.min(self.config.cache_ttl))
                        .or(Some(self.config.cache_ttl));

                    // Cache and return
                    self.cache.write().insert(key.to_string(), credential.clone(), ttl);
                    return Ok(credential);
                }
                Err(CredentialError::NotFound(_)) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(CredentialError::NotFound(key.to_string()))
    }

    /// Get credential with specific provider
    pub fn get_credential_from(
        &self,
        key: &str,
        provider_name: &str,
    ) -> Result<DatabaseCredential, CredentialError> {
        let provider = self.providers
            .iter()
            .find(|p| p.provider_name() == provider_name)
            .ok_or_else(|| CredentialError::ProviderUnavailable(provider_name.to_string()))?;

        provider.get_credential(key)
    }

    /// Refresh credential
    pub fn refresh_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        // Invalidate cache
        self.cache.write().invalidate(key);

        // Get fresh credential
        for provider in &self.providers {
            match provider.refresh_credential(key) {
                Ok(credential) => {
                    let ttl = credential.time_until_expiration()
                        .map(|d| d.min(self.config.cache_ttl))
                        .or(Some(self.config.cache_ttl));

                    self.cache.write().insert(key.to_string(), credential.clone(), ttl);
                    return Ok(credential);
                }
                Err(CredentialError::NotFound(_)) => continue,
                Err(e) => return Err(e),
            }
        }

        Err(CredentialError::NotFound(key.to_string()))
    }

    /// List all available credentials
    pub fn list_credentials(&self) -> Vec<(String, String)> {
        let mut result = Vec::new();

        for provider in &self.providers {
            if let Ok(keys) = provider.list_credentials() {
                for key in keys {
                    result.push((key, provider.provider_name().to_string()));
                }
            }
        }

        result
    }

    /// Invalidate cached credential
    pub fn invalidate(&self, key: &str) {
        self.cache.write().invalidate(key);
    }

    /// Clear credential cache
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        let cache = self.cache.read();
        CacheStats {
            entries: cache.entries.len(),
            max_size: cache.max_size,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub max_size: usize,
}

/// Credential manager builder
pub struct CredentialManagerBuilder {
    config: CredentialConfig,
    providers: Vec<Box<dyn CredentialProvider>>,
}

impl CredentialManagerBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: CredentialConfig::default(),
            providers: Vec::new(),
        }
    }

    /// Set cache TTL
    pub fn cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.cache_ttl = ttl;
        self
    }

    /// Add static provider
    pub fn with_static_credentials(mut self, credentials: HashMap<String, DatabaseCredential>) -> Self {
        self.providers.push(Box::new(StaticCredentialProvider::new(credentials)));
        self
    }

    /// Add environment provider
    pub fn with_environment(mut self, prefix: &str) -> Self {
        self.providers.push(Box::new(EnvironmentCredentialProvider::new(prefix)));
        self
    }

    /// Add Vault provider
    pub fn with_vault(mut self, address: &str, token: &str, mount: &str) -> Self {
        self.providers.push(Box::new(VaultCredentialProvider::new(address, token, mount)));
        self
    }

    /// Add AWS Secrets Manager provider
    pub fn with_aws_secrets_manager(mut self, region: &str) -> Self {
        self.providers.push(Box::new(AwsSecretsManagerProvider::new(region)));
        self
    }

    /// Add custom provider
    pub fn with_provider(mut self, provider: Box<dyn CredentialProvider>) -> Self {
        self.providers.push(provider);
        self
    }

    /// Build the manager
    pub fn build(self) -> CredentialManager {
        let mut manager = CredentialManager::new(self.config);
        for provider in self.providers {
            manager.add_provider(provider);
        }
        manager
    }
}

impl Default for CredentialManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Static credential provider
pub struct StaticCredentialProvider {
    credentials: HashMap<String, DatabaseCredential>,
}

impl StaticCredentialProvider {
    /// Create a new static provider
    pub fn new(credentials: HashMap<String, DatabaseCredential>) -> Self {
        Self { credentials }
    }

    /// Add a credential
    pub fn add(&mut self, key: String, credential: DatabaseCredential) {
        self.credentials.insert(key, credential);
    }
}

impl CredentialProvider for StaticCredentialProvider {
    fn get_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        self.credentials
            .get(key)
            .cloned()
            .ok_or_else(|| CredentialError::NotFound(key.to_string()))
    }

    fn refresh_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        self.get_credential(key)
    }

    fn list_credentials(&self) -> Result<Vec<String>, CredentialError> {
        Ok(self.credentials.keys().cloned().collect())
    }

    fn provider_name(&self) -> &str {
        "static"
    }
}

/// Environment variable credential provider
pub struct EnvironmentCredentialProvider {
    prefix: String,
}

impl EnvironmentCredentialProvider {
    /// Create a new environment provider
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }

    fn var_name(&self, key: &str, suffix: &str) -> String {
        format!("{}_{}{}", self.prefix, key.to_uppercase(), suffix)
    }
}

impl CredentialProvider for EnvironmentCredentialProvider {
    fn get_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        let username = std::env::var(self.var_name(key, "_USERNAME"))
            .or_else(|_| std::env::var(self.var_name(key, "_USER")))
            .map_err(|_| CredentialError::NotFound(key.to_string()))?;

        let password = std::env::var(self.var_name(key, "_PASSWORD"))
            .or_else(|_| std::env::var(self.var_name(key, "_PASS")))
            .map_err(|_| CredentialError::NotFound(format!("{}_PASSWORD", key)))?;

        let database = std::env::var(self.var_name(key, "_DATABASE")).ok();
        let host = std::env::var(self.var_name(key, "_HOST")).ok();
        let port = std::env::var(self.var_name(key, "_PORT"))
            .ok()
            .and_then(|p| p.parse().ok());

        Ok(DatabaseCredential {
            username,
            password,
            database,
            host,
            port,
            options: HashMap::new(),
            expires_at: None,
            source: CredentialSource::Environment,
        })
    }

    fn refresh_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        self.get_credential(key)
    }

    fn list_credentials(&self) -> Result<Vec<String>, CredentialError> {
        // Scan environment for matching credentials
        let mut keys = Vec::new();
        let prefix_upper = format!("{}_", self.prefix.to_uppercase());

        for (key, _) in std::env::vars() {
            if key.starts_with(&prefix_upper) && key.ends_with("_USERNAME") {
                let name = key
                    .strip_prefix(&prefix_upper)
                    .and_then(|s| s.strip_suffix("_USERNAME"))
                    .map(|s| s.to_lowercase());
                if let Some(name) = name {
                    keys.push(name);
                }
            }
        }

        Ok(keys)
    }

    fn provider_name(&self) -> &str {
        "environment"
    }
}

/// HashiCorp Vault credential provider
pub struct VaultCredentialProvider {
    address: String,
    token: String,
    mount: String,
}

impl VaultCredentialProvider {
    /// Create a new Vault provider
    pub fn new(address: &str, token: &str, mount: &str) -> Self {
        Self {
            address: address.to_string(),
            token: token.to_string(),
            mount: mount.to_string(),
        }
    }
}

impl CredentialProvider for VaultCredentialProvider {
    fn get_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        // In a real implementation, this would make an HTTP request to Vault
        // For demonstration, we return a placeholder
        //
        // Real implementation would:
        // 1. POST to {address}/v1/{mount}/creds/{key}
        // 2. Parse response for username/password
        // 3. Handle lease renewal

        let _ = (key, &self.address, &self.token, &self.mount);

        Ok(DatabaseCredential {
            username: format!("vault_user_{}", key),
            password: "vault_generated_password".to_string(),
            database: None,
            host: None,
            port: None,
            options: HashMap::new(),
            expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            source: CredentialSource::Vault,
        })
    }

    fn refresh_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        // In Vault, this would renew the lease or get a new credential
        self.get_credential(key)
    }

    fn list_credentials(&self) -> Result<Vec<String>, CredentialError> {
        // In a real implementation, this would list roles from Vault
        Ok(Vec::new())
    }

    fn provider_name(&self) -> &str {
        "vault"
    }
}

/// AWS Secrets Manager credential provider
pub struct AwsSecretsManagerProvider {
    region: String,
}

impl AwsSecretsManagerProvider {
    /// Create a new AWS Secrets Manager provider
    pub fn new(region: &str) -> Self {
        Self {
            region: region.to_string(),
        }
    }
}

impl CredentialProvider for AwsSecretsManagerProvider {
    fn get_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        // In a real implementation, this would use the AWS SDK
        // For demonstration, we return a placeholder
        //
        // Real implementation would:
        // 1. Use aws_sdk_secretsmanager to get secret value
        // 2. Parse JSON for username/password
        // 3. Handle rotation

        let _ = (key, &self.region);

        Ok(DatabaseCredential {
            username: format!("aws_user_{}", key),
            password: "aws_managed_password".to_string(),
            database: None,
            host: None,
            port: None,
            options: HashMap::new(),
            expires_at: None,
            source: CredentialSource::AwsSecretsManager,
        })
    }

    fn refresh_credential(&self, key: &str) -> Result<DatabaseCredential, CredentialError> {
        self.get_credential(key)
    }

    fn list_credentials(&self) -> Result<Vec<String>, CredentialError> {
        // In a real implementation, this would list secrets from AWS
        Ok(Vec::new())
    }

    fn provider_name(&self) -> &str {
        "aws_secrets_manager"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_credential() -> DatabaseCredential {
        DatabaseCredential {
            username: "testuser".to_string(),
            password: "testpass".to_string(),
            database: Some("testdb".to_string()),
            host: Some("localhost".to_string()),
            port: Some(5432),
            options: HashMap::new(),
            expires_at: None,
            source: CredentialSource::Static,
        }
    }

    #[test]
    fn test_static_provider() {
        let mut credentials = HashMap::new();
        credentials.insert("db1".to_string(), test_credential());

        let provider = StaticCredentialProvider::new(credentials);

        let cred = provider.get_credential("db1").unwrap();
        assert_eq!(cred.username, "testuser");

        assert!(provider.get_credential("db2").is_err());
    }

    #[test]
    fn test_connection_string() {
        let cred = test_credential();
        let conn_str = cred.connection_string();

        assert!(conn_str.contains("testuser"));
        assert!(conn_str.contains("testpass"));
        assert!(conn_str.contains("localhost"));
        assert!(conn_str.contains("5432"));
        assert!(conn_str.contains("testdb"));
    }

    #[test]
    fn test_credential_expiration() {
        let mut cred = test_credential();

        // Not expired
        cred.expires_at = Some(chrono::Utc::now() + chrono::Duration::hours(1));
        assert!(!cred.is_expired());
        assert!(cred.time_until_expiration().is_some());

        // Expired
        cred.expires_at = Some(chrono::Utc::now() - chrono::Duration::hours(1));
        assert!(cred.is_expired());
        assert!(cred.time_until_expiration().is_none());
    }

    #[test]
    fn test_credential_manager() {
        let mut credentials = HashMap::new();
        credentials.insert("primary".to_string(), test_credential());

        let manager = CredentialManager::builder()
            .cache_ttl(Duration::from_secs(60))
            .with_static_credentials(credentials)
            .build();

        let cred = manager.get_credential("primary").unwrap();
        assert_eq!(cred.username, "testuser");

        // Should be cached
        let cached = manager.get_credential("primary").unwrap();
        assert_eq!(cached.username, "testuser");

        // Check stats
        let stats = manager.cache_stats();
        assert_eq!(stats.entries, 1);
    }

    #[test]
    fn test_list_credentials() {
        let mut credentials = HashMap::new();
        credentials.insert("db1".to_string(), test_credential());
        credentials.insert("db2".to_string(), test_credential());

        let manager = CredentialManager::builder()
            .with_static_credentials(credentials)
            .build();

        let list = manager.list_credentials();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_cache_invalidation() {
        let mut credentials = HashMap::new();
        credentials.insert("db1".to_string(), test_credential());

        let manager = CredentialManager::builder()
            .with_static_credentials(credentials)
            .build();

        // Cache it
        let _ = manager.get_credential("db1").unwrap();
        assert_eq!(manager.cache_stats().entries, 1);

        // Invalidate
        manager.invalidate("db1");
        assert_eq!(manager.cache_stats().entries, 0);
    }

    #[test]
    fn test_credential_source() {
        assert_eq!(CredentialSource::Static, CredentialSource::Static);
        assert_ne!(CredentialSource::Vault, CredentialSource::Environment);
    }
}
