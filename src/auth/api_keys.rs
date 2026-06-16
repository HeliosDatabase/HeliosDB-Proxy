//! API Key Management
//!
//! Provides API key generation, validation, and lifecycle management.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use thiserror::Error;

use super::config::{Identity, ApiKeyConfig};

/// API key errors
#[derive(Debug, Error)]
pub enum ApiKeyError {
    #[error("API key not found")]
    NotFound,

    #[error("API key expired")]
    Expired,

    #[error("API key revoked")]
    Revoked,

    #[error("API key rate limited")]
    RateLimited,

    #[error("Invalid API key format")]
    InvalidFormat,

    #[error("Insufficient scope: {0}")]
    InsufficientScope(String),

    #[error("Key generation failed: {0}")]
    GenerationFailed(String),

    #[error("Storage error: {0}")]
    StorageError(String),
}

/// API key entry
#[derive(Debug, Clone)]
pub struct ApiKey {
    /// Unique key ID
    pub id: String,

    /// Key prefix (visible part, e.g., "hdb_live_")
    pub prefix: String,

    /// Hashed key value
    pub key_hash: String,

    /// Associated user identity
    pub identity: Identity,

    /// Key name/description
    pub name: String,

    /// Creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// Expiration timestamp
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Last used timestamp
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Whether key is active
    pub active: bool,

    /// Allowed scopes
    pub scopes: Vec<String>,

    /// Rate limit (requests per minute)
    pub rate_limit: Option<u32>,

    /// Allowed IP addresses (empty = all allowed)
    pub allowed_ips: Vec<std::net::IpAddr>,

    /// Metadata
    pub metadata: HashMap<String, String>,
}

impl ApiKey {
    /// Check if the key is valid
    pub fn is_valid(&self) -> bool {
        if !self.active {
            return false;
        }

        if let Some(expires_at) = self.expires_at {
            if chrono::Utc::now() > expires_at {
                return false;
            }
        }

        true
    }

    /// Check if key has a specific scope
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|s| s == scope || s == "*")
    }

    /// Check if IP is allowed
    pub fn is_ip_allowed(&self, ip: &std::net::IpAddr) -> bool {
        if self.allowed_ips.is_empty() {
            return true;
        }
        self.allowed_ips.contains(ip)
    }
}

/// API key manager
pub struct ApiKeyManager {
    /// Configuration
    #[allow(dead_code)]
    config: ApiKeyConfig,

    /// Key store by ID
    keys_by_id: Arc<RwLock<HashMap<String, ApiKey>>>,

    /// Key lookup by hash
    keys_by_hash: Arc<RwLock<HashMap<String, String>>>,

    /// Rate limit state
    rate_limits: Arc<RwLock<HashMap<String, RateLimitState>>>,

    /// Key prefix
    key_prefix: String,
}

/// Rate limit state for a key
struct RateLimitState {
    /// Request count in current window
    count: u32,

    /// Window start time
    window_start: Instant,
}

impl RateLimitState {
    fn new() -> Self {
        Self {
            count: 0,
            window_start: Instant::now(),
        }
    }

    fn check_and_increment(&mut self, limit: u32) -> bool {
        let window = Duration::from_secs(60);

        if self.window_start.elapsed() > window {
            self.count = 1;
            self.window_start = Instant::now();
            true
        } else if self.count < limit {
            self.count += 1;
            true
        } else {
            false
        }
    }
}

impl ApiKeyManager {
    /// Create a new API key manager
    pub fn new(config: ApiKeyConfig) -> Self {
        let key_prefix = config.prefix.clone().unwrap_or_else(|| "hdb_".to_string());

        Self {
            config,
            keys_by_id: Arc::new(RwLock::new(HashMap::new())),
            keys_by_hash: Arc::new(RwLock::new(HashMap::new())),
            rate_limits: Arc::new(RwLock::new(HashMap::new())),
            key_prefix,
        }
    }

    /// Generate a new API key
    pub fn generate_key(
        &self,
        identity: Identity,
        name: String,
        scopes: Vec<String>,
        expires_in: Option<Duration>,
        rate_limit: Option<u32>,
    ) -> Result<(ApiKey, String), ApiKeyError> {
        // Generate random key value
        let key_value = self.generate_random_key();
        let full_key = format!("{}{}", self.key_prefix, key_value);

        // Hash the key
        let key_hash = self.hash_key(&full_key);

        // Generate key ID
        let key_id = self.generate_key_id();

        let expires_at = expires_in.map(|d| chrono::Utc::now() + chrono::Duration::from_std(d).unwrap());

        let api_key = ApiKey {
            id: key_id.clone(),
            prefix: self.key_prefix.clone(),
            key_hash: key_hash.clone(),
            identity,
            name,
            created_at: chrono::Utc::now(),
            expires_at,
            last_used_at: None,
            active: true,
            scopes,
            rate_limit,
            allowed_ips: Vec::new(),
            metadata: HashMap::new(),
        };

        // Store the key
        self.keys_by_id.write().insert(key_id.clone(), api_key.clone());
        self.keys_by_hash.write().insert(key_hash, key_id);

        Ok((api_key, full_key))
    }

    /// Validate an API key
    pub fn validate(&self, key: &str) -> Result<ApiKey, ApiKeyError> {
        // Check format
        if !key.starts_with(&self.key_prefix) {
            return Err(ApiKeyError::InvalidFormat);
        }

        let key_hash = self.hash_key(key);

        // Look up by hash
        let key_id = self.keys_by_hash.read()
            .get(&key_hash)
            .cloned()
            .ok_or(ApiKeyError::NotFound)?;

        let mut keys = self.keys_by_id.write();
        let api_key = keys.get_mut(&key_id)
            .ok_or(ApiKeyError::NotFound)?;

        // Check if active
        if !api_key.active {
            return Err(ApiKeyError::Revoked);
        }

        // Check expiration
        if let Some(expires_at) = api_key.expires_at {
            if chrono::Utc::now() > expires_at {
                return Err(ApiKeyError::Expired);
            }
        }

        // Check rate limit
        if let Some(limit) = api_key.rate_limit {
            if !self.check_rate_limit(&key_id, limit) {
                return Err(ApiKeyError::RateLimited);
            }
        }

        // Update last used
        api_key.last_used_at = Some(chrono::Utc::now());

        Ok(api_key.clone())
    }

    /// Validate key and convert to identity
    pub fn validate_to_identity(&self, key: &str) -> Result<Identity, ApiKeyError> {
        let api_key = self.validate(key)?;
        Ok(api_key.identity)
    }

    /// Validate key with required scopes
    pub fn validate_with_scopes(
        &self,
        key: &str,
        required_scopes: &[&str],
    ) -> Result<ApiKey, ApiKeyError> {
        let api_key = self.validate(key)?;

        for scope in required_scopes {
            if !api_key.has_scope(scope) {
                return Err(ApiKeyError::InsufficientScope((*scope).to_string()));
            }
        }

        Ok(api_key)
    }

    /// Validate key with IP check
    pub fn validate_with_ip(
        &self,
        key: &str,
        client_ip: &std::net::IpAddr,
    ) -> Result<ApiKey, ApiKeyError> {
        let api_key = self.validate(key)?;

        if !api_key.is_ip_allowed(client_ip) {
            return Err(ApiKeyError::InsufficientScope("IP not allowed".to_string()));
        }

        Ok(api_key)
    }

    /// Revoke an API key
    pub fn revoke(&self, key_id: &str) -> Result<(), ApiKeyError> {
        let mut keys = self.keys_by_id.write();
        let api_key = keys.get_mut(key_id)
            .ok_or(ApiKeyError::NotFound)?;

        api_key.active = false;
        Ok(())
    }

    /// Delete an API key
    pub fn delete(&self, key_id: &str) -> Result<(), ApiKeyError> {
        let api_key = self.keys_by_id.write().remove(key_id)
            .ok_or(ApiKeyError::NotFound)?;

        self.keys_by_hash.write().remove(&api_key.key_hash);
        self.rate_limits.write().remove(key_id);

        Ok(())
    }

    /// Get an API key by ID
    pub fn get(&self, key_id: &str) -> Option<ApiKey> {
        self.keys_by_id.read().get(key_id).cloned()
    }

    /// List all API keys for a user
    pub fn list_by_user(&self, user_id: &str) -> Vec<ApiKey> {
        self.keys_by_id.read()
            .values()
            .filter(|k| k.identity.user_id == user_id)
            .cloned()
            .collect()
    }

    /// List all active API keys
    pub fn list_active(&self) -> Vec<ApiKey> {
        self.keys_by_id.read()
            .values()
            .filter(|k| k.is_valid())
            .cloned()
            .collect()
    }

    /// Update API key metadata
    pub fn update_metadata(
        &self,
        key_id: &str,
        metadata: HashMap<String, String>,
    ) -> Result<(), ApiKeyError> {
        let mut keys = self.keys_by_id.write();
        let api_key = keys.get_mut(key_id)
            .ok_or(ApiKeyError::NotFound)?;

        api_key.metadata.extend(metadata);
        Ok(())
    }

    /// Update API key scopes
    pub fn update_scopes(&self, key_id: &str, scopes: Vec<String>) -> Result<(), ApiKeyError> {
        let mut keys = self.keys_by_id.write();
        let api_key = keys.get_mut(key_id)
            .ok_or(ApiKeyError::NotFound)?;

        api_key.scopes = scopes;
        Ok(())
    }

    /// Update API key allowed IPs
    pub fn update_allowed_ips(
        &self,
        key_id: &str,
        ips: Vec<std::net::IpAddr>,
    ) -> Result<(), ApiKeyError> {
        let mut keys = self.keys_by_id.write();
        let api_key = keys.get_mut(key_id)
            .ok_or(ApiKeyError::NotFound)?;

        api_key.allowed_ips = ips;
        Ok(())
    }

    /// Rotate an API key (generate new key value, same ID)
    pub fn rotate(&self, key_id: &str) -> Result<String, ApiKeyError> {
        let old_hash = {
            let keys = self.keys_by_id.read();
            let api_key = keys.get(key_id).ok_or(ApiKeyError::NotFound)?;
            api_key.key_hash.clone()
        };

        // Generate new key value
        let key_value = self.generate_random_key();
        let full_key = format!("{}{}", self.key_prefix, key_value);
        let new_hash = self.hash_key(&full_key);

        // Update key
        {
            let mut keys = self.keys_by_id.write();
            let api_key = keys.get_mut(key_id).ok_or(ApiKeyError::NotFound)?;
            api_key.key_hash = new_hash.clone();
        }

        // Update hash lookup
        {
            let mut hashes = self.keys_by_hash.write();
            hashes.remove(&old_hash);
            hashes.insert(new_hash, key_id.to_string());
        }

        Ok(full_key)
    }

    /// Get key statistics
    pub fn stats(&self) -> ApiKeyStats {
        let keys = self.keys_by_id.read();
        let total = keys.len();
        let active = keys.values().filter(|k| k.active).count();
        let expired = keys.values().filter(|k| {
            k.expires_at.map(|e| chrono::Utc::now() > e).unwrap_or(false)
        }).count();

        ApiKeyStats {
            total,
            active,
            expired,
            revoked: total - active - expired,
        }
    }

    /// Cleanup expired keys
    pub fn cleanup_expired(&self) {
        let expired_ids: Vec<String> = self.keys_by_id.read()
            .iter()
            .filter(|(_, k)| {
                k.expires_at.map(|e| chrono::Utc::now() > e).unwrap_or(false)
            })
            .map(|(id, _)| id.clone())
            .collect();

        for id in expired_ids {
            let _ = self.delete(&id);
        }
    }

    /// Check rate limit for a key
    fn check_rate_limit(&self, key_id: &str, limit: u32) -> bool {
        let mut limits = self.rate_limits.write();
        let state = limits.entry(key_id.to_string())
            .or_insert_with(RateLimitState::new);
        state.check_and_increment(limit)
    }

    /// Generate a random key value
    fn generate_random_key(&self) -> String {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos());
        hasher.write_usize(std::process::id() as usize);

        let hash1 = hasher.finish();

        hasher.write_u64(hash1);
        let hash2 = hasher.finish();

        format!("{:016x}{:016x}", hash1, hash2)
    }

    /// Generate a key ID
    fn generate_key_id(&self) -> String {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos());

        format!("key_{:016x}", hasher.finish())
    }

    /// Hash a key value
    fn hash_key(&self, key: &str) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);

        // In production, use a cryptographic hash like SHA-256
        format!("{:016x}", hasher.finish())
    }

    /// Get the key prefix
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }
}

/// API key statistics
#[derive(Debug, Clone)]
pub struct ApiKeyStats {
    /// Total number of keys
    pub total: usize,

    /// Number of active keys
    pub active: usize,

    /// Number of expired keys
    pub expired: usize,

    /// Number of revoked keys
    pub revoked: usize,
}

/// API key builder
pub struct ApiKeyBuilder {
    identity: Identity,
    name: String,
    scopes: Vec<String>,
    expires_in: Option<Duration>,
    rate_limit: Option<u32>,
    allowed_ips: Vec<std::net::IpAddr>,
    metadata: HashMap<String, String>,
}

impl ApiKeyBuilder {
    /// Create a new builder
    pub fn new(identity: Identity, name: impl Into<String>) -> Self {
        Self {
            identity,
            name: name.into(),
            scopes: Vec::new(),
            expires_in: None,
            rate_limit: None,
            allowed_ips: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Add a scope
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scopes.push(scope.into());
        self
    }

    /// Add multiple scopes
    pub fn scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes.extend(scopes);
        self
    }

    /// Set expiration
    pub fn expires_in(mut self, duration: Duration) -> Self {
        self.expires_in = Some(duration);
        self
    }

    /// Set rate limit
    pub fn rate_limit(mut self, requests_per_minute: u32) -> Self {
        self.rate_limit = Some(requests_per_minute);
        self
    }

    /// Add allowed IP
    pub fn allow_ip(mut self, ip: std::net::IpAddr) -> Self {
        self.allowed_ips.push(ip);
        self
    }

    /// Add metadata
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Build the API key using the manager
    pub fn build(self, manager: &ApiKeyManager) -> Result<(ApiKey, String), ApiKeyError> {
        let (mut api_key, key_value) = manager.generate_key(
            self.identity,
            self.name,
            self.scopes,
            self.expires_in,
            self.rate_limit,
        )?;

        api_key.allowed_ips = self.allowed_ips;
        api_key.metadata = self.metadata;

        // Update the stored key
        manager.keys_by_id.write().insert(api_key.id.clone(), api_key.clone());

        Ok((api_key, key_value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ApiKeyConfig {
        ApiKeyConfig {
            header_name: "X-API-Key".to_string(),
            query_param: Some("api_key".to_string()),
            prefix: Some("hdb_test_".to_string()),
            hash_algorithm: "sha256".to_string(),
        }
    }

    fn test_identity() -> Identity {
        Identity {
            user_id: "user123".to_string(),
            name: Some("Test User".to_string()),
            email: Some("test@example.com".to_string()),
            roles: vec!["user".to_string()],
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "api_key".to_string(),
            authenticated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_generate_key() {
        let manager = ApiKeyManager::new(test_config());
        let (api_key, key_value) = manager.generate_key(
            test_identity(),
            "Test Key".to_string(),
            vec!["read".to_string()],
            None,
            None,
        ).unwrap();

        assert!(key_value.starts_with("hdb_test_"));
        assert!(api_key.active);
        assert!(api_key.has_scope("read"));
    }

    #[test]
    fn test_validate_key() {
        let manager = ApiKeyManager::new(test_config());
        let (_, key_value) = manager.generate_key(
            test_identity(),
            "Test Key".to_string(),
            vec!["read".to_string()],
            None,
            None,
        ).unwrap();

        let validated = manager.validate(&key_value).unwrap();
        assert_eq!(validated.identity.user_id, "user123");
    }

    #[test]
    fn test_validate_invalid_key() {
        let manager = ApiKeyManager::new(test_config());
        let result = manager.validate("hdb_test_invalid");
        assert!(matches!(result, Err(ApiKeyError::NotFound)));
    }

    #[test]
    fn test_revoke_key() {
        let manager = ApiKeyManager::new(test_config());
        let (api_key, key_value) = manager.generate_key(
            test_identity(),
            "Test Key".to_string(),
            vec!["read".to_string()],
            None,
            None,
        ).unwrap();

        manager.revoke(&api_key.id).unwrap();

        let result = manager.validate(&key_value);
        assert!(matches!(result, Err(ApiKeyError::Revoked)));
    }

    #[test]
    fn test_key_expiration() {
        let manager = ApiKeyManager::new(test_config());
        let (_, key_value) = manager.generate_key(
            test_identity(),
            "Test Key".to_string(),
            vec!["read".to_string()],
            Some(Duration::from_secs(0)), // Expired immediately
            None,
        ).unwrap();

        // Give it a moment to expire
        std::thread::sleep(Duration::from_millis(10));

        let result = manager.validate(&key_value);
        assert!(matches!(result, Err(ApiKeyError::Expired)));
    }

    #[test]
    fn test_scope_validation() {
        let manager = ApiKeyManager::new(test_config());
        let (_, key_value) = manager.generate_key(
            test_identity(),
            "Test Key".to_string(),
            vec!["read".to_string()],
            None,
            None,
        ).unwrap();

        // Should succeed for read
        assert!(manager.validate_with_scopes(&key_value, &["read"]).is_ok());

        // Should fail for write
        assert!(matches!(
            manager.validate_with_scopes(&key_value, &["write"]),
            Err(ApiKeyError::InsufficientScope(_))
        ));
    }

    #[test]
    fn test_list_by_user() {
        let manager = ApiKeyManager::new(test_config());

        let identity1 = test_identity();
        let mut identity2 = test_identity();
        identity2.user_id = "user456".to_string();

        let _ = manager.generate_key(identity1, "Key 1".to_string(), vec![], None, None).unwrap();
        let _ = manager.generate_key(identity2, "Key 2".to_string(), vec![], None, None).unwrap();

        let user_keys = manager.list_by_user("user123");
        assert_eq!(user_keys.len(), 1);
    }

    #[test]
    fn test_key_stats() {
        let manager = ApiKeyManager::new(test_config());

        let (key1, _) = manager.generate_key(
            test_identity(),
            "Key 1".to_string(),
            vec![],
            None,
            None,
        ).unwrap();

        let _ = manager.generate_key(
            test_identity(),
            "Key 2".to_string(),
            vec![],
            None,
            None,
        ).unwrap();

        manager.revoke(&key1.id).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.total, 2);
        assert_eq!(stats.active, 1);
    }

    #[test]
    fn test_rotate_key() {
        let manager = ApiKeyManager::new(test_config());
        let (api_key, old_key) = manager.generate_key(
            test_identity(),
            "Test Key".to_string(),
            vec!["read".to_string()],
            None,
            None,
        ).unwrap();

        // Rotate
        let new_key = manager.rotate(&api_key.id).unwrap();

        // Old key should fail
        assert!(manager.validate(&old_key).is_err());

        // New key should work
        assert!(manager.validate(&new_key).is_ok());
    }

    #[test]
    fn test_api_key_builder() {
        let manager = ApiKeyManager::new(test_config());

        let (api_key, key_value) = ApiKeyBuilder::new(test_identity(), "Builder Key")
            .scope("read")
            .scope("write")
            .rate_limit(100)
            .expires_in(Duration::from_secs(3600))
            .metadata("env", "test")
            .build(&manager)
            .unwrap();

        assert!(key_value.starts_with("hdb_test_"));
        assert!(api_key.has_scope("read"));
        assert!(api_key.has_scope("write"));
        assert_eq!(api_key.rate_limit, Some(100));
        assert_eq!(api_key.metadata.get("env"), Some(&"test".to_string()));
    }
}
