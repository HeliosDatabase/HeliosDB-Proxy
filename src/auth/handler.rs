//! Authentication Handler
//!
//! Main entry point for authentication operations. Coordinates between
//! different authentication providers (JWT, OAuth, LDAP, API keys).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use thiserror::Error;

use super::config::{
    ApiKeyConfig, AuthConfig, AuthMethod, Identity, JwtConfig, LdapConfig, OAuthConfig,
};
use super::jwt::{JwtError, JwtValidator};

/// Authentication errors
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Authentication required")]
    AuthenticationRequired,

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Token expired")]
    TokenExpired,

    #[error("Insufficient permissions: {0}")]
    InsufficientPermissions(String),

    #[error("Rate limited: retry after {0} seconds")]
    RateLimited(u64),

    #[error("Authentication provider unavailable: {0}")]
    ProviderUnavailable(String),

    #[error("Invalid authentication method: {0}")]
    InvalidMethod(String),

    #[error("JWT error: {0}")]
    Jwt(#[from] JwtError),

    #[error("OAuth error: {0}")]
    OAuth(String),

    #[error("LDAP error: {0}")]
    Ldap(String),

    #[error("API key error: {0}")]
    ApiKey(String),

    #[error("Session error: {0}")]
    Session(String),

    #[error("Configuration error: {0}")]
    Configuration(String),
}

/// Authentication request context
#[derive(Debug, Clone)]
pub struct AuthRequest {
    /// HTTP headers
    pub headers: HashMap<String, String>,

    /// Username (from connection)
    pub username: Option<String>,

    /// Password (from connection)
    pub password: Option<String>,

    /// Client IP address
    pub client_ip: Option<std::net::IpAddr>,

    /// Database being accessed
    pub database: Option<String>,

    /// Request timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl AuthRequest {
    /// Create a new authentication request
    pub fn new() -> Self {
        Self {
            headers: HashMap::new(),
            username: None,
            password: None,
            client_ip: None,
            database: None,
            timestamp: chrono::Utc::now(),
        }
    }

    /// Set username
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Set password
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set client IP
    pub fn with_client_ip(mut self, ip: std::net::IpAddr) -> Self {
        self.client_ip = Some(ip);
        self
    }

    /// Set database
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Add header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Get Authorization header
    pub fn authorization_header(&self) -> Option<&str> {
        self.headers
            .get("authorization")
            .or_else(|| self.headers.get("Authorization"))
            .map(|s| s.as_str())
    }

    /// Get Bearer token from Authorization header
    pub fn bearer_token(&self) -> Option<&str> {
        self.authorization_header()
            .and_then(|h| h.strip_prefix("Bearer "))
            .or_else(|| self.authorization_header()?.strip_prefix("bearer "))
    }

    /// Get API key from header
    pub fn api_key(&self, header_name: &str) -> Option<&str> {
        self.headers
            .get(header_name)
            .or_else(|| self.headers.get(&header_name.to_lowercase()))
            .map(|s| s.as_str())
    }
}

impl Default for AuthRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Authentication result
#[derive(Debug, Clone)]
pub struct AuthResult {
    /// Authenticated identity
    pub identity: Identity,

    /// Session token (if created)
    pub session_token: Option<String>,

    /// Token expiration time
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl AuthResult {
    /// Create a new authentication result
    pub fn new(identity: Identity) -> Self {
        Self {
            identity,
            session_token: None,
            expires_at: None,
            metadata: HashMap::new(),
        }
    }

    /// Set session token
    pub fn with_session_token(mut self, token: String) -> Self {
        self.session_token = Some(token);
        self
    }

    /// Set expiration
    pub fn with_expiration(mut self, expires_at: chrono::DateTime<chrono::Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Main authentication handler
pub struct AuthenticationHandler {
    /// Configuration
    config: AuthConfig,

    /// JWT validator
    jwt_validator: Option<JwtValidator>,

    /// OAuth client (placeholder - would use actual OAuth client)
    oauth_enabled: bool,

    /// LDAP client (placeholder - would use actual LDAP client)
    ldap_enabled: bool,

    /// API key store
    api_keys: Arc<RwLock<HashMap<String, ApiKeyEntry>>>,

    /// Rate limiter state
    rate_limiter: Arc<RwLock<RateLimiterState>>,

    /// Authentication cache
    auth_cache: Arc<RwLock<AuthCache>>,
}

/// API key entry
#[derive(Debug, Clone)]
struct ApiKeyEntry {
    /// Key ID
    #[allow(dead_code)]
    key_id: String,

    /// Hashed key value
    key_hash: String,

    /// Associated identity
    identity: Identity,

    /// Creation time
    #[allow(dead_code)]
    created_at: chrono::DateTime<chrono::Utc>,

    /// Expiration time
    expires_at: Option<chrono::DateTime<chrono::Utc>>,

    /// Whether key is active
    active: bool,

    /// Allowed scopes
    #[allow(dead_code)]
    scopes: Vec<String>,

    /// Rate limit override
    #[allow(dead_code)]
    rate_limit: Option<u32>,
}

/// Rate limiter state
struct RateLimiterState {
    /// Request counts by IP
    by_ip: HashMap<std::net::IpAddr, RateLimitBucket>,

    /// Request counts by user
    by_user: HashMap<String, RateLimitBucket>,

    /// Last cleanup time
    last_cleanup: Instant,
}

/// Rate limit bucket
struct RateLimitBucket {
    /// Request count
    count: u32,

    /// Window start time
    window_start: Instant,
}

impl RateLimitBucket {
    fn new() -> Self {
        Self {
            count: 0,
            window_start: Instant::now(),
        }
    }

    fn increment(&mut self, window: Duration) -> u32 {
        if self.window_start.elapsed() > window {
            self.count = 1;
            self.window_start = Instant::now();
        } else {
            self.count += 1;
        }
        self.count
    }
}

/// Authentication cache
struct AuthCache {
    /// Cached results by token/key
    entries: HashMap<String, CachedAuth>,

    /// Max cache size
    max_size: usize,

    /// TTL for cached entries
    ttl: Duration,
}

/// Cached authentication result
struct CachedAuth {
    /// Result
    result: AuthResult,

    /// Cached at
    cached_at: Instant,
}

impl AuthCache {
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
            ttl,
        }
    }

    fn get(&self, key: &str) -> Option<&AuthResult> {
        self.entries.get(key).and_then(|cached| {
            if cached.cached_at.elapsed() < self.ttl {
                Some(&cached.result)
            } else {
                None
            }
        })
    }

    fn insert(&mut self, key: String, result: AuthResult) {
        if self.entries.len() >= self.max_size {
            self.evict_expired();
        }
        self.entries.insert(
            key,
            CachedAuth {
                result,
                cached_at: Instant::now(),
            },
        );
    }

    fn evict_expired(&mut self) {
        self.entries
            .retain(|_, cached| cached.cached_at.elapsed() < self.ttl);
    }
}

impl AuthenticationHandler {
    /// Create a new authentication handler
    pub fn new(config: AuthConfig) -> Self {
        let jwt_validator = config
            .jwt
            .as_ref()
            .map(|jwt_config| JwtValidator::new(jwt_config.clone()));

        let oauth_enabled = config.oauth.is_some();
        let ldap_enabled = config.ldap.is_some();

        Self {
            config,
            jwt_validator,
            oauth_enabled,
            ldap_enabled,
            api_keys: Arc::new(RwLock::new(HashMap::new())),
            rate_limiter: Arc::new(RwLock::new(RateLimiterState {
                by_ip: HashMap::new(),
                by_user: HashMap::new(),
                last_cleanup: Instant::now(),
            })),
            auth_cache: Arc::new(RwLock::new(AuthCache::new(1000, Duration::from_secs(60)))),
        }
    }

    /// Create a builder for the handler
    pub fn builder() -> AuthenticationHandlerBuilder {
        AuthenticationHandlerBuilder::new()
    }

    /// Authenticate a request
    pub async fn authenticate(&self, request: &AuthRequest) -> Result<AuthResult, AuthError> {
        // Check if authentication is enabled
        if !self.config.enabled {
            // Return anonymous identity
            return Ok(AuthResult::new(Identity::anonymous()));
        }

        // Check rate limit
        self.check_rate_limit(request)?;

        // Try each authentication method in order
        let methods = &self.config.auth_methods;

        for method in methods {
            match self.try_authenticate(request, method).await {
                Ok(result) => return Ok(result),
                Err(AuthError::AuthenticationRequired) => continue,
                Err(e) => return Err(e),
            }
        }

        // No method succeeded
        Err(AuthError::AuthenticationRequired)
    }

    /// Try a specific authentication method
    async fn try_authenticate(
        &self,
        request: &AuthRequest,
        method: &AuthMethod,
    ) -> Result<AuthResult, AuthError> {
        match method {
            AuthMethod::Jwt => self.authenticate_jwt(request).await,
            AuthMethod::OAuth => self.authenticate_oauth(request).await,
            AuthMethod::Ldap => self.authenticate_ldap(request).await,
            AuthMethod::ApiKey => self.authenticate_api_key(request).await,
            AuthMethod::Basic => self.authenticate_basic(request).await,
            AuthMethod::Trust => self.authenticate_trust(request),
            AuthMethod::AgentToken | AuthMethod::Session | AuthMethod::Anonymous => {
                self.authenticate_trust(request)
            }
        }
    }

    /// Authenticate using JWT
    async fn authenticate_jwt(&self, request: &AuthRequest) -> Result<AuthResult, AuthError> {
        let validator = self
            .jwt_validator
            .as_ref()
            .ok_or(AuthError::Configuration("JWT not configured".to_string()))?;

        let token = request
            .bearer_token()
            .ok_or(AuthError::AuthenticationRequired)?;

        // Check cache first
        if let Some(cached) = self.auth_cache.read().get(token) {
            return Ok(cached.clone());
        }

        // Validate token
        let identity = validator.validate_to_identity(token)?;
        let result = AuthResult::new(identity);

        // Cache result
        self.auth_cache
            .write()
            .insert(token.to_string(), result.clone());

        Ok(result)
    }

    /// Authenticate using OAuth token introspection
    async fn authenticate_oauth(&self, request: &AuthRequest) -> Result<AuthResult, AuthError> {
        if !self.oauth_enabled {
            return Err(AuthError::Configuration("OAuth not configured".to_string()));
        }

        let token = request
            .bearer_token()
            .ok_or(AuthError::AuthenticationRequired)?;

        // Check cache first
        if let Some(cached) = self.auth_cache.read().get(token) {
            return Ok(cached.clone());
        }

        // In a real implementation, this would call the OAuth introspection endpoint
        // For demonstration, we create a placeholder identity
        let identity = Identity {
            user_id: "oauth_user".to_string(),
            name: Some("OAuth User".to_string()),
            email: None,
            roles: vec!["user".to_string()],
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "oauth".to_string(),
            authenticated_at: chrono::Utc::now(),
        };

        let result = AuthResult::new(identity);
        self.auth_cache
            .write()
            .insert(token.to_string(), result.clone());

        Ok(result)
    }

    /// Authenticate using LDAP
    async fn authenticate_ldap(&self, request: &AuthRequest) -> Result<AuthResult, AuthError> {
        if !self.ldap_enabled {
            return Err(AuthError::Configuration("LDAP not configured".to_string()));
        }

        let username = request
            .username
            .as_ref()
            .ok_or(AuthError::AuthenticationRequired)?;
        let password = request
            .password
            .as_ref()
            .ok_or(AuthError::AuthenticationRequired)?;

        // In a real implementation, this would bind to LDAP and verify credentials
        // For demonstration, we create a placeholder identity
        if password.is_empty() {
            return Err(AuthError::InvalidCredentials);
        }

        let identity = Identity {
            user_id: username.clone(),
            name: Some(username.clone()),
            email: None,
            roles: vec!["user".to_string()],
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "ldap".to_string(),
            authenticated_at: chrono::Utc::now(),
        };

        Ok(AuthResult::new(identity))
    }

    /// Authenticate using API key
    async fn authenticate_api_key(&self, request: &AuthRequest) -> Result<AuthResult, AuthError> {
        let api_key_config = self
            .config
            .api_keys
            .as_ref()
            .ok_or(AuthError::Configuration(
                "API keys not configured".to_string(),
            ))?;

        let header_name = &api_key_config.header_name;
        let key = request
            .api_key(header_name)
            .ok_or(AuthError::AuthenticationRequired)?;

        // Check cache first
        if let Some(cached) = self.auth_cache.read().get(key) {
            return Ok(cached.clone());
        }

        // Validate API key
        let api_keys = self.api_keys.read();
        let entry = api_keys
            .values()
            .find(|e| self.verify_api_key(key, &e.key_hash) && e.active)
            .ok_or(AuthError::InvalidCredentials)?;

        // Check expiration
        if let Some(expires_at) = entry.expires_at {
            if chrono::Utc::now() > expires_at {
                return Err(AuthError::TokenExpired);
            }
        }

        let result = AuthResult::new(entry.identity.clone());
        self.auth_cache
            .write()
            .insert(key.to_string(), result.clone());

        Ok(result)
    }

    /// Authenticate using HTTP Basic auth
    async fn authenticate_basic(&self, request: &AuthRequest) -> Result<AuthResult, AuthError> {
        let auth_header = request
            .authorization_header()
            .ok_or(AuthError::AuthenticationRequired)?;

        if !auth_header.starts_with("Basic ") {
            return Err(AuthError::AuthenticationRequired);
        }

        let encoded = &auth_header[6..];
        let decoded = base64_decode(encoded).map_err(|_| AuthError::InvalidCredentials)?;
        let credentials = String::from_utf8(decoded).map_err(|_| AuthError::InvalidCredentials)?;

        let parts: Vec<&str> = credentials.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(AuthError::InvalidCredentials);
        }

        let username = parts[0];
        let password = parts[1];

        // In a real implementation, this would verify against a user store
        // For demonstration, accept any non-empty password
        if password.is_empty() {
            return Err(AuthError::InvalidCredentials);
        }

        let identity = Identity {
            user_id: username.to_string(),
            name: Some(username.to_string()),
            email: None,
            roles: vec!["user".to_string()],
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "basic".to_string(),
            authenticated_at: chrono::Utc::now(),
        };

        Ok(AuthResult::new(identity))
    }

    /// Trust-based authentication (e.g., for internal services)
    fn authenticate_trust(&self, request: &AuthRequest) -> Result<AuthResult, AuthError> {
        // Trust authentication based on username or other context
        let username = request
            .username
            .as_ref()
            .unwrap_or(&"anonymous".to_string())
            .clone();

        let identity = Identity {
            user_id: username.clone(),
            name: Some(username),
            email: None,
            roles: vec!["trusted".to_string()],
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "trust".to_string(),
            authenticated_at: chrono::Utc::now(),
        };

        Ok(AuthResult::new(identity))
    }

    /// Check rate limit
    fn check_rate_limit(&self, request: &AuthRequest) -> Result<(), AuthError> {
        let config = &self.config.rate_limit;
        if !config.enabled {
            return Ok(());
        }

        let mut limiter = self.rate_limiter.write();

        // Cleanup old entries periodically
        if limiter.last_cleanup.elapsed() > Duration::from_secs(60) {
            let window = Duration::from_secs(config.window_seconds);
            limiter
                .by_ip
                .retain(|_, b| b.window_start.elapsed() < window);
            limiter
                .by_user
                .retain(|_, b| b.window_start.elapsed() < window);
            limiter.last_cleanup = Instant::now();
        }

        let window = Duration::from_secs(config.window_seconds);

        // Check IP rate limit
        if let Some(ip) = request.client_ip {
            let bucket = limiter.by_ip.entry(ip).or_insert_with(RateLimitBucket::new);
            let count = bucket.increment(window);
            if count > config.max_requests_per_ip {
                let retry_after = window
                    .as_secs()
                    .saturating_sub(bucket.window_start.elapsed().as_secs());
                return Err(AuthError::RateLimited(retry_after));
            }
        }

        // Check user rate limit
        if let Some(username) = &request.username {
            let bucket = limiter
                .by_user
                .entry(username.clone())
                .or_insert_with(RateLimitBucket::new);
            let count = bucket.increment(window);
            if count > config.max_requests_per_user {
                let retry_after = window
                    .as_secs()
                    .saturating_sub(bucket.window_start.elapsed().as_secs());
                return Err(AuthError::RateLimited(retry_after));
            }
        }

        Ok(())
    }

    /// Verify API key against hash
    fn verify_api_key(&self, key: &str, hash: &str) -> bool {
        // In production, use a proper constant-time comparison
        // and secure hashing (e.g., Argon2, bcrypt)
        let key_hash = self.hash_api_key(key);
        key_hash == hash
    }

    /// Hash an API key
    fn hash_api_key(&self, key: &str) -> String {
        // Placeholder: in production, use secure hashing
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    /// Register an API key
    pub fn register_api_key(
        &self,
        key_id: String,
        key_value: String,
        identity: Identity,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
        scopes: Vec<String>,
    ) {
        let entry = ApiKeyEntry {
            key_id: key_id.clone(),
            key_hash: self.hash_api_key(&key_value),
            identity,
            created_at: chrono::Utc::now(),
            expires_at,
            active: true,
            scopes,
            rate_limit: None,
        };

        self.api_keys.write().insert(key_id, entry);
    }

    /// Revoke an API key
    pub fn revoke_api_key(&self, key_id: &str) -> bool {
        if let Some(entry) = self.api_keys.write().get_mut(key_id) {
            entry.active = false;
            true
        } else {
            false
        }
    }

    /// Refresh JWKS if needed
    pub async fn refresh_jwks_if_needed(&self) -> Result<(), AuthError> {
        if let Some(validator) = &self.jwt_validator {
            if validator.needs_refresh() {
                validator.refresh_jwks().await?;
            }
        }
        Ok(())
    }

    /// Clear authentication cache
    pub fn clear_cache(&self) {
        self.auth_cache.write().entries.clear();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        let cache = self.auth_cache.read();
        CacheStats {
            entries: cache.entries.len(),
            max_size: cache.max_size,
            ttl_seconds: cache.ttl.as_secs(),
        }
    }

    /// Check if authentication is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of cached entries
    pub entries: usize,

    /// Maximum cache size
    pub max_size: usize,

    /// TTL in seconds
    pub ttl_seconds: u64,
}

/// Builder for AuthenticationHandler
pub struct AuthenticationHandlerBuilder {
    config: AuthConfig,
}

impl AuthenticationHandlerBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: AuthConfig::default(),
        }
    }

    /// Enable authentication
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Configure JWT authentication
    pub fn with_jwt(mut self, config: JwtConfig) -> Self {
        self.config.jwt = Some(config);
        self.config.auth_methods.push(AuthMethod::Jwt);
        self
    }

    /// Configure OAuth authentication
    pub fn with_oauth(mut self, config: OAuthConfig) -> Self {
        self.config.oauth = Some(config);
        self.config.auth_methods.push(AuthMethod::OAuth);
        self
    }

    /// Configure LDAP authentication
    pub fn with_ldap(mut self, config: LdapConfig) -> Self {
        self.config.ldap = Some(config);
        self.config.auth_methods.push(AuthMethod::Ldap);
        self
    }

    /// Configure API key authentication
    pub fn with_api_keys(mut self, config: ApiKeyConfig) -> Self {
        self.config.api_keys = Some(config);
        self.config.auth_methods.push(AuthMethod::ApiKey);
        self
    }

    /// Enable basic authentication
    pub fn with_basic_auth(mut self) -> Self {
        self.config.auth_methods.push(AuthMethod::Basic);
        self
    }

    /// Enable trust authentication
    pub fn with_trust_auth(mut self) -> Self {
        self.config.auth_methods.push(AuthMethod::Trust);
        self
    }

    /// Set default role
    pub fn default_role(mut self, role: impl Into<String>) -> Self {
        self.config.default_role = Some(role.into());
        self
    }

    /// Build the handler
    pub fn build(self) -> AuthenticationHandler {
        AuthenticationHandler::new(self.config)
    }
}

impl Default for AuthenticationHandlerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Base64 decode helper
fn base64_decode(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.decode(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AuthConfig {
        let mut config = AuthConfig::default();
        config.enabled = true;
        config.auth_methods = vec![AuthMethod::Trust];
        config
    }

    #[tokio::test]
    async fn test_authentication_disabled() {
        let mut config = AuthConfig::default();
        config.enabled = false;
        let handler = AuthenticationHandler::new(config);

        let request = AuthRequest::new();
        let result = handler.authenticate(&request).await.unwrap();

        assert_eq!(result.identity.auth_method, "anonymous");
    }

    #[tokio::test]
    async fn test_trust_authentication() {
        let handler = AuthenticationHandler::new(test_config());

        let request = AuthRequest::new().with_username("testuser");
        let result = handler.authenticate(&request).await.unwrap();

        assert_eq!(result.identity.user_id, "testuser");
        assert_eq!(result.identity.auth_method, "trust");
    }

    #[test]
    fn test_auth_request_builder() {
        let request = AuthRequest::new()
            .with_username("user")
            .with_password("pass")
            .with_database("mydb")
            .with_header("Authorization", "Bearer token123");

        assert_eq!(request.username, Some("user".to_string()));
        assert_eq!(request.password, Some("pass".to_string()));
        assert_eq!(request.database, Some("mydb".to_string()));
        assert_eq!(request.bearer_token(), Some("token123"));
    }

    #[test]
    fn test_bearer_token_extraction() {
        let request = AuthRequest::new().with_header("Authorization", "Bearer my-jwt-token");

        assert_eq!(request.bearer_token(), Some("my-jwt-token"));
    }

    #[test]
    fn test_api_key_extraction() {
        let request = AuthRequest::new().with_header("X-API-Key", "secret-key-123");

        assert_eq!(request.api_key("X-API-Key"), Some("secret-key-123"));
    }

    #[tokio::test]
    async fn test_api_key_registration_and_auth() {
        let mut config = AuthConfig::default();
        config.enabled = true;
        config.api_keys = Some(ApiKeyConfig {
            header_name: "X-API-Key".to_string(),
            query_param: None,
            prefix: None,
            hash_algorithm: "sha256".to_string(),
        });
        config.auth_methods = vec![AuthMethod::ApiKey];

        let handler = AuthenticationHandler::new(config);

        // Register an API key
        let identity = Identity {
            user_id: "api_user".to_string(),
            name: Some("API User".to_string()),
            email: None,
            roles: vec!["api".to_string()],
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "api_key".to_string(),
            authenticated_at: chrono::Utc::now(),
        };

        handler.register_api_key(
            "key1".to_string(),
            "secret123".to_string(),
            identity,
            None,
            vec!["read".to_string()],
        );

        // Authenticate with the key
        let request = AuthRequest::new().with_header("X-API-Key", "secret123");

        let result = handler.authenticate(&request).await.unwrap();
        assert_eq!(result.identity.user_id, "api_user");
    }

    #[test]
    fn test_cache_stats() {
        let handler = AuthenticationHandler::new(test_config());
        let stats = handler.cache_stats();

        assert_eq!(stats.entries, 0);
        assert_eq!(stats.max_size, 1000);
    }

    #[test]
    fn test_handler_builder() {
        let handler = AuthenticationHandler::builder()
            .enabled(true)
            .with_trust_auth()
            .default_role("user")
            .build();

        assert!(handler.is_enabled());
    }
}
