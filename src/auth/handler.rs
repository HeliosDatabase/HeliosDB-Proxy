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

    /// Whether OAuth is configured (RFC 7662 introspection is performed
    /// per-request against `config.oauth.introspection_url`).
    oauth_enabled: bool,

    /// Whether LDAP is configured. With the `ldap-auth` feature,
    /// `authenticate_ldap` performs a real search + bind against the directory;
    /// without it, LDAP denies by default.
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

        let cfg = self
            .config
            .oauth
            .as_ref()
            .ok_or_else(|| AuthError::Configuration("OAuth not configured".to_string()))?;

        // RFC 7662 token introspection: POST the token to the introspection
        // endpoint with the client's credentials and trust only an explicit
        // `"active": true`. A bearer token is never accepted without this
        // round-trip.
        let body = self.oauth_introspect(cfg, token).await?;

        // `active` is REQUIRED by RFC 7662; anything but true is a denial.
        if body.get("active").and_then(|v| v.as_bool()) != Some(true) {
            return Err(AuthError::InvalidCredentials);
        }

        // Optional issuer pin.
        if !cfg.issuer.is_empty() {
            if let Some(iss) = body.get("iss").and_then(|v| v.as_str()) {
                if iss != cfg.issuer {
                    return Err(AuthError::InvalidCredentials);
                }
            }
        }

        // Optional audience check (`aud` may be a string or an array).
        if let Some(expected) = &cfg.audience {
            let ok = match body.get("aud") {
                Some(serde_json::Value::String(s)) => s == expected,
                Some(serde_json::Value::Array(a)) => {
                    a.iter().any(|v| v.as_str() == Some(expected.as_str()))
                }
                _ => false,
            };
            if !ok {
                return Err(AuthError::InvalidCredentials);
            }
        }

        // Scopes are a space-delimited string (RFC 7662 §2.2). Enforce any
        // required scopes before minting an identity.
        let scopes: Vec<String> = body
            .get("scope")
            .and_then(|v| v.as_str())
            .map(|s| s.split_whitespace().map(String::from).collect())
            .unwrap_or_default();
        for required in &cfg.required_scopes {
            if !scopes.contains(required) {
                return Err(AuthError::InsufficientPermissions(format!(
                    "missing required scope: {}",
                    required
                )));
            }
        }

        // Subject identifies the principal; fall back to `username`.
        let subject = body
            .get("sub")
            .and_then(|v| v.as_str())
            .or_else(|| body.get("username").and_then(|v| v.as_str()))
            .ok_or_else(|| {
                AuthError::OAuth("introspection response missing sub/username".to_string())
            })?;

        let mut identity = Identity::new(subject, "oauth");
        identity.name = body
            .get("username")
            .and_then(|v| v.as_str())
            .map(String::from);
        identity.roles = scopes;

        let mut result = AuthResult::new(identity);
        if let Some(exp) = body
            .get("exp")
            .and_then(|v| v.as_i64())
            .and_then(|e| chrono::DateTime::from_timestamp(e, 0))
        {
            result = result.with_expiration(exp);
        }

        // Cache the validated result (bounded by the cache's own TTL).
        self.auth_cache
            .write()
            .insert(token.to_string(), result.clone());

        Ok(result)
    }

    /// Perform an RFC 7662 introspection POST and return the parsed JSON
    /// response body. Client authentication is HTTP Basic with the
    /// configured client id/secret.
    async fn oauth_introspect(
        &self,
        cfg: &OAuthConfig,
        token: &str,
    ) -> Result<serde_json::Value, AuthError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| AuthError::ProviderUnavailable(format!("http client: {}", e)))?;

        let resp = client
            .post(&cfg.introspection_url)
            .basic_auth(&cfg.client_id, Some(&cfg.client_secret))
            .form(&[("token", token), ("token_type_hint", "access_token")])
            .send()
            .await
            .map_err(|e| AuthError::ProviderUnavailable(format!("introspection request: {}", e)))?;

        if !resp.status().is_success() {
            return Err(AuthError::ProviderUnavailable(format!(
                "introspection endpoint returned HTTP {}",
                resp.status()
            )));
        }

        resp.json::<serde_json::Value>()
            .await
            .map_err(|e| AuthError::OAuth(format!("introspection response body: {}", e)))
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

        #[cfg(feature = "ldap-auth")]
        {
            let cfg = self
                .config
                .ldap
                .as_ref()
                .ok_or_else(|| AuthError::Configuration("LDAP not configured".to_string()))?;
            let groups = self.ldap_search_and_bind(cfg, username, password).await?;
            let mut identity = Identity::new(username.clone(), "ldap");
            identity.groups = groups;
            Ok(AuthResult::new(identity))
        }

        // Without the `ldap-auth` feature there is no LDAP client compiled in;
        // deny rather than accept any password (never a fabricated success).
        #[cfg(not(feature = "ldap-auth"))]
        {
            let _ = (username, password);
            Err(AuthError::Configuration(
                "LDAP bind not implemented (build with the `ldap-auth` feature)".to_string(),
            ))
        }
    }

    /// Authenticate a user against an LDAP directory using the standard
    /// search-then-bind flow:
    ///
    /// 1. Bind as the configured service account (anonymous if `bind_dn` is
    ///    empty) and search `user_search_base` with `user_filter` (with `{0}`
    ///    replaced by the RFC 4515-escaped username) to resolve the user's DN.
    /// 2. Open a fresh connection and bind as that DN with the supplied
    ///    password — a successful bind is proof the credentials are valid.
    ///
    /// Returns the user's group memberships (values of `group_attribute`).
    /// An unknown user, an empty password (which would be an unauthenticated
    /// bind per RFC 4513), or a failed user bind all map to
    /// [`AuthError::InvalidCredentials`].
    #[cfg(feature = "ldap-auth")]
    async fn ldap_search_and_bind(
        &self,
        cfg: &LdapConfig,
        username: &str,
        password: &str,
    ) -> Result<Vec<String>, AuthError> {
        use ldap3::{LdapConnAsync, LdapConnSettings, Scope, SearchEntry};

        // ldap3's rustls 0.23 backend builds its TLS connector eagerly (even
        // for plain ldap://) and needs a process-default crypto provider, or
        // the connection driver dies with "channel closed". Install the ring
        // provider once; ignore the error if another component already set one.
        static LDAP_TLS_PROVIDER: std::sync::Once = std::sync::Once::new();
        LDAP_TLS_PROVIDER.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });

        let settings = LdapConnSettings::new()
            .set_starttls(cfg.starttls)
            .set_conn_timeout(cfg.timeout);

        // --- Phase 1: service bind + search for the user DN -----------------
        let (conn, mut ldap) = LdapConnAsync::with_settings(settings.clone(), &cfg.server_url)
            .await
            .map_err(|e| AuthError::Ldap(format!("connect {}: {}", cfg.server_url, e)))?;
        ldap3::drive!(conn);

        if !cfg.bind_dn.is_empty() {
            ldap.simple_bind(&cfg.bind_dn, &cfg.bind_password)
                .await
                .map_err(|e| AuthError::Ldap(format!("service bind: {}", e)))?
                .success()
                .map_err(|e| AuthError::Ldap(format!("service bind rejected: {}", e)))?;
        }

        let filter = cfg
            .user_filter
            .replace("{0}", &ldap3::ldap_escape(username));
        let (entries, _res) = ldap
            .search(
                &cfg.user_search_base,
                Scope::Subtree,
                &filter,
                vec![cfg.group_attribute.as_str()],
            )
            .await
            .map_err(|e| AuthError::Ldap(format!("search: {}", e)))?
            .success()
            .map_err(|e| AuthError::Ldap(format!("search rejected: {}", e)))?;

        let entry = match entries.into_iter().next() {
            Some(e) => SearchEntry::construct(e),
            // No such user — deny without leaking which half failed.
            None => {
                let _ = ldap.unbind().await;
                return Err(AuthError::InvalidCredentials);
            }
        };
        let user_dn = entry.dn.clone();
        let groups = entry
            .attrs
            .get(&cfg.group_attribute)
            .cloned()
            .unwrap_or_default();
        let _ = ldap.unbind().await;

        if user_dn.is_empty() {
            return Err(AuthError::InvalidCredentials);
        }
        // Reject empty passwords up front: an empty password is an
        // "unauthenticated bind" that some servers report as success.
        if password.is_empty() {
            return Err(AuthError::InvalidCredentials);
        }

        // --- Phase 2: bind AS the user to verify the password ---------------
        let (conn2, mut ldap2) = LdapConnAsync::with_settings(settings, &cfg.server_url)
            .await
            .map_err(|e| AuthError::Ldap(format!("connect (user bind): {}", e)))?;
        ldap3::drive!(conn2);
        let bind = ldap2
            .simple_bind(&user_dn, password)
            .await
            .map_err(|e| AuthError::Ldap(format!("user bind: {}", e)))?;
        let _ = ldap2.unbind().await;
        bind.success().map_err(|_| AuthError::InvalidCredentials)?;

        Ok(groups)
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

        // No user store is wired, so HTTP Basic cannot verify a password.
        // Deny rather than accept any non-empty password.
        let _ = (username, password);
        Err(AuthError::InvalidCredentials)
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

    /// Verify an API key against its stored hash using a constant-time
    /// comparison (so verification time doesn't leak how much of the hash
    /// matched).
    fn verify_api_key(&self, key: &str, hash: &str) -> bool {
        let computed = self.hash_api_key(key);
        let a = computed.as_bytes();
        let b = hash.as_bytes();
        if a.len() != b.len() {
            return false;
        }
        let mut diff = 0u8;
        for (x, y) in a.iter().zip(b.iter()) {
            diff |= x ^ y;
        }
        diff == 0
    }

    /// Hash an API key with SHA-256 (hex). Keys are high-entropy secrets, so a
    /// fast cryptographic digest is appropriate (unlike user passwords, which
    /// would warrant a slow KDF).
    fn hash_api_key(&self, key: &str) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(key.as_bytes());
        let mut out = String::with_capacity(64);
        for b in digest {
            use std::fmt::Write;
            let _ = write!(out, "{:02x}", b);
        }
        out
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
        AuthConfig {
            enabled: true,
            auth_methods: vec![AuthMethod::Trust],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_authentication_disabled() {
        let config = AuthConfig {
            enabled: false,
            ..Default::default()
        };
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
        let config = AuthConfig {
            enabled: true,
            api_keys: Some(ApiKeyConfig {
                header_name: "X-API-Key".to_string(),
                query_param: None,
                prefix: None,
                hash_algorithm: "sha256".to_string(),
            }),
            auth_methods: vec![AuthMethod::ApiKey],
            ..Default::default()
        };

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

    #[tokio::test]
    async fn basic_auth_denies_without_user_store() {
        // Hardening: HTTP Basic used to accept ANY non-empty password. With no
        // user store wired it must now deny.
        let config = AuthConfig {
            enabled: true,
            auth_methods: vec![AuthMethod::Basic],
            ..Default::default()
        };
        let handler = AuthenticationHandler::new(config);

        use base64::{engine::general_purpose::STANDARD, Engine};
        let creds = STANDARD.encode("alice:any-password");
        let request = AuthRequest::new().with_header("Authorization", format!("Basic {creds}"));
        let result = handler.authenticate(&request).await;
        assert!(
            result.is_err(),
            "basic auth must deny without a user store, got {result:?}"
        );
    }

    // --- OAuth RFC 7662 introspection -------------------------------------

    /// Minimal HTTP/1.1 server that answers every request with a fixed JSON
    /// body — stands in for an OAuth introspection endpoint. Returns its URL.
    async fn spawn_introspection_mock(body: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                // Drain (best-effort) the request, then respond.
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        format!("http://{}/introspect", addr)
    }

    fn oauth_cfg(introspection_url: String, required_scopes: Vec<String>) -> OAuthConfig {
        OAuthConfig {
            introspection_url,
            client_id: "proxy".into(),
            client_secret: "secret".into(),
            token_url: None,
            scopes: Vec::new(),
            cache_ttl: std::time::Duration::from_secs(60),
            required_scopes,
            issuer: String::new(),
            authorization_url: None,
            audience: None,
        }
    }

    #[tokio::test]
    async fn oauth_introspection_accepts_active_token() {
        let url = spawn_introspection_mock(
            r#"{"active":true,"sub":"alice","username":"alice","scope":"read write"}"#,
        )
        .await;
        let handler = AuthenticationHandlerBuilder::new()
            .enabled(true)
            .with_oauth(oauth_cfg(url, Vec::new()))
            .build();
        let req = AuthRequest::new().with_header("Authorization", "Bearer tok-abc");
        let result = handler
            .authenticate_oauth(&req)
            .await
            .expect("active token must authenticate");
        assert_eq!(result.identity.user_id, "alice");
        assert!(result.identity.roles.contains(&"read".to_string()));
        assert!(result.identity.roles.contains(&"write".to_string()));
    }

    #[tokio::test]
    async fn oauth_introspection_denies_inactive_token() {
        let url = spawn_introspection_mock(r#"{"active":false}"#).await;
        let handler = AuthenticationHandlerBuilder::new()
            .enabled(true)
            .with_oauth(oauth_cfg(url, Vec::new()))
            .build();
        let req = AuthRequest::new().with_header("Authorization", "Bearer dead");
        let err = handler.authenticate_oauth(&req).await.unwrap_err();
        assert!(
            matches!(err, AuthError::InvalidCredentials),
            "inactive token must be denied, got {err:?}"
        );
    }

    #[tokio::test]
    async fn oauth_introspection_enforces_required_scopes() {
        let url = spawn_introspection_mock(r#"{"active":true,"sub":"bob","scope":"read"}"#).await;
        let handler = AuthenticationHandlerBuilder::new()
            .enabled(true)
            .with_oauth(oauth_cfg(url, vec!["admin".to_string()]))
            .build();
        let req = AuthRequest::new().with_header("Authorization", "Bearer tok");
        let err = handler.authenticate_oauth(&req).await.unwrap_err();
        assert!(
            matches!(err, AuthError::InsufficientPermissions(_)),
            "missing required scope must deny, got {err:?}"
        );
    }

    // --- LDAP search + bind (live, against a real directory) --------------

    /// Live LDAP search-and-bind test against a real directory server. Gated
    /// on `HELIOS_LDAP_URL` (e.g. `ldap://127.0.0.1:1389`); skips when unset so
    /// CI without a directory stays green. `scripts/regress/ldap-test.sh`
    /// stands up an OpenLDAP container, seeds a user, and exports the env.
    ///
    /// Asserts: the right user+password authenticates and yields an `ldap`
    /// identity; a wrong password is denied; an unknown user is denied.
    #[cfg(feature = "ldap-auth")]
    #[tokio::test]
    async fn ldap_live_search_and_bind() {
        use std::time::Duration;

        let url = match std::env::var("HELIOS_LDAP_URL") {
            Ok(u) if !u.is_empty() => u,
            _ => {
                eprintln!("skipping ldap_live_search_and_bind: set HELIOS_LDAP_URL");
                return;
            }
        };
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let cfg = LdapConfig {
            server_url: url,
            bind_dn: env("HELIOS_LDAP_BIND_DN", ""),
            bind_password: env("HELIOS_LDAP_BIND_PW", ""),
            user_search_base: env("HELIOS_LDAP_BASE", "ou=users,dc=example,dc=org"),
            user_filter: env("HELIOS_LDAP_FILTER", "(uid={0})"),
            group_search_base: None,
            group_attribute: "memberOf".to_string(),
            timeout: Duration::from_secs(5),
            starttls: false,
        };
        let user = env("HELIOS_LDAP_USER", "alice");
        let pass = env("HELIOS_LDAP_PASS", "alicepw");

        let handler = AuthenticationHandlerBuilder::new()
            .enabled(true)
            .with_ldap(cfg)
            .build();

        // 1. Correct credentials authenticate.
        let ok_req = AuthRequest::new()
            .with_username(user.clone())
            .with_password(pass.clone());
        let result = handler
            .authenticate_ldap(&ok_req)
            .await
            .expect("valid LDAP credentials must authenticate");
        assert_eq!(result.identity.user_id, user);
        assert_eq!(result.identity.auth_method, "ldap");

        // 2. Wrong password is denied.
        let bad_pw = AuthRequest::new()
            .with_username(user.clone())
            .with_password("definitely-wrong");
        assert!(
            matches!(
                handler.authenticate_ldap(&bad_pw).await,
                Err(AuthError::InvalidCredentials)
            ),
            "wrong password must be denied"
        );

        // 3. Unknown user is denied.
        let unknown = AuthRequest::new()
            .with_username("nosuchuser")
            .with_password("whatever");
        assert!(
            matches!(
                handler.authenticate_ldap(&unknown).await,
                Err(AuthError::InvalidCredentials)
            ),
            "unknown user must be denied"
        );
    }
}
