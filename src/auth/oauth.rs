//! OAuth Token Introspection
//!
//! Validates OAuth access tokens using RFC 7662 token introspection.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use thiserror::Error;

use super::config::{Identity, OAuthConfig};

/// OAuth errors
#[derive(Debug, Error)]
pub enum OAuthError {
    #[error("Token introspection failed: {0}")]
    IntrospectionFailed(String),

    #[error("Token is not active")]
    TokenNotActive,

    #[error("Token expired")]
    TokenExpired,

    #[error("Invalid token scope")]
    InvalidScope,

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),
}

/// OAuth client for token introspection
pub struct OAuthClient {
    /// Configuration
    config: OAuthConfig,

    /// Token cache
    cache: Arc<RwLock<TokenCache>>,

    /// HTTP client (placeholder - would use reqwest in real impl)
    client_id: String,
    #[allow(dead_code)]
    client_secret: String,
}

/// Token introspection response
#[derive(Debug, Clone, serde::Deserialize)]
pub struct IntrospectionResponse {
    /// Whether the token is active
    pub active: bool,

    /// Token scopes
    #[serde(default)]
    pub scope: Option<String>,

    /// Client ID
    #[serde(default)]
    pub client_id: Option<String>,

    /// Username
    #[serde(default)]
    pub username: Option<String>,

    /// Token type
    #[serde(default)]
    pub token_type: Option<String>,

    /// Expiration time (Unix timestamp)
    #[serde(default)]
    pub exp: Option<i64>,

    /// Issued at time (Unix timestamp)
    #[serde(default)]
    pub iat: Option<i64>,

    /// Not before time (Unix timestamp)
    #[serde(default)]
    pub nbf: Option<i64>,

    /// Subject
    #[serde(default)]
    pub sub: Option<String>,

    /// Audience
    #[serde(default)]
    pub aud: Option<String>,

    /// Issuer
    #[serde(default)]
    pub iss: Option<String>,

    /// JWT ID
    #[serde(default)]
    pub jti: Option<String>,

    /// Additional claims
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

impl IntrospectionResponse {
    /// Convert to Identity
    pub fn to_identity(&self) -> Identity {
        let roles = self
            .scope
            .as_ref()
            .map(|s| s.split_whitespace().map(String::from).collect())
            .unwrap_or_default();

        Identity {
            user_id: self
                .sub
                .clone()
                .or_else(|| self.username.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            name: self.username.clone(),
            email: self
                .extra
                .get("email")
                .and_then(|v| v.as_str())
                .map(String::from),
            roles,
            groups: self
                .extra
                .get("groups")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            tenant_id: self
                .extra
                .get("tenant_id")
                .and_then(|v| v.as_str())
                .map(String::from),
            claims: self.extra.clone(),
            auth_method: "oauth".to_string(),
            authenticated_at: chrono::Utc::now(),
        }
    }

    /// Check if token is valid
    pub fn is_valid(&self) -> bool {
        if !self.active {
            return false;
        }

        // Check expiration
        if let Some(exp) = self.exp {
            let now = chrono::Utc::now().timestamp();
            if now > exp {
                return false;
            }
        }

        // Check not-before
        if let Some(nbf) = self.nbf {
            let now = chrono::Utc::now().timestamp();
            if now < nbf {
                return false;
            }
        }

        true
    }

    /// Get scopes as a list
    pub fn scopes(&self) -> Vec<String> {
        self.scope
            .as_ref()
            .map(|s| s.split_whitespace().map(String::from).collect())
            .unwrap_or_default()
    }

    /// Check if token has a specific scope
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes().iter().any(|s| s == scope)
    }
}

/// Token cache entry
struct CachedToken {
    response: IntrospectionResponse,
    cached_at: Instant,
}

/// Token cache
struct TokenCache {
    entries: HashMap<String, CachedToken>,
    max_size: usize,
    ttl: Duration,
}

impl TokenCache {
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
            ttl,
        }
    }

    fn get(&self, token: &str) -> Option<&IntrospectionResponse> {
        self.entries.get(token).and_then(|cached| {
            if cached.cached_at.elapsed() < self.ttl {
                Some(&cached.response)
            } else {
                None
            }
        })
    }

    fn insert(&mut self, token: String, response: IntrospectionResponse) {
        if self.entries.len() >= self.max_size {
            self.evict_expired();
        }
        self.entries.insert(
            token,
            CachedToken {
                response,
                cached_at: Instant::now(),
            },
        );
    }

    fn evict_expired(&mut self) {
        self.entries
            .retain(|_, cached| cached.cached_at.elapsed() < self.ttl);
    }

    fn invalidate(&mut self, token: &str) {
        self.entries.remove(token);
    }

    fn clear(&mut self) {
        self.entries.clear();
    }
}

impl OAuthClient {
    /// Create a new OAuth client
    pub fn new(config: OAuthConfig) -> Self {
        let client_id = config.client_id.clone();
        let client_secret = config.client_secret.clone();
        let cache_ttl = config.cache_ttl;

        Self {
            config,
            cache: Arc::new(RwLock::new(TokenCache::new(10000, cache_ttl))),
            client_id,
            client_secret,
        }
    }

    /// Introspect a token
    pub async fn introspect(&self, token: &str) -> Result<IntrospectionResponse, OAuthError> {
        // Check cache first
        if let Some(cached) = self.cache.read().get(token) {
            if cached.is_valid() {
                return Ok(cached.clone());
            }
        }

        // Perform introspection
        let response = self.do_introspect(token).await?;

        // Validate response
        if !response.active {
            return Err(OAuthError::TokenNotActive);
        }

        if !response.is_valid() {
            return Err(OAuthError::TokenExpired);
        }

        // Cache successful response
        self.cache
            .write()
            .insert(token.to_string(), response.clone());

        Ok(response)
    }

    /// Perform the actual introspection request
    async fn do_introspect(&self, token: &str) -> Result<IntrospectionResponse, OAuthError> {
        // In a real implementation, this would make an HTTP POST request to the
        // introspection endpoint. For demonstration, we return a placeholder.
        //
        // Real implementation would look like:
        //
        // let response = reqwest::Client::new()
        //     .post(&self.config.introspection_url)
        //     .basic_auth(&self.client_id, Some(&self.client_secret))
        //     .form(&[("token", token)])
        //     .send()
        //     .await
        //     .map_err(|e| OAuthError::NetworkError(e.to_string()))?;
        //
        // let body: IntrospectionResponse = response
        //     .json()
        //     .await
        //     .map_err(|e| OAuthError::InvalidResponse(e.to_string()))?;

        // Placeholder: create a demo response
        // In production, this would be the actual HTTP call
        let _ = token; // Suppress unused warning

        Ok(IntrospectionResponse {
            active: true,
            scope: Some("read write".to_string()),
            client_id: Some(self.client_id.clone()),
            username: Some("oauth_user".to_string()),
            token_type: Some("Bearer".to_string()),
            exp: Some(chrono::Utc::now().timestamp() + 3600),
            iat: Some(chrono::Utc::now().timestamp()),
            nbf: None,
            sub: Some("user123".to_string()),
            aud: self.config.audience.clone(),
            iss: Some(self.config.issuer.clone()),
            jti: Some("token-id-123".to_string()),
            extra: HashMap::new(),
        })
    }

    /// Validate a token and return identity
    pub async fn validate_to_identity(&self, token: &str) -> Result<Identity, OAuthError> {
        let response = self.introspect(token).await?;

        // Check required scopes
        if !self.config.required_scopes.is_empty() {
            for scope in &self.config.required_scopes {
                if !response.has_scope(scope) {
                    return Err(OAuthError::InvalidScope);
                }
            }
        }

        Ok(response.to_identity())
    }

    /// Invalidate a cached token
    pub fn invalidate_token(&self, token: &str) {
        self.cache.write().invalidate(token);
    }

    /// Clear the token cache
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Get cache statistics
    pub fn cache_size(&self) -> usize {
        self.cache.read().entries.len()
    }

    /// Get introspection URL
    pub fn introspection_url(&self) -> &str {
        &self.config.introspection_url
    }

    /// Get issuer
    pub fn issuer(&self) -> &str {
        &self.config.issuer
    }
}

/// OAuth token exchange
pub struct TokenExchange {
    /// Configuration
    config: OAuthConfig,
}

impl TokenExchange {
    /// Create a new token exchange
    pub fn new(config: OAuthConfig) -> Self {
        Self { config }
    }

    /// Exchange an authorization code for tokens
    pub async fn exchange_code(
        &self,
        code: &str,
        redirect_uri: &str,
    ) -> Result<TokenResponse, OAuthError> {
        // In a real implementation, this would make an HTTP POST to the token endpoint
        // For demonstration, return a placeholder
        let _ = (code, redirect_uri);

        Ok(TokenResponse {
            access_token: "access_token_placeholder".to_string(),
            token_type: "Bearer".to_string(),
            expires_in: Some(3600),
            refresh_token: Some("refresh_token_placeholder".to_string()),
            scope: Some("read write".to_string()),
            id_token: None,
        })
    }

    /// Refresh an access token
    pub async fn refresh_token(&self, refresh_token: &str) -> Result<TokenResponse, OAuthError> {
        // In a real implementation, this would make an HTTP POST to the token endpoint
        let _ = refresh_token;

        Ok(TokenResponse {
            access_token: "new_access_token".to_string(),
            token_type: "Bearer".to_string(),
            expires_in: Some(3600),
            refresh_token: Some("new_refresh_token".to_string()),
            scope: Some("read write".to_string()),
            id_token: None,
        })
    }

    /// Get authorization URL
    pub fn authorization_url(&self, state: &str, scopes: &[&str]) -> String {
        let scope = scopes.join(" ");
        format!(
            "{}?response_type=code&client_id={}&state={}&scope={}",
            self.config.authorization_url.as_deref().unwrap_or(""),
            self.config.client_id,
            state,
            urlencoding::encode(&scope),
        )
    }
}

/// Token response from OAuth server
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TokenResponse {
    /// Access token
    pub access_token: String,

    /// Token type (usually "Bearer")
    pub token_type: String,

    /// Expires in seconds
    pub expires_in: Option<u64>,

    /// Refresh token
    pub refresh_token: Option<String>,

    /// Granted scopes
    pub scope: Option<String>,

    /// ID token (for OpenID Connect)
    pub id_token: Option<String>,
}

/// URL encoding module
mod urlencoding {
    pub fn encode(s: &str) -> String {
        let mut result = String::new();
        for c in s.chars() {
            match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => {
                    result.push(c);
                }
                ' ' => {
                    result.push_str("%20");
                }
                _ => {
                    for byte in c.to_string().as_bytes() {
                        result.push_str(&format!("%{:02X}", byte));
                    }
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_config() -> OAuthConfig {
        OAuthConfig {
            introspection_url: "https://auth.example.com/introspect".to_string(),
            client_id: "test-client".to_string(),
            client_secret: "test-secret".to_string(),
            issuer: "https://auth.example.com".to_string(),
            audience: Some("test-api".to_string()),
            required_scopes: vec!["read".to_string()],
            scopes: Vec::new(),
            cache_ttl: Duration::from_secs(60),
            authorization_url: Some("https://auth.example.com/authorize".to_string()),
            token_url: Some("https://auth.example.com/token".to_string()),
        }
    }

    #[test]
    fn test_introspection_response_validity() {
        let response = IntrospectionResponse {
            active: true,
            scope: Some("read write".to_string()),
            client_id: None,
            username: Some("testuser".to_string()),
            token_type: None,
            exp: Some(chrono::Utc::now().timestamp() + 3600),
            iat: None,
            nbf: None,
            sub: Some("user123".to_string()),
            aud: None,
            iss: None,
            jti: None,
            extra: HashMap::new(),
        };

        assert!(response.is_valid());
        assert!(response.has_scope("read"));
        assert!(response.has_scope("write"));
        assert!(!response.has_scope("admin"));
    }

    #[test]
    fn test_introspection_response_expired() {
        let response = IntrospectionResponse {
            active: true,
            scope: None,
            client_id: None,
            username: None,
            token_type: None,
            exp: Some(chrono::Utc::now().timestamp() - 3600), // Expired
            iat: None,
            nbf: None,
            sub: None,
            aud: None,
            iss: None,
            jti: None,
            extra: HashMap::new(),
        };

        assert!(!response.is_valid());
    }

    #[test]
    fn test_introspection_response_inactive() {
        let response = IntrospectionResponse {
            active: false,
            scope: None,
            client_id: None,
            username: None,
            token_type: None,
            exp: None,
            iat: None,
            nbf: None,
            sub: None,
            aud: None,
            iss: None,
            jti: None,
            extra: HashMap::new(),
        };

        assert!(!response.is_valid());
    }

    #[test]
    fn test_introspection_to_identity() {
        let mut extra = HashMap::new();
        extra.insert("email".to_string(), serde_json::json!("test@example.com"));
        extra.insert("tenant_id".to_string(), serde_json::json!("tenant1"));

        let response = IntrospectionResponse {
            active: true,
            scope: Some("read write".to_string()),
            client_id: None,
            username: Some("testuser".to_string()),
            token_type: None,
            exp: None,
            iat: None,
            nbf: None,
            sub: Some("user123".to_string()),
            aud: None,
            iss: None,
            jti: None,
            extra,
        };

        let identity = response.to_identity();
        assert_eq!(identity.user_id, "user123");
        assert_eq!(identity.name, Some("testuser".to_string()));
        assert_eq!(identity.email, Some("test@example.com".to_string()));
        assert_eq!(identity.tenant_id, Some("tenant1".to_string()));
        assert!(identity.roles.contains(&"read".to_string()));
    }

    #[tokio::test]
    async fn test_oauth_client_introspect() {
        let client = OAuthClient::new(test_config());
        let result = client.introspect("test_token").await.unwrap();

        assert!(result.active);
        assert!(result.is_valid());
    }

    #[tokio::test]
    async fn test_oauth_client_cache() {
        let client = OAuthClient::new(test_config());

        // First call caches
        let _ = client.introspect("test_token").await.unwrap();
        assert_eq!(client.cache_size(), 1);

        // Second call uses cache
        let _ = client.introspect("test_token").await.unwrap();
        assert_eq!(client.cache_size(), 1);

        // Different token adds to cache
        let _ = client.introspect("another_token").await.unwrap();
        assert_eq!(client.cache_size(), 2);

        // Clear cache
        client.clear_cache();
        assert_eq!(client.cache_size(), 0);
    }

    #[test]
    fn test_authorization_url() {
        let exchange = TokenExchange::new(test_config());
        let url = exchange.authorization_url("state123", &["read", "write"]);

        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=test-client"));
        assert!(url.contains("state=state123"));
    }

    #[test]
    fn test_url_encoding() {
        assert_eq!(urlencoding::encode("hello world"), "hello%20world");
        assert_eq!(urlencoding::encode("test-value"), "test-value");
        assert_eq!(urlencoding::encode("a=b&c=d"), "a%3Db%26c%3Dd");
    }
}
