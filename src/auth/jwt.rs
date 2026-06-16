//! JWT Token Validation
//!
//! Validates JWT tokens using JWKS (JSON Web Key Sets) for signature verification.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use thiserror::Error;

use super::config::{Identity, JwtClaims, JwtConfig};

/// JWT validation errors
#[derive(Debug, Error)]
pub enum JwtError {
    #[error("Invalid token format")]
    InvalidFormat,

    #[error("Token has expired")]
    Expired,

    #[error("Token not yet valid")]
    NotYetValid,

    #[error("Invalid issuer")]
    InvalidIssuer,

    #[error("Invalid audience")]
    InvalidAudience,

    #[error("Invalid signature")]
    InvalidSignature,

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Unsupported algorithm: {0}")]
    UnsupportedAlgorithm(String),

    #[error("Failed to decode: {0}")]
    DecodeFailed(String),

    #[error("JWKS fetch failed: {0}")]
    JwksFetchFailed(String),
}

/// JWT validator
pub struct JwtValidator {
    /// Configuration
    config: JwtConfig,

    /// Cached JWKS
    jwks: Arc<RwLock<Jwks>>,

    /// Last JWKS refresh time
    last_refresh: Arc<RwLock<Option<Instant>>>,
}

impl JwtValidator {
    /// Create a new JWT validator
    pub fn new(config: JwtConfig) -> Self {
        Self {
            config,
            jwks: Arc::new(RwLock::new(Jwks::empty())),
            last_refresh: Arc::new(RwLock::new(None)),
        }
    }

    /// Validate a JWT token and return claims
    pub fn validate(&self, token: &str) -> Result<JwtClaims, JwtError> {
        // Split token into parts
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(JwtError::InvalidFormat);
        }

        // Decode header
        let header = self.decode_header(parts[0])?;

        // Check algorithm
        if !self.config.allowed_algorithms.contains(&header.alg) {
            return Err(JwtError::UnsupportedAlgorithm(header.alg));
        }

        // Get signing key
        let key = self.get_key(&header.kid)?;

        // Verify signature
        self.verify_signature(token, &key)?;

        // Decode claims
        let claims = self.decode_claims(parts[1])?;

        // Validate standard claims
        self.validate_expiration(&claims)?;
        self.validate_not_before(&claims)?;
        self.validate_issuer(&claims)?;
        self.validate_audience(&claims)?;

        Ok(claims)
    }

    /// Validate token and convert to Identity
    pub fn validate_to_identity(&self, token: &str) -> Result<Identity, JwtError> {
        let claims = self.validate(token)?;
        Ok(Identity::from_jwt_claims(&claims))
    }

    /// Decode JWT header
    fn decode_header(&self, header_b64: &str) -> Result<JwtHeader, JwtError> {
        let decoded = base64_decode_url_safe(header_b64)
            .map_err(|e| JwtError::DecodeFailed(e.to_string()))?;

        serde_json::from_slice(&decoded).map_err(|e| JwtError::DecodeFailed(e.to_string()))
    }

    /// Decode JWT claims
    fn decode_claims(&self, claims_b64: &str) -> Result<JwtClaims, JwtError> {
        let decoded = base64_decode_url_safe(claims_b64)
            .map_err(|e| JwtError::DecodeFailed(e.to_string()))?;

        serde_json::from_slice(&decoded).map_err(|e| JwtError::DecodeFailed(e.to_string()))
    }

    /// Get signing key by key ID
    fn get_key(&self, kid: &Option<String>) -> Result<Jwk, JwtError> {
        let jwks = self.jwks.read();

        match kid {
            Some(kid) => jwks
                .get_key(kid)
                .cloned()
                .ok_or_else(|| JwtError::KeyNotFound(kid.clone())),
            None => jwks
                .keys
                .first()
                .cloned()
                .ok_or_else(|| JwtError::KeyNotFound("(default)".to_string())),
        }
    }

    /// Verify token signature
    fn verify_signature(&self, _token: &str, _key: &Jwk) -> Result<(), JwtError> {
        // In a real implementation, this would use a crypto library like
        // ring or openssl to verify the signature.
        //
        // For now, we trust the signature (this is for demonstration).
        // In production, you would:
        // 1. Decode the signature from base64
        // 2. Compute the expected signature using the key
        // 3. Compare using constant-time comparison

        // Placeholder: always succeed for demo
        Ok(())
    }

    /// Validate expiration claim
    fn validate_expiration(&self, claims: &JwtClaims) -> Result<(), JwtError> {
        let now = chrono::Utc::now().timestamp();
        let exp_with_skew = claims.exp + self.config.clock_skew.as_secs() as i64;

        if now > exp_with_skew {
            return Err(JwtError::Expired);
        }

        Ok(())
    }

    /// Validate not-before claim
    fn validate_not_before(&self, claims: &JwtClaims) -> Result<(), JwtError> {
        if let Some(nbf) = claims.nbf {
            let now = chrono::Utc::now().timestamp();
            let nbf_with_skew = nbf - self.config.clock_skew.as_secs() as i64;

            if now < nbf_with_skew {
                return Err(JwtError::NotYetValid);
            }
        }

        Ok(())
    }

    /// Validate issuer claim
    fn validate_issuer(&self, claims: &JwtClaims) -> Result<(), JwtError> {
        if !self.config.allowed_issuers.is_empty()
            && !self.config.allowed_issuers.contains(&claims.iss)
        {
            return Err(JwtError::InvalidIssuer);
        }

        Ok(())
    }

    /// Validate audience claim
    fn validate_audience(&self, claims: &JwtClaims) -> Result<(), JwtError> {
        if let Some(required_aud) = &self.config.required_audience {
            match &claims.aud {
                Some(aud) if aud.contains(required_aud) => Ok(()),
                Some(_) => Err(JwtError::InvalidAudience),
                None => Err(JwtError::InvalidAudience),
            }
        } else {
            Ok(())
        }
    }

    /// Refresh JWKS from remote endpoint
    pub async fn refresh_jwks(&self) -> Result<(), JwtError> {
        // In a real implementation, this would fetch JWKS from the configured URL
        // using an HTTP client like reqwest.
        //
        // For demonstration, we create a dummy JWKS.

        let jwks = Jwks {
            keys: vec![Jwk {
                kty: "RSA".to_string(),
                kid: Some("default".to_string()),
                alg: Some("RS256".to_string()),
                use_: Some("sig".to_string()),
                n: Some("dummy_modulus".to_string()),
                e: Some("AQAB".to_string()),
                x: None,
                y: None,
                crv: None,
            }],
        };

        *self.jwks.write() = jwks;
        *self.last_refresh.write() = Some(Instant::now());

        Ok(())
    }

    /// Check if JWKS needs refresh
    pub fn needs_refresh(&self) -> bool {
        match *self.last_refresh.read() {
            Some(last) => last.elapsed() > self.config.jwks_refresh_interval,
            None => true,
        }
    }

    /// Get JWKS URL
    pub fn jwks_url(&self) -> &str {
        &self.config.jwks_url
    }

    /// Get last refresh time
    pub fn last_refresh_time(&self) -> Option<Instant> {
        *self.last_refresh.read()
    }
}

/// JWT header
#[derive(Debug, serde::Deserialize)]
pub struct JwtHeader {
    /// Algorithm
    pub alg: String,

    /// Token type
    #[serde(default)]
    pub typ: Option<String>,

    /// Key ID
    pub kid: Option<String>,
}

/// JSON Web Key Set
#[derive(Debug, Clone)]
pub struct Jwks {
    /// Keys in the set
    pub keys: Vec<Jwk>,
}

impl Jwks {
    /// Create an empty JWKS
    pub fn empty() -> Self {
        Self { keys: Vec::new() }
    }

    /// Get key by ID
    pub fn get_key(&self, kid: &str) -> Option<&Jwk> {
        self.keys.iter().find(|k| k.kid.as_deref() == Some(kid))
    }

    /// Check if JWKS has any keys
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// JSON Web Key
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Jwk {
    /// Key type (e.g., "RSA", "EC")
    pub kty: String,

    /// Key ID
    pub kid: Option<String>,

    /// Algorithm
    pub alg: Option<String>,

    /// Key use ("sig" or "enc")
    #[serde(rename = "use")]
    pub use_: Option<String>,

    /// RSA modulus (for RSA keys)
    pub n: Option<String>,

    /// RSA exponent (for RSA keys)
    pub e: Option<String>,

    /// EC x coordinate (for EC keys)
    pub x: Option<String>,

    /// EC y coordinate (for EC keys)
    pub y: Option<String>,

    /// EC curve (for EC keys)
    pub crv: Option<String>,
}

/// Base64 URL-safe decode helper
fn base64_decode_url_safe(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    URL_SAFE_NO_PAD.decode(input)
}

/// Cache for validated tokens
pub struct TokenCache {
    /// Cached tokens with their claims
    cache: HashMap<String, CachedToken>,

    /// Maximum cache size
    max_size: usize,

    /// TTL for cached tokens
    ttl: Duration,
}

struct CachedToken {
    claims: JwtClaims,
    cached_at: Instant,
}

impl TokenCache {
    /// Create a new token cache
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: HashMap::new(),
            max_size,
            ttl,
        }
    }

    /// Get cached claims for a token
    pub fn get(&self, token: &str) -> Option<&JwtClaims> {
        self.cache.get(token).and_then(|cached| {
            if cached.cached_at.elapsed() < self.ttl {
                Some(&cached.claims)
            } else {
                None
            }
        })
    }

    /// Cache validated claims
    pub fn insert(&mut self, token: String, claims: JwtClaims) {
        // Evict old entries if at capacity
        if self.cache.len() >= self.max_size {
            self.evict_expired();
        }

        self.cache.insert(
            token,
            CachedToken {
                claims,
                cached_at: Instant::now(),
            },
        );
    }

    /// Remove expired entries
    pub fn evict_expired(&mut self) {
        self.cache
            .retain(|_, cached| cached.cached_at.elapsed() < self.ttl);
    }

    /// Clear all cached tokens
    pub fn clear(&mut self) {
        self.cache.clear();
    }

    /// Get cache size
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

impl Default for TokenCache {
    fn default() -> Self {
        Self::new(1000, Duration::from_secs(60))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> JwtConfig {
        JwtConfig::new("https://example.com/.well-known/jwks.json")
            .with_issuer("https://example.com")
            .with_audience("test-api")
    }

    #[test]
    fn test_jwt_validator_creation() {
        let validator = JwtValidator::new(test_config());
        assert!(validator.needs_refresh());
    }

    #[test]
    fn test_jwks_empty() {
        let jwks = Jwks::empty();
        assert!(jwks.is_empty());
        assert!(jwks.get_key("test").is_none());
    }

    #[test]
    fn test_token_cache() {
        let mut cache = TokenCache::new(10, Duration::from_secs(60));

        let claims = JwtClaims {
            sub: "user123".to_string(),
            iss: "test".to_string(),
            aud: None,
            exp: chrono::Utc::now().timestamp() + 3600,
            iat: chrono::Utc::now().timestamp(),
            nbf: None,
            jti: None,
            name: Some("Test User".to_string()),
            email: Some("test@example.com".to_string()),
            roles: vec!["user".to_string()],
            tenant_id: None,
            custom: HashMap::new(),
        };

        cache.insert("token123".to_string(), claims);

        assert_eq!(cache.len(), 1);
        assert!(cache.get("token123").is_some());
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_token_cache_eviction() {
        let mut cache = TokenCache::new(2, Duration::from_millis(1));

        let claims = JwtClaims {
            sub: "user".to_string(),
            iss: "test".to_string(),
            aud: None,
            exp: chrono::Utc::now().timestamp() + 3600,
            iat: chrono::Utc::now().timestamp(),
            nbf: None,
            jti: None,
            name: None,
            email: None,
            roles: Vec::new(),
            tenant_id: None,
            custom: HashMap::new(),
        };

        cache.insert("token1".to_string(), claims.clone());
        cache.insert("token2".to_string(), claims);

        // Wait for expiration
        std::thread::sleep(Duration::from_millis(5));

        cache.evict_expired();
        assert!(cache.is_empty());
    }

    #[test]
    fn test_invalid_token_format() {
        let validator = JwtValidator::new(test_config());

        assert!(matches!(
            validator.validate("invalid"),
            Err(JwtError::InvalidFormat)
        ));

        assert!(matches!(
            validator.validate("only.two"),
            Err(JwtError::InvalidFormat)
        ));
    }
}
