//! Tenant Identification Strategies
//!
//! This module provides different strategies for identifying tenants from incoming requests.
//!
//! # Strategies
//!
//! - **Header**: Extract tenant ID from HTTP header (e.g., X-Tenant-Id)
//! - **UsernamePrefix**: Extract from username prefix (e.g., tenant.user -> tenant)
//! - **JWT**: Extract from JWT claim
//! - **DatabaseName**: Use database name as tenant ID
//! - **SqlContext**: Extract from SQL context variable

use std::collections::HashMap;
use std::sync::Arc;

use super::config::{IdentificationMethod, TenantId};

/// Request context for tenant identification
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    /// HTTP headers (or similar protocol headers)
    pub headers: HashMap<String, String>,

    /// Username from authentication
    pub username: Option<String>,

    /// Database name from connection
    pub database: Option<String>,

    /// Authentication token (e.g., JWT)
    pub auth_token: Option<String>,

    /// SQL context variables
    pub sql_context: HashMap<String, String>,

    /// Client IP address
    pub client_ip: Option<String>,

    /// Connection ID
    pub connection_id: Option<u64>,
}

impl RequestContext {
    /// Create a new empty request context
    pub fn new() -> Self {
        Self::default()
    }

    /// Set a header
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Set username
    pub fn with_username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Set database
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set auth token
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set SQL context variable
    pub fn with_sql_context(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.sql_context.insert(name.into(), value.into());
        self
    }

    /// Set client IP
    pub fn with_client_ip(mut self, ip: impl Into<String>) -> Self {
        self.client_ip = Some(ip.into());
        self
    }

    /// Get header value
    pub fn get_header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|s| s.as_str())
    }

    /// Get SQL context variable
    pub fn get_sql_context(&self, name: &str) -> Option<&str> {
        self.sql_context.get(name).map(|s| s.as_str())
    }
}

/// Trait for tenant identification strategies
pub trait TenantIdentifier: Send + Sync {
    /// Identify tenant from request context
    fn identify(&self, request: &RequestContext) -> Option<TenantId>;

    /// Get the name of this identification strategy
    fn strategy_name(&self) -> &'static str;
}

/// Header-based tenant identification
///
/// Extracts tenant ID from a specific HTTP header.
#[derive(Debug, Clone)]
pub struct HeaderTenantIdentifier {
    /// Header name to extract tenant ID from
    header_name: String,

    /// Whether to lowercase the tenant ID
    lowercase: bool,
}

impl HeaderTenantIdentifier {
    /// Create a new header identifier
    pub fn new(header_name: impl Into<String>) -> Self {
        Self {
            header_name: header_name.into(),
            lowercase: true,
        }
    }

    /// Create with X-Tenant-Id header
    pub fn default_header() -> Self {
        Self::new("X-Tenant-Id")
    }

    /// Don't lowercase the tenant ID
    pub fn case_sensitive(mut self) -> Self {
        self.lowercase = false;
        self
    }
}

impl TenantIdentifier for HeaderTenantIdentifier {
    fn identify(&self, request: &RequestContext) -> Option<TenantId> {
        request
            .get_header(&self.header_name)
            .filter(|v| !v.is_empty())
            .map(|v| {
                if self.lowercase {
                    TenantId::new(v.to_lowercase())
                } else {
                    TenantId::new(v)
                }
            })
    }

    fn strategy_name(&self) -> &'static str {
        "header"
    }
}

/// Username prefix-based tenant identification
///
/// Extracts tenant ID from username prefix (e.g., "tenant_a.user" -> "tenant_a")
#[derive(Debug, Clone)]
pub struct UsernamePrefixIdentifier {
    /// Separator character between tenant and username
    separator: char,

    /// Whether to lowercase the tenant ID
    lowercase: bool,
}

impl UsernamePrefixIdentifier {
    /// Create a new username prefix identifier
    pub fn new(separator: char) -> Self {
        Self {
            separator,
            lowercase: true,
        }
    }

    /// Create with dot separator
    pub fn with_dot() -> Self {
        Self::new('.')
    }

    /// Create with underscore separator
    pub fn with_underscore() -> Self {
        Self::new('_')
    }

    /// Don't lowercase the tenant ID
    pub fn case_sensitive(mut self) -> Self {
        self.lowercase = false;
        self
    }
}

impl TenantIdentifier for UsernamePrefixIdentifier {
    fn identify(&self, request: &RequestContext) -> Option<TenantId> {
        request
            .username
            .as_ref()
            .and_then(|username| username.split(self.separator).next())
            .filter(|prefix| !prefix.is_empty())
            .map(|prefix| {
                if self.lowercase {
                    TenantId::new(prefix.to_lowercase())
                } else {
                    TenantId::new(prefix)
                }
            })
    }

    fn strategy_name(&self) -> &'static str {
        "username_prefix"
    }
}

/// Database name-based tenant identification
///
/// Uses the database name as the tenant ID.
#[derive(Debug, Clone, Default)]
pub struct DatabaseNameIdentifier {
    /// Prefix to strip from database name (e.g., "tenant_")
    prefix: Option<String>,

    /// Suffix to strip from database name (e.g., "_db")
    suffix: Option<String>,

    /// Whether to lowercase the tenant ID
    lowercase: bool,
}

impl DatabaseNameIdentifier {
    /// Create a new database name identifier
    pub fn new() -> Self {
        Self::default()
    }

    /// Strip prefix from database name
    pub fn strip_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Strip suffix from database name
    pub fn strip_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.suffix = Some(suffix.into());
        self
    }

    /// Don't lowercase the tenant ID
    pub fn case_sensitive(mut self) -> Self {
        self.lowercase = false;
        self
    }
}

impl TenantIdentifier for DatabaseNameIdentifier {
    fn identify(&self, request: &RequestContext) -> Option<TenantId> {
        request.database.as_ref().map(|db| {
            let mut name = db.as_str();

            if let Some(prefix) = &self.prefix {
                name = name.strip_prefix(prefix.as_str()).unwrap_or(name);
            }

            if let Some(suffix) = &self.suffix {
                name = name.strip_suffix(suffix.as_str()).unwrap_or(name);
            }

            if self.lowercase {
                TenantId::new(name.to_lowercase())
            } else {
                TenantId::new(name)
            }
        })
    }

    fn strategy_name(&self) -> &'static str {
        "database_name"
    }
}

/// SQL context variable-based tenant identification
///
/// Extracts tenant ID from a SQL session variable (e.g., SET helios.tenant_id = 'tenant_a')
#[derive(Debug, Clone)]
pub struct SqlContextIdentifier {
    /// Variable name to look for
    variable_name: String,
}

impl SqlContextIdentifier {
    /// Create a new SQL context identifier
    pub fn new(variable_name: impl Into<String>) -> Self {
        Self {
            variable_name: variable_name.into(),
        }
    }

    /// Create with default variable name
    pub fn default_variable() -> Self {
        Self::new("helios.tenant_id")
    }
}

impl TenantIdentifier for SqlContextIdentifier {
    fn identify(&self, request: &RequestContext) -> Option<TenantId> {
        request
            .get_sql_context(&self.variable_name)
            .filter(|v| !v.is_empty())
            .map(|v| TenantId::new(v.to_lowercase()))
    }

    fn strategy_name(&self) -> &'static str {
        "sql_context"
    }
}

/// JWT claim-based tenant identification
///
/// Extracts tenant ID from a JWT token claim.
#[derive(Debug, Clone)]
pub struct JwtClaimIdentifier {
    /// JWT claim name
    claim_name: String,

    /// Expected issuer (optional validation)
    issuer: Option<String>,

    /// JWT verification key (simplified - in real impl would be more complex)
    /// In production, this would integrate with a proper JWT library
    _verification_key: Option<String>,
}

impl JwtClaimIdentifier {
    /// Create a new JWT claim identifier
    pub fn new(claim_name: impl Into<String>) -> Self {
        Self {
            claim_name: claim_name.into(),
            issuer: None,
            _verification_key: None,
        }
    }

    /// Set expected issuer
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = Some(issuer.into());
        self
    }

    /// Simple JWT payload extraction (base64 decode middle part)
    /// In production, proper signature verification would be required
    fn extract_claim(&self, token: &str) -> Option<String> {
        use base64::Engine;

        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return None;
        }

        // Decode payload (middle part)
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[1])
            .ok()?;

        let payload_str = String::from_utf8(payload).ok()?;

        // Simple JSON parsing for claim extraction
        // In production, use serde_json
        self.extract_json_string(&payload_str, &self.claim_name)
    }

    /// Simple JSON string extraction (for demo purposes)
    fn extract_json_string(&self, json: &str, key: &str) -> Option<String> {
        // Look for "key":"value" or "key": "value"
        let pattern = format!("\"{}\"", key);
        let pos = json.find(&pattern)?;
        let after_key = &json[pos + pattern.len()..];

        // Skip whitespace and colon
        let after_colon = after_key.trim_start().strip_prefix(':')?;
        let after_colon = after_colon.trim_start();

        // Extract quoted value
        if let Some(inner) = after_colon.strip_prefix('"') {
            let value_end = inner.find('"')?;
            Some(inner[..value_end].to_string())
        } else {
            None
        }
    }
}

impl TenantIdentifier for JwtClaimIdentifier {
    fn identify(&self, request: &RequestContext) -> Option<TenantId> {
        request
            .auth_token
            .as_ref()
            .and_then(|token| self.extract_claim(token))
            .filter(|claim| !claim.is_empty())
            .map(|claim| TenantId::new(claim.to_lowercase()))
    }

    fn strategy_name(&self) -> &'static str {
        "jwt_claim"
    }
}

/// Composite identifier that tries multiple strategies in order
#[derive(Clone)]
pub struct CompositeIdentifier {
    /// Identifiers to try in order
    identifiers: Vec<Arc<dyn TenantIdentifier>>,
}

impl CompositeIdentifier {
    /// Create a new composite identifier
    pub fn new() -> Self {
        Self {
            identifiers: Vec::new(),
        }
    }

    /// Add an identifier to try
    #[allow(clippy::should_implement_trait)]
    pub fn add<I: TenantIdentifier + 'static>(mut self, identifier: I) -> Self {
        self.identifiers.push(Arc::new(identifier));
        self
    }

    /// Add an identifier wrapped in Arc
    pub fn add_arc(mut self, identifier: Arc<dyn TenantIdentifier>) -> Self {
        self.identifiers.push(identifier);
        self
    }
}

impl Default for CompositeIdentifier {
    fn default() -> Self {
        Self::new()
    }
}

impl TenantIdentifier for CompositeIdentifier {
    fn identify(&self, request: &RequestContext) -> Option<TenantId> {
        for identifier in &self.identifiers {
            if let Some(tenant) = identifier.identify(request) {
                return Some(tenant);
            }
        }
        None
    }

    fn strategy_name(&self) -> &'static str {
        "composite"
    }
}

/// Create a tenant identifier from identification method
pub fn create_identifier(method: &IdentificationMethod) -> Box<dyn TenantIdentifier> {
    match method {
        IdentificationMethod::Header { header_name } => {
            Box::new(HeaderTenantIdentifier::new(header_name))
        }
        IdentificationMethod::UsernamePrefix { separator } => {
            Box::new(UsernamePrefixIdentifier::new(*separator))
        }
        IdentificationMethod::JwtClaim { claim_name, issuer } => {
            let mut identifier = JwtClaimIdentifier::new(claim_name);
            if let Some(iss) = issuer {
                identifier = identifier.with_issuer(iss);
            }
            Box::new(identifier)
        }
        IdentificationMethod::DatabaseName => Box::new(DatabaseNameIdentifier::new()),
        IdentificationMethod::SqlContext { variable_name } => {
            Box::new(SqlContextIdentifier::new(variable_name))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_identifier() {
        let identifier = HeaderTenantIdentifier::new("X-Tenant-Id");

        let ctx = RequestContext::new().with_header("X-Tenant-Id", "TenantA");
        assert_eq!(
            identifier.identify(&ctx).map(|t| t.0),
            Some("tenanta".to_string())
        );

        let ctx_missing = RequestContext::new();
        assert!(identifier.identify(&ctx_missing).is_none());

        let ctx_empty = RequestContext::new().with_header("X-Tenant-Id", "");
        assert!(identifier.identify(&ctx_empty).is_none());
    }

    #[test]
    fn test_header_identifier_case_sensitive() {
        let identifier = HeaderTenantIdentifier::new("X-Tenant-Id").case_sensitive();

        let ctx = RequestContext::new().with_header("X-Tenant-Id", "TenantA");
        assert_eq!(
            identifier.identify(&ctx).map(|t| t.0),
            Some("TenantA".to_string())
        );
    }

    #[test]
    fn test_username_prefix_identifier() {
        let identifier = UsernamePrefixIdentifier::with_dot();

        let ctx = RequestContext::new().with_username("tenant_a.admin");
        assert_eq!(
            identifier.identify(&ctx).map(|t| t.0),
            Some("tenant_a".to_string())
        );

        let ctx_no_prefix = RequestContext::new().with_username("admin");
        assert_eq!(
            identifier.identify(&ctx_no_prefix).map(|t| t.0),
            Some("admin".to_string())
        );

        let ctx_missing = RequestContext::new();
        assert!(identifier.identify(&ctx_missing).is_none());
    }

    #[test]
    fn test_database_name_identifier() {
        let identifier = DatabaseNameIdentifier::new()
            .strip_prefix("tenant_")
            .strip_suffix("_db");

        let ctx = RequestContext::new().with_database("tenant_acme_db");
        assert_eq!(
            identifier.identify(&ctx).map(|t| t.0),
            Some("acme".to_string())
        );

        let ctx_no_fix = RequestContext::new().with_database("mydb");
        assert_eq!(
            identifier.identify(&ctx_no_fix).map(|t| t.0),
            Some("mydb".to_string())
        );
    }

    #[test]
    fn test_sql_context_identifier() {
        let identifier = SqlContextIdentifier::default_variable();

        let ctx = RequestContext::new().with_sql_context("helios.tenant_id", "tenant_x");
        assert_eq!(
            identifier.identify(&ctx).map(|t| t.0),
            Some("tenant_x".to_string())
        );

        let ctx_missing = RequestContext::new();
        assert!(identifier.identify(&ctx_missing).is_none());
    }

    #[test]
    fn test_jwt_claim_identifier() {
        let identifier = JwtClaimIdentifier::new("tenant_id");

        // Create a simple JWT-like token (header.payload.signature)
        // Payload: {"tenant_id":"acme","sub":"user1"}
        use base64::Engine;
        let payload = r#"{"tenant_id":"acme","sub":"user1"}"#;
        let encoded_payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
        let token = format!("header.{}.signature", encoded_payload);

        let ctx = RequestContext::new().with_auth_token(&token);
        assert_eq!(
            identifier.identify(&ctx).map(|t| t.0),
            Some("acme".to_string())
        );
    }

    #[test]
    fn test_composite_identifier() {
        let identifier = CompositeIdentifier::new()
            .add(HeaderTenantIdentifier::new("X-Tenant-Id"))
            .add(UsernamePrefixIdentifier::with_dot());

        // Header takes precedence
        let ctx = RequestContext::new()
            .with_header("X-Tenant-Id", "header_tenant")
            .with_username("user_tenant.admin");
        assert_eq!(
            identifier.identify(&ctx).map(|t| t.0),
            Some("header_tenant".to_string())
        );

        // Falls back to username prefix
        let ctx_no_header = RequestContext::new().with_username("user_tenant.admin");
        assert_eq!(
            identifier.identify(&ctx_no_header).map(|t| t.0),
            Some("user_tenant".to_string())
        );

        // No match
        let ctx_empty = RequestContext::new();
        assert!(identifier.identify(&ctx_empty).is_none());
    }

    #[test]
    fn test_create_identifier() {
        let method = IdentificationMethod::header("X-Org-Id");
        let identifier = create_identifier(&method);
        assert_eq!(identifier.strategy_name(), "header");

        let method = IdentificationMethod::username_prefix('_');
        let identifier = create_identifier(&method);
        assert_eq!(identifier.strategy_name(), "username_prefix");
    }
}
