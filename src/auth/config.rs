//! Authentication Proxy Configuration
//!
//! Configuration types for authentication, authorization, and credential management.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Main authentication configuration
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Whether authentication is enabled
    pub enabled: bool,

    /// JWT configuration
    pub jwt: Option<JwtConfig>,

    /// OAuth configuration
    pub oauth: Option<OAuthConfig>,

    /// LDAP configuration
    pub ldap: Option<LdapConfig>,

    /// API key configuration
    pub api_keys: Option<ApiKeyConfig>,

    /// Role mapping rules
    pub role_mapping: Vec<RoleMappingRule>,

    /// Default role if no mapping matches
    pub default_role: Option<String>,

    /// Credential providers configuration
    pub credentials: CredentialConfig,

    /// Session configuration
    pub session: SessionConfig,

    /// Rate limiting for auth endpoints
    pub rate_limit: AuthRateLimitConfig,

    /// Ordered list of authentication methods to try
    pub auth_methods: Vec<AuthMethod>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            jwt: None,
            oauth: None,
            ldap: None,
            api_keys: None,
            role_mapping: Vec::new(),
            default_role: Some("db_minimal".to_string()),
            credentials: CredentialConfig::default(),
            session: SessionConfig::default(),
            rate_limit: AuthRateLimitConfig::default(),
            auth_methods: Vec::new(),
        }
    }
}

impl AuthConfig {
    /// Create an enabled config with JWT
    pub fn jwt(jwks_url: impl Into<String>) -> Self {
        Self {
            enabled: true,
            jwt: Some(JwtConfig::new(jwks_url)),
            ..Default::default()
        }
    }

    /// Create an enabled config with API keys
    pub fn api_keys() -> Self {
        Self {
            enabled: true,
            api_keys: Some(ApiKeyConfig::default()),
            ..Default::default()
        }
    }

    /// Builder for AuthConfig
    pub fn builder() -> AuthConfigBuilder {
        AuthConfigBuilder::new()
    }
}

/// Builder for AuthConfig
#[derive(Default)]
pub struct AuthConfigBuilder {
    config: AuthConfig,
}

impl AuthConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: AuthConfig {
                enabled: true,
                ..Default::default()
            },
        }
    }

    pub fn jwt(mut self, config: JwtConfig) -> Self {
        self.config.jwt = Some(config);
        self
    }

    pub fn oauth(mut self, config: OAuthConfig) -> Self {
        self.config.oauth = Some(config);
        self
    }

    pub fn ldap(mut self, config: LdapConfig) -> Self {
        self.config.ldap = Some(config);
        self
    }

    pub fn api_keys(mut self, config: ApiKeyConfig) -> Self {
        self.config.api_keys = Some(config);
        self
    }

    pub fn add_role_mapping(mut self, rule: RoleMappingRule) -> Self {
        self.config.role_mapping.push(rule);
        self
    }

    pub fn default_role(mut self, role: impl Into<String>) -> Self {
        self.config.default_role = Some(role.into());
        self
    }

    pub fn credentials(mut self, config: CredentialConfig) -> Self {
        self.config.credentials = config;
        self
    }

    pub fn session(mut self, config: SessionConfig) -> Self {
        self.config.session = config;
        self
    }

    pub fn build(self) -> AuthConfig {
        self.config
    }
}

/// JWT authentication configuration
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// JWKS URL for key fetching
    pub jwks_url: String,

    /// How often to refresh JWKS
    pub jwks_refresh_interval: Duration,

    /// Allowed token issuers
    pub allowed_issuers: HashSet<String>,

    /// Required audience claim
    pub required_audience: Option<String>,

    /// Clock skew tolerance
    pub clock_skew: Duration,

    /// Claim to use as user ID
    pub user_id_claim: String,

    /// Claim to use as roles
    pub roles_claim: Option<String>,

    /// Algorithm restrictions
    pub allowed_algorithms: Vec<String>,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            jwks_url: String::new(),
            jwks_refresh_interval: Duration::from_secs(3600),
            allowed_issuers: HashSet::new(),
            required_audience: None,
            clock_skew: Duration::from_secs(60),
            user_id_claim: "sub".to_string(),
            roles_claim: Some("roles".to_string()),
            allowed_algorithms: vec!["RS256".to_string(), "ES256".to_string()],
        }
    }
}

impl JwtConfig {
    pub fn new(jwks_url: impl Into<String>) -> Self {
        Self {
            jwks_url: jwks_url.into(),
            ..Default::default()
        }
    }

    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        self.allowed_issuers.insert(issuer.into());
        self
    }

    pub fn with_audience(mut self, audience: impl Into<String>) -> Self {
        self.required_audience = Some(audience.into());
        self
    }
}

/// OAuth introspection configuration
#[derive(Debug, Clone)]
pub struct OAuthConfig {
    /// Token introspection endpoint
    pub introspection_url: String,

    /// Client ID for introspection
    pub client_id: String,

    /// Client secret for introspection
    pub client_secret: String,

    /// Token endpoint (for client credentials)
    pub token_url: Option<String>,

    /// Scopes to request
    pub scopes: Vec<String>,

    /// Cache introspection results
    pub cache_ttl: Duration,

    /// Required scopes that must be present on a validated token
    pub required_scopes: Vec<String>,

    /// Token issuer identifier
    pub issuer: String,

    /// Authorization endpoint URL (for authorization code flow)
    pub authorization_url: Option<String>,

    /// Expected audience claim
    pub audience: Option<String>,
}

impl Default for OAuthConfig {
    fn default() -> Self {
        Self {
            introspection_url: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            token_url: None,
            scopes: Vec::new(),
            cache_ttl: Duration::from_secs(60),
            required_scopes: Vec::new(),
            issuer: String::new(),
            authorization_url: None,
            audience: None,
        }
    }
}

impl OAuthConfig {
    pub fn new(
        introspection_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        Self {
            introspection_url: introspection_url.into(),
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            ..Default::default()
        }
    }
}

/// LDAP authentication configuration
#[derive(Debug, Clone)]
pub struct LdapConfig {
    /// LDAP server URL
    pub server_url: String,

    /// Bind DN for searches
    pub bind_dn: String,

    /// Bind password
    pub bind_password: String,

    /// User search base
    pub user_search_base: String,

    /// User search filter (use {0} for username placeholder)
    pub user_filter: String,

    /// Group search base
    pub group_search_base: Option<String>,

    /// Group attribute to read
    pub group_attribute: String,

    /// Connection timeout
    pub timeout: Duration,

    /// Use STARTTLS
    pub starttls: bool,
}

impl Default for LdapConfig {
    fn default() -> Self {
        Self {
            server_url: "ldap://localhost:389".to_string(),
            bind_dn: String::new(),
            bind_password: String::new(),
            user_search_base: String::new(),
            user_filter: "(uid={0})".to_string(),
            group_search_base: None,
            group_attribute: "memberOf".to_string(),
            timeout: Duration::from_secs(10),
            starttls: false,
        }
    }
}

/// API key authentication configuration
#[derive(Debug, Clone)]
pub struct ApiKeyConfig {
    /// Header name to read API key from
    pub header_name: String,

    /// Query parameter name to read API key from (optional)
    pub query_param: Option<String>,

    /// Key prefix for generated keys (e.g., "hdb_")
    pub prefix: Option<String>,

    /// Hash algorithm for storage
    pub hash_algorithm: String,
}

impl Default for ApiKeyConfig {
    fn default() -> Self {
        Self {
            header_name: "X-API-Key".to_string(),
            query_param: None,
            prefix: Some("hpk_".to_string()),
            hash_algorithm: "sha256".to_string(),
        }
    }
}

/// Role mapping rule
#[derive(Debug, Clone)]
pub struct RoleMappingRule {
    /// Rule name for identification
    pub name: String,

    /// Condition to match
    pub condition: RoleCondition,

    /// Database role to assign
    pub db_role: String,

    /// Priority (higher = evaluated first)
    pub priority: i32,

    /// Roles to assign when this rule matches
    pub assign_roles: Vec<String>,

    /// Permissions granted by this rule
    pub permissions: Vec<String>,

    /// Conditions that must be met (using RoleMappingCondition)
    pub conditions: Vec<RoleMappingCondition>,
}

impl RoleMappingRule {
    pub fn new(condition: RoleCondition, db_role: impl Into<String>) -> Self {
        Self {
            name: String::new(),
            condition,
            db_role: db_role.into(),
            priority: 0,
            assign_roles: Vec::new(),
            permissions: Vec::new(),
            conditions: Vec::new(),
        }
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}

/// Conditions for role mapping
#[derive(Debug, Clone)]
pub enum RoleCondition {
    /// Match JWT claim value
    JwtClaim { name: String, value: String },

    /// Match any JWT claim value from list
    JwtClaimAny { name: String, values: Vec<String> },

    /// Match OAuth scope
    OAuthScope(String),

    /// Match group membership
    Group(String),

    /// Match email domain
    EmailDomain(String),

    /// Match tenant ID
    TenantId(String),

    /// Compound AND condition
    And(Vec<RoleCondition>),

    /// Compound OR condition
    Or(Vec<RoleCondition>),

    /// Always match (catch-all)
    Always,
}

/// Role mapping condition (alias for backward compatibility)
/// Provides a more expressive condition language for role mapping
#[derive(Debug, Clone)]
pub enum RoleMappingCondition {
    /// Match a specific claim value
    HasClaim { claim: String, value: Option<String> },

    /// Match group membership
    InGroup { group: String },

    /// Match existing role
    HasRole { role: String },

    /// Match tenant ID
    FromTenant { tenant_id: String },

    /// Match authentication method
    AuthMethod { method: String },

    /// Match email domain
    EmailDomain { domain: String },

    /// Match username pattern (supports wildcards)
    UsernamePattern { pattern: String },

    /// All conditions must match
    And { conditions: Vec<RoleMappingCondition> },

    /// Any condition must match
    Or { conditions: Vec<RoleMappingCondition> },

    /// Negate a condition
    Not { condition: Box<RoleMappingCondition> },
}

impl RoleMappingCondition {
    /// Create a HasClaim condition
    pub fn has_claim(claim: impl Into<String>, value: Option<String>) -> Self {
        Self::HasClaim {
            claim: claim.into(),
            value,
        }
    }

    /// Create an InGroup condition
    pub fn in_group(group: impl Into<String>) -> Self {
        Self::InGroup {
            group: group.into(),
        }
    }

    /// Create a HasRole condition
    pub fn has_role(role: impl Into<String>) -> Self {
        Self::HasRole {
            role: role.into(),
        }
    }

    /// Create an AuthMethod condition
    pub fn auth_method(method: impl Into<String>) -> Self {
        Self::AuthMethod {
            method: method.into(),
        }
    }
}

impl RoleCondition {
    pub fn jwt_claim(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::JwtClaim {
            name: name.into(),
            value: value.into(),
        }
    }

    pub fn group(name: impl Into<String>) -> Self {
        Self::Group(name.into())
    }

    pub fn email_domain(domain: impl Into<String>) -> Self {
        Self::EmailDomain(domain.into())
    }
}

/// Credential provider configuration
#[derive(Debug, Clone)]
pub struct CredentialConfig {
    /// Default credential provider
    pub default_provider: CredentialProvider,

    /// Static credentials
    pub static_credentials: HashMap<String, Credentials>,

    /// Vault configuration
    pub vault: Option<VaultConfig>,

    /// AWS Secrets Manager configuration
    pub aws_secrets: Option<AwsSecretsConfig>,

    /// Credential cache TTL
    pub cache_ttl: Duration,
}

impl Default for CredentialConfig {
    fn default() -> Self {
        Self {
            default_provider: CredentialProvider::Static,
            static_credentials: HashMap::new(),
            vault: None,
            aws_secrets: None,
            cache_ttl: Duration::from_secs(300),
        }
    }
}

/// Credential provider type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialProvider {
    Static,
    Vault,
    AwsSecrets,
}

/// Database credentials
#[derive(Debug, Clone)]
pub struct Credentials {
    /// Username
    pub username: String,

    /// Password
    pub password: String,

    /// Time-to-live (for dynamic credentials)
    pub ttl: Option<Duration>,

    /// Additional connection options
    pub options: HashMap<String, String>,
}

impl Credentials {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            ttl: None,
            options: HashMap::new(),
        }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

/// Vault configuration
#[derive(Debug, Clone)]
pub struct VaultConfig {
    /// Vault address
    pub address: String,

    /// Authentication method
    pub auth_method: VaultAuthMethod,

    /// Vault role
    pub role: String,

    /// Secret path prefix
    pub secret_path: String,

    /// TLS configuration
    pub tls_verify: bool,
}

/// Vault authentication method
#[derive(Debug, Clone)]
pub enum VaultAuthMethod {
    Token(String),
    Kubernetes { role: String },
    AppRole { role_id: String, secret_id: String },
}

/// AWS Secrets Manager configuration
#[derive(Debug, Clone)]
pub struct AwsSecretsConfig {
    /// AWS region
    pub region: String,

    /// Secret name prefix
    pub secret_prefix: String,

    /// Use IAM role for authentication
    pub use_iam_role: bool,
}

/// Session configuration
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Session timeout
    pub timeout: Duration,

    /// Maximum sessions per identity
    pub max_sessions_per_identity: usize,

    /// Maximum sessions per user (used by session manager)
    pub max_sessions_per_user: usize,

    /// Idle timeout — session expires after this duration of inactivity
    pub idle_timeout: Duration,

    /// Absolute timeout — maximum session lifetime regardless of activity
    pub absolute_timeout: Duration,

    /// Whether to use secure cookies
    pub secure_cookies: bool,

    /// Session variables to set
    pub session_vars: HashMap<String, String>,

    /// Extend session on activity
    pub extend_on_activity: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(3600),
            max_sessions_per_identity: 10,
            max_sessions_per_user: 10,
            idle_timeout: Duration::from_secs(1800),
            absolute_timeout: Duration::from_secs(86400),
            secure_cookies: true,
            session_vars: HashMap::new(),
            extend_on_activity: true,
        }
    }
}

/// Rate limiting configuration for auth
#[derive(Debug, Clone)]
pub struct AuthRateLimitConfig {
    /// Enable rate limiting
    pub enabled: bool,

    /// Max auth attempts per minute per IP
    pub max_attempts_per_ip: u32,

    /// Max auth failures per minute per IP
    pub max_failures_per_ip: u32,

    /// Lockout duration after too many failures
    pub lockout_duration: Duration,

    /// Rate limit window in seconds
    pub window_seconds: u64,

    /// Max requests per user within the window
    pub max_requests_per_user: u32,

    /// Max requests per IP within the window
    pub max_requests_per_ip: u32,
}

impl Default for AuthRateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts_per_ip: 60,
            max_failures_per_ip: 10,
            lockout_duration: Duration::from_secs(300),
            window_seconds: 60,
            max_requests_per_user: 120,
            max_requests_per_ip: 60,
        }
    }
}

/// Authentication type in request
#[derive(Debug, Clone)]
pub enum AuthType {
    /// JWT bearer token
    Jwt(String),

    /// OAuth access token
    OAuth(String),

    /// Basic auth (username/password)
    Basic { username: String, password: String },

    /// API key
    ApiKey(String),

    /// No authentication
    None,
}

/// Authentication method enum
/// Used for role mapping and audit logging
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthMethod {
    /// JWT-based authentication
    Jwt,

    /// OAuth token introspection
    OAuth,

    /// LDAP authentication
    Ldap,

    /// API key authentication
    ApiKey,

    /// HTTP Basic authentication
    Basic,

    /// Trust-based (internal services)
    Trust,

    /// Agent token authentication
    AgentToken,

    /// Session token authentication
    Session,

    /// No authentication (anonymous)
    Anonymous,
}

impl std::fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Jwt => write!(f, "jwt"),
            Self::OAuth => write!(f, "oauth"),
            Self::Ldap => write!(f, "ldap"),
            Self::ApiKey => write!(f, "api_key"),
            Self::Basic => write!(f, "basic"),
            Self::Trust => write!(f, "trust"),
            Self::AgentToken => write!(f, "agent_token"),
            Self::Session => write!(f, "session"),
            Self::Anonymous => write!(f, "anonymous"),
        }
    }
}

impl AuthMethod {
    /// Parse from string
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "jwt" => Some(Self::Jwt),
            "oauth" => Some(Self::OAuth),
            "ldap" => Some(Self::Ldap),
            "api_key" | "apikey" => Some(Self::ApiKey),
            "basic" => Some(Self::Basic),
            "trust" => Some(Self::Trust),
            "agent_token" | "agent" => Some(Self::AgentToken),
            "session" => Some(Self::Session),
            "anonymous" | "none" => Some(Self::Anonymous),
            _ => None,
        }
    }
}

/// Authenticated identity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    /// Unique user identifier
    pub user_id: String,

    /// Display name
    pub name: Option<String>,

    /// Email address
    pub email: Option<String>,

    /// Assigned roles
    pub roles: Vec<String>,

    /// Group memberships
    pub groups: Vec<String>,

    /// Tenant ID (for multi-tenancy)
    pub tenant_id: Option<String>,

    /// Additional claims/attributes
    pub claims: HashMap<String, serde_json::Value>,

    /// How the identity was authenticated
    pub auth_method: String,

    /// When authentication occurred
    pub authenticated_at: chrono::DateTime<chrono::Utc>,
}

impl Identity {
    /// Create a new identity
    pub fn new(user_id: impl Into<String>, auth_method: impl Into<String>) -> Self {
        Self {
            user_id: user_id.into(),
            name: None,
            email: None,
            roles: Vec::new(),
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: auth_method.into(),
            authenticated_at: chrono::Utc::now(),
        }
    }

    /// Create from JWT claims
    pub fn from_jwt_claims(claims: &JwtClaims) -> Self {
        let mut identity = Self::new(&claims.sub, "jwt");
        identity.name = claims.name.clone();
        identity.email = claims.email.clone();
        identity.roles = claims.roles.clone();
        identity.tenant_id = claims.tenant_id.clone();
        identity.claims = claims.custom.clone();
        identity
    }

    /// Check if identity has a role
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    /// Check if identity is in a group
    pub fn in_group(&self, group: &str) -> bool {
        self.groups.iter().any(|g| g == group)
    }

    /// Check if identity is admin
    pub fn is_admin(&self) -> bool {
        self.has_role("admin") || self.has_role("db_admin")
    }

    /// Get claim value
    pub fn get_claim(&self, name: &str) -> Option<&serde_json::Value> {
        self.claims.get(name)
    }

    /// Get email domain
    pub fn email_domain(&self) -> Option<&str> {
        self.email.as_ref().and_then(|e| e.split('@').nth(1))
    }

    /// Create an anonymous identity
    pub fn anonymous() -> Self {
        Self {
            user_id: "anonymous".to_string(),
            name: None,
            email: None,
            roles: Vec::new(),
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "anonymous".to_string(),
            authenticated_at: chrono::Utc::now(),
        }
    }
}

/// JWT claims structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject (user ID)
    pub sub: String,

    /// Issuer
    pub iss: String,

    /// Audience
    pub aud: Option<Vec<String>>,

    /// Expiration time
    pub exp: i64,

    /// Issued at
    pub iat: i64,

    /// Not before
    pub nbf: Option<i64>,

    /// JWT ID
    pub jti: Option<String>,

    /// User's name
    pub name: Option<String>,

    /// User's email
    pub email: Option<String>,

    /// User's roles
    #[serde(default)]
    pub roles: Vec<String>,

    /// Tenant ID
    pub tenant_id: Option<String>,

    /// Custom claims
    #[serde(flatten)]
    pub custom: HashMap<String, serde_json::Value>,
}

/// Agent-specific identity for AI workloads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentIdentity {
    /// Agent ID
    pub id: String,

    /// Agent type (e.g., "claude", "gpt", "custom")
    pub agent_type: String,

    /// Allowed tools
    pub allowed_tools: Vec<String>,

    /// Resource quota
    pub quota: AgentQuota,

    /// Conversation ID (optional)
    pub conversation_id: Option<String>,

    /// Parent identity (human that authorized the agent)
    pub parent_identity: Option<String>,
}

/// Agent resource quota
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentQuota {
    /// Maximum queries per conversation
    pub max_queries_per_conversation: u32,

    /// Maximum rows per query
    pub max_rows_per_query: u32,

    /// Token budget (for LLM cost tracking)
    pub token_budget: u64,

    /// Allowed tables
    pub allowed_tables: Option<Vec<String>>,
}

impl Default for AgentQuota {
    fn default() -> Self {
        Self {
            max_queries_per_conversation: 100,
            max_rows_per_query: 1000,
            token_budget: 100000,
            allowed_tables: None,
        }
    }
}

/// Tool permission for AI agents
#[derive(Debug, Clone)]
pub struct ToolPermission {
    /// Database role to use
    pub db_role: String,

    /// Tables allowed for this tool
    pub allowed_tables: Vec<String>,

    /// Read-only mode
    pub read_only: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_config_builder() {
        let config = AuthConfig::builder()
            .jwt(JwtConfig::new("https://auth.example.com/.well-known/jwks.json"))
            .add_role_mapping(RoleMappingRule::new(
                RoleCondition::jwt_claim("role", "admin"),
                "db_admin",
            ).with_priority(100))
            .default_role("db_minimal")
            .build();

        assert!(config.enabled);
        assert!(config.jwt.is_some());
        assert_eq!(config.role_mapping.len(), 1);
    }

    #[test]
    fn test_identity() {
        let identity = Identity::new("user123", "jwt");
        assert_eq!(identity.user_id, "user123");
        assert_eq!(identity.auth_method, "jwt");
        assert!(!identity.is_admin());
    }

    #[test]
    fn test_identity_roles() {
        let mut identity = Identity::new("admin123", "jwt");
        identity.roles = vec!["admin".to_string(), "db_readwrite".to_string()];

        assert!(identity.is_admin());
        assert!(identity.has_role("admin"));
        assert!(identity.has_role("db_readwrite"));
        assert!(!identity.has_role("superuser"));
    }

    #[test]
    fn test_email_domain() {
        let mut identity = Identity::new("user", "jwt");
        identity.email = Some("alice@example.com".to_string());

        assert_eq!(identity.email_domain(), Some("example.com"));
    }

    #[test]
    fn test_credentials() {
        let creds = Credentials::new("dbuser", "password123")
            .with_ttl(Duration::from_secs(3600));

        assert_eq!(creds.username, "dbuser");
        assert!(creds.ttl.is_some());
    }

    #[test]
    fn test_role_mapping() {
        let rule = RoleMappingRule::new(
            RoleCondition::group("developers"),
            "db_readwrite",
        ).with_priority(50);

        assert_eq!(rule.db_role, "db_readwrite");
        assert_eq!(rule.priority, 50);
    }
}
