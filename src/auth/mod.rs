//! Authentication Proxy Module
//!
//! Provides comprehensive authentication and authorization for HeliosProxy.
//!
//! # Features
//!
//! - **JWT Validation**: JWKS-based JWT token validation
//! - **OAuth Introspection**: RFC 7662 token introspection
//! - **API Key Management**: Generate, validate, and revoke API keys
//! - **Role Mapping**: Map identities to database roles
//! - **Credential Providers**: Vault, AWS Secrets Manager, environment
//! - **Session Management**: Token-based session handling
//!
//! # Architecture
//!
//! ```text
//!                    ┌─────────────────────────────────────┐
//!                    │      AuthenticationHandler          │
//!                    │  (Main entry point for auth)        │
//!                    └─────────────┬───────────────────────┘
//!                                  │
//!          ┌───────────────────────┼───────────────────────┐
//!          │                       │                       │
//!   ┌──────▼──────┐        ┌───────▼───────┐       ┌───────▼───────┐
//!   │ JwtValidator│        │  OAuthClient  │       │ ApiKeyManager │
//!   │  (JWKS)     │        │(Introspection)│       │ (Key mgmt)    │
//!   └─────────────┘        └───────────────┘       └───────────────┘
//!          │                       │                       │
//!          └───────────────────────┼───────────────────────┘
//!                                  │
//!                    ┌─────────────▼───────────────────────┐
//!                    │          Identity                    │
//!                    │  (Unified user representation)       │
//!                    └─────────────┬───────────────────────┘
//!                                  │
//!                    ┌─────────────▼───────────────────────┐
//!                    │          RoleMapper                  │
//!                    │  (Identity → Database Roles)         │
//!                    └─────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use heliosdb::proxy::auth::{
//!     AuthenticationHandler, AuthRequest, JwtConfig,
//!     RoleMapper, SessionManager,
//! };
//!
//! // Create authentication handler
//! let handler = AuthenticationHandler::builder()
//!     .enabled(true)
//!     .with_jwt(JwtConfig::new("https://auth.example.com/.well-known/jwks.json"))
//!     .with_api_keys(ApiKeyConfig::default())
//!     .default_role("db_user")
//!     .build();
//!
//! // Authenticate a request
//! let request = AuthRequest::new()
//!     .with_header("Authorization", "Bearer eyJ...");
//!
//! let result = handler.authenticate(&request).await?;
//! println!("Authenticated: {}", result.identity.user_id);
//!
//! // Map to database roles
//! let mapper = RoleMapper::builder()
//!     .group_role("admins", "db_admin")
//!     .group_role("developers", "db_developer")
//!     .default_role("db_readonly")
//!     .build();
//!
//! let roles = mapper.map_roles(&result.identity);
//! ```
//!
//! # AI/Agent Authentication
//!
//! HeliosProxy supports special authentication patterns for AI agents:
//!
//! - **Agent Tokens**: Short-lived tokens with conversation scope
//! - **Tool Authorization**: Role-based tool access control
//! - **Quota Management**: Per-agent resource quotas
//!
//! ```rust,ignore
//! use heliosdb::proxy::auth::{AgentIdentity, AgentQuota};
//!
//! let agent_identity = AgentIdentity {
//!     agent_id: "claude-code".to_string(),
//!     parent_user_id: "user123".to_string(),
//!     conversation_id: Some("conv_abc".to_string()),
//!     allowed_tools: vec!["query", "insert".to_string()],
//!     quota: AgentQuota::default(),
//! };
//! ```

pub mod api_keys;
pub mod config;
pub mod credentials;
pub mod handler;
pub mod jwt;
pub mod oauth;
pub mod role_mapper;
pub mod session;

// Re-export main types
pub use config::{
    AgentIdentity, AgentQuota, ApiKeyConfig, AuthConfig, AuthMethod, AuthRateLimitConfig,
    CredentialConfig, Identity, JwtClaims, JwtConfig, LdapConfig, OAuthConfig,
    RoleMappingCondition, RoleMappingRule, SessionConfig,
};

pub use jwt::{Jwk, Jwks, JwtError, JwtHeader, JwtValidator, TokenCache};

pub use handler::{
    AuthError, AuthRequest, AuthResult, AuthenticationHandler, AuthenticationHandlerBuilder,
    CacheStats,
};

pub use oauth::{IntrospectionResponse, OAuthClient, OAuthError, TokenExchange, TokenResponse};

pub use api_keys::{ApiKey, ApiKeyBuilder, ApiKeyError, ApiKeyManager, ApiKeyStats};

pub use role_mapper::{
    AuthorizationContext, Operation, PermissionSet, RoleMapper, RoleMapperBuilder,
};

pub use credentials::{
    AwsSecretsManagerProvider, CredentialError, CredentialManager, CredentialManagerBuilder,
    CredentialProvider, CredentialSource, DatabaseCredential, EnvironmentCredentialProvider,
    StaticCredentialProvider, VaultCredentialProvider,
};

pub use session::{
    CookieOptions, SameSite, Session, SessionError, SessionManager, SessionManagerBuilder,
    SessionStats,
};

/// Authentication proxy facade
///
/// High-level facade that combines authentication, authorization, and
/// session management into a single interface.
pub struct AuthProxy {
    /// Authentication handler
    handler: AuthenticationHandler,

    /// Role mapper
    role_mapper: RoleMapper,

    /// Session manager
    session_manager: SessionManager,

    /// Credential manager
    credential_manager: Option<CredentialManager>,
}

impl AuthProxy {
    /// Create a new auth proxy
    pub fn new(
        handler: AuthenticationHandler,
        role_mapper: RoleMapper,
        session_manager: SessionManager,
    ) -> Self {
        Self {
            handler,
            role_mapper,
            session_manager,
            credential_manager: None,
        }
    }

    /// Create a builder
    pub fn builder() -> AuthProxyBuilder {
        AuthProxyBuilder::new()
    }

    /// Authenticate a request
    pub async fn authenticate(&self, request: &AuthRequest) -> Result<AuthResult, AuthError> {
        self.handler.authenticate(request).await
    }

    /// Authenticate and create session
    pub async fn authenticate_and_create_session(
        &self,
        request: &AuthRequest,
    ) -> Result<(AuthResult, Session), AuthError> {
        let result = self.handler.authenticate(request).await?;

        let session = self
            .session_manager
            .create_session(
                result.identity.clone(),
                request.client_ip,
                request.headers.get("user-agent").cloned(),
            )
            .map_err(|e| AuthError::Session(e.to_string()))?;

        Ok((result, session))
    }

    /// Validate session token
    pub fn validate_session(&self, token: &str) -> Result<Identity, AuthError> {
        self.session_manager
            .validate_token(token)
            .map_err(|e| AuthError::Session(e.to_string()))
    }

    /// Map identity to database roles
    pub fn map_roles(&self, identity: &Identity) -> Vec<String> {
        self.role_mapper.map_roles(identity)
    }

    /// Get database credentials for an identity
    pub fn get_credentials(&self, key: &str) -> Result<DatabaseCredential, AuthError> {
        self.credential_manager
            .as_ref()
            .ok_or_else(|| {
                AuthError::Configuration("Credential manager not configured".to_string())
            })?
            .get_credential(key)
            .map_err(|e| AuthError::Configuration(e.to_string()))
    }

    /// Invalidate session
    pub fn invalidate_session(&self, token: &str) -> Result<(), AuthError> {
        self.session_manager
            .invalidate_session(token)
            .map_err(|e| AuthError::Session(e.to_string()))
    }

    /// Get authentication handler
    pub fn handler(&self) -> &AuthenticationHandler {
        &self.handler
    }

    /// Get role mapper
    pub fn role_mapper(&self) -> &RoleMapper {
        &self.role_mapper
    }

    /// Get session manager
    pub fn session_manager(&self) -> &SessionManager {
        &self.session_manager
    }
}

/// Auth proxy builder
pub struct AuthProxyBuilder {
    handler: Option<AuthenticationHandler>,
    role_mapper: Option<RoleMapper>,
    session_manager: Option<SessionManager>,
    credential_manager: Option<CredentialManager>,
}

impl AuthProxyBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            handler: None,
            role_mapper: None,
            session_manager: None,
            credential_manager: None,
        }
    }

    /// Set authentication handler
    pub fn handler(mut self, handler: AuthenticationHandler) -> Self {
        self.handler = Some(handler);
        self
    }

    /// Set role mapper
    pub fn role_mapper(mut self, mapper: RoleMapper) -> Self {
        self.role_mapper = Some(mapper);
        self
    }

    /// Set session manager
    pub fn session_manager(mut self, manager: SessionManager) -> Self {
        self.session_manager = Some(manager);
        self
    }

    /// Set credential manager
    pub fn credential_manager(mut self, manager: CredentialManager) -> Self {
        self.credential_manager = Some(manager);
        self
    }

    /// Build the auth proxy
    pub fn build(self) -> AuthProxy {
        let handler = self
            .handler
            .unwrap_or_else(|| AuthenticationHandler::builder().enabled(false).build());

        let role_mapper = self.role_mapper.unwrap_or_else(|| RoleMapper::new());

        let session_manager = self
            .session_manager
            .unwrap_or_else(|| SessionManager::new(SessionConfig::default()));

        let mut proxy = AuthProxy::new(handler, role_mapper, session_manager);
        proxy.credential_manager = self.credential_manager;
        proxy
    }
}

impl Default for AuthProxyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_identity() -> Identity {
        Identity {
            user_id: "testuser".to_string(),
            name: Some("Test User".to_string()),
            email: Some("test@example.com".to_string()),
            roles: vec!["user".to_string()],
            groups: vec!["developers".to_string()],
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "test".to_string(),
            authenticated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_auth_proxy_builder() {
        let proxy = AuthProxy::builder()
            .handler(AuthenticationHandler::builder().enabled(false).build())
            .role_mapper(RoleMapper::builder().default_role("user").build())
            .session_manager(SessionManager::new(SessionConfig::default()))
            .build();

        assert!(!proxy.handler().is_enabled());
    }

    #[test]
    fn test_auth_proxy_map_roles() {
        let proxy = AuthProxy::builder()
            .role_mapper(
                RoleMapper::builder()
                    .group_role("developers", "db_developer")
                    .build(),
            )
            .build();

        let roles = proxy.map_roles(&test_identity());
        assert!(roles.contains(&"db_developer".to_string()));
    }

    #[tokio::test]
    async fn test_auth_proxy_disabled() {
        let proxy = AuthProxy::builder().build();

        let request = AuthRequest::new();
        let result = proxy.authenticate(&request).await.unwrap();

        assert_eq!(result.identity.auth_method, "anonymous");
    }

    #[test]
    fn test_session_integration() {
        let proxy = AuthProxy::builder()
            .session_manager(SessionManager::builder().max_sessions_per_user(5).build())
            .build();

        // Create session directly via session manager
        let session = proxy
            .session_manager()
            .create_session(test_identity(), None, None)
            .unwrap();

        // Validate via proxy
        let identity = proxy.validate_session(&session.token).unwrap();
        assert_eq!(identity.user_id, "testuser");

        // Invalidate via proxy
        proxy.invalidate_session(&session.token).unwrap();
        assert!(proxy.validate_session(&session.token).is_err());
    }
}
