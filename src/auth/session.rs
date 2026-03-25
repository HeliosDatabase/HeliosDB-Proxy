//! Session Management
//!
//! Manages authenticated sessions with token generation, validation, and lifecycle.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use thiserror::Error;

use super::config::{Identity, SessionConfig};

/// Session errors
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("Session not found")]
    NotFound,

    #[error("Session expired")]
    Expired,

    #[error("Session invalidated")]
    Invalidated,

    #[error("Session limit exceeded")]
    LimitExceeded,

    #[error("Token generation failed")]
    TokenGenerationFailed,

    #[error("Invalid token format")]
    InvalidTokenFormat,
}

/// Session information
#[derive(Debug, Clone)]
pub struct Session {
    /// Session ID
    pub id: String,

    /// Session token
    pub token: String,

    /// Associated identity
    pub identity: Identity,

    /// Creation time
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// Last activity time
    pub last_activity: chrono::DateTime<chrono::Utc>,

    /// Expiration time
    pub expires_at: chrono::DateTime<chrono::Utc>,

    /// Absolute expiration (max session lifetime)
    pub absolute_expires_at: chrono::DateTime<chrono::Utc>,

    /// Client IP address
    pub client_ip: Option<std::net::IpAddr>,

    /// User agent
    pub user_agent: Option<String>,

    /// Session metadata
    pub metadata: HashMap<String, String>,

    /// Whether session is active
    pub active: bool,
}

impl Session {
    /// Check if session is expired
    pub fn is_expired(&self) -> bool {
        let now = chrono::Utc::now();
        now > self.expires_at || now > self.absolute_expires_at
    }

    /// Check if session is valid
    pub fn is_valid(&self) -> bool {
        self.active && !self.is_expired()
    }

    /// Get remaining time
    pub fn remaining_time(&self) -> Option<Duration> {
        let now = chrono::Utc::now();
        let expires = self.expires_at.min(self.absolute_expires_at);

        if expires > now {
            (expires - now).to_std().ok()
        } else {
            None
        }
    }

    /// Get session duration
    pub fn duration(&self) -> Duration {
        let now = chrono::Utc::now();
        (now - self.created_at).to_std().unwrap_or(Duration::ZERO)
    }
}

/// Session manager
pub struct SessionManager {
    /// Configuration
    config: SessionConfig,

    /// Active sessions by ID
    sessions: Arc<RwLock<HashMap<String, Session>>>,

    /// Session lookup by token
    tokens: Arc<RwLock<HashMap<String, String>>>,

    /// Sessions by user
    user_sessions: Arc<RwLock<HashMap<String, Vec<String>>>>,

    /// Last cleanup time
    last_cleanup: Arc<RwLock<Instant>>,
}

impl SessionManager {
    /// Create a new session manager
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            tokens: Arc::new(RwLock::new(HashMap::new())),
            user_sessions: Arc::new(RwLock::new(HashMap::new())),
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Create a builder
    pub fn builder() -> SessionManagerBuilder {
        SessionManagerBuilder::new()
    }

    /// Create a new session
    pub fn create_session(
        &self,
        identity: Identity,
        client_ip: Option<std::net::IpAddr>,
        user_agent: Option<String>,
    ) -> Result<Session, SessionError> {
        // Check session limit
        if self.config.max_sessions_per_user > 0 {
            let user_sessions = self.user_sessions.read();
            if let Some(sessions) = user_sessions.get(&identity.user_id) {
                if sessions.len() >= self.config.max_sessions_per_user {
                    return Err(SessionError::LimitExceeded);
                }
            }
        }

        // Generate session ID and token
        let session_id = self.generate_session_id();
        let token = self.generate_token();

        let now = chrono::Utc::now();
        let expires_at = now + chrono::Duration::from_std(self.config.idle_timeout)
            .unwrap_or(chrono::Duration::hours(1));
        let absolute_expires_at = now + chrono::Duration::from_std(self.config.absolute_timeout)
            .unwrap_or(chrono::Duration::hours(24));

        let session = Session {
            id: session_id.clone(),
            token: token.clone(),
            identity: identity.clone(),
            created_at: now,
            last_activity: now,
            expires_at,
            absolute_expires_at,
            client_ip,
            user_agent,
            metadata: HashMap::new(),
            active: true,
        };

        // Store session
        self.sessions.write().insert(session_id.clone(), session.clone());
        self.tokens.write().insert(token.clone(), session_id.clone());
        self.user_sessions.write()
            .entry(identity.user_id.clone())
            .or_insert_with(Vec::new)
            .push(session_id);

        // Cleanup old sessions periodically
        self.maybe_cleanup();

        Ok(session)
    }

    /// Get session by token
    pub fn get_session(&self, token: &str) -> Result<Session, SessionError> {
        let session_id = self.tokens.read()
            .get(token)
            .cloned()
            .ok_or(SessionError::NotFound)?;

        let session = self.sessions.read()
            .get(&session_id)
            .cloned()
            .ok_or(SessionError::NotFound)?;

        if !session.active {
            return Err(SessionError::Invalidated);
        }

        if session.is_expired() {
            return Err(SessionError::Expired);
        }

        Ok(session)
    }

    /// Get session by ID
    pub fn get_session_by_id(&self, session_id: &str) -> Result<Session, SessionError> {
        let session = self.sessions.read()
            .get(session_id)
            .cloned()
            .ok_or(SessionError::NotFound)?;

        if !session.active {
            return Err(SessionError::Invalidated);
        }

        if session.is_expired() {
            return Err(SessionError::Expired);
        }

        Ok(session)
    }

    /// Validate token and return identity
    pub fn validate_token(&self, token: &str) -> Result<Identity, SessionError> {
        let session = self.get_session(token)?;
        Ok(session.identity)
    }

    /// Refresh session (extend expiration)
    pub fn refresh_session(&self, token: &str) -> Result<Session, SessionError> {
        let session_id = self.tokens.read()
            .get(token)
            .cloned()
            .ok_or(SessionError::NotFound)?;

        let mut sessions = self.sessions.write();
        let session = sessions.get_mut(&session_id)
            .ok_or(SessionError::NotFound)?;

        if !session.active {
            return Err(SessionError::Invalidated);
        }

        if session.is_expired() {
            return Err(SessionError::Expired);
        }

        // Update activity time and expiration
        let now = chrono::Utc::now();
        session.last_activity = now;

        // Extend idle timeout but not beyond absolute timeout
        let new_expires = now + chrono::Duration::from_std(self.config.idle_timeout)
            .unwrap_or(chrono::Duration::hours(1));
        session.expires_at = new_expires.min(session.absolute_expires_at);

        Ok(session.clone())
    }

    /// Invalidate a session
    pub fn invalidate_session(&self, token: &str) -> Result<(), SessionError> {
        let session_id = self.tokens.read()
            .get(token)
            .cloned()
            .ok_or(SessionError::NotFound)?;

        self.invalidate_session_by_id(&session_id)
    }

    /// Invalidate session by ID
    pub fn invalidate_session_by_id(&self, session_id: &str) -> Result<(), SessionError> {
        let mut sessions = self.sessions.write();
        let session = sessions.get_mut(session_id)
            .ok_or(SessionError::NotFound)?;

        session.active = false;

        // Remove from token lookup
        self.tokens.write().remove(&session.token);

        // Remove from user sessions
        let user_id = session.identity.user_id.clone();
        let mut user_sessions = self.user_sessions.write();
        if let Some(sessions) = user_sessions.get_mut(&user_id) {
            sessions.retain(|id| id != session_id);
        }

        Ok(())
    }

    /// Invalidate all sessions for a user
    pub fn invalidate_user_sessions(&self, user_id: &str) {
        let session_ids: Vec<String> = self.user_sessions.read()
            .get(user_id)
            .cloned()
            .unwrap_or_default();

        for session_id in session_ids {
            let _ = self.invalidate_session_by_id(&session_id);
        }
    }

    /// List all sessions for a user
    pub fn list_user_sessions(&self, user_id: &str) -> Vec<Session> {
        let session_ids: Vec<String> = self.user_sessions.read()
            .get(user_id)
            .cloned()
            .unwrap_or_default();

        let sessions = self.sessions.read();
        session_ids.iter()
            .filter_map(|id| sessions.get(id).cloned())
            .filter(|s| s.is_valid())
            .collect()
    }

    /// Update session metadata
    pub fn update_metadata(
        &self,
        token: &str,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), SessionError> {
        let session_id = self.tokens.read()
            .get(token)
            .cloned()
            .ok_or(SessionError::NotFound)?;

        let mut sessions = self.sessions.write();
        let session = sessions.get_mut(&session_id)
            .ok_or(SessionError::NotFound)?;

        session.metadata.insert(key.into(), value.into());
        Ok(())
    }

    /// Get session statistics
    pub fn stats(&self) -> SessionStats {
        let sessions = self.sessions.read();
        let active = sessions.values().filter(|s| s.is_valid()).count();
        let expired = sessions.values().filter(|s| s.is_expired()).count();
        let invalidated = sessions.values().filter(|s| !s.active).count();

        SessionStats {
            total: sessions.len(),
            active,
            expired,
            invalidated,
        }
    }

    /// Cleanup expired sessions
    pub fn cleanup(&self) {
        let expired_ids: Vec<String> = {
            let sessions = self.sessions.read();
            sessions.iter()
                .filter(|(_, s)| s.is_expired() || !s.active)
                .map(|(id, _)| id.clone())
                .collect()
        };

        for id in expired_ids {
            let _ = self.invalidate_session_by_id(&id);
            self.sessions.write().remove(&id);
        }

        *self.last_cleanup.write() = Instant::now();
    }

    /// Maybe run cleanup if enough time has passed
    fn maybe_cleanup(&self) {
        let should_cleanup = {
            let last = self.last_cleanup.read();
            last.elapsed() > Duration::from_secs(60)
        };

        if should_cleanup {
            self.cleanup();
        }
    }

    /// Generate a session ID
    fn generate_session_id(&self) -> String {
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

        format!("sess_{:016x}{:016x}", hash1, hash2)
    }

    /// Generate a session token
    fn generate_token(&self) -> String {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u128(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos());

        let mut token_bytes = Vec::new();
        for _ in 0..4 {
            hasher.write_u64(hasher.finish());
            token_bytes.extend_from_slice(&hasher.finish().to_le_bytes());
        }

        // Encode as URL-safe base64
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        URL_SAFE_NO_PAD.encode(&token_bytes)
    }
}

/// Session statistics
#[derive(Debug, Clone)]
pub struct SessionStats {
    /// Total sessions
    pub total: usize,

    /// Active sessions
    pub active: usize,

    /// Expired sessions
    pub expired: usize,

    /// Invalidated sessions
    pub invalidated: usize,
}

/// Session manager builder
pub struct SessionManagerBuilder {
    config: SessionConfig,
}

impl SessionManagerBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            config: SessionConfig::default(),
        }
    }

    /// Set idle timeout
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.config.idle_timeout = timeout;
        self
    }

    /// Set absolute timeout
    pub fn absolute_timeout(mut self, timeout: Duration) -> Self {
        self.config.absolute_timeout = timeout;
        self
    }

    /// Set max sessions per user
    pub fn max_sessions_per_user(mut self, max: usize) -> Self {
        self.config.max_sessions_per_user = max;
        self
    }

    /// Enable secure cookies
    pub fn secure_cookies(mut self, secure: bool) -> Self {
        self.config.secure_cookies = secure;
        self
    }

    /// Build the session manager
    pub fn build(self) -> SessionManager {
        SessionManager::new(self.config)
    }
}

impl Default for SessionManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Session cookie options
#[derive(Debug, Clone)]
pub struct CookieOptions {
    /// Cookie name
    pub name: String,

    /// Cookie path
    pub path: String,

    /// Cookie domain
    pub domain: Option<String>,

    /// Secure flag
    pub secure: bool,

    /// HttpOnly flag
    pub http_only: bool,

    /// SameSite attribute
    pub same_site: SameSite,

    /// Max age
    pub max_age: Option<Duration>,
}

/// SameSite attribute
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SameSite {
    Strict,
    Lax,
    None,
}

impl Default for CookieOptions {
    fn default() -> Self {
        Self {
            name: "session".to_string(),
            path: "/".to_string(),
            domain: None,
            secure: true,
            http_only: true,
            same_site: SameSite::Lax,
            max_age: None,
        }
    }
}

impl CookieOptions {
    /// Build Set-Cookie header value
    pub fn to_set_cookie_header(&self, token: &str) -> String {
        let mut parts = vec![
            format!("{}={}", self.name, token),
            format!("Path={}", self.path),
        ];

        if let Some(domain) = &self.domain {
            parts.push(format!("Domain={}", domain));
        }

        if self.secure {
            parts.push("Secure".to_string());
        }

        if self.http_only {
            parts.push("HttpOnly".to_string());
        }

        parts.push(match self.same_site {
            SameSite::Strict => "SameSite=Strict".to_string(),
            SameSite::Lax => "SameSite=Lax".to_string(),
            SameSite::None => "SameSite=None".to_string(),
        });

        if let Some(max_age) = self.max_age {
            parts.push(format!("Max-Age={}", max_age.as_secs()));
        }

        parts.join("; ")
    }

    /// Build deletion cookie header
    pub fn to_delete_cookie_header(&self) -> String {
        let mut parts = vec![
            format!("{}=", self.name),
            format!("Path={}", self.path),
            "Max-Age=0".to_string(),
            "Expires=Thu, 01 Jan 1970 00:00:00 GMT".to_string(),
        ];

        if let Some(domain) = &self.domain {
            parts.push(format!("Domain={}", domain));
        }

        parts.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_identity() -> Identity {
        Identity {
            user_id: "user123".to_string(),
            name: Some("Test User".to_string()),
            email: Some("test@example.com".to_string()),
            roles: vec!["user".to_string()],
            groups: Vec::new(),
            tenant_id: None,
            claims: HashMap::new(),
            auth_method: "test".to_string(),
            authenticated_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_create_session() {
        let manager = SessionManager::builder()
            .idle_timeout(Duration::from_secs(3600))
            .absolute_timeout(Duration::from_secs(86400))
            .build();

        let session = manager.create_session(
            test_identity(),
            None,
            Some("Test Agent".to_string()),
        ).unwrap();

        assert!(session.is_valid());
        assert!(session.active);
        assert!(!session.is_expired());
    }

    #[test]
    fn test_get_session() {
        let manager = SessionManager::new(SessionConfig::default());

        let session = manager.create_session(test_identity(), None, None).unwrap();
        let token = session.token.clone();

        let retrieved = manager.get_session(&token).unwrap();
        assert_eq!(retrieved.id, session.id);
    }

    #[test]
    fn test_validate_token() {
        let manager = SessionManager::new(SessionConfig::default());

        let session = manager.create_session(test_identity(), None, None).unwrap();
        let identity = manager.validate_token(&session.token).unwrap();

        assert_eq!(identity.user_id, "user123");
    }

    #[test]
    fn test_refresh_session() {
        let manager = SessionManager::new(SessionConfig::default());

        let session = manager.create_session(test_identity(), None, None).unwrap();
        let original_expires = session.expires_at;

        std::thread::sleep(Duration::from_millis(10));

        let refreshed = manager.refresh_session(&session.token).unwrap();
        assert!(refreshed.last_activity > session.last_activity);
    }

    #[test]
    fn test_invalidate_session() {
        let manager = SessionManager::new(SessionConfig::default());

        let session = manager.create_session(test_identity(), None, None).unwrap();
        manager.invalidate_session(&session.token).unwrap();

        assert!(manager.get_session(&session.token).is_err());
    }

    #[test]
    fn test_session_limit() {
        let manager = SessionManager::builder()
            .max_sessions_per_user(2)
            .build();

        let _ = manager.create_session(test_identity(), None, None).unwrap();
        let _ = manager.create_session(test_identity(), None, None).unwrap();

        let result = manager.create_session(test_identity(), None, None);
        assert!(matches!(result, Err(SessionError::LimitExceeded)));
    }

    #[test]
    fn test_list_user_sessions() {
        let manager = SessionManager::new(SessionConfig::default());

        let _ = manager.create_session(test_identity(), None, None).unwrap();
        let _ = manager.create_session(test_identity(), None, None).unwrap();

        let sessions = manager.list_user_sessions("user123");
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn test_invalidate_user_sessions() {
        let manager = SessionManager::new(SessionConfig::default());

        let s1 = manager.create_session(test_identity(), None, None).unwrap();
        let s2 = manager.create_session(test_identity(), None, None).unwrap();

        manager.invalidate_user_sessions("user123");

        assert!(manager.get_session(&s1.token).is_err());
        assert!(manager.get_session(&s2.token).is_err());
    }

    #[test]
    fn test_session_stats() {
        let manager = SessionManager::new(SessionConfig::default());

        let _ = manager.create_session(test_identity(), None, None).unwrap();
        let s2 = manager.create_session(test_identity(), None, None).unwrap();
        manager.invalidate_session(&s2.token).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.total, 2);
        assert_eq!(stats.active, 1);
    }

    #[test]
    fn test_update_metadata() {
        let manager = SessionManager::new(SessionConfig::default());

        let session = manager.create_session(test_identity(), None, None).unwrap();
        manager.update_metadata(&session.token, "key", "value").unwrap();

        let updated = manager.get_session(&session.token).unwrap();
        assert_eq!(updated.metadata.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_cookie_options() {
        let options = CookieOptions {
            name: "session".to_string(),
            path: "/".to_string(),
            domain: Some("example.com".to_string()),
            secure: true,
            http_only: true,
            same_site: SameSite::Strict,
            max_age: Some(Duration::from_secs(3600)),
        };

        let header = options.to_set_cookie_header("token123");

        assert!(header.contains("session=token123"));
        assert!(header.contains("Path=/"));
        assert!(header.contains("Domain=example.com"));
        assert!(header.contains("Secure"));
        assert!(header.contains("HttpOnly"));
        assert!(header.contains("SameSite=Strict"));
        assert!(header.contains("Max-Age=3600"));
    }

    #[test]
    fn test_delete_cookie() {
        let options = CookieOptions::default();
        let header = options.to_delete_cookie_header();

        assert!(header.contains("session="));
        assert!(header.contains("Max-Age=0"));
        assert!(header.contains("Expires=Thu, 01 Jan 1970"));
    }

    #[test]
    fn test_session_remaining_time() {
        let manager = SessionManager::builder()
            .idle_timeout(Duration::from_secs(3600))
            .build();

        let session = manager.create_session(test_identity(), None, None).unwrap();

        let remaining = session.remaining_time().unwrap();
        assert!(remaining > Duration::from_secs(3500)); // Should be close to 1 hour
        assert!(remaining <= Duration::from_secs(3600));
    }
}
