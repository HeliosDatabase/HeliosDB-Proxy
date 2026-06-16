//! Agent Retry Strategies and Conversation Fallback
//!
//! Provides retry guidance for AI agents and maintains conversation context
//! during circuit breaker outages.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

/// Decision from retry strategy
#[derive(Debug, Clone)]
pub enum RetryDecision {
    /// Retry after the specified delay
    Retry { delay: Duration, attempt: u32 },
    /// Don't retry, fail immediately
    Fail { reason: String },
    /// Use fallback/cached data
    Fallback { message: String },
}

impl RetryDecision {
    /// Check if should retry
    pub fn should_retry(&self) -> bool {
        matches!(self, RetryDecision::Retry { .. })
    }

    /// Get retry delay if applicable
    pub fn retry_delay(&self) -> Option<Duration> {
        match self {
            RetryDecision::Retry { delay, .. } => Some(*delay),
            _ => None,
        }
    }

    /// Generate LLM-friendly message
    pub fn to_llm_message(&self) -> String {
        match self {
            RetryDecision::Retry { delay, attempt } => {
                format!(
                    r#"{{"action":"retry","delay_ms":{},"attempt":{},"message":"Request failed temporarily. Retry in {} milliseconds."}}"#,
                    delay.as_millis(),
                    attempt,
                    delay.as_millis()
                )
            }
            RetryDecision::Fail { reason } => {
                format!(
                    r#"{{"action":"fail","reason":"{}","message":"Request cannot be retried: {}"}}"#,
                    reason, reason
                )
            }
            RetryDecision::Fallback { message } => {
                format!(
                    r#"{{"action":"fallback","message":"{}","note":"Using cached or fallback data due to temporary outage."}}"#,
                    message
                )
            }
        }
    }
}

/// Retry strategy for AI agents
#[derive(Debug, Clone)]
pub struct AgentRetryStrategy {
    /// Base delay for exponential backoff
    base_delay: Duration,
    /// Maximum delay
    max_delay: Duration,
    /// Maximum retry attempts
    max_attempts: u32,
    /// Jitter factor (0.0 - 1.0)
    jitter_factor: f64,
    /// Retryable error patterns
    retryable_patterns: Vec<String>,
    /// Non-retryable error patterns
    non_retryable_patterns: Vec<String>,
}

impl Default for AgentRetryStrategy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            max_attempts: 5,
            jitter_factor: 0.3,
            retryable_patterns: vec![
                "circuit_open".to_string(),
                "rate_limit".to_string(),
                "timeout".to_string(),
                "connection".to_string(),
                "temporary".to_string(),
                "unavailable".to_string(),
            ],
            non_retryable_patterns: vec![
                "invalid_query".to_string(),
                "permission_denied".to_string(),
                "authentication".to_string(),
                "not_found".to_string(),
                "constraint_violation".to_string(),
            ],
        }
    }
}

impl AgentRetryStrategy {
    /// Create a new retry strategy
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with custom configuration
    pub fn with_config(base_delay: Duration, max_delay: Duration, max_attempts: u32) -> Self {
        Self {
            base_delay,
            max_delay,
            max_attempts,
            ..Default::default()
        }
    }

    /// Set jitter factor
    pub fn with_jitter(mut self, factor: f64) -> Self {
        self.jitter_factor = factor.clamp(0.0, 1.0);
        self
    }

    /// Add retryable pattern
    pub fn with_retryable_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.retryable_patterns.push(pattern.into());
        self
    }

    /// Add non-retryable pattern
    pub fn with_non_retryable_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.non_retryable_patterns.push(pattern.into());
        self
    }

    /// Calculate retry delay with exponential backoff and jitter
    pub fn get_retry_delay(&self, attempt: u32) -> Duration {
        // Exponential backoff: base * 2^attempt
        let exp_delay = self.base_delay * 2u32.pow(attempt.min(10));

        // Cap at max delay
        let capped_delay = exp_delay.min(self.max_delay);

        // Add jitter (random variation to prevent thundering herd)
        let jitter = rand::random::<f64>() * self.jitter_factor;
        let jittered = capped_delay.mul_f64(1.0 + jitter);

        jittered.min(self.max_delay)
    }

    /// Determine if an error is retryable
    pub fn is_retryable(&self, error: &str) -> bool {
        let error_lower = error.to_lowercase();

        // Check non-retryable patterns first
        for pattern in &self.non_retryable_patterns {
            if error_lower.contains(pattern) {
                return false;
            }
        }

        // Check retryable patterns
        for pattern in &self.retryable_patterns {
            if error_lower.contains(pattern) {
                return true;
            }
        }

        // Default: retry for unknown errors
        true
    }

    /// Get retry decision for an error
    pub fn should_retry(&self, error: &str, attempt: u32) -> RetryDecision {
        if attempt >= self.max_attempts {
            return RetryDecision::Fail {
                reason: format!("Maximum retry attempts ({}) exceeded", self.max_attempts),
            };
        }

        if !self.is_retryable(error) {
            return RetryDecision::Fail {
                reason: format!("Error is not retryable: {}", error),
            };
        }

        let delay = self.get_retry_delay(attempt);
        RetryDecision::Retry {
            delay,
            attempt: attempt + 1,
        }
    }

    /// Get recommended delay for specific error types
    pub fn get_delay_for_error(&self, error: &str, attempt: u32) -> Option<Duration> {
        let decision = self.should_retry(error, attempt);
        decision.retry_delay()
    }
}

/// Cached conversation context for fallback during outages
#[derive(Debug, Clone)]
pub struct ConversationContext {
    /// Conversation ID
    pub conversation_id: String,
    /// Last successful query
    pub last_query: Option<String>,
    /// Last successful result (serialized)
    pub last_result: Option<String>,
    /// Conversation metadata
    pub metadata: HashMap<String, String>,
    /// Cache timestamp
    pub cached_at: Instant,
    /// Cache TTL
    pub ttl: Duration,
}

impl ConversationContext {
    /// Create a new conversation context
    pub fn new(conversation_id: impl Into<String>) -> Self {
        Self {
            conversation_id: conversation_id.into(),
            last_query: None,
            last_result: None,
            metadata: HashMap::new(),
            cached_at: Instant::now(),
            ttl: Duration::from_secs(3600), // 1 hour default
        }
    }

    /// Update with latest query/result
    pub fn update(&mut self, query: impl Into<String>, result: impl Into<String>) {
        self.last_query = Some(query.into());
        self.last_result = Some(result.into());
        self.cached_at = Instant::now();
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Check if context is still valid
    pub fn is_valid(&self) -> bool {
        self.cached_at.elapsed() < self.ttl
    }

    /// Get age of cached data
    pub fn age(&self) -> Duration {
        self.cached_at.elapsed()
    }
}

/// Conversation fallback manager
///
/// Maintains cached conversation contexts to provide fallback responses
/// during circuit breaker outages.
pub struct ConversationFallback {
    /// Cached contexts per conversation
    contexts: DashMap<String, ConversationContext>,

    /// Default TTL for new contexts
    default_ttl: RwLock<Duration>,

    /// Maximum cached contexts
    max_contexts: usize,
}

impl ConversationFallback {
    /// Create a new conversation fallback manager
    pub fn new() -> Self {
        Self {
            contexts: DashMap::new(),
            default_ttl: RwLock::new(Duration::from_secs(3600)),
            max_contexts: 10000,
        }
    }

    /// Create with custom configuration
    pub fn with_config(default_ttl: Duration, max_contexts: usize) -> Self {
        Self {
            contexts: DashMap::new(),
            default_ttl: RwLock::new(default_ttl),
            max_contexts,
        }
    }

    /// Update context for a conversation
    pub fn update_context(
        &self,
        conversation_id: &str,
        query: impl Into<String>,
        result: impl Into<String>,
    ) {
        let ttl = *self.default_ttl.read();

        if let Some(mut ctx) = self.contexts.get_mut(conversation_id) {
            ctx.update(query, result);
        } else {
            // Enforce max contexts
            if self.contexts.len() >= self.max_contexts {
                self.cleanup_expired();
            }

            let mut ctx = ConversationContext::new(conversation_id).with_ttl(ttl);
            ctx.update(query, result);
            self.contexts.insert(conversation_id.to_string(), ctx);
        }
    }

    /// Get fallback for a conversation
    pub fn get_fallback(&self, conversation_id: &str) -> Option<ConversationContext> {
        self.contexts
            .get(conversation_id)
            .filter(|ctx| ctx.is_valid())
            .map(|ctx| ctx.clone())
    }

    /// Execute with fallback on error
    pub fn execute_with_fallback<T, E>(
        &self,
        conversation_id: &str,
        execute: impl FnOnce() -> Result<T, E>,
        fallback: impl FnOnce(&ConversationContext) -> T,
    ) -> Result<T, E>
    where
        E: std::fmt::Display,
    {
        match execute() {
            Ok(result) => Ok(result),
            Err(e) => {
                if let Some(ctx) = self.get_fallback(conversation_id) {
                    Ok(fallback(&ctx))
                } else {
                    Err(e) // Return original error
                }
            }
        }
    }

    /// Cleanup expired contexts
    pub fn cleanup_expired(&self) {
        self.contexts.retain(|_, ctx| ctx.is_valid());
    }

    /// Remove specific conversation
    pub fn remove(&self, conversation_id: &str) -> Option<ConversationContext> {
        self.contexts.remove(conversation_id).map(|(_, ctx)| ctx)
    }

    /// Get number of cached contexts
    pub fn cached_count(&self) -> usize {
        self.contexts.len()
    }

    /// Set default TTL
    pub fn set_default_ttl(&self, ttl: Duration) {
        *self.default_ttl.write() = ttl;
    }

    /// Check if conversation has cached context
    pub fn has_context(&self, conversation_id: &str) -> bool {
        self.contexts
            .get(conversation_id)
            .map(|ctx| ctx.is_valid())
            .unwrap_or(false)
    }
}

impl Default for ConversationFallback {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_decision_messages() {
        let retry = RetryDecision::Retry {
            delay: Duration::from_millis(100),
            attempt: 1,
        };
        let msg = retry.to_llm_message();
        assert!(msg.contains("retry"));
        assert!(msg.contains("100"));

        let fail = RetryDecision::Fail {
            reason: "test error".to_string(),
        };
        let msg = fail.to_llm_message();
        assert!(msg.contains("fail"));
        assert!(msg.contains("test error"));
    }

    #[test]
    fn test_retry_strategy_delay() {
        let strategy = AgentRetryStrategy::new();

        let delay0 = strategy.get_retry_delay(0);
        let delay1 = strategy.get_retry_delay(1);
        let delay2 = strategy.get_retry_delay(2);

        // Each delay should be roughly 2x the previous
        assert!(delay1 >= delay0);
        assert!(delay2 >= delay1);
        assert!(delay2 <= strategy.max_delay);
    }

    #[test]
    fn test_retry_strategy_retryable() {
        let strategy = AgentRetryStrategy::new();

        assert!(strategy.is_retryable("circuit_open for node"));
        assert!(strategy.is_retryable("rate_limit exceeded"));
        assert!(strategy.is_retryable("connection timeout"));

        assert!(!strategy.is_retryable("permission_denied"));
        assert!(!strategy.is_retryable("authentication failed"));
    }

    #[test]
    fn test_retry_strategy_should_retry() {
        let strategy =
            AgentRetryStrategy::with_config(Duration::from_millis(100), Duration::from_secs(10), 3);

        // Should retry circuit_open
        let decision = strategy.should_retry("circuit_open", 0);
        assert!(decision.should_retry());

        // Should not retry after max attempts
        let decision = strategy.should_retry("circuit_open", 3);
        assert!(!decision.should_retry());

        // Should not retry non-retryable errors
        let decision = strategy.should_retry("permission_denied", 0);
        assert!(!decision.should_retry());
    }

    #[test]
    fn test_conversation_context() {
        let mut ctx = ConversationContext::new("conv-123")
            .with_metadata("user", "alice")
            .with_ttl(Duration::from_secs(60));

        assert_eq!(ctx.conversation_id, "conv-123");
        assert!(ctx.is_valid());

        ctx.update("SELECT * FROM users", r#"[{"id": 1}]"#);
        assert_eq!(ctx.last_query, Some("SELECT * FROM users".to_string()));
    }

    #[test]
    fn test_conversation_fallback() {
        let fallback = ConversationFallback::new();

        fallback.update_context("conv-1", "query1", "result1");
        assert!(fallback.has_context("conv-1"));
        assert_eq!(fallback.cached_count(), 1);

        let ctx = fallback.get_fallback("conv-1").unwrap();
        assert_eq!(ctx.last_query, Some("query1".to_string()));
        assert_eq!(ctx.last_result, Some("result1".to_string()));
    }

    #[test]
    fn test_conversation_fallback_expired() {
        let fallback = ConversationFallback::with_config(Duration::from_millis(10), 100);

        fallback.update_context("conv-1", "query1", "result1");
        assert!(fallback.has_context("conv-1"));

        std::thread::sleep(Duration::from_millis(20));
        assert!(!fallback.has_context("conv-1"));
    }

    #[test]
    fn test_execute_with_fallback() {
        let fallback = ConversationFallback::new();
        fallback.update_context("conv-1", "query", "cached_result");

        // Successful execution
        let result: Result<String, &str> = fallback.execute_with_fallback(
            "conv-1",
            || Ok("new_result".to_string()),
            |_| "fallback".to_string(),
        );
        assert_eq!(result.unwrap(), "new_result");
    }
}
