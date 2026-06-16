//! Conversation context cache for AI agents
//!
//! Caches recent conversation turns for quick context retrieval.

use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Instant;

/// Conversation identifier
pub type ConversationId = String;

/// A conversation turn
#[derive(Debug, Clone)]
pub struct Turn {
    /// Turn identifier
    pub id: String,
    /// Role (user, assistant, system)
    pub role: String,
    /// Content
    pub content: String,
    /// Timestamp
    pub timestamp: Instant,
    /// Token count (approximate)
    pub token_count: usize,
    /// Metadata
    pub metadata: Option<serde_json::Value>,
}

impl Turn {
    /// Create a new turn
    pub fn new(id: impl Into<String>, role: impl Into<String>, content: impl Into<String>) -> Self {
        let content = content.into();
        let token_count = content.split_whitespace().count() * 4 / 3; // Rough estimate

        Self {
            id: id.into(),
            role: role.into(),
            content,
            timestamp: Instant::now(),
            token_count,
            metadata: None,
        }
    }

    /// Add metadata
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Approximate size in bytes
    pub fn size(&self) -> usize {
        self.id.len() + self.role.len() + self.content.len() + 64
    }
}

/// Conversation context
#[derive(Debug)]
pub struct ConversationContext {
    /// Conversation ID
    pub id: ConversationId,
    /// Conversation turns
    pub turns: VecDeque<Turn>,
    /// Maximum turns to keep
    max_turns: usize,
    /// Total token count
    total_tokens: usize,
    /// Last access time
    last_access: Instant,
}

impl ConversationContext {
    fn new(id: ConversationId, max_turns: usize) -> Self {
        Self {
            id,
            turns: VecDeque::with_capacity(max_turns),
            max_turns,
            total_tokens: 0,
            last_access: Instant::now(),
        }
    }

    fn append(&mut self, turn: Turn) {
        self.total_tokens += turn.token_count;
        self.turns.push_back(turn);

        // Maintain size limit
        while self.turns.len() > self.max_turns {
            if let Some(removed) = self.turns.pop_front() {
                self.total_tokens = self.total_tokens.saturating_sub(removed.token_count);
            }
        }

        self.last_access = Instant::now();
    }

    fn get_recent(&self, count: usize) -> Vec<Turn> {
        self.turns.iter()
            .rev()
            .take(count)
            .rev()
            .cloned()
            .collect()
    }

    fn size(&self) -> usize {
        self.turns.iter().map(|t| t.size()).sum()
    }
}

/// LRU cache for conversation eviction
struct LruTracker {
    order: Mutex<VecDeque<ConversationId>>,
    #[allow(dead_code)]
    max_size: usize,
}

impl LruTracker {
    fn new(max_size: usize) -> Self {
        Self {
            order: Mutex::new(VecDeque::with_capacity(max_size)),
            max_size,
        }
    }

    fn touch(&self, id: &ConversationId) {
        let mut order = self.order.lock().unwrap();

        // Remove existing entry
        if let Some(pos) = order.iter().position(|x| x == id) {
            order.remove(pos);
        }

        // Add to end (most recent)
        order.push_back(id.clone());
    }

    fn evict_oldest(&self) -> Option<ConversationId> {
        self.order.lock().unwrap().pop_front()
    }
}

/// Conversation context cache
pub struct ConversationContextCache {
    /// Contexts per conversation
    contexts: DashMap<ConversationId, ConversationContext>,

    /// LRU tracker
    lru: LruTracker,

    /// Maximum turns per conversation
    max_turns: usize,

    /// Maximum conversations to cache
    max_conversations: usize,
}

impl ConversationContextCache {
    /// Create a new cache
    pub fn new(max_conversations: usize, max_turns: usize) -> Self {
        Self {
            contexts: DashMap::new(),
            lru: LruTracker::new(max_conversations),
            max_turns,
            max_conversations,
        }
    }

    /// Get context for a conversation
    pub fn get_context(&self, conv_id: &str, max_turns: usize) -> Option<Vec<Turn>> {
        self.lru.touch(&conv_id.to_string());

        let ctx = self.contexts.get(conv_id)?;
        Some(ctx.get_recent(max_turns))
    }

    /// Get full context
    pub fn get_full_context(&self, conv_id: &str) -> Option<Vec<Turn>> {
        self.lru.touch(&conv_id.to_string());

        let ctx = self.contexts.get(conv_id)?;
        Some(ctx.turns.iter().cloned().collect())
    }

    /// Append a turn to a conversation
    pub fn append_turn(&self, conv_id: &str, turn: Turn) {
        self.lru.touch(&conv_id.to_string());

        // Evict if at capacity
        while self.contexts.len() >= self.max_conversations {
            if let Some(old_id) = self.lru.evict_oldest() {
                self.contexts.remove(&old_id);
            } else {
                break;
            }
        }

        // Get or create context
        let mut ctx = self.contexts
            .entry(conv_id.to_string())
            .or_insert_with(|| ConversationContext::new(conv_id.to_string(), self.max_turns));

        ctx.append(turn);
    }

    /// Clear a conversation
    pub fn clear_conversation(&self, conv_id: &str) {
        self.contexts.remove(conv_id);
    }

    /// Get conversation count
    pub fn conversation_count(&self) -> usize {
        self.contexts.len()
    }

    /// Get total tokens cached
    pub fn total_tokens(&self) -> usize {
        self.contexts.iter()
            .map(|ctx| ctx.total_tokens)
            .sum()
    }

    /// Get stats
    pub fn stats(&self) -> ConversationCacheStats {
        let mut total_turns = 0;
        let mut total_size = 0;

        for ctx in self.contexts.iter() {
            total_turns += ctx.turns.len();
            total_size += ctx.size();
        }

        ConversationCacheStats {
            conversations: self.contexts.len(),
            total_turns,
            total_size_bytes: total_size,
            total_tokens: self.total_tokens(),
        }
    }
}

/// Conversation cache statistics
#[derive(Debug, Clone)]
pub struct ConversationCacheStats {
    pub conversations: usize,
    pub total_turns: usize,
    pub total_size_bytes: usize,
    pub total_tokens: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_turn_creation() {
        let turn = Turn::new("1", "user", "Hello, how are you?");
        assert_eq!(turn.role, "user");
        assert!(turn.token_count > 0);
    }

    #[test]
    fn test_append_and_get_context() {
        let cache = ConversationContextCache::new(100, 50);

        cache.append_turn("conv-1", Turn::new("1", "user", "Hello"));
        cache.append_turn("conv-1", Turn::new("2", "assistant", "Hi there!"));
        cache.append_turn("conv-1", Turn::new("3", "user", "How are you?"));

        let context = cache.get_context("conv-1", 2).unwrap();
        assert_eq!(context.len(), 2);
        assert_eq!(context[0].content, "Hi there!");
        assert_eq!(context[1].content, "How are you?");
    }

    #[test]
    fn test_max_turns_limit() {
        let cache = ConversationContextCache::new(100, 3);

        for i in 0..5 {
            cache.append_turn("conv-1", Turn::new(
                format!("{}", i),
                "user",
                format!("Message {}", i),
            ));
        }

        let context = cache.get_full_context("conv-1").unwrap();
        assert_eq!(context.len(), 3);
        assert_eq!(context[0].content, "Message 2");
    }

    #[test]
    fn test_lru_eviction() {
        let cache = ConversationContextCache::new(2, 10);

        cache.append_turn("conv-1", Turn::new("1", "user", "Hello 1"));
        cache.append_turn("conv-2", Turn::new("1", "user", "Hello 2"));

        // This should evict conv-1
        cache.append_turn("conv-3", Turn::new("1", "user", "Hello 3"));

        assert!(cache.get_context("conv-1", 1).is_none());
        assert!(cache.get_context("conv-2", 1).is_some());
        assert!(cache.get_context("conv-3", 1).is_some());
    }

    #[test]
    fn test_stats() {
        let cache = ConversationContextCache::new(100, 50);

        cache.append_turn("conv-1", Turn::new("1", "user", "Hello"));
        cache.append_turn("conv-1", Turn::new("2", "assistant", "Hi"));
        cache.append_turn("conv-2", Turn::new("1", "user", "Test"));

        let stats = cache.stats();
        assert_eq!(stats.conversations, 2);
        assert_eq!(stats.total_turns, 3);
        assert!(stats.total_tokens > 0);
    }
}
