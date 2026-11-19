//! AI/Agent-specific cache optimizations
//!
//! Specialized caching for AI workloads including:
//! - Conversation context cache
//! - RAG chunk cache
//! - Tool result cache
//! - Semantic query cache
//! - Cross-feature AI integration
//!
//! Library-only: none of these is wired into the daemon. The daemon caches
//! MCP tool results with `[mcp] tool_cache_*` (`crate::mcp_cache`), retired
//! by the C-02 write generations, and SQL results with `[cache]`.

mod conversation;
mod integration;
mod rag;
mod semantic;
mod tools;

pub use conversation::{
    ConversationCacheStats, ConversationContext, ConversationContextCache, Turn,
};
pub use rag::{Chunk, ChunkId, RagCacheStatsSnapshot, RagChunkCache};
pub use semantic::{
    cosine_similarity, AIWorkloadContext, BranchContext, BranchId, Embedding,
    SemanticCacheStatsSnapshot, SemanticEntry, SemanticIndex, SemanticQueryCache, SimilarityResult,
    VectorId,
};
pub use tools::{ToolCacheStatsSnapshot, ToolCallKey, ToolResult, ToolResultCache};
// Note: SessionId is defined as a newtype struct in the parent distribcache module
pub use integration::{
    AIIntegrationConfig, AIIntegrationCoordinator, AIIntegrationStatsSnapshot, AIWorkloadDetection,
    CachePriority, CacheRecommendation, RecommendedTier, SessionTrackingInfo,
};
