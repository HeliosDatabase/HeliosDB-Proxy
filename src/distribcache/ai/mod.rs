//! AI/Agent-specific cache optimizations
//!
//! Specialized caching for AI workloads including:
//! - Conversation context cache
//! - RAG chunk cache
//! - Tool result cache
//! - Semantic query cache
//! - Cross-feature AI integration

mod conversation;
mod rag;
mod tools;
mod semantic;
mod integration;

pub use conversation::{ConversationContextCache, ConversationContext, Turn, ConversationCacheStats};
pub use rag::{RagChunkCache, Chunk, ChunkId, RagCacheStatsSnapshot};
pub use tools::{ToolResultCache, ToolCallKey, ToolResult, ToolCacheStatsSnapshot};
pub use semantic::{
    SemanticQueryCache, SemanticEntry, SemanticCacheStatsSnapshot, cosine_similarity,
    BranchContext, BranchId, AIWorkloadContext, VectorId, Embedding,
    SemanticIndex, SemanticIndexConfig, SimilarityResult,
};
// Note: SessionId is defined as a newtype struct in the parent distribcache module
pub use integration::{
    AIIntegrationCoordinator, AIIntegrationConfig, AIIntegrationStatsSnapshot,
    AIWorkloadDetection, SessionTrackingInfo, CacheRecommendation,
    CachePriority, RecommendedTier,
};
