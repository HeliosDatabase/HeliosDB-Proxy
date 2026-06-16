//! Cross-feature AI/Agent integration module
//!
//! Integrates semantic caching with other HeliosProxy features:
//! - TWR (Transaction Write Replay) session tracking
//! - Sync mode / lag routing awareness
//! - Workload scheduler coordination
//! - Branch-aware time-travel queries

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use super::semantic::{AIWorkloadContext, BranchContext, BranchId, SemanticQueryCache};
use crate::distribcache::classifier::WorkloadType;
use crate::distribcache::scheduler::{ScheduleResult, ScheduledQuery, WorkloadScheduler};
use crate::distribcache::SessionId;

/// AI workload detection result
#[derive(Debug, Clone)]
pub struct AIWorkloadDetection {
    /// Detected workload type
    pub workload_type: WorkloadType,
    /// AI-specific context
    pub ai_context: AIWorkloadContext,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
    /// Detected patterns
    pub patterns: Vec<String>,
}

/// Session tracking for TWR integration
#[derive(Debug, Clone)]
pub struct SessionTrackingInfo {
    /// Session identifier
    pub session_id: SessionId,
    /// Active branch context
    pub branch: Option<BranchContext>,
    /// Last query time
    pub last_activity: Instant,
    /// Transaction depth (0 = not in transaction)
    pub transaction_depth: u32,
    /// AI workload context
    pub ai_context: AIWorkloadContext,
    /// Total queries in session
    pub query_count: u64,
    /// Cache hit count
    pub cache_hits: u64,
}

impl SessionTrackingInfo {
    /// Create a new session tracking info
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: SessionId::new(session_id),
            branch: None,
            last_activity: Instant::now(),
            transaction_depth: 0,
            ai_context: AIWorkloadContext::General,
            query_count: 0,
            cache_hits: 0,
        }
    }

    /// Set branch context
    pub fn with_branch(mut self, branch: BranchContext) -> Self {
        self.branch = Some(branch);
        self
    }

    /// Set AI context
    pub fn with_ai_context(mut self, context: AIWorkloadContext) -> Self {
        self.ai_context = context;
        self
    }

    /// Record a query
    pub fn record_query(&mut self) {
        self.query_count += 1;
        self.last_activity = Instant::now();
    }

    /// Record a cache hit
    pub fn record_cache_hit(&mut self) {
        self.cache_hits += 1;
    }

    /// Get cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        if self.query_count == 0 {
            0.0
        } else {
            self.cache_hits as f64 / self.query_count as f64
        }
    }

    /// Check if session is idle
    pub fn is_idle(&self, timeout: Duration) -> bool {
        self.last_activity.elapsed() > timeout
    }
}

/// Cross-feature AI integration coordinator
pub struct AIIntegrationCoordinator {
    /// Semantic cache reference
    semantic_cache: Arc<SemanticQueryCache>,

    /// Session tracking
    sessions: DashMap<SessionId, SessionTrackingInfo>,

    /// Workload scheduler reference (optional)
    scheduler: Option<Arc<WorkloadScheduler>>,

    /// Configuration
    config: AIIntegrationConfig,

    /// Statistics
    stats: AIIntegrationStats,
}

/// Configuration for AI integration
#[derive(Debug, Clone)]
pub struct AIIntegrationConfig {
    /// Enable TWR session tracking
    pub twr_tracking: bool,
    /// Session idle timeout
    pub session_idle_timeout: Duration,
    /// Maximum sessions to track
    pub max_sessions: usize,
    /// Enable workload detection
    pub workload_detection: bool,
    /// RAG pattern detection threshold
    pub rag_detection_threshold: f32,
    /// Agent conversation detection threshold
    pub agent_detection_threshold: f32,
}

impl Default for AIIntegrationConfig {
    fn default() -> Self {
        Self {
            twr_tracking: true,
            session_idle_timeout: Duration::from_secs(3600), // 1 hour
            max_sessions: 10000,
            workload_detection: true,
            rag_detection_threshold: 0.7,
            agent_detection_threshold: 0.8,
        }
    }
}

/// Statistics for AI integration
#[derive(Debug, Default)]
struct AIIntegrationStats {
    /// Total workload detections
    detections: AtomicU64,
    /// RAG workloads detected
    rag_detected: AtomicU64,
    /// Agent workloads detected
    agent_detected: AtomicU64,
    /// Tool workloads detected
    tool_detected: AtomicU64,
    /// Sessions tracked
    sessions_tracked: AtomicU64,
    /// Cross-feature cache hits
    cross_feature_hits: AtomicU64,
}

impl AIIntegrationCoordinator {
    /// Create a new integration coordinator
    pub fn new(semantic_cache: Arc<SemanticQueryCache>, config: AIIntegrationConfig) -> Self {
        Self {
            semantic_cache,
            sessions: DashMap::new(),
            scheduler: None,
            config,
            stats: AIIntegrationStats::default(),
        }
    }

    /// Set the workload scheduler reference
    pub fn with_scheduler(mut self, scheduler: Arc<WorkloadScheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// Detect AI workload from query patterns
    pub fn detect_workload(&self, query: &str, session: Option<&SessionId>) -> AIWorkloadDetection {
        self.stats.detections.fetch_add(1, Ordering::Relaxed);

        let mut patterns = Vec::new();
        let mut confidence = 0.0f32;
        let mut ai_context = AIWorkloadContext::General;
        let mut workload_type = WorkloadType::Mixed;

        // Pattern matching for workload detection
        let query_lower = query.to_lowercase();

        // RAG retrieval patterns
        if self.is_rag_pattern(&query_lower) {
            patterns.push("RAG retrieval".to_string());
            confidence = 0.85;
            ai_context = AIWorkloadContext::RAGRetrieval;
            workload_type = WorkloadType::RAG;
            self.stats.rag_detected.fetch_add(1, Ordering::Relaxed);
        }

        // Agent conversation patterns
        if self.is_agent_pattern(&query_lower, session) {
            patterns.push("Agent conversation".to_string());
            confidence = confidence.max(0.80);
            ai_context = AIWorkloadContext::AgentConversation;
            workload_type = WorkloadType::AIAgent;
            self.stats.agent_detected.fetch_add(1, Ordering::Relaxed);
        }

        // Tool result patterns
        if self.is_tool_pattern(&query_lower) {
            patterns.push("Tool result".to_string());
            confidence = confidence.max(0.90);
            ai_context = AIWorkloadContext::ToolResult;
            workload_type = WorkloadType::AIAgent;
            self.stats.tool_detected.fetch_add(1, Ordering::Relaxed);
        }

        // Vector search patterns (only if not already classified as RAG)
        if workload_type != WorkloadType::RAG
            && (query_lower.contains("embedding") || query_lower.contains("vector") || query_lower.contains("similarity")) {
                patterns.push("Vector search".to_string());
                confidence = confidence.max(0.75);
                workload_type = WorkloadType::Vector;
            }

        // OLAP patterns
        if self.is_olap_pattern(&query_lower) {
            patterns.push("OLAP analytics".to_string());
            confidence = confidence.max(0.70);
            workload_type = WorkloadType::OLAP;
        }

        AIWorkloadDetection {
            workload_type,
            ai_context,
            confidence,
            patterns,
        }
    }

    /// Check if query matches RAG retrieval patterns
    fn is_rag_pattern(&self, query: &str) -> bool {
        // RAG typically involves:
        // - Semantic search / similarity queries
        // - Chunk retrieval
        // - Document lookups
        let rag_patterns = [
            "chunk", "retrieve", "context", "passage", "document",
            "semantic", "similarity", "cosine", "embedding",
        ];

        rag_patterns.iter().any(|p| query.contains(p))
    }

    /// Check if query matches agent conversation patterns
    fn is_agent_pattern(&self, query: &str, session: Option<&SessionId>) -> bool {
        // Check session history for conversation patterns
        if let Some(sid) = session {
            if let Some(info) = self.sessions.get(sid) {
                if info.ai_context == AIWorkloadContext::AgentConversation {
                    return true;
                }
                // Conversation pattern: multiple sequential queries
                if info.query_count > 5 && info.cache_hit_rate() > 0.3 {
                    return true;
                }
            }
        }

        // Query pattern matching
        let agent_patterns = ["conversation", "history", "context", "message", "response"];
        agent_patterns.iter().any(|p| query.contains(p))
    }

    /// Check if query matches tool result patterns
    fn is_tool_pattern(&self, query: &str) -> bool {
        let tool_patterns = [
            "tool_", "function_", "api_result", "calculate",
            "format_", "convert_", "lookup_",
        ];

        tool_patterns.iter().any(|p| query.contains(p))
    }

    /// Check if query matches OLAP patterns
    fn is_olap_pattern(&self, query: &str) -> bool {
        let olap_patterns = [
            "group by", "having", "aggregate", "sum(", "count(",
            "avg(", "window", "partition by", "rollup", "cube",
        ];

        olap_patterns.iter().any(|p| query.contains(p))
    }

    /// Track session for TWR integration
    pub fn track_session(
        &self,
        session_id: impl Into<String>,
        branch: Option<BranchContext>,
        ai_context: AIWorkloadContext,
    ) {
        let sid = SessionId::new(session_id);

        // Limit session count
        if self.sessions.len() >= self.config.max_sessions {
            self.cleanup_idle_sessions();
        }

        let mut info = SessionTrackingInfo::new(sid.0.clone())
            .with_ai_context(ai_context);

        if let Some(b) = branch {
            info = info.with_branch(b);
        }

        self.sessions.insert(sid, info);
        self.stats.sessions_tracked.fetch_add(1, Ordering::Relaxed);
    }

    /// Update session activity
    pub fn update_session(&self, session_id: &SessionId, cache_hit: bool) {
        if let Some(mut info) = self.sessions.get_mut(session_id) {
            info.record_query();
            if cache_hit {
                info.record_cache_hit();
                self.stats.cross_feature_hits.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get session info
    pub fn get_session(&self, session_id: &SessionId) -> Option<SessionTrackingInfo> {
        self.sessions.get(session_id).map(|r| r.clone())
    }

    /// Begin transaction for session
    pub fn begin_transaction(&self, session_id: &SessionId) {
        if let Some(mut info) = self.sessions.get_mut(session_id) {
            info.transaction_depth += 1;
        }
    }

    /// End transaction for session
    pub fn end_transaction(&self, session_id: &SessionId) {
        if let Some(mut info) = self.sessions.get_mut(session_id) {
            if info.transaction_depth > 0 {
                info.transaction_depth -= 1;
            }
        }
    }

    /// Check if session is in transaction
    pub fn is_in_transaction(&self, session_id: &SessionId) -> bool {
        self.sessions
            .get(session_id)
            .map(|info| info.transaction_depth > 0)
            .unwrap_or(false)
    }

    /// Get recommended cache behavior based on workload
    pub fn get_cache_recommendation(&self, detection: &AIWorkloadDetection) -> CacheRecommendation {
        match detection.ai_context {
            AIWorkloadContext::RAGRetrieval => CacheRecommendation {
                should_cache: true,
                ttl: Duration::from_secs(300),
                priority: CachePriority::High,
                tier: RecommendedTier::L1,
            },
            AIWorkloadContext::RAGGeneration => CacheRecommendation {
                should_cache: true,
                ttl: Duration::from_secs(1800),
                priority: CachePriority::Medium,
                tier: RecommendedTier::L2,
            },
            AIWorkloadContext::AgentConversation => CacheRecommendation {
                should_cache: true,
                ttl: Duration::from_secs(3600),
                priority: CachePriority::High,
                tier: RecommendedTier::L1,
            },
            AIWorkloadContext::ToolResult => CacheRecommendation {
                should_cache: true,
                ttl: Duration::from_secs(86400),
                priority: CachePriority::Low,
                tier: RecommendedTier::L2,
            },
            AIWorkloadContext::General => {
                match detection.workload_type {
                    WorkloadType::OLTP => CacheRecommendation {
                        should_cache: true,
                        ttl: Duration::from_secs(60),
                        priority: CachePriority::High,
                        tier: RecommendedTier::L1,
                    },
                    WorkloadType::OLAP => CacheRecommendation {
                        should_cache: true,
                        ttl: Duration::from_secs(3600),
                        priority: CachePriority::Low,
                        tier: RecommendedTier::L3,
                    },
                    WorkloadType::Vector => CacheRecommendation {
                        should_cache: true,
                        ttl: Duration::from_secs(600),
                        priority: CachePriority::Medium,
                        tier: RecommendedTier::L2,
                    },
                    _ => CacheRecommendation::default(),
                }
            }
        }
    }

    /// Schedule query with AI-aware priority
    pub fn schedule_with_ai_priority(
        &self,
        query_id: u64,
        detection: &AIWorkloadDetection,
    ) -> Option<ScheduleResult> {
        let scheduler = self.scheduler.as_ref()?;

        let query = ScheduledQuery {
            id: query_id,
            workload_type: detection.workload_type,
            timestamp: std::time::Instant::now(),
        };

        Some(scheduler.schedule(query))
    }

    /// Cleanup idle sessions
    pub fn cleanup_idle_sessions(&self) {
        let timeout = self.config.session_idle_timeout;
        let to_remove: Vec<_> = self.sessions
            .iter()
            .filter(|e| e.is_idle(timeout))
            .map(|e| e.key().clone())
            .collect();

        for sid in to_remove {
            self.sessions.remove(&sid);
        }
    }

    /// Invalidate cache entries for branch
    pub fn invalidate_branch(&self, branch: &BranchId) -> usize {
        self.semantic_cache.invalidate_branch(branch)
    }

    /// Invalidate cache entries for table
    pub fn invalidate_table(&self, table: &str) -> usize {
        self.semantic_cache.invalidate_by_table(table)
    }

    /// Get integration statistics
    pub fn stats(&self) -> AIIntegrationStatsSnapshot {
        AIIntegrationStatsSnapshot {
            total_detections: self.stats.detections.load(Ordering::Relaxed),
            rag_detected: self.stats.rag_detected.load(Ordering::Relaxed),
            agent_detected: self.stats.agent_detected.load(Ordering::Relaxed),
            tool_detected: self.stats.tool_detected.load(Ordering::Relaxed),
            active_sessions: self.sessions.len(),
            cross_feature_hits: self.stats.cross_feature_hits.load(Ordering::Relaxed),
        }
    }
}

/// Cache recommendation based on workload
#[derive(Debug, Clone)]
pub struct CacheRecommendation {
    /// Whether to cache this query
    pub should_cache: bool,
    /// Recommended TTL
    pub ttl: Duration,
    /// Cache priority
    pub priority: CachePriority,
    /// Recommended cache tier
    pub tier: RecommendedTier,
}

impl Default for CacheRecommendation {
    fn default() -> Self {
        Self {
            should_cache: true,
            ttl: Duration::from_secs(300),
            priority: CachePriority::Medium,
            tier: RecommendedTier::L1,
        }
    }
}

/// Cache priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachePriority {
    High,
    Medium,
    Low,
}

/// Recommended cache tier
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecommendedTier {
    L1,
    L2,
    L3,
}

/// Statistics snapshot
#[derive(Debug, Clone)]
pub struct AIIntegrationStatsSnapshot {
    pub total_detections: u64,
    pub rag_detected: u64,
    pub agent_detected: u64,
    pub tool_detected: u64,
    pub active_sessions: usize,
    pub cross_feature_hits: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_detection_rag() {
        let cache = Arc::new(SemanticQueryCache::new(0.9));
        let coordinator = AIIntegrationCoordinator::new(cache, AIIntegrationConfig::default());

        let detection = coordinator.detect_workload(
            "SELECT * FROM chunks WHERE document_id = 1 AND similarity > 0.8",
            None,
        );

        assert_eq!(detection.workload_type, WorkloadType::RAG);
        assert_eq!(detection.ai_context, AIWorkloadContext::RAGRetrieval);
        assert!(detection.confidence > 0.7);
    }

    #[test]
    fn test_workload_detection_agent() {
        let cache = Arc::new(SemanticQueryCache::new(0.9));
        let coordinator = AIIntegrationCoordinator::new(cache, AIIntegrationConfig::default());

        let detection = coordinator.detect_workload(
            "SELECT * FROM conversation_history WHERE session_id = 'abc'",
            None,
        );

        assert_eq!(detection.ai_context, AIWorkloadContext::AgentConversation);
        assert!(detection.patterns.contains(&"Agent conversation".to_string()));
    }

    #[test]
    fn test_workload_detection_tool() {
        let cache = Arc::new(SemanticQueryCache::new(0.9));
        let coordinator = AIIntegrationCoordinator::new(cache, AIIntegrationConfig::default());

        let detection = coordinator.detect_workload(
            "SELECT tool_calculate_result FROM api_result WHERE id = 1",
            None,
        );

        assert_eq!(detection.ai_context, AIWorkloadContext::ToolResult);
    }

    #[test]
    fn test_workload_detection_olap() {
        let cache = Arc::new(SemanticQueryCache::new(0.9));
        let coordinator = AIIntegrationCoordinator::new(cache, AIIntegrationConfig::default());

        let detection = coordinator.detect_workload(
            "SELECT category, SUM(amount) FROM orders GROUP BY category HAVING COUNT(*) > 10",
            None,
        );

        assert_eq!(detection.workload_type, WorkloadType::OLAP);
    }

    #[test]
    fn test_session_tracking() {
        let cache = Arc::new(SemanticQueryCache::new(0.9));
        let coordinator = AIIntegrationCoordinator::new(cache, AIIntegrationConfig::default());

        let sid = SessionId::new("session-1");

        // Track session
        coordinator.track_session(
            "session-1",
            Some(BranchContext::main()),
            AIWorkloadContext::AgentConversation,
        );

        // Update session
        coordinator.update_session(&sid, true);
        coordinator.update_session(&sid, false);

        let info = coordinator.get_session(&sid).unwrap();
        assert_eq!(info.query_count, 2);
        assert_eq!(info.cache_hits, 1);
        assert!((info.cache_hit_rate() - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_transaction_tracking() {
        let cache = Arc::new(SemanticQueryCache::new(0.9));
        let coordinator = AIIntegrationCoordinator::new(cache, AIIntegrationConfig::default());

        let sid = SessionId::new("session-tx");
        coordinator.track_session("session-tx", None, AIWorkloadContext::General);

        assert!(!coordinator.is_in_transaction(&sid));

        coordinator.begin_transaction(&sid);
        assert!(coordinator.is_in_transaction(&sid));

        coordinator.end_transaction(&sid);
        assert!(!coordinator.is_in_transaction(&sid));
    }

    #[test]
    fn test_cache_recommendation() {
        let cache = Arc::new(SemanticQueryCache::new(0.9));
        let coordinator = AIIntegrationCoordinator::new(cache, AIIntegrationConfig::default());

        // RAG retrieval should have short TTL and high priority
        let rag_detection = AIWorkloadDetection {
            workload_type: WorkloadType::RAG,
            ai_context: AIWorkloadContext::RAGRetrieval,
            confidence: 0.9,
            patterns: vec![],
        };
        let rag_rec = coordinator.get_cache_recommendation(&rag_detection);
        assert_eq!(rag_rec.priority, CachePriority::High);
        assert_eq!(rag_rec.tier, RecommendedTier::L1);

        // Tool result should have long TTL
        let tool_detection = AIWorkloadDetection {
            workload_type: WorkloadType::AIAgent,
            ai_context: AIWorkloadContext::ToolResult,
            confidence: 0.9,
            patterns: vec![],
        };
        let tool_rec = coordinator.get_cache_recommendation(&tool_detection);
        assert_eq!(tool_rec.ttl, Duration::from_secs(86400));
    }

    #[test]
    fn test_stats() {
        let cache = Arc::new(SemanticQueryCache::new(0.9));
        let coordinator = AIIntegrationCoordinator::new(cache, AIIntegrationConfig::default());

        // Detect various workloads
        coordinator.detect_workload("SELECT * FROM chunks", None);
        coordinator.detect_workload("SELECT conversation_history", None);
        coordinator.detect_workload("SELECT tool_result", None);

        let stats = coordinator.stats();
        assert_eq!(stats.total_detections, 3);
    }

    #[test]
    fn test_invalidation() {
        let cache = Arc::new(SemanticQueryCache::new(0.9));

        // Insert some entries
        cache.insert_with_context(
            "query1",
            vec![1.0, 0.0],
            serde_json::json!(1),
            Some(BranchContext::main()),
            None,
            AIWorkloadContext::General,
            vec!["users".to_string()],
        );

        let coordinator = AIIntegrationCoordinator::new(cache.clone(), AIIntegrationConfig::default());

        // Invalidate by table
        let removed = coordinator.invalidate_table("users");
        assert_eq!(removed, 1);
        assert_eq!(cache.len(), 0);
    }
}
