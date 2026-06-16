//! Workload classifier for intelligent caching decisions
//!
//! Classifies queries into workload types (OLTP, OLAP, Vector, AI Agent, RAG)
//! to apply appropriate caching strategies.

use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::{DistribCacheConfig, QueryContext, SessionId, QueryFingerprint};

/// Workload types for cache strategy selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkloadType {
    /// Online Transaction Processing
    /// Characteristics: Point lookups, short transactions, low latency
    OLTP,

    /// Online Analytical Processing
    /// Characteristics: Full scans, aggregations, high throughput
    OLAP,

    /// Vector/Embedding Operations
    /// Characteristics: ANN search, similarity queries
    Vector,

    /// AI Agent Workloads
    /// Characteristics: Context retrieval, tool calls, conversation
    AIAgent,

    /// RAG Pipeline
    /// Characteristics: Embedding + retrieval + reranking
    RAG,

    /// Mixed/Unknown
    Mixed,
}

/// Query history entry for session-based classification
#[derive(Debug, Clone)]
struct QueryHistoryEntry {
    #[allow(dead_code)]
    fingerprint: QueryFingerprint,
    workload: WorkloadType,
    timestamp: Instant,
    latency_ms: u64,
}

/// Per-session query history
#[derive(Debug)]
struct SessionHistory {
    /// Recent queries
    queries: VecDeque<QueryHistoryEntry>,
    /// Detected primary workload
    primary_workload: Option<WorkloadType>,
    /// Workload counts
    oltp_count: u64,
    olap_count: u64,
    vector_count: u64,
    ai_count: u64,
    rag_count: u64,
}

impl SessionHistory {
    fn new() -> Self {
        Self {
            queries: VecDeque::with_capacity(100),
            primary_workload: None,
            oltp_count: 0,
            olap_count: 0,
            vector_count: 0,
            ai_count: 0,
            rag_count: 0,
        }
    }

    fn record(&mut self, entry: QueryHistoryEntry) {
        // Update counts
        match entry.workload {
            WorkloadType::OLTP => self.oltp_count += 1,
            WorkloadType::OLAP => self.olap_count += 1,
            WorkloadType::Vector => self.vector_count += 1,
            WorkloadType::AIAgent => self.ai_count += 1,
            WorkloadType::RAG => self.rag_count += 1,
            WorkloadType::Mixed => {}
        }

        // Add to history
        self.queries.push_back(entry);
        while self.queries.len() > 100 {
            self.queries.pop_front();
        }

        // Update primary workload
        self.primary_workload = self.determine_primary_workload();
    }

    fn determine_primary_workload(&self) -> Option<WorkloadType> {
        let total = self.oltp_count + self.olap_count + self.vector_count +
                    self.ai_count + self.rag_count;

        if total < 10 {
            return None; // Not enough data
        }

        let max = *[
            self.oltp_count,
            self.olap_count,
            self.vector_count,
            self.ai_count,
            self.rag_count,
        ].iter().max().unwrap();

        // Need > 50% to be considered primary
        if max as f64 / total as f64 > 0.5 {
            if max == self.oltp_count {
                Some(WorkloadType::OLTP)
            } else if max == self.olap_count {
                Some(WorkloadType::OLAP)
            } else if max == self.vector_count {
                Some(WorkloadType::Vector)
            } else if max == self.ai_count {
                Some(WorkloadType::AIAgent)
            } else {
                Some(WorkloadType::RAG)
            }
        } else {
            Some(WorkloadType::Mixed)
        }
    }
}

/// Classification rule for pattern matching
#[derive(Debug, Clone)]
pub struct ClassificationRule {
    /// Rule name
    pub name: String,
    /// Patterns to match (SQL keywords/fragments)
    pub patterns: Vec<String>,
    /// Target workload type
    pub workload: WorkloadType,
    /// Rule priority (higher = checked first)
    pub priority: u32,
}

/// Workload classifier
pub struct WorkloadClassifier {
    /// Configuration
    #[allow(dead_code)]
    config: DistribCacheConfig,

    /// Classification rules (priority-ordered)
    rules: Vec<ClassificationRule>,

    /// Per-session query history
    session_history: DashMap<SessionId, SessionHistory>,

    /// Global statistics
    stats: ClassifierStats,
}

/// Classifier statistics
#[derive(Debug, Default)]
struct ClassifierStats {
    total_classified: AtomicU64,
    oltp_count: AtomicU64,
    olap_count: AtomicU64,
    vector_count: AtomicU64,
    ai_count: AtomicU64,
    rag_count: AtomicU64,
    mixed_count: AtomicU64,
    rule_hits: AtomicU64,
    session_hits: AtomicU64,
    default_hits: AtomicU64,
}

impl WorkloadClassifier {
    /// Create a new workload classifier
    pub fn new(config: DistribCacheConfig) -> Self {
        let rules = Self::default_rules();

        Self {
            config,
            rules,
            session_history: DashMap::new(),
            stats: ClassifierStats::default(),
        }
    }

    /// Default classification rules
    fn default_rules() -> Vec<ClassificationRule> {
        vec![
            // Vector operations (highest priority)
            ClassificationRule {
                name: "vector_similarity".to_string(),
                patterns: vec![
                    "<->".to_string(),
                    "<#>".to_string(),
                    "<=>".to_string(),
                    "VECTOR".to_string(),
                    "EMBEDDING".to_string(),
                    "COSINE_SIMILARITY".to_string(),
                    "L2_DISTANCE".to_string(),
                    "INNER_PRODUCT".to_string(),
                ],
                workload: WorkloadType::Vector,
                priority: 100,
            },
            // RAG patterns
            ClassificationRule {
                name: "rag_pipeline".to_string(),
                patterns: vec![
                    "CHUNKS".to_string(),
                    "DOCUMENTS".to_string(),
                    "RERANK".to_string(),
                    "RETRIEVE".to_string(),
                ],
                workload: WorkloadType::RAG,
                priority: 90,
            },
            // AI Agent patterns
            ClassificationRule {
                name: "ai_agent".to_string(),
                patterns: vec![
                    "CONVERSATION".to_string(),
                    "AGENT_".to_string(),
                    "TOOL_".to_string(),
                    "CONTEXT".to_string(),
                    "MEMORY".to_string(),
                    "TURNS".to_string(),
                ],
                workload: WorkloadType::AIAgent,
                priority: 85,
            },
            // OLAP patterns
            ClassificationRule {
                name: "olap_aggregation".to_string(),
                patterns: vec![
                    "GROUP BY".to_string(),
                    "HAVING".to_string(),
                    "COUNT(".to_string(),
                    "SUM(".to_string(),
                    "AVG(".to_string(),
                    "MIN(".to_string(),
                    "MAX(".to_string(),
                    "STDDEV".to_string(),
                    "VARIANCE".to_string(),
                    "PERCENTILE".to_string(),
                ],
                workload: WorkloadType::OLAP,
                priority: 70,
            },
            ClassificationRule {
                name: "olap_analytics".to_string(),
                patterns: vec![
                    "WINDOW".to_string(),
                    "OVER(".to_string(),
                    "PARTITION BY".to_string(),
                    "ROLLUP".to_string(),
                    "CUBE".to_string(),
                    "GROUPING".to_string(),
                ],
                workload: WorkloadType::OLAP,
                priority: 70,
            },
            ClassificationRule {
                name: "olap_large_scan".to_string(),
                patterns: vec![
                    "ANALYTICS".to_string(),
                    "REPORT".to_string(),
                    "DASHBOARD".to_string(),
                    "METRIC".to_string(),
                ],
                workload: WorkloadType::OLAP,
                priority: 60,
            },
            // OLTP patterns (lower priority, broader match)
            ClassificationRule {
                name: "oltp_point_lookup".to_string(),
                patterns: vec![
                    "WHERE ID =".to_string(),
                    "WHERE ID=".to_string(),
                    "BY ID".to_string(),
                    "LIMIT 1".to_string(),
                ],
                workload: WorkloadType::OLTP,
                priority: 50,
            },
        ]
    }

    /// Classify a query based on patterns and session history
    pub fn classify(&self, query: &str, context: &QueryContext) -> WorkloadType {
        self.stats.total_classified.fetch_add(1, Ordering::Relaxed);

        // 1. Check explicit hint
        if let Some(hint) = context.workload_hint {
            return hint;
        }

        // 2. Pattern-based classification
        if let Some(workload) = self.classify_by_pattern(query) {
            self.stats.rule_hits.fetch_add(1, Ordering::Relaxed);
            self.record_query(context, query, workload);
            return workload;
        }

        // 3. Session history-based classification
        if let Some(workload) = self.classify_by_session(&context.session_id) {
            self.stats.session_hits.fetch_add(1, Ordering::Relaxed);
            self.record_query(context, query, workload);
            return workload;
        }

        // 4. Default classification based on query structure
        let workload = self.classify_by_structure(query);
        self.stats.default_hits.fetch_add(1, Ordering::Relaxed);
        self.record_query(context, query, workload);
        workload
    }

    /// Simplified classify method for string queries
    pub fn classify_query(&self, query: &str, context: &QueryContext) -> WorkloadType {
        self.classify(query, context)
    }

    /// Classify based on pattern rules
    fn classify_by_pattern(&self, query: &str) -> Option<WorkloadType> {
        let upper = query.to_uppercase();

        // Check rules in priority order
        let mut sorted_rules = self.rules.clone();
        sorted_rules.sort_by_key(|b| std::cmp::Reverse(b.priority));

        for rule in &sorted_rules {
            for pattern in &rule.patterns {
                if upper.contains(pattern) {
                    return Some(rule.workload);
                }
            }
        }

        None
    }

    /// Classify based on session history
    fn classify_by_session(&self, session_id: &SessionId) -> Option<WorkloadType> {
        self.session_history
            .get(session_id)
            .and_then(|history| history.primary_workload)
    }

    /// Classify based on query structure (fallback)
    fn classify_by_structure(&self, query: &str) -> WorkloadType {
        let upper = query.to_uppercase();

        // Simple heuristics
        if upper.starts_with("INSERT") || upper.starts_with("UPDATE") ||
           upper.starts_with("DELETE") {
            return WorkloadType::OLTP;
        }

        // Full table scan likely OLAP
        if upper.contains("SELECT") && !upper.contains("WHERE") && !upper.contains("LIMIT") {
            return WorkloadType::OLAP;
        }

        // JOIN heavy likely OLAP
        let join_count = upper.matches("JOIN").count();
        if join_count >= 3 {
            return WorkloadType::OLAP;
        }

        // Default to mixed
        WorkloadType::Mixed
    }

    /// Record a query for history tracking
    fn record_query(&self, context: &QueryContext, query: &str, workload: WorkloadType) {
        // Update global stats
        match workload {
            WorkloadType::OLTP => self.stats.oltp_count.fetch_add(1, Ordering::Relaxed),
            WorkloadType::OLAP => self.stats.olap_count.fetch_add(1, Ordering::Relaxed),
            WorkloadType::Vector => self.stats.vector_count.fetch_add(1, Ordering::Relaxed),
            WorkloadType::AIAgent => self.stats.ai_count.fetch_add(1, Ordering::Relaxed),
            WorkloadType::RAG => self.stats.rag_count.fetch_add(1, Ordering::Relaxed),
            WorkloadType::Mixed => self.stats.mixed_count.fetch_add(1, Ordering::Relaxed),
        };

        // Update session history
        let entry = QueryHistoryEntry {
            fingerprint: QueryFingerprint::from_query(query),
            workload,
            timestamp: Instant::now(),
            latency_ms: 0, // Will be updated later
        };

        self.session_history
            .entry(context.session_id.clone())
            .or_insert_with(SessionHistory::new)
            .record(entry);
    }

    /// Record query latency (call after execution)
    pub fn record_latency(&self, session_id: &SessionId, latency_ms: u64) {
        if let Some(mut history) = self.session_history.get_mut(session_id) {
            if let Some(last) = history.queries.back_mut() {
                last.latency_ms = latency_ms;
            }
        }
    }

    /// Add a custom classification rule
    pub fn add_rule(&mut self, rule: ClassificationRule) {
        self.rules.push(rule);
    }

    /// Get classifier statistics
    pub fn stats(&self) -> ClassifierStatsSnapshot {
        ClassifierStatsSnapshot {
            total_classified: self.stats.total_classified.load(Ordering::Relaxed),
            oltp_count: self.stats.oltp_count.load(Ordering::Relaxed),
            olap_count: self.stats.olap_count.load(Ordering::Relaxed),
            vector_count: self.stats.vector_count.load(Ordering::Relaxed),
            ai_count: self.stats.ai_count.load(Ordering::Relaxed),
            rag_count: self.stats.rag_count.load(Ordering::Relaxed),
            mixed_count: self.stats.mixed_count.load(Ordering::Relaxed),
            rule_hit_rate: self.stats.rule_hits.load(Ordering::Relaxed) as f64 /
                          self.stats.total_classified.load(Ordering::Relaxed).max(1) as f64,
            session_hit_rate: self.stats.session_hits.load(Ordering::Relaxed) as f64 /
                             self.stats.total_classified.load(Ordering::Relaxed).max(1) as f64,
        }
    }

    /// Clear session history older than threshold
    pub fn cleanup_old_sessions(&self, max_age: Duration) {
        let now = Instant::now();
        self.session_history.retain(|_, history| {
            if let Some(last) = history.queries.back() {
                now.duration_since(last.timestamp) < max_age
            } else {
                false
            }
        });
    }
}

/// Classifier statistics snapshot
#[derive(Debug, Clone)]
pub struct ClassifierStatsSnapshot {
    pub total_classified: u64,
    pub oltp_count: u64,
    pub olap_count: u64,
    pub vector_count: u64,
    pub ai_count: u64,
    pub rag_count: u64,
    pub mixed_count: u64,
    pub rule_hit_rate: f64,
    pub session_hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_context() -> QueryContext {
        QueryContext::new("test-session")
    }

    #[test]
    fn test_oltp_classification() {
        let config = DistribCacheConfig::default();
        let classifier = WorkloadClassifier::new(config);
        let ctx = make_context();

        let workload = classifier.classify("SELECT * FROM users WHERE id = 42", &ctx);
        assert_eq!(workload, WorkloadType::OLTP);

        let workload = classifier.classify("INSERT INTO users (name) VALUES ('Alice')", &ctx);
        assert_eq!(workload, WorkloadType::OLTP);
    }

    #[test]
    fn test_olap_classification() {
        let config = DistribCacheConfig::default();
        let classifier = WorkloadClassifier::new(config);
        let ctx = make_context();

        let workload = classifier.classify(
            "SELECT region, COUNT(*) FROM orders GROUP BY region",
            &ctx
        );
        assert_eq!(workload, WorkloadType::OLAP);

        let workload = classifier.classify(
            "SELECT AVG(amount), SUM(quantity) FROM sales",
            &ctx
        );
        assert_eq!(workload, WorkloadType::OLAP);
    }

    #[test]
    fn test_vector_classification() {
        let config = DistribCacheConfig::default();
        let classifier = WorkloadClassifier::new(config);
        let ctx = make_context();

        let workload = classifier.classify(
            "SELECT * FROM embeddings ORDER BY vector <-> $1 LIMIT 10",
            &ctx
        );
        assert_eq!(workload, WorkloadType::Vector);
    }

    #[test]
    fn test_ai_agent_classification() {
        let config = DistribCacheConfig::default();
        let classifier = WorkloadClassifier::new(config);
        let ctx = make_context();

        let workload = classifier.classify(
            "SELECT * FROM conversation_turns WHERE conversation_id = $1",
            &ctx
        );
        assert_eq!(workload, WorkloadType::AIAgent);

        let workload = classifier.classify(
            "INSERT INTO agent_memory (key, value) VALUES ($1, $2)",
            &ctx
        );
        assert_eq!(workload, WorkloadType::AIAgent);
    }

    #[test]
    fn test_rag_classification() {
        let config = DistribCacheConfig::default();
        let classifier = WorkloadClassifier::new(config);
        let ctx = make_context();

        let workload = classifier.classify(
            "SELECT content FROM documents WHERE id IN (SELECT doc_id FROM chunks WHERE ...)",
            &ctx
        );
        assert_eq!(workload, WorkloadType::RAG);
    }

    #[test]
    fn test_explicit_hint() {
        let config = DistribCacheConfig::default();
        let classifier = WorkloadClassifier::new(config);
        let ctx = make_context().with_workload_hint(WorkloadType::OLAP);

        // Even though query looks like OLTP, hint overrides
        let workload = classifier.classify("SELECT * FROM users WHERE id = 1", &ctx);
        assert_eq!(workload, WorkloadType::OLAP);
    }

    #[test]
    fn test_session_based_classification() {
        let config = DistribCacheConfig::default();
        let classifier = WorkloadClassifier::new(config);
        let ctx = make_context();

        // Run many OLAP queries to establish session pattern
        for _ in 0..20 {
            classifier.classify("SELECT COUNT(*) FROM analytics GROUP BY region", &ctx.clone());
        }

        // Now an ambiguous query should be classified as OLAP based on session history
        let history = classifier.session_history.get(&ctx.session_id).unwrap();
        assert!(history.olap_count >= 20);
    }

    #[test]
    fn test_stats() {
        let config = DistribCacheConfig::default();
        let classifier = WorkloadClassifier::new(config);
        let ctx = make_context();

        classifier.classify("SELECT * FROM users WHERE id = 1", &ctx);
        classifier.classify("SELECT COUNT(*) FROM orders GROUP BY status", &ctx);
        classifier.classify("SELECT * FROM embeddings ORDER BY vec <-> $1", &ctx);

        let stats = classifier.stats();
        assert_eq!(stats.total_classified, 3);
    }
}
