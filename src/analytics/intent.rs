//! Query Intent Classification
//!
//! Classify queries by intent for AI/Agent workload analysis.
//! Supports RAG analytics, workflow tracing, and cost attribution.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;

use super::statistics::QueryExecution;
use super::{AgentCost, CostReport, UserCost};

/// Query intent classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryIntent {
    /// Data retrieval (SELECT for reading data)
    Retrieval,

    /// Data storage (INSERT, UPDATE, DELETE)
    Storage,

    /// Embedding storage/retrieval (vector operations)
    Embedding,

    /// Schema operations (DDL)
    Schema,

    /// Transaction control
    Transaction,

    /// Session/utility operations
    Utility,

    /// RAG context retrieval
    RagRetrieval,

    /// RAG document indexing
    RagIndexing,

    /// Agent memory operations
    AgentMemory,

    /// Unknown/unclassified
    Unknown,
}

impl std::fmt::Display for QueryIntent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryIntent::Retrieval => write!(f, "retrieval"),
            QueryIntent::Storage => write!(f, "storage"),
            QueryIntent::Embedding => write!(f, "embedding"),
            QueryIntent::Schema => write!(f, "schema"),
            QueryIntent::Transaction => write!(f, "transaction"),
            QueryIntent::Utility => write!(f, "utility"),
            QueryIntent::RagRetrieval => write!(f, "rag_retrieval"),
            QueryIntent::RagIndexing => write!(f, "rag_indexing"),
            QueryIntent::AgentMemory => write!(f, "agent_memory"),
            QueryIntent::Unknown => write!(f, "unknown"),
        }
    }
}

/// Query classifier for intent detection
pub struct QueryClassifier {
    /// Embedding table patterns
    embedding_tables: Vec<String>,

    /// RAG table patterns
    rag_tables: Vec<String>,

    /// Memory table patterns
    memory_tables: Vec<String>,
}

impl QueryClassifier {
    /// Create new classifier with default patterns
    pub fn new() -> Self {
        Self {
            embedding_tables: vec![
                "embeddings".to_string(),
                "vectors".to_string(),
                "embedding".to_string(),
                "vector_store".to_string(),
            ],
            rag_tables: vec![
                "documents".to_string(),
                "chunks".to_string(),
                "doc_chunks".to_string(),
                "knowledge_base".to_string(),
                "context".to_string(),
            ],
            memory_tables: vec![
                "memory".to_string(),
                "agent_memory".to_string(),
                "conversation_history".to_string(),
                "chat_history".to_string(),
                "sessions".to_string(),
            ],
        }
    }

    /// Create classifier with custom patterns
    pub fn with_patterns(
        embedding_tables: Vec<String>,
        rag_tables: Vec<String>,
        memory_tables: Vec<String>,
    ) -> Self {
        Self {
            embedding_tables,
            rag_tables,
            memory_tables,
        }
    }

    /// Classify query intent.
    ///
    /// Allocates one lowercase copy of the statement; callers that already
    /// hold one (the analytics ingest path does — see
    /// [`QueryClassifier::classify_lower`]) should pass it in instead. This
    /// previously built BOTH an uppercase and a lowercase copy of every
    /// statement.
    pub fn classify(&self, query: &str) -> QueryIntent {
        self.classify_lower(&query.to_lowercase())
    }

    /// Classify from an already-lowercased copy of the statement.
    ///
    /// Behaviourally identical to [`QueryClassifier::classify`]: the keyword
    /// tests only ever inspected the leading token case-insensitively, so
    /// matching lowercase keywords against the lowercase copy gives the same
    /// answer while saving a whole-SQL allocation per query.
    #[allow(clippy::if_same_then_else)]
    pub fn classify_lower(&self, lower: &str) -> QueryIntent {
        let head = lower.trim_start();

        // Check for transaction control
        if head.starts_with("begin")
            || head.starts_with("commit")
            || head.starts_with("rollback")
            || head.starts_with("start transaction")
            || head.starts_with("savepoint")
        {
            return QueryIntent::Transaction;
        }

        // Check for utility operations
        if head.starts_with("set")
            || head.starts_with("show")
            || head.starts_with("explain")
            || head.starts_with("analyze")
            || head.starts_with("vacuum")
        {
            return QueryIntent::Utility;
        }

        // Check for schema operations
        if head.starts_with("create")
            || head.starts_with("alter")
            || head.starts_with("drop")
            || head.starts_with("truncate")
        {
            return QueryIntent::Schema;
        }

        // Check for RAG operations (before embedding — RAG tables like
        // "chunks" may contain an "embedding" column, so check RAG first)
        if self.matches_table_pattern(lower, &self.rag_tables) {
            if head.starts_with("select") {
                return QueryIntent::RagRetrieval;
            } else if head.starts_with("insert") || head.starts_with("update") {
                return QueryIntent::RagIndexing;
            }
        }

        // Check for embedding operations
        if self.matches_table_pattern(lower, &self.embedding_tables) {
            if head.starts_with("select") {
                return QueryIntent::Embedding;
            } else if head.starts_with("insert") || head.starts_with("update") {
                return QueryIntent::Embedding;
            }
        }

        // Check for agent memory operations
        if self.matches_table_pattern(lower, &self.memory_tables) {
            return QueryIntent::AgentMemory;
        }

        // Check for vector similarity search patterns
        if lower.contains("cosine_similarity")
            || lower.contains("l2_distance")
            || lower.contains("inner_product")
            || lower.contains("<->")  // pgvector operator
            || lower.contains("<=>")
        // pgvector operator
        {
            return QueryIntent::Embedding;
        }

        // Basic classification by operation
        if head.starts_with("select") {
            return QueryIntent::Retrieval;
        }

        if head.starts_with("insert") || head.starts_with("update") || head.starts_with("delete") {
            return QueryIntent::Storage;
        }

        QueryIntent::Unknown
    }

    /// Check if query matches any table pattern
    fn matches_table_pattern(&self, query: &str, patterns: &[String]) -> bool {
        for pattern in patterns {
            if query.contains(pattern) {
                return true;
            }
        }
        false
    }

    /// Add embedding table pattern
    pub fn add_embedding_pattern(&mut self, pattern: impl Into<String>) {
        self.embedding_tables.push(pattern.into());
    }

    /// Add RAG table pattern
    pub fn add_rag_pattern(&mut self, pattern: impl Into<String>) {
        self.rag_tables.push(pattern.into());
    }

    /// Add memory table pattern
    pub fn add_memory_pattern(&mut self, pattern: impl Into<String>) {
        self.memory_tables.push(pattern.into());
    }
}

impl Default for QueryClassifier {
    fn default() -> Self {
        Self::new()
    }
}

/// RAG analytics
pub struct RagAnalytics {
    /// Retrieval count
    retrieval_count: AtomicU64,
    /// Retrieval time (microseconds)
    retrieval_time_us: AtomicU64,
    /// Indexing count
    indexing_count: AtomicU64,
    /// Indexing time (microseconds)
    indexing_time_us: AtomicU64,
    /// Documents indexed
    documents_indexed: AtomicU64,
    /// Chunks created
    chunks_created: AtomicU64,
}

impl RagAnalytics {
    /// Create new RAG analytics
    pub fn new() -> Self {
        Self {
            retrieval_count: AtomicU64::new(0),
            retrieval_time_us: AtomicU64::new(0),
            indexing_count: AtomicU64::new(0),
            indexing_time_us: AtomicU64::new(0),
            documents_indexed: AtomicU64::new(0),
            chunks_created: AtomicU64::new(0),
        }
    }

    /// Record retrieval operation
    pub fn record_retrieval(&self, duration: Duration) {
        self.retrieval_count.fetch_add(1, Ordering::Relaxed);
        self.retrieval_time_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record indexing operation
    pub fn record_indexing(&self, duration: Duration, chunks: u64) {
        self.indexing_count.fetch_add(1, Ordering::Relaxed);
        self.indexing_time_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
        self.chunks_created.fetch_add(chunks, Ordering::Relaxed);
    }

    /// Get retrieval stats
    pub fn retrieval_stats(&self) -> (u64, Duration) {
        let count = self.retrieval_count.load(Ordering::Relaxed);
        let time = Duration::from_micros(self.retrieval_time_us.load(Ordering::Relaxed));
        (count, time)
    }

    /// Get indexing stats
    pub fn indexing_stats(&self) -> (u64, Duration, u64) {
        let count = self.indexing_count.load(Ordering::Relaxed);
        let time = Duration::from_micros(self.indexing_time_us.load(Ordering::Relaxed));
        let chunks = self.chunks_created.load(Ordering::Relaxed);
        (count, time, chunks)
    }

    /// Reset
    pub fn reset(&self) {
        self.retrieval_count.store(0, Ordering::Relaxed);
        self.retrieval_time_us.store(0, Ordering::Relaxed);
        self.indexing_count.store(0, Ordering::Relaxed);
        self.indexing_time_us.store(0, Ordering::Relaxed);
        self.documents_indexed.store(0, Ordering::Relaxed);
        self.chunks_created.store(0, Ordering::Relaxed);
    }
}

impl Default for RagAnalytics {
    fn default() -> Self {
        Self::new()
    }
}

/// Workflow step
#[derive(Debug, Clone)]
pub struct WorkflowStep {
    /// Step index
    pub index: usize,
    /// Query executed
    pub query: String,
    /// Duration
    pub duration: Duration,
    /// Timestamp
    pub timestamp_nanos: u64,
    /// Intent classification
    pub intent: QueryIntent,
    /// Rows affected/returned
    pub rows: usize,
    /// Error if failed
    pub error: Option<String>,
}

/// Workflow trace
#[derive(Debug, Clone)]
pub struct WorkflowTrace {
    /// Workflow ID
    pub workflow_id: String,
    /// Start timestamp
    pub start_nanos: u64,
    /// End timestamp (if completed)
    pub end_nanos: Option<u64>,
    /// Steps in workflow
    pub steps: Vec<WorkflowStep>,
    /// Total duration
    pub total_duration: Duration,
    /// User who initiated
    pub user: String,
    /// Agent/client identifier
    pub agent_id: Option<String>,
}

impl WorkflowTrace {
    /// Create new workflow trace
    pub fn new(workflow_id: impl Into<String>, user: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            start_nanos: now_nanos(),
            end_nanos: None,
            steps: Vec::new(),
            total_duration: Duration::ZERO,
            user: user.into(),
            agent_id: None,
        }
    }

    /// Add step
    pub fn add_step(&mut self, step: WorkflowStep) {
        self.steps.push(step);
        self.update_duration();
    }

    /// Complete workflow
    pub fn complete(&mut self) {
        self.end_nanos = Some(now_nanos());
        self.update_duration();
    }

    /// Update total duration
    fn update_duration(&mut self) {
        self.total_duration = self.steps.iter().map(|s| s.duration).sum();
    }

    /// Check if completed
    pub fn is_complete(&self) -> bool {
        self.end_nanos.is_some()
    }

    /// Get step count
    pub fn step_count(&self) -> usize {
        self.steps.len()
    }

    /// Get error count
    pub fn error_count(&self) -> usize {
        self.steps.iter().filter(|s| s.error.is_some()).count()
    }
}

/// Workflow tracer
pub struct WorkflowTracer {
    /// Active workflows
    workflows: DashMap<String, WorkflowTrace>,
    /// Completed workflows (recent)
    completed: RwLock<VecDeque<WorkflowTrace>>,
    /// Max completed to keep
    max_completed: usize,
    /// Total workflows
    total_workflows: AtomicU64,
}

impl WorkflowTracer {
    /// Create new workflow tracer
    pub fn new() -> Self {
        Self::with_max_completed(100)
    }

    /// Create with custom limit
    pub fn with_max_completed(max: usize) -> Self {
        Self {
            workflows: DashMap::new(),
            completed: RwLock::new(VecDeque::new()),
            max_completed: max,
            total_workflows: AtomicU64::new(0),
        }
    }

    /// Record workflow step
    pub fn record_step(&self, workflow_id: &str, execution: &QueryExecution) {
        let classifier = QueryClassifier::new();
        let intent = classifier.classify(&execution.query);

        let mut workflow = self
            .workflows
            .entry(workflow_id.to_string())
            .or_insert_with(|| {
                self.total_workflows.fetch_add(1, Ordering::Relaxed);
                WorkflowTrace::new(workflow_id, &execution.user)
            });

        let step = WorkflowStep {
            index: workflow.steps.len(),
            query: execution.query.clone(),
            duration: execution.duration,
            timestamp_nanos: now_nanos(),
            intent,
            rows: execution.rows,
            error: execution.error.clone(),
        };

        workflow.add_step(step);
    }

    /// Complete workflow
    pub fn complete_workflow(&self, workflow_id: &str) {
        if let Some((_, mut workflow)) = self.workflows.remove(workflow_id) {
            workflow.complete();

            let mut completed = self.completed.write();
            completed.push_back(workflow);

            while completed.len() > self.max_completed {
                completed.pop_front();
            }
        }
    }

    /// Get active workflow
    pub fn get_workflow(&self, workflow_id: &str) -> Option<WorkflowTrace> {
        self.workflows.get(workflow_id).map(|w| w.clone())
    }

    /// Get recent completed workflows
    pub fn recent(&self, limit: usize) -> Vec<WorkflowTrace> {
        self.completed
            .read()
            .iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    /// Get active workflow count
    pub fn active_count(&self) -> usize {
        self.workflows.len()
    }

    /// Get total workflow count
    pub fn total_count(&self) -> u64 {
        self.total_workflows.load(Ordering::Relaxed)
    }

    /// Reset
    pub fn reset(&self) {
        self.workflows.clear();
        self.completed.write().clear();
        self.total_workflows.store(0, Ordering::Relaxed);
    }
}

impl Default for WorkflowTracer {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-user cost tracking
struct UserCostTracker {
    queries: AtomicU64,
    time_us: AtomicU64,
}

impl UserCostTracker {
    fn new() -> Self {
        Self {
            queries: AtomicU64::new(0),
            time_us: AtomicU64::new(0),
        }
    }

    fn record(&self, duration: Duration) {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.time_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }
}

/// Cost attribution tracker
pub struct CostAttribution {
    /// Per-user costs
    users: DashMap<String, UserCostTracker>,
    /// Per-agent costs
    agents: DashMap<String, UserCostTracker>,
    /// Total queries
    total_queries: AtomicU64,
    /// Total time (microseconds)
    total_time_us: AtomicU64,
    /// Cost per query-second (configurable, default $0.0001)
    cost_per_query_second: f64,
}

impl CostAttribution {
    /// Create new cost attribution
    pub fn new() -> Self {
        Self {
            users: DashMap::new(),
            agents: DashMap::new(),
            total_queries: AtomicU64::new(0),
            total_time_us: AtomicU64::new(0),
            cost_per_query_second: 0.0001,
        }
    }

    /// Set cost per query-second
    pub fn set_cost_rate(&mut self, rate: f64) {
        self.cost_per_query_second = rate;
    }

    /// Record execution
    pub fn record(&self, execution: &QueryExecution) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
        self.total_time_us
            .fetch_add(execution.duration.as_micros() as u64, Ordering::Relaxed);

        // Track by user
        self.users
            .entry(execution.user.clone())
            .or_insert_with(UserCostTracker::new)
            .record(execution.duration);

        // Track by agent (if workflow is present, use as agent ID)
        if let Some(ref workflow_id) = execution.workflow_id {
            // Extract agent ID from workflow ID (e.g., "agent-123-workflow-456" -> "agent-123")
            let agent_id = workflow_id.split('-').take(2).collect::<Vec<_>>().join("-");

            self.agents
                .entry(agent_id)
                .or_insert_with(UserCostTracker::new)
                .record(execution.duration);
        }
    }

    /// Generate cost report
    pub fn report(&self) -> CostReport {
        let total_queries = self.total_queries.load(Ordering::Relaxed);
        let total_time_us = self.total_time_us.load(Ordering::Relaxed);
        let total_time_seconds = total_time_us as f64 / 1_000_000.0;
        let estimated_cost = total_time_seconds * self.cost_per_query_second;

        let by_user: Vec<_> = self
            .users
            .iter()
            .map(|entry| {
                let queries = entry.value().queries.load(Ordering::Relaxed);
                let time_us = entry.value().time_us.load(Ordering::Relaxed);
                let time_seconds = time_us as f64 / 1_000_000.0;

                UserCost {
                    user: entry.key().clone(),
                    queries,
                    time_seconds,
                    cost_usd: time_seconds * self.cost_per_query_second,
                }
            })
            .collect();

        let by_agent: Vec<_> = self
            .agents
            .iter()
            .map(|entry| {
                let queries = entry.value().queries.load(Ordering::Relaxed);
                let time_us = entry.value().time_us.load(Ordering::Relaxed);
                let time_seconds = time_us as f64 / 1_000_000.0;

                AgentCost {
                    agent_id: entry.key().clone(),
                    queries,
                    time_seconds,
                    cost_usd: time_seconds * self.cost_per_query_second,
                }
            })
            .collect();

        CostReport {
            total_queries,
            total_time_seconds,
            estimated_cost_usd: estimated_cost,
            by_user,
            by_agent,
        }
    }

    /// Reset
    pub fn reset(&self) {
        self.users.clear();
        self.agents.clear();
        self.total_queries.store(0, Ordering::Relaxed);
        self.total_time_us.store(0, Ordering::Relaxed);
    }
}

impl Default for CostAttribution {
    fn default() -> Self {
        Self::new()
    }
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `classify_lower` is the allocation-free entry point used by the ingest
    /// path; it must agree with `classify` on every branch, including the
    /// leading-whitespace and mixed-case cases.
    #[test]
    fn test_classify_lower_matches_classify() {
        let classifier = QueryClassifier::new();
        let cases = [
            "BEGIN",
            "  begin",
            "Start Transaction",
            "SAVEPOINT sp1",
            "SET search_path = public",
            "SHOW all",
            "EXPLAIN SELECT 1",
            "VACUUM ANALYZE",
            "CREATE TABLE t (id INT)",
            "TRUNCATE t",
            "SELECT * FROM chunks WHERE id = 1",
            "INSERT INTO documents VALUES (1)",
            "SELECT * FROM embeddings ORDER BY v <-> '[1,2]'",
            "UPDATE vectors SET v = '[1]'",
            "SELECT * FROM agent_memory",
            "SELECT cosine_similarity(a, b) FROM t",
            "  SELECT * FROM t",
            "DELETE FROM t WHERE id = 1",
            "GRANT SELECT ON t TO bob",
            "",
        ];
        for sql in cases {
            assert_eq!(
                classifier.classify(sql),
                classifier.classify_lower(&sql.to_lowercase()),
                "classify/classify_lower disagree on {sql:?}"
            );
        }
    }

    #[test]
    fn test_query_classifier_basic() {
        let classifier = QueryClassifier::new();

        assert_eq!(
            classifier.classify("SELECT * FROM users"),
            QueryIntent::Retrieval
        );
        assert_eq!(
            classifier.classify("INSERT INTO users VALUES (1)"),
            QueryIntent::Storage
        );
        assert_eq!(
            classifier.classify("UPDATE users SET name = 'Bob'"),
            QueryIntent::Storage
        );
        assert_eq!(
            classifier.classify("DELETE FROM users WHERE id = 1"),
            QueryIntent::Storage
        );
    }

    #[test]
    fn test_query_classifier_transaction() {
        let classifier = QueryClassifier::new();

        assert_eq!(classifier.classify("BEGIN"), QueryIntent::Transaction);
        assert_eq!(classifier.classify("COMMIT"), QueryIntent::Transaction);
        assert_eq!(classifier.classify("ROLLBACK"), QueryIntent::Transaction);
        assert_eq!(
            classifier.classify("START TRANSACTION"),
            QueryIntent::Transaction
        );
    }

    #[test]
    fn test_query_classifier_schema() {
        let classifier = QueryClassifier::new();

        assert_eq!(
            classifier.classify("CREATE TABLE foo (id INT)"),
            QueryIntent::Schema
        );
        assert_eq!(
            classifier.classify("ALTER TABLE foo ADD COLUMN bar TEXT"),
            QueryIntent::Schema
        );
        assert_eq!(classifier.classify("DROP TABLE foo"), QueryIntent::Schema);
    }

    #[test]
    fn test_query_classifier_embedding() {
        let classifier = QueryClassifier::new();

        assert_eq!(
            classifier.classify("SELECT * FROM embeddings WHERE id = 1"),
            QueryIntent::Embedding
        );
        assert_eq!(
            classifier.classify("INSERT INTO vectors (embedding) VALUES (?)"),
            QueryIntent::Embedding
        );
        assert_eq!(
            classifier.classify("SELECT * FROM items ORDER BY embedding <-> '[1,2,3]'"),
            QueryIntent::Embedding
        );
    }

    #[test]
    fn test_query_classifier_rag() {
        let classifier = QueryClassifier::new();

        assert_eq!(
            classifier.classify("SELECT * FROM documents WHERE topic = 'AI'"),
            QueryIntent::RagRetrieval
        );
        assert_eq!(
            classifier.classify("INSERT INTO chunks (content, embedding) VALUES (?, ?)"),
            QueryIntent::RagIndexing
        );
    }

    #[test]
    fn test_query_classifier_agent_memory() {
        let classifier = QueryClassifier::new();

        assert_eq!(
            classifier.classify("SELECT * FROM agent_memory WHERE session_id = ?"),
            QueryIntent::AgentMemory
        );
        assert_eq!(
            classifier.classify("INSERT INTO conversation_history (message) VALUES (?)"),
            QueryIntent::AgentMemory
        );
    }

    #[test]
    fn test_workflow_tracer() {
        let tracer = WorkflowTracer::new();

        let execution =
            QueryExecution::new("SELECT 1", Duration::from_millis(5)).with_user("alice");

        tracer.record_step("workflow-1", &execution);
        tracer.record_step("workflow-1", &execution);

        let workflow = tracer.get_workflow("workflow-1").unwrap();
        assert_eq!(workflow.step_count(), 2);
        assert_eq!(workflow.user, "alice");

        tracer.complete_workflow("workflow-1");
        assert!(tracer.get_workflow("workflow-1").is_none());

        let recent = tracer.recent(10);
        assert_eq!(recent.len(), 1);
        assert!(recent[0].is_complete());
    }

    #[test]
    fn test_cost_attribution() {
        let cost = CostAttribution::new();

        let execution = QueryExecution::new("SELECT 1", Duration::from_secs(1)).with_user("alice");

        cost.record(&execution);
        cost.record(&execution);

        let report = cost.report();
        assert_eq!(report.total_queries, 2);
        assert!((report.total_time_seconds - 2.0).abs() < 0.001);
        assert!(report
            .by_user
            .iter()
            .any(|u| u.user == "alice" && u.queries == 2));
    }

    #[test]
    fn test_rag_analytics() {
        let rag = RagAnalytics::new();

        rag.record_retrieval(Duration::from_millis(50));
        rag.record_retrieval(Duration::from_millis(30));
        rag.record_indexing(Duration::from_millis(100), 5);

        let (retrieval_count, retrieval_time) = rag.retrieval_stats();
        assert_eq!(retrieval_count, 2);
        assert_eq!(retrieval_time, Duration::from_millis(80));

        let (indexing_count, indexing_time, chunks) = rag.indexing_stats();
        assert_eq!(indexing_count, 1);
        assert_eq!(indexing_time, Duration::from_millis(100));
        assert_eq!(chunks, 5);
    }

    #[test]
    fn test_intent_display() {
        assert_eq!(QueryIntent::Retrieval.to_string(), "retrieval");
        assert_eq!(QueryIntent::RagRetrieval.to_string(), "rag_retrieval");
        assert_eq!(QueryIntent::AgentMemory.to_string(), "agent_memory");
    }
}
