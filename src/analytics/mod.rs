//! Query Analytics & Slow Query Log
//!
//! Comprehensive query analytics at the proxy layer:
//! - Query fingerprinting and normalization
//! - Execution statistics and histograms
//! - Slow query logging
//! - Pattern detection (N+1, bursts)
//! - AI/Agent workload classification

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use tokio::sync::{mpsc, oneshot};

pub mod config;
pub mod fingerprinter;
pub mod histogram;
pub mod intent;
pub mod metrics;
pub mod patterns;
pub mod slow_log;
pub mod statistics;

// Re-exports
pub use config::{
    AnalyticsConfig, AnalyticsConfigBuilder, PatternConfig, SamplingConfig, SlowQueryConfig,
    DEFAULT_ANALYTICS_QUEUE_CAPACITY,
};
pub use fingerprinter::{ascii_lower, OperationType, QueryFingerprint, QueryFingerprinter};
pub use histogram::{HistogramBucket, HistogramSnapshot, LatencyHistogram};
pub use intent::{
    CostAttribution, QueryClassifier, QueryIntent, RagAnalytics, WorkflowTrace, WorkflowTracer,
};
pub use metrics::{AnalyticsMetrics, AnalyticsSnapshot, QueryMetricEntry};
pub use patterns::{NplusOnePattern, PatternAlert, PatternDetector, QueryBurst};
pub use slow_log::{SlowQueryEntry, SlowQueryLog, SlowQueryReader};
pub use statistics::{QueryExecution, QueryStatistics, QueryStats, StatisticsStore};

/// A unit of work on the analytics ingest queue.
///
/// `Record` is boxed so the enum stays pointer-sized: a `QueryExecution`
/// carries five `String`s and would otherwise inflate every queue slot (and
/// trip `clippy::large_enum_variant`).
enum AnalyticsMsg {
    /// Fingerprint, meter and pattern-match this execution.
    Record(Box<QueryExecution>),
    /// Barrier — acked once everything queued ahead of it has been ingested.
    Flush(oneshot::Sender<()>),
}

/// Main analytics engine
pub struct QueryAnalytics {
    /// Configuration
    config: AnalyticsConfig,

    /// Query fingerprinter
    fingerprinter: QueryFingerprinter,

    /// Statistics store (fingerprint -> stats)
    statistics: StatisticsStore,

    /// Slow query log
    slow_log: SlowQueryLog,

    /// Pattern detector
    patterns: PatternDetector,

    /// Metrics
    metrics: AnalyticsMetrics,

    /// Query classifier (AI intent)
    classifier: QueryClassifier,

    /// Workflow tracer
    workflows: WorkflowTracer,

    /// Cost attribution
    costs: CostAttribution,

    /// Queue to the single background consumer, installed by
    /// [`QueryAnalytics::start_consumer`]. While unset (library and unit-test
    /// default) `record` does the whole ingest inline on the caller's task,
    /// exactly as before.
    queue: OnceLock<mpsc::Sender<AnalyticsMsg>>,

    /// Executions dropped because the queue was full or closed, plus any whose
    /// ingest panicked (see [`QueryAnalytics::ingest_guarded`]). Surfaced as
    /// `analytics_dropped_total` on `GET /api/analytics`.
    dropped: AtomicU64,

    /// Test-only injection point for [`QueryAnalytics::ingest_guarded`]: makes
    /// the next ingest panic so the consumer's panic guard can be exercised.
    #[cfg(test)]
    panic_next_ingest: std::sync::atomic::AtomicBool,
}

impl QueryAnalytics {
    /// Create new analytics engine
    pub fn new(config: AnalyticsConfig) -> Self {
        let slow_log = SlowQueryLog::new(config.slow_query.clone());
        let patterns = PatternDetector::new(config.patterns.clone());
        let statistics = StatisticsStore::new(config.max_fingerprints);
        let fingerprinter = QueryFingerprinter::with_cache_limits(
            config.fingerprint_cache_size,
            config.fingerprint_cache_max_sql_bytes,
        );

        Self {
            fingerprinter,
            statistics,
            slow_log,
            patterns,
            metrics: AnalyticsMetrics::new(),
            classifier: QueryClassifier::new(),
            workflows: WorkflowTracer::new(),
            costs: CostAttribution::new(),
            queue: OnceLock::new(),
            dropped: AtomicU64::new(0),
            #[cfg(test)]
            panic_next_ingest: std::sync::atomic::AtomicBool::new(false),
            config,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(AnalyticsConfig::default())
    }

    /// Record a query execution.
    ///
    /// When the background consumer is running (see
    /// [`QueryAnalytics::start_consumer`]) this only hands the record to a
    /// bounded queue and returns: fingerprinting, metrics, pattern detection
    /// and cost attribution all happen on that task, OFF the connection task
    /// that served the query. If the queue is full the record is dropped and
    /// [`QueryAnalytics::dropped_total`] is incremented — the query relay is
    /// never blocked by analytics.
    ///
    /// With no consumer installed the work is done inline on the caller's
    /// task, byte-for-byte the previous behaviour.
    pub fn record(&self, execution: QueryExecution) {
        if !self.config.enabled {
            return;
        }

        // Apply sampling if configured
        if self.config.sampling.enabled && !self.should_sample() {
            return;
        }

        if let Some(tx) = self.queue.get() {
            if tx
                .try_send(AnalyticsMsg::Record(Box::new(execution)))
                .is_err()
            {
                // Full or closed: drop the sample rather than stall the relay.
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
            return;
        }

        self.ingest(&execution);
    }

    /// Synchronous [`QueryAnalytics::record`]: performs the whole ingest on
    /// the calling thread even when the background consumer is running. For
    /// tests and callers that must observe the effect immediately.
    pub fn record_now(&self, execution: QueryExecution) {
        if !self.config.enabled {
            return;
        }

        if self.config.sampling.enabled && !self.should_sample() {
            return;
        }

        self.ingest(&execution);
    }

    /// The actual analytics work. Runs on the background consumer task when
    /// one is installed, otherwise on the caller's task.
    fn ingest(&self, execution: &QueryExecution) {
        #[cfg(test)]
        if self
            .panic_next_ingest
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            panic!("test-injected ingest panic");
        }

        // ONE case conversion for the whole ingest, shared by fingerprinting
        // and intent classification (they took two of their own before, on top
        // of the two the fingerprinter itself used internally). ASCII folding
        // keeps byte offsets aligned with `execution.query` — see
        // [`analytics::ascii_lower`].
        let lower = ascii_lower(&execution.query);

        // Fingerprint the query (memoized: repeat SQL skips all regex work).
        let fingerprint = self
            .fingerprinter
            .fingerprint_cached_lower(&execution.query, &lower);

        // Record statistics
        self.statistics.record(&fingerprint, execution);

        // Check for slow query
        self.slow_log.log_if_slow(execution, &fingerprint);

        // Detect patterns
        if let Some(session) = &execution.session_id {
            self.patterns.record_query(session, execution, &fingerprint);
        }

        // Classify intent from the copy taken above: no second conversion.
        let intent = self.classifier.classify_lower(&lower);

        // Record metrics
        self.metrics.record(&fingerprint, execution, intent);

        // Track workflow if applicable
        if let Some(workflow_id) = &execution.workflow_id {
            self.workflows.record_step(workflow_id, execution);
        }

        // Attribute costs
        self.costs.record(execution);
    }

    /// [`QueryAnalytics::ingest`] with a panic guard.
    ///
    /// The consumer is a SINGLE task shared by every connection: a panic there
    /// would drop the receiver, so every later `record` would fail its
    /// `try_send` and analytics would be silently dead for the life of the
    /// process. Instead a panicking ingest is logged, counted as a dropped
    /// sample, and the consumer carries on with the next execution. State
    /// touched by the panicking ingest may be partially updated (the
    /// `AssertUnwindSafe`), which is strictly better than losing all of it.
    fn ingest_guarded(&self, execution: &QueryExecution) {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.ingest(execution)));
        if result.is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                query_len = execution.query.len(),
                "analytics ingest panicked; sample dropped, consumer continues"
            );
        }
    }

    /// Spawn the single background consumer and install its queue.
    ///
    /// Called once at server startup. A second call is a no-op returning
    /// `None`. The returned handle is aborted by the server at shutdown; the
    /// task also exits on its own once the engine is dropped (it holds only a
    /// `Weak`, so it never keeps the engine alive).
    ///
    /// Consumers of the shared state (`/api/analytics`, `/anomalies`) read the
    /// SAME structures as before — they are simply written from the consumer
    /// task, so a reading endpoint may lag the newest query by the time it
    /// takes to drain the queue (microseconds while the consumer keeps up).
    ///
    /// One semantic consequence: everything the ingest stamps with the current
    /// time — the slow-query entry timestamp, and the windows
    /// `PatternDetector` uses for N+1 and burst detection — is now stamped
    /// when the execution is INGESTED, not when the query completed. Under a
    /// queue backlog those windows stretch by the backlog delay (the recorded
    /// query DURATION is measured on the connection task and is unaffected).
    pub fn start_consumer(self: &Arc<Self>) -> Option<tokio::task::JoinHandle<()>> {
        let capacity = self.config.queue_capacity.max(1);
        let (tx, mut rx) = mpsc::channel::<AnalyticsMsg>(capacity);

        if self.queue.set(tx).is_err() {
            tracing::warn!("analytics consumer already running; ignoring duplicate start");
            return None;
        }

        let weak = Arc::downgrade(self);
        Some(tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let Some(this) = weak.upgrade() else {
                    break;
                };
                match msg {
                    AnalyticsMsg::Record(execution) => this.ingest_guarded(&execution),
                    AnalyticsMsg::Flush(ack) => {
                        let _ = ack.send(());
                    }
                }
            }
        }))
    }

    /// Wait until every execution queued before this call has been ingested.
    /// No-op when no consumer is running (records are already applied
    /// synchronously in that case).
    pub async fn flush(&self) {
        let Some(tx) = self.queue.get() else {
            return;
        };
        let (ack, done) = oneshot::channel();
        if tx.send(AnalyticsMsg::Flush(ack)).await.is_ok() {
            let _ = done.await;
        }
    }

    /// Executions dropped because the ingest queue was full (or closed), plus
    /// any whose ingest panicked. Also exposed as
    /// `heliosdb_proxy_analytics_dropped_total` on `/metrics/prometheus`.
    pub fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Check if we should sample this query
    fn should_sample(&self) -> bool {
        rand::random::<f64>() < self.config.sampling.rate
    }

    /// Get fingerprinter for external use
    pub fn fingerprinter(&self) -> &QueryFingerprinter {
        &self.fingerprinter
    }

    /// Get statistics for a fingerprint
    pub fn get_stats(&self, fingerprint_hash: u64) -> Option<QueryStats> {
        self.statistics.get(fingerprint_hash)
    }

    /// Get top queries by a metric
    pub fn top_queries(&self, order_by: OrderBy, limit: usize) -> Vec<QueryStats> {
        self.statistics.top(order_by, limit)
    }

    /// Get recent slow queries
    pub fn slow_queries(&self, limit: usize) -> Vec<SlowQueryEntry> {
        self.slow_log.recent(limit)
    }

    /// Get detected patterns
    pub fn get_patterns(&self) -> Vec<PatternAlert> {
        self.patterns.get_alerts()
    }

    /// Get metrics snapshot
    pub fn get_metrics(&self) -> AnalyticsSnapshot {
        self.metrics.snapshot()
    }

    /// Get analytics by query intent
    pub fn by_intent(&self) -> std::collections::HashMap<QueryIntent, IntentStats> {
        self.metrics.by_intent()
    }

    /// Get workflow traces
    pub fn get_workflows(&self, limit: usize) -> Vec<WorkflowTrace> {
        self.workflows.recent(limit)
    }

    /// Get cost attribution
    pub fn get_costs(&self) -> CostReport {
        self.costs.report()
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.statistics.reset();
        self.metrics.reset();
        self.workflows.reset();
        self.costs.reset();
    }
}

/// Order by options for top queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBy {
    TotalTime,
    AvgTime,
    Calls,
    Errors,
    P99Time,
    Rows,
}

impl std::str::FromStr for OrderBy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "total_time" | "totaltime" => Ok(OrderBy::TotalTime),
            "avg_time" | "avgtime" => Ok(OrderBy::AvgTime),
            "calls" | "count" => Ok(OrderBy::Calls),
            "errors" => Ok(OrderBy::Errors),
            "p99" | "p99_time" => Ok(OrderBy::P99Time),
            "rows" => Ok(OrderBy::Rows),
            _ => Err(format!("Unknown order by: {}", s)),
        }
    }
}

/// Intent statistics
#[derive(Debug, Clone)]
pub struct IntentStats {
    pub calls: u64,
    pub total_time_ms: u64,
    pub avg_time_ms: f64,
    pub cache_hit_ratio: f64,
}

/// Cost report
#[derive(Debug, Clone)]
pub struct CostReport {
    pub total_queries: u64,
    pub total_time_seconds: f64,
    pub estimated_cost_usd: f64,
    pub by_user: Vec<UserCost>,
    pub by_agent: Vec<AgentCost>,
}

/// Per-user cost
#[derive(Debug, Clone)]
pub struct UserCost {
    pub user: String,
    pub queries: u64,
    pub time_seconds: f64,
    pub cost_usd: f64,
}

/// Per-agent cost
#[derive(Debug, Clone)]
pub struct AgentCost {
    pub agent_id: String,
    pub queries: u64,
    pub time_seconds: f64,
    pub cost_usd: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_analytics_basic() {
        let analytics = QueryAnalytics::with_defaults();

        let execution = QueryExecution {
            query: "SELECT * FROM users WHERE id = 1".to_string(),
            duration: Duration::from_millis(5),
            rows: 1,
            error: None,
            user: "test_user".to_string(),
            client_ip: "127.0.0.1".to_string(),
            database: "test_db".to_string(),
            node: "primary".to_string(),
            session_id: Some("session_1".to_string()),
            workflow_id: None,
            parameters: None,
        };

        analytics.record(execution);

        let top = analytics.top_queries(OrderBy::Calls, 10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].calls, 1);
    }

    /// Building an execution record for the tests below.
    fn exec(query: &str, session: &str) -> QueryExecution {
        QueryExecution {
            query: query.to_string(),
            duration: Duration::from_millis(5),
            rows: 1,
            error: None,
            user: "test_user".to_string(),
            client_ip: "127.0.0.1".to_string(),
            database: "test_db".to_string(),
            node: "primary".to_string(),
            session_id: Some(session.to_string()),
            workflow_id: None,
            parameters: None,
        }
    }

    /// With the background consumer running, `record` must return without
    /// doing the work, and `flush` must make every queued execution visible
    /// in exactly the same shared state the admin endpoints read.
    #[tokio::test]
    async fn test_async_consumer_ingests_queued_executions() {
        let analytics = Arc::new(QueryAnalytics::with_defaults());
        let handle = analytics
            .start_consumer()
            .expect("first start_consumer must install the queue");

        for _ in 0..8 {
            analytics.record(exec("SELECT * FROM users WHERE id = 1", "s1"));
        }
        analytics.flush().await;

        let top = analytics.top_queries(OrderBy::Calls, 10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].calls, 8);
        assert_eq!(analytics.dropped_total(), 0);

        handle.abort();
    }

    /// A second `start_consumer` must not install a second queue.
    #[tokio::test]
    async fn test_start_consumer_is_idempotent() {
        let analytics = Arc::new(QueryAnalytics::with_defaults());
        let first = analytics.start_consumer();
        assert!(first.is_some());
        assert!(analytics.start_consumer().is_none());
        first.unwrap().abort();
    }

    /// A full queue must DROP the record (never block the relay) and count it.
    /// Runs on the single-threaded test runtime and never awaits, so the
    /// consumer cannot drain between `record` calls.
    #[tokio::test]
    async fn test_record_drops_when_queue_full() {
        let config = AnalyticsConfig::builder().queue_capacity(1).build();
        let analytics = Arc::new(QueryAnalytics::new(config));
        let handle = analytics.start_consumer().unwrap();

        for _ in 0..32 {
            analytics.record(exec("SELECT 1", "s1"));
        }

        assert!(
            analytics.dropped_total() > 0,
            "a capacity-1 queue with no consumer progress must drop records"
        );
        handle.abort();
    }

    /// `record_now` bypasses the queue: the effect is visible immediately even
    /// while the consumer is installed.
    #[tokio::test]
    async fn test_record_now_is_synchronous() {
        let analytics = Arc::new(QueryAnalytics::with_defaults());
        let handle = analytics.start_consumer().unwrap();

        analytics.record_now(exec("SELECT * FROM orders WHERE id = 7", "s1"));

        let top = analytics.top_queries(OrderBy::Calls, 10);
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].calls, 1);
        handle.abort();
    }

    /// Without a consumer, `flush` must return immediately rather than hang.
    #[tokio::test]
    async fn test_flush_without_consumer_is_noop() {
        let analytics = QueryAnalytics::with_defaults();
        analytics.record(exec("SELECT 1", "s1"));
        analytics.flush().await;
        assert_eq!(analytics.top_queries(OrderBy::Calls, 10).len(), 1);
    }

    /// Regression: a statement whose Unicode lowercase is LONGER than the
    /// original (U+0130 folds to two chars / three bytes) used to make the
    /// fingerprinter slice the original statement with an offset taken from
    /// the folded copy — an out-of-range / mid-char slice panic. On the shared
    /// consumer that panic killed the task and silently disabled analytics
    /// process-wide. It must now ingest cleanly, and later records must still
    /// be ingested.
    #[tokio::test]
    async fn test_consumer_survives_length_changing_unicode() {
        let analytics = Arc::new(QueryAnalytics::with_defaults());
        let handle = analytics.start_consumer().unwrap();

        // 14 bytes; 15 lowercased, and `find("from") + 4 == 15` — the exact
        // input that panicked "byte index 15 is out of range".
        analytics.record(exec("SELECT \u{130} from", "s1"));
        analytics.flush().await;
        analytics.record(exec("SELECT * FROM users WHERE id = 1", "s1"));
        analytics.flush().await;

        assert_eq!(analytics.dropped_total(), 0, "no sample may be lost");
        assert_eq!(
            analytics.top_queries(OrderBy::Calls, 10).len(),
            2,
            "the consumer must still be alive after the exotic statement"
        );
        handle.abort();
    }

    /// Defence in depth for the above: even if some future ingest panics, the
    /// single shared consumer must survive it — counting the sample as dropped
    /// and continuing — instead of dropping the receiver and killing analytics
    /// for the life of the process.
    #[tokio::test]
    async fn test_consumer_survives_panicking_ingest() {
        let analytics = Arc::new(QueryAnalytics::with_defaults());
        let handle = analytics.start_consumer().unwrap();

        analytics
            .panic_next_ingest
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        analytics.record(exec("SELECT 1", "s1"));
        analytics.flush().await;
        std::panic::set_hook(hook);

        assert_eq!(analytics.dropped_total(), 1, "panicked sample is dropped");
        assert!(analytics.top_queries(OrderBy::Calls, 10).is_empty());

        analytics.record(exec("SELECT * FROM users WHERE id = 1", "s1"));
        analytics.flush().await;
        assert_eq!(
            analytics.top_queries(OrderBy::Calls, 10).len(),
            1,
            "the consumer must keep ingesting after a panic"
        );
        assert_eq!(analytics.dropped_total(), 1);
        handle.abort();
    }

    /// The ingest path takes exactly ONE case conversion and shares it: the
    /// intent it records must equal what `QueryClassifier::classify` would
    /// have produced from the raw statement.
    #[test]
    fn test_ingest_classifies_from_the_shared_lowercase_copy() {
        let analytics = QueryAnalytics::with_defaults();
        let classifier = QueryClassifier::new();

        for sql in [
            "SELECT * FROM embeddings ORDER BY v <-> '[1,2]'",
            "INSERT INTO chunks (id) VALUES (1)",
            "BEGIN",
            "VACUUM ANALYZE",
            "UPDATE agent_memory SET v = 1",
        ] {
            assert_eq!(
                classifier.classify_lower(&ascii_lower(sql)),
                classifier.classify(sql),
                "shared ASCII-folded copy must classify {sql} identically"
            );
            analytics.record_now(exec(sql, "s1"));
        }

        assert_eq!(analytics.top_queries(OrderBy::Calls, 10).len(), 5);
    }

    #[test]
    fn test_order_by_parse() {
        assert_eq!("total_time".parse::<OrderBy>().unwrap(), OrderBy::TotalTime);
        assert_eq!("calls".parse::<OrderBy>().unwrap(), OrderBy::Calls);
        assert_eq!("p99".parse::<OrderBy>().unwrap(), OrderBy::P99Time);
    }
}
