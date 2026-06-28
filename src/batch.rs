//! INSERT Batching for HeliosProxy
//!
//! Batches multiple INSERT statements into combined bulk operations for
//! improved throughput. Reduces round-trips and enables lock-free bulk ingestion.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

/// Table identifier
pub type TableId = String;

/// Batch ticket ID
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BatchTicketId(u64);

/// Batch configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Enable INSERT batching
    pub enabled: bool,
    /// Maximum batch size (number of rows)
    pub max_batch_size: usize,
    /// Maximum batch wait time (ms) before flushing
    pub max_wait_ms: u64,
    /// Maximum memory per batch (bytes)
    pub max_batch_bytes: usize,
    /// Enable automatic flushing
    pub auto_flush: bool,
    /// Tables to batch (empty = all tables)
    pub batch_tables: Vec<String>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_batch_size: 1000,
            max_wait_ms: 10,
            max_batch_bytes: 16 * 1024 * 1024, // 16MB
            auto_flush: true,
            batch_tables: Vec::new(), // Batch all tables by default
        }
    }
}

/// An individual INSERT request
#[derive(Debug)]
pub struct InsertRequest {
    /// Table name
    pub table: String,
    /// Column names
    pub columns: Vec<String>,
    /// Row values (each inner vec is a row)
    pub values: Vec<Vec<String>>,
    /// Original SQL (for fallback)
    pub original_sql: String,
    /// Request timestamp
    pub submitted_at: Instant,
    /// Response channel
    response_tx: Option<oneshot::Sender<BatchResult>>,
}

/// Result of a batch operation
#[derive(Debug, Clone)]
pub struct BatchResult {
    /// Ticket ID
    pub ticket_id: BatchTicketId,
    /// Number of rows inserted
    pub rows_inserted: u64,
    /// Whether the batch succeeded
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
    /// Time spent waiting in batch
    pub wait_time: Duration,
    /// Execution time
    pub execution_time: Duration,
}

/// Ticket for awaiting batch completion
pub struct BatchTicket {
    id: BatchTicketId,
    rx: oneshot::Receiver<BatchResult>,
}

impl BatchTicket {
    /// Wait for the batch to complete
    pub async fn wait(self) -> Result<BatchResult, BatchError> {
        self.rx.await.map_err(|_| BatchError::ChannelClosed)
    }

    /// Wait with timeout
    pub async fn wait_timeout(self, timeout: Duration) -> Result<BatchResult, BatchError> {
        tokio::time::timeout(timeout, self.rx)
            .await
            .map_err(|_| BatchError::Timeout)?
            .map_err(|_| BatchError::ChannelClosed)
    }

    /// Get the ticket ID
    pub fn id(&self) -> BatchTicketId {
        self.id
    }
}

/// Batch error types
#[derive(Debug, Clone)]
pub enum BatchError {
    /// Batching is disabled
    Disabled,
    /// Batch is full
    BatchFull,
    /// Timeout waiting for batch
    Timeout,
    /// Channel closed
    ChannelClosed,
    /// Execution failed
    ExecutionFailed(String),
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disabled => write!(f, "Batching is disabled"),
            Self::BatchFull => write!(f, "Batch is full"),
            Self::Timeout => write!(f, "Batch timeout"),
            Self::ChannelClosed => write!(f, "Channel closed"),
            Self::ExecutionFailed(e) => write!(f, "Execution failed: {}", e),
        }
    }
}

impl std::error::Error for BatchError {}

/// Statistics for batch operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchStats {
    /// Total inserts received
    pub inserts_received: u64,
    /// Total rows received
    pub rows_received: u64,
    /// Total batches flushed
    pub batches_flushed: u64,
    /// Total rows inserted
    pub rows_inserted: u64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Average wait time (ms)
    pub avg_wait_time_ms: f64,
    /// Average execution time (ms)
    pub avg_execution_time_ms: f64,
    /// Batches flushed due to size limit
    pub size_triggered_flushes: u64,
    /// Batches flushed due to time limit
    pub time_triggered_flushes: u64,
    /// Batches whose combined INSERT failed to execute (no backend,
    /// connect error, or SQL error).
    pub flush_failures: u64,
}

/// Pending batch for a table
struct PendingBatch {
    /// INSERT requests in this batch
    requests: Vec<InsertRequest>,
    /// Total rows in batch
    row_count: usize,
    /// Total bytes in batch (estimated)
    byte_count: usize,
    /// First request timestamp
    first_submitted: Instant,
}

impl PendingBatch {
    fn new() -> Self {
        Self {
            requests: Vec::with_capacity(100),
            row_count: 0,
            byte_count: 0,
            first_submitted: Instant::now(),
        }
    }

    fn add(&mut self, request: InsertRequest) {
        let row_count = request.values.len();
        let byte_estimate = request.original_sql.len();

        if self.requests.is_empty() {
            self.first_submitted = request.submitted_at;
        }

        self.row_count += row_count;
        self.byte_count += byte_estimate;
        self.requests.push(request);
    }

    fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    fn should_flush(&self, config: &BatchConfig) -> bool {
        self.row_count >= config.max_batch_size
            || self.byte_count >= config.max_batch_bytes
            || self.first_submitted.elapsed().as_millis() as u64 >= config.max_wait_ms
    }

    fn drain(&mut self) -> (Vec<InsertRequest>, usize) {
        let row_count = self.row_count;
        self.row_count = 0;
        self.byte_count = 0;
        (std::mem::take(&mut self.requests), row_count)
    }
}

/// INSERT Batcher
///
/// Batches INSERT statements for improved throughput.
pub struct InsertBatcher {
    /// Configuration
    config: BatchConfig,
    /// Pending batches per table
    pending: DashMap<TableId, PendingBatch>,
    /// Next ticket ID
    next_ticket_id: AtomicU64,
    /// Statistics
    stats: Arc<parking_lot::RwLock<BatchStats>>,
    /// Shutdown flag
    shutdown: AtomicBool,
    /// Backend the combined INSERTs actually execute against. When
    /// `None` the batcher still batches/seals but cannot execute — it
    /// reports an honest failure to waiters rather than claiming a
    /// silent success.
    backend: Option<crate::backend::BackendConfig>,
}

/// A batch sealed out of `pending` and ready to execute: the drained
/// requests, the row count, and the combined bulk-INSERT SQL.
struct SealedBatch {
    requests: Vec<InsertRequest>,
    row_count: usize,
    sql: String,
}

impl InsertBatcher {
    /// Create a new INSERT batcher
    pub fn new(config: BatchConfig) -> Self {
        Self {
            config,
            pending: DashMap::new(),
            next_ticket_id: AtomicU64::new(1),
            stats: Arc::new(parking_lot::RwLock::new(BatchStats::default())),
            shutdown: AtomicBool::new(false),
            backend: None,
        }
    }

    /// Attach the backend the combined INSERTs execute against. Without
    /// it the batcher seals and combines but cannot run the SQL.
    pub fn with_backend(mut self, backend: crate::backend::BackendConfig) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Add an INSERT to the batch.
    ///
    /// Takes `&Arc<Self>` so a size-triggered flush can seal the batch
    /// synchronously (callers observing `batch_size` see it drop
    /// immediately) and then execute the combined INSERT for real on a
    /// spawned task. Must be called from within a Tokio runtime.
    pub fn add(
        self: &Arc<Self>,
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<String>>,
        original_sql: String,
    ) -> Result<BatchTicket, BatchError> {
        if !self.config.enabled {
            return Err(BatchError::Disabled);
        }

        if self.shutdown.load(Ordering::Relaxed) {
            return Err(BatchError::ExecutionFailed("Batcher shutdown".to_string()));
        }

        // Check if table should be batched
        if !self.config.batch_tables.is_empty() && !self.config.batch_tables.contains(&table) {
            return Err(BatchError::Disabled);
        }

        let ticket_id = BatchTicketId(self.next_ticket_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();

        let row_count = values.len();

        let request = InsertRequest {
            table: table.clone(),
            columns,
            values,
            original_sql,
            submitted_at: Instant::now(),
            response_tx: Some(tx),
        };

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.inserts_received += 1;
            stats.rows_received += row_count as u64;
        }

        // Add to pending batch
        let should_flush = {
            let mut batch = self
                .pending
                .entry(table.clone())
                .or_insert_with(PendingBatch::new);
            batch.add(request);
            batch.should_flush(&self.config)
        };

        // Trigger flush if needed. Seal synchronously so `batch_size`
        // reflects the flush immediately, then execute the combined
        // INSERT for real on a spawned task.
        if should_flush {
            if let Some(sealed) = self.seal(&table) {
                let me = Arc::clone(self);
                tokio::spawn(async move {
                    me.execute_sealed(sealed).await;
                });
            }
        }

        Ok(BatchTicket { id: ticket_id, rx })
    }

    /// Seal a table's pending batch: remove it from `pending`, drain it,
    /// and combine the rows into one bulk INSERT. Synchronous, so
    /// observers of `batch_size` see the flush immediately. Returns
    /// `None` if there was nothing pending.
    fn seal(&self, table: &str) -> Option<SealedBatch> {
        let (_, mut batch) = self.pending.remove(table)?;
        if batch.is_empty() {
            return None;
        }
        let (requests, row_count) = batch.drain();
        let sql = self.combine_inserts(&requests);
        Some(SealedBatch {
            requests,
            row_count,
            sql,
        })
    }

    /// Execute a sealed batch's combined INSERT against the backend and
    /// notify every waiting request with the real outcome. When no
    /// backend is configured (or the connection/execution fails) the
    /// waiters receive `success = false` with the reason — never a
    /// fabricated success.
    async fn execute_sealed(&self, sealed: SealedBatch) {
        let SealedBatch {
            requests,
            row_count,
            sql,
        } = sealed;
        let execution_start = Instant::now();

        // Actually run the combined INSERT.
        let (success, error) = match &self.backend {
            Some(cfg) => match crate::backend::BackendClient::connect(cfg).await {
                Ok(mut client) => {
                    let outcome = client.execute(&sql).await;
                    client.close().await;
                    match outcome {
                        Ok(_tag) => (true, None),
                        Err(e) => (false, Some(format!("execute: {}", e))),
                    }
                }
                Err(e) => (false, Some(format!("connect: {}", e))),
            },
            None => (false, Some("no backend configured".to_string())),
        };

        let execution_time = execution_start.elapsed();

        // Update statistics (only count rows that actually landed).
        {
            let mut stats = self.stats.write();
            stats.batches_flushed += 1;
            if success {
                stats.rows_inserted += row_count as u64;
            } else {
                stats.flush_failures += 1;
            }

            if stats.batches_flushed == 1 {
                stats.avg_batch_size = row_count as f64;
            } else {
                stats.avg_batch_size = stats.avg_batch_size * 0.9 + row_count as f64 * 0.1;
            }

            let exec_ms = execution_time.as_millis() as f64;
            if stats.batches_flushed == 1 {
                stats.avg_execution_time_ms = exec_ms;
            } else {
                stats.avg_execution_time_ms = stats.avg_execution_time_ms * 0.9 + exec_ms * 0.1;
            }
        }

        // Send responses to all waiting requests.
        for mut req in requests {
            let wait_time = req
                .submitted_at
                .elapsed()
                .checked_sub(execution_time)
                .unwrap_or_default();

            if let Some(tx) = req.response_tx.take() {
                let _ = tx.send(BatchResult {
                    ticket_id: BatchTicketId(0), // Individual tickets not tracked
                    rows_inserted: if success { req.values.len() as u64 } else { 0 },
                    success,
                    error: error.clone(),
                    wait_time,
                    execution_time,
                });
            }
        }
    }

    /// Flush a single table's batch: seal it and execute the combined
    /// INSERT against the backend.
    pub async fn flush_batch(&self, table: &str) {
        if let Some(sealed) = self.seal(table) {
            self.execute_sealed(sealed).await;
        }
    }

    /// Combine multiple INSERT requests into a single SQL statement
    fn combine_inserts(&self, requests: &[InsertRequest]) -> String {
        if requests.is_empty() {
            return String::new();
        }

        let first = &requests[0];
        let table = &first.table;
        let columns = &first.columns;

        let mut sql = format!("INSERT INTO {} ({}) VALUES ", table, columns.join(", "));

        let mut value_parts: Vec<String> = Vec::new();

        for req in requests {
            for row in &req.values {
                value_parts.push(format!("({})", row.join(", ")));
            }
        }

        sql.push_str(&value_parts.join(", "));

        sql
    }

    /// Flush all pending batches, executing each combined INSERT.
    pub async fn flush_all(&self) {
        let tables: Vec<TableId> = self.pending.iter().map(|r| r.key().clone()).collect();
        for table in tables {
            self.flush_batch(&table).await;
        }
    }

    /// Get the current batch size for a table
    pub fn batch_size(&self, table: &str) -> usize {
        self.pending.get(table).map(|b| b.row_count).unwrap_or(0)
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> BatchStats {
        self.stats.read().clone()
    }

    /// Shutdown the batcher: stop accepting work and flush whatever is
    /// pending (executing each combined INSERT).
    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.flush_all().await;
    }

    /// Start auto-flush background task
    pub fn start_auto_flush(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let interval = Duration::from_millis(self.config.max_wait_ms);

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);

            loop {
                interval_timer.tick().await;

                if self.shutdown.load(Ordering::Relaxed) {
                    break;
                }

                // Check each batch for timeout
                let tables: Vec<TableId> = self
                    .pending
                    .iter()
                    .filter(|r| {
                        r.first_submitted.elapsed().as_millis() as u64 >= self.config.max_wait_ms
                    })
                    .map(|r| r.key().clone())
                    .collect();

                for table in tables {
                    self.flush_batch(&table).await;
                    self.stats.write().time_triggered_flushes += 1;
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_batch_add() {
        let batcher = Arc::new(InsertBatcher::new(BatchConfig::default()));

        let _ticket = batcher
            .add(
                "users".to_string(),
                vec!["id".to_string(), "name".to_string()],
                vec![vec!["1".to_string(), "'Alice'".to_string()]],
                "INSERT INTO users (id, name) VALUES (1, 'Alice')".to_string(),
            )
            .unwrap();

        assert_eq!(batcher.batch_size("users"), 1);
    }

    #[tokio::test]
    async fn test_batch_flush_on_size() {
        let config = BatchConfig {
            max_batch_size: 2,
            ..Default::default()
        };
        let batcher = Arc::new(InsertBatcher::new(config));

        // Add first INSERT
        batcher
            .add(
                "users".to_string(),
                vec!["id".to_string()],
                vec![vec!["1".to_string()]],
                "INSERT INTO users VALUES (1)".to_string(),
            )
            .unwrap();

        assert_eq!(batcher.batch_size("users"), 1);

        // Add second INSERT - should trigger flush
        batcher
            .add(
                "users".to_string(),
                vec!["id".to_string()],
                vec![vec!["2".to_string()]],
                "INSERT INTO users VALUES (2)".to_string(),
            )
            .unwrap();

        // Batch should be flushed
        assert_eq!(batcher.batch_size("users"), 0);
    }

    #[test]
    fn test_combine_inserts() {
        let batcher = InsertBatcher::new(BatchConfig::default());

        let requests = vec![
            InsertRequest {
                table: "users".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                values: vec![vec!["1".to_string(), "'Alice'".to_string()]],
                original_sql: String::new(),
                submitted_at: Instant::now(),
                response_tx: None,
            },
            InsertRequest {
                table: "users".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                values: vec![vec!["2".to_string(), "'Bob'".to_string()]],
                original_sql: String::new(),
                submitted_at: Instant::now(),
                response_tx: None,
            },
        ];

        let combined = batcher.combine_inserts(&requests);
        assert!(combined.contains("INSERT INTO users"));
        assert!(combined.contains("(1, 'Alice')"));
        assert!(combined.contains("(2, 'Bob')"));
    }

    #[test]
    fn test_batch_stats() {
        // Default config won't trigger a size/time flush for 2 rows, so no
        // task is spawned and this stays a plain (non-async) test.
        let batcher = Arc::new(InsertBatcher::new(BatchConfig::default()));

        batcher
            .add(
                "users".to_string(),
                vec!["id".to_string()],
                vec![vec!["1".to_string()], vec!["2".to_string()]],
                "INSERT INTO users VALUES (1), (2)".to_string(),
            )
            .unwrap();

        let stats = batcher.stats();
        assert_eq!(stats.inserts_received, 1);
        assert_eq!(stats.rows_received, 2);
    }

    /// Live proof that a flushed batch's combined INSERT actually
    /// executes against PostgreSQL and the rows land. Gated on
    /// `HELIOS_LIVE_PG` (`host:port`, e.g. `127.0.0.1:25433`); skips when
    /// unset so CI without a backend stays green. Before this change the
    /// flush discarded the SQL and faked success — this test would then
    /// find zero rows.
    #[tokio::test]
    async fn flush_executes_against_live_backend() {
        use crate::backend::{tls::default_client_config, BackendClient, BackendConfig, TlsMode};

        let addr = match std::env::var("HELIOS_LIVE_PG") {
            Ok(a) if !a.is_empty() => a,
            _ => {
                eprintln!("skipping flush_executes_against_live_backend: set HELIOS_LIVE_PG");
                return;
            }
        };
        let (host, port_s) = addr.rsplit_once(':').unwrap();
        let port: u16 = port_s.parse().unwrap();
        let user = std::env::var("HELIOS_LIVE_USER").unwrap_or_else(|_| "bench".into());
        let pass = std::env::var("HELIOS_LIVE_PASS").unwrap_or_else(|_| "benchpass".into());
        let db = std::env::var("HELIOS_LIVE_DB").unwrap_or_else(|_| "benchdb".into());

        let cfg = BackendConfig {
            host: host.to_string(),
            port,
            user,
            password: Some(pass),
            database: Some(db),
            application_name: Some("helios-batch-test".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_secs(5),
            query_timeout: Duration::from_secs(5),
            tls_config: default_client_config(),
        };

        // Seed a clean probe table.
        let mut seed = BackendClient::connect(&cfg).await.expect("connect seed");
        seed.execute("DROP TABLE IF EXISTS batch_probe")
            .await
            .unwrap();
        seed.execute("CREATE TABLE batch_probe(id int, total numeric)")
            .await
            .unwrap();
        seed.close().await;

        // Large batch size so nothing auto-flushes; we flush explicitly
        // and await the real execution.
        let batcher = Arc::new(
            InsertBatcher::new(BatchConfig {
                max_batch_size: 1000,
                max_wait_ms: 60_000,
                ..Default::default()
            })
            .with_backend(cfg.clone()),
        );
        batcher
            .add(
                "batch_probe".to_string(),
                vec!["id".to_string(), "total".to_string()],
                vec![
                    vec!["1".to_string(), "99.99".to_string()],
                    vec!["2".to_string(), "12.50".to_string()],
                ],
                String::new(),
            )
            .unwrap();
        batcher.flush_batch("batch_probe").await;

        // The two rows must actually be in PostgreSQL now.
        let mut verify = BackendClient::connect(&cfg).await.expect("connect verify");
        let n = verify
            .query_scalar("SELECT count(*) AS n FROM batch_probe")
            .await
            .unwrap()
            .as_i64("n")
            .unwrap()
            .unwrap_or(0);
        let _ = verify.execute("DROP TABLE IF EXISTS batch_probe").await;
        verify.close().await;

        assert_eq!(n, 2, "expected 2 batched rows to land, found {}", n);
        assert_eq!(batcher.stats().rows_inserted, 2);
        assert_eq!(batcher.stats().flush_failures, 0);
    }
}
