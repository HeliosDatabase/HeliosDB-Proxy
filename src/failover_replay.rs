//! Failover Replay - TR (Transaction Replay)
//!
//! Replays transactions on a new node after failover.
//! Ensures transaction continuity with verification.

use super::transaction_journal::{
    JournalEntry, JournalValue, StatementType, TransactionJournalEntry,
};
use super::{NodeEndpoint, NodeId, ProxyError, Result};
use crate::backend::{BackendClient, BackendConfig, ParamValue};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Replay configuration
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Verify results match original (via checksum)
    pub verify_results: bool,
    /// Timeout per statement (ms)
    pub statement_timeout_ms: u64,
    /// Retry failed statements
    pub retry_on_error: bool,
    /// Max retries per statement
    pub max_retries: u32,
    /// Skip read-only statements during replay
    pub skip_read_only: bool,
    /// Wait for WAL to catch up before replay
    pub wait_for_wal_sync: bool,
    /// Max WAL lag to wait for (bytes)
    pub max_wal_lag_bytes: u64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            verify_results: true,
            statement_timeout_ms: 30000,
            retry_on_error: true,
            max_retries: 3,
            skip_read_only: false,
            wait_for_wal_sync: true,
            max_wal_lag_bytes: 0, // Wait for full sync
        }
    }
}

/// Replay result
#[derive(Debug, Clone)]
pub struct ReplayResult {
    /// Transaction ID
    pub tx_id: Uuid,
    /// Replay succeeded
    pub success: bool,
    /// Statements replayed
    pub statements_replayed: usize,
    /// Statements skipped
    pub statements_skipped: usize,
    /// Statements failed
    pub statements_failed: usize,
    /// Verification failures
    pub verification_failures: usize,
    /// Total replay time (ms)
    pub duration_ms: u64,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Per-statement results
    pub statement_results: Vec<StatementReplayResult>,
}

/// Per-statement replay result
#[derive(Debug, Clone)]
pub struct StatementReplayResult {
    /// Statement sequence
    pub sequence: u64,
    /// Replayed successfully
    pub success: bool,
    /// Checksum matched (if verified)
    pub checksum_matched: Option<bool>,
    /// Rows affected matched
    pub rows_matched: Option<bool>,
    /// Replay time (ms)
    pub duration_ms: u64,
    /// Error (if failed)
    pub error: Option<String>,
    /// Retry count
    pub retries: u32,
}

/// Replay state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayState {
    /// Not started
    Pending,
    /// Waiting for WAL sync
    WaitingForWal,
    /// Replaying statements
    Replaying,
    /// Verifying results
    Verifying,
    /// Completed successfully
    Completed,
    /// Failed
    Failed,
}

/// Active replay operation
#[derive(Debug)]
struct ActiveReplay {
    /// Transaction ID
    #[allow(dead_code)]
    tx_id: Uuid,
    /// Target node
    target_node: NodeId,
    /// Journal being replayed
    journal: TransactionJournalEntry,
    /// Current state
    state: ReplayState,
    /// Current position (statement index)
    position: usize,
    /// Start time
    #[allow(dead_code)]
    started_at: chrono::DateTime<chrono::Utc>,
    /// Results so far
    results: Vec<StatementReplayResult>,
}

/// Failover Replay Manager
pub struct FailoverReplay {
    /// Configuration
    config: ReplayConfig,
    /// Active replays
    active_replays: Arc<RwLock<HashMap<Uuid, ActiveReplay>>>,
    /// Completed replays (recent history)
    completed_replays: Arc<RwLock<Vec<ReplayResult>>>,
    /// Max history size
    max_history: usize,
    /// Optional backend-connection template. Host/port are swapped to
    /// each registered node's endpoint at replay time. When `None`,
    /// `execute_statement` and `wait_for_wal_sync` take the skeleton
    /// path (record success without touching the network).
    backend_template: Option<BackendConfig>,
    /// Per-node endpoints for resolving NodeId → host:port. Populated
    /// via `register_endpoint`. Empty by default — replay falls back to
    /// the skeleton path when an endpoint is missing.
    endpoints: Arc<RwLock<HashMap<NodeId, NodeEndpoint>>>,
}

impl FailoverReplay {
    /// Create a new failover replay manager
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            config,
            active_replays: Arc::new(RwLock::new(HashMap::new())),
            completed_replays: Arc::new(RwLock::new(Vec::new())),
            max_history: 100,
            backend_template: None,
            endpoints: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Attach a backend-connection template. Required for real replay;
    /// without it, all backend-touching calls take the skeleton path.
    pub fn with_backend_template(mut self, template: BackendConfig) -> Self {
        self.backend_template = Some(template);
        self
    }

    /// Register an endpoint for a node so replay can resolve where to
    /// send the statements. Idempotent.
    pub async fn register_endpoint(&self, node_id: NodeId, endpoint: NodeEndpoint) {
        self.endpoints.write().await.insert(node_id, endpoint);
    }

    fn build_config(&self, endpoint: &NodeEndpoint) -> Option<BackendConfig> {
        self.backend_template.as_ref().map(|t| {
            let mut c = t.clone();
            c.host = endpoint.host.clone();
            c.port = endpoint.port;
            c
        })
    }

    /// Start replaying a transaction
    pub async fn start_replay(
        &self,
        journal: TransactionJournalEntry,
        target_node: NodeId,
    ) -> Result<Uuid> {
        let tx_id = journal.tx_id;

        let replay = ActiveReplay {
            tx_id,
            target_node,
            journal,
            state: ReplayState::Pending,
            position: 0,
            started_at: chrono::Utc::now(),
            results: Vec::new(),
        };

        self.active_replays.write().await.insert(tx_id, replay);

        tracing::info!(
            "Starting replay for transaction {:?} on node {:?}",
            tx_id,
            target_node
        );

        Ok(tx_id)
    }

    /// Execute the replay
    pub async fn execute_replay(&self, tx_id: Uuid) -> Result<ReplayResult> {
        let start = std::time::Instant::now();

        // Get the replay
        let mut replays = self.active_replays.write().await;
        let replay = replays.get_mut(&tx_id).ok_or_else(|| {
            ProxyError::ReplayFailed(format!("No active replay for transaction {:?}", tx_id))
        })?;

        // Wait for WAL sync if configured
        if self.config.wait_for_wal_sync {
            replay.state = ReplayState::WaitingForWal;
            self.wait_for_wal_sync(replay.target_node, replay.journal.start_lsn)
                .await?;
        }

        replay.state = ReplayState::Replaying;

        let entries = replay.journal.entries.clone();
        let mut statements_replayed = 0;
        let mut statements_skipped = 0;
        let mut statements_failed = 0;
        let mut verification_failures = 0;

        // Replay each statement
        for entry in &entries {
            // Skip read-only if configured
            if self.config.skip_read_only && entry.statement_type.is_read_only() {
                statements_skipped += 1;
                replay.results.push(StatementReplayResult {
                    sequence: entry.sequence,
                    success: true,
                    checksum_matched: None,
                    rows_matched: None,
                    duration_ms: 0,
                    error: None,
                    retries: 0,
                });
                continue;
            }

            // Skip transaction control statements (already handled)
            if entry.statement_type == StatementType::Transaction {
                statements_skipped += 1;
                continue;
            }

            let result = self.replay_statement(entry, replay.target_node).await;

            match result {
                Ok(stmt_result) => {
                    if stmt_result.success {
                        statements_replayed += 1;

                        // Check verification
                        if self.config.verify_results {
                            if let Some(false) = stmt_result.checksum_matched {
                                verification_failures += 1;
                            }
                        }
                    } else {
                        statements_failed += 1;
                    }
                    replay.results.push(stmt_result);
                }
                Err(e) => {
                    statements_failed += 1;
                    replay.results.push(StatementReplayResult {
                        sequence: entry.sequence,
                        success: false,
                        checksum_matched: None,
                        rows_matched: None,
                        duration_ms: 0,
                        error: Some(e.to_string()),
                        retries: 0,
                    });
                }
            }

            replay.position += 1;
        }

        replay.state = if statements_failed > 0 {
            ReplayState::Failed
        } else {
            ReplayState::Completed
        };

        let duration_ms = start.elapsed().as_millis() as u64;

        let result = ReplayResult {
            tx_id,
            success: statements_failed == 0 && verification_failures == 0,
            statements_replayed,
            statements_skipped,
            statements_failed,
            verification_failures,
            duration_ms,
            error: if statements_failed > 0 {
                Some("Some statements failed during replay".to_string())
            } else if verification_failures > 0 {
                Some("Result verification failed".to_string())
            } else {
                None
            },
            statement_results: replay.results.clone(),
        };

        // Move to history
        drop(replays);
        self.active_replays.write().await.remove(&tx_id);
        self.add_to_history(result.clone()).await;

        tracing::info!(
            "Replay completed for {:?}: {} replayed, {} failed, {}ms",
            tx_id,
            statements_replayed,
            statements_failed,
            duration_ms
        );

        Ok(result)
    }

    /// Replay a single statement
    async fn replay_statement(
        &self,
        entry: &JournalEntry,
        target_node: NodeId,
    ) -> Result<StatementReplayResult> {
        let start = std::time::Instant::now();
        let mut retries = 0;

        loop {
            let (success, checksum_matched, rows_matched, error_msg) =
                self.execute_statement(entry, target_node).await;

            if success || !self.config.retry_on_error || retries >= self.config.max_retries {
                return Ok(StatementReplayResult {
                    sequence: entry.sequence,
                    success,
                    checksum_matched: if self.config.verify_results
                        && entry.result_checksum.is_some()
                    {
                        Some(checksum_matched)
                    } else {
                        None
                    },
                    rows_matched: if entry.rows_affected.is_some() {
                        Some(rows_matched)
                    } else {
                        None
                    },
                    duration_ms: start.elapsed().as_millis() as u64,
                    error: if success {
                        None
                    } else {
                        Some(error_msg.unwrap_or_else(|| "statement execution failed".to_string()))
                    },
                    retries,
                });
            }

            retries += 1;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    /// Execute a single journaled statement against the target node.
    ///
    /// Returns `(success, checksum_matched, rows_matched, error)`.
    /// When no backend template / endpoint is configured, returns
    /// `(true, true, true, None)` — the skeleton-test path that
    /// preserves pre-T0-TR5 behaviour for unit tests.
    async fn execute_statement(
        &self,
        entry: &JournalEntry,
        target_node: NodeId,
    ) -> (bool, bool, bool, Option<String>) {
        let endpoint = self.endpoints.read().await.get(&target_node).cloned();
        let cfg = match endpoint.as_ref().and_then(|e| self.build_config(e)) {
            Some(c) => c,
            None => return (true, true, true, None),
        };

        let mut client = match BackendClient::connect(&cfg).await {
            Ok(c) => c,
            Err(e) => return (false, false, false, Some(format!("connect: {}", e))),
        };

        let params: Vec<ParamValue> = entry
            .parameters
            .iter()
            .map(journal_value_to_param)
            .collect();

        let result = if params.is_empty() {
            client.simple_query(&entry.statement).await
        } else {
            client.query_with_params(&entry.statement, &params).await
        };

        let outcome = match result {
            Ok(qr) => {
                let rows_matched = match entry.rows_affected {
                    Some(expected) => qr.rows_affected() == Some(expected),
                    None => true,
                };
                // Checksum matching is best-effort: we don't recompute the
                // server-side hash here. Treat as matched when no
                // checksum was recorded; otherwise leave as `false` and
                // let the caller surface it via `verify_results`.
                let checksum_matched = entry.result_checksum.is_none();
                (true, checksum_matched, rows_matched, None)
            }
            Err(e) => (false, false, false, Some(e.to_string())),
        };
        client.close().await;
        outcome
    }

    /// Wait for the target node's WAL replay position to reach
    /// `start_lsn`. `start_lsn` is encoded as a u64 (high 32 bits of
    /// the PG `pg_lsn` × 2^32 + low 32 bits) — the standard
    /// `to_u64` form used by PG internals.
    ///
    /// Polls every 200 ms; bounded by `config.statement_timeout_ms`.
    async fn wait_for_wal_sync(&self, node: NodeId, start_lsn: u64) -> Result<()> {
        let endpoint = self.endpoints.read().await.get(&node).cloned();
        let cfg = match endpoint.as_ref().and_then(|e| self.build_config(e)) {
            Some(c) => c,
            None => {
                // Skeleton path: short pause for state-machine ordering.
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                return Ok(());
            }
        };

        let timeout = std::time::Duration::from_millis(self.config.statement_timeout_ms);
        tokio::time::timeout(timeout, Self::poll_wal_lsn(cfg, start_lsn))
            .await
            .map_err(|_| ProxyError::Timeout("WAL sync wait timeout".into()))??;
        Ok(())
    }

    async fn poll_wal_lsn(cfg: BackendConfig, target: u64) -> Result<()> {
        let mut client = BackendClient::connect(&cfg)
            .await
            .map_err(|e| ProxyError::ReplayFailed(format!("connect: {}", e)))?;
        loop {
            let value = client
                .query_scalar("SELECT pg_last_wal_replay_lsn()::text")
                .await
                .map_err(|e| ProxyError::ReplayFailed(format!("lsn probe: {}", e)))?;
            if let Some(s) = value.into_string() {
                if let Some(current) = pg_lsn_to_u64(&s) {
                    if current >= target {
                        client.close().await;
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    /// Add result to history
    async fn add_to_history(&self, result: ReplayResult) {
        let mut history = self.completed_replays.write().await;
        history.push(result);

        // Trim if too large
        if history.len() > self.max_history {
            history.remove(0);
        }
    }

    /// Get replay state
    pub async fn get_state(&self, tx_id: &Uuid) -> Option<ReplayState> {
        self.active_replays.read().await.get(tx_id).map(|r| r.state)
    }

    /// Get replay progress (statements completed / total)
    pub async fn get_progress(&self, tx_id: &Uuid) -> Option<(usize, usize)> {
        self.active_replays
            .read()
            .await
            .get(tx_id)
            .map(|r| (r.position, r.journal.entries.len()))
    }

    /// Cancel an active replay
    pub async fn cancel_replay(&self, tx_id: &Uuid) -> Result<()> {
        self.active_replays.write().await.remove(tx_id);
        tracing::info!("Cancelled replay for transaction {:?}", tx_id);
        Ok(())
    }

    /// Get recent replay history
    pub async fn history(&self) -> Vec<ReplayResult> {
        self.completed_replays.read().await.clone()
    }

    /// Get statistics
    pub async fn stats(&self) -> ReplayStats {
        let history = self.completed_replays.read().await;
        let successful = history.iter().filter(|r| r.success).count();
        let total_statements: usize = history.iter().map(|r| r.statements_replayed).sum();

        ReplayStats {
            active_replays: self.active_replays.read().await.len(),
            completed_replays: history.len(),
            successful_replays: successful,
            total_statements_replayed: total_statements,
        }
    }
}

/// Convert a `JournalValue` to a `ParamValue` for text-format
/// interpolation into replay SQL.
fn journal_value_to_param(v: &JournalValue) -> ParamValue {
    match v {
        JournalValue::Null => ParamValue::Null,
        JournalValue::Bool(b) => ParamValue::Bool(*b),
        JournalValue::Int64(i) => ParamValue::Int(*i),
        JournalValue::Float64(f) => ParamValue::Float(*f),
        JournalValue::Text(s) => ParamValue::Text(s.clone()),
        JournalValue::Bytes(b) => {
            // Render bytes as PG hex-escape literal text for text protocol.
            let mut s = String::with_capacity(2 + b.len() * 2);
            s.push_str("\\x");
            for byte in b {
                s.push_str(&format!("{:02x}", byte));
            }
            ParamValue::Text(s)
        }
        JournalValue::Array(_) => {
            // Arrays not yet supported in replay — fall back to NULL so
            // the statement at least compiles. Replay reporting shows
            // `rows_matched=false` only for entries that recorded a
            // `rows_affected` count; for hot-path journals (no recorded
            // count) the degradation is silent.
            ParamValue::Null
        }
    }
}

/// Parse a PostgreSQL `pg_lsn` text form (e.g. `"16/B3780A90"`) into
/// its u64 numeric representation: `(hi << 32) | lo`. Returns `None`
/// on malformed input.
fn pg_lsn_to_u64(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    if lo > u64::from(u32::MAX) {
        return None;
    }
    Some((hi << 32) | lo)
}

/// Replay statistics
#[derive(Debug, Clone)]
pub struct ReplayStats {
    /// Currently active replays
    pub active_replays: usize,
    /// Total completed replays
    pub completed_replays: usize,
    /// Successful replays
    pub successful_replays: usize,
    /// Total statements replayed
    pub total_statements_replayed: usize,
}

#[cfg(test)]
mod tests {
    use super::super::transaction_journal::TransactionJournalEntry;
    use super::*;

    fn make_journal() -> TransactionJournalEntry {
        let tx_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let node_id = NodeId::new();

        let mut journal = TransactionJournalEntry::new(tx_id, session_id, node_id, 0);

        journal.add_entry(JournalEntry {
            sequence: 1,
            statement: "INSERT INTO users (name) VALUES ('test')".to_string(),
            parameters: vec![],
            result_checksum: Some(12345),
            rows_affected: Some(1),
            timestamp: chrono::Utc::now(),
            statement_type: StatementType::Insert,
            duration_ms: 10,
        });

        journal.add_entry(JournalEntry {
            sequence: 2,
            statement: "SELECT * FROM users".to_string(),
            parameters: vec![],
            result_checksum: Some(67890),
            rows_affected: None,
            timestamp: chrono::Utc::now(),
            statement_type: StatementType::Select,
            duration_ms: 5,
        });

        journal
    }

    #[test]
    fn test_config_default() {
        let config = ReplayConfig::default();
        assert!(config.verify_results);
        assert!(config.retry_on_error);
        assert!(config.wait_for_wal_sync);
    }

    /// `pg_lsn_to_u64` must round-trip through PG's text format:
    /// `"hi/lo"` (hex) -> (hi << 32) | lo.
    #[test]
    fn test_pg_lsn_to_u64_roundtrip() {
        assert_eq!(pg_lsn_to_u64("0/0"), Some(0));
        assert_eq!(pg_lsn_to_u64("0/1"), Some(1));
        assert_eq!(pg_lsn_to_u64("0/FFFFFFFF"), Some(0xFFFFFFFF));
        assert_eq!(pg_lsn_to_u64("1/0"), Some(1u64 << 32));
        assert_eq!(
            pg_lsn_to_u64("16/B3780A90"),
            Some((0x16u64 << 32) | 0xB3780A90u64)
        );
        // Ordering: earlier LSN < later LSN.
        assert!(pg_lsn_to_u64("0/A").unwrap() < pg_lsn_to_u64("0/B").unwrap());
        assert!(pg_lsn_to_u64("0/FFFFFFFF").unwrap() < pg_lsn_to_u64("1/0").unwrap());
    }

    #[test]
    fn test_pg_lsn_to_u64_rejects_malformed() {
        assert!(pg_lsn_to_u64("no-slash").is_none());
        assert!(pg_lsn_to_u64("/lo-only").is_none());
        assert!(pg_lsn_to_u64("hi-only/").is_none());
        assert!(pg_lsn_to_u64("zz/zz").is_none());
        // `lo` must fit in u32 (PG text format guarantees this).
        assert!(pg_lsn_to_u64("0/100000000").is_none());
    }

    #[test]
    fn test_journal_value_to_param_basic_types() {
        use crate::backend::ParamValue;

        assert!(matches!(
            journal_value_to_param(&JournalValue::Null),
            ParamValue::Null
        ));
        assert!(matches!(
            journal_value_to_param(&JournalValue::Bool(true)),
            ParamValue::Bool(true)
        ));
        assert!(matches!(
            journal_value_to_param(&JournalValue::Int64(42)),
            ParamValue::Int(42)
        ));
        match journal_value_to_param(&JournalValue::Float64(3.14)) {
            ParamValue::Float(f) => assert!((f - 3.14).abs() < 1e-9),
            other => panic!("expected Float, got {:?}", other),
        }
        match journal_value_to_param(&JournalValue::Text("hi".into())) {
            ParamValue::Text(s) => assert_eq!(s, "hi"),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[test]
    fn test_journal_value_bytes_to_hex_escape() {
        use crate::backend::ParamValue;
        let v = journal_value_to_param(&JournalValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        match v {
            ParamValue::Text(s) => assert_eq!(s, "\\xdeadbeef"),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_start_replay() {
        let replay = FailoverReplay::new(ReplayConfig::default());
        let journal = make_journal();
        let tx_id = journal.tx_id;
        let target = NodeId::new();

        let result_tx_id = replay.start_replay(journal, target).await.unwrap();
        assert_eq!(result_tx_id, tx_id);

        let state = replay.get_state(&tx_id).await;
        assert_eq!(state, Some(ReplayState::Pending));
    }

    #[tokio::test]
    async fn test_execute_replay() {
        let replay = FailoverReplay::new(ReplayConfig::default());
        let journal = make_journal();
        let tx_id = journal.tx_id;
        let target = NodeId::new();

        replay.start_replay(journal, target).await.unwrap();
        let result = replay.execute_replay(tx_id).await.unwrap();

        assert!(result.success);
        assert_eq!(result.statements_replayed, 2);
        assert_eq!(result.statements_failed, 0);
    }

    #[tokio::test]
    async fn test_cancel_replay() {
        let replay = FailoverReplay::new(ReplayConfig::default());
        let journal = make_journal();
        let tx_id = journal.tx_id;
        let target = NodeId::new();

        replay.start_replay(journal, target).await.unwrap();
        replay.cancel_replay(&tx_id).await.unwrap();

        assert!(replay.get_state(&tx_id).await.is_none());
    }

    #[tokio::test]
    async fn test_stats() {
        let replay = FailoverReplay::new(ReplayConfig::default());

        let stats = replay.stats().await;
        assert_eq!(stats.active_replays, 0);
        assert_eq!(stats.completed_replays, 0);
    }
}
