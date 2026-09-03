//! Transaction Journal - TR (Transaction Replay)
//!
//! Logs all statements within a transaction for replay after failover.
//! Enables Oracle-grade TAF+TAC merged functionality.

use super::{NodeId, ProxyError, Result};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Journal entry for a single statement
#[derive(Debug, Clone)]
pub struct JournalEntry {
    /// Entry sequence number
    pub sequence: u64,
    /// SQL statement text
    pub statement: String,
    /// Bound parameters
    pub parameters: Vec<JournalValue>,
    /// Result checksum (for verification after replay)
    pub result_checksum: Option<u64>,
    /// Number of rows affected
    pub rows_affected: Option<u64>,
    /// Timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Statement type
    pub statement_type: StatementType,
    /// Execution duration (ms)
    pub duration_ms: u64,
}

/// Serializable parameter value
#[derive(Debug, Clone)]
pub enum JournalValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Array(Vec<JournalValue>),
}

/// Statement type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    /// SELECT query
    Select,
    /// INSERT statement
    Insert,
    /// UPDATE statement
    Update,
    /// DELETE statement
    Delete,
    /// DDL (CREATE, ALTER, DROP)
    Ddl,
    /// Transaction control (BEGIN, COMMIT, ROLLBACK)
    Transaction,
    /// SET statement
    Set,
    /// Other/unknown
    Other,
}

impl StatementType {
    /// Determine statement type from SQL
    pub fn from_sql(sql: &str) -> Self {
        let upper = sql.trim().to_uppercase();
        if upper.starts_with("SELECT") {
            StatementType::Select
        } else if upper.starts_with("INSERT") {
            StatementType::Insert
        } else if upper.starts_with("UPDATE") {
            StatementType::Update
        } else if upper.starts_with("DELETE") {
            StatementType::Delete
        } else if upper.starts_with("CREATE")
            || upper.starts_with("ALTER")
            || upper.starts_with("DROP")
        {
            StatementType::Ddl
        } else if upper.starts_with("BEGIN")
            || upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("SAVEPOINT")
        {
            StatementType::Transaction
        } else if upper.starts_with("SET") {
            StatementType::Set
        } else {
            StatementType::Other
        }
    }

    /// Is this a read-only statement?
    pub fn is_read_only(&self) -> bool {
        matches!(self, StatementType::Select)
    }

    /// Is this a mutating statement?
    pub fn is_mutation(&self) -> bool {
        matches!(
            self,
            StatementType::Insert
                | StatementType::Update
                | StatementType::Delete
                | StatementType::Ddl
        )
    }
}

/// Transaction journal for a single transaction
#[derive(Debug, Clone)]
pub struct TransactionJournalEntry {
    /// Transaction ID
    pub tx_id: Uuid,
    /// Session ID
    pub session_id: Uuid,
    /// Node where transaction started
    pub node_id: NodeId,
    /// Transaction start time
    pub started_at: chrono::DateTime<chrono::Utc>,
    /// Start LSN (for WAL synchronization)
    pub start_lsn: u64,
    /// Journal entries
    pub entries: Vec<JournalEntry>,
    /// Current sequence
    pub current_sequence: u64,
    /// Is transaction active
    pub active: bool,
    /// Has mutations
    pub has_mutations: bool,
    /// Savepoints
    pub savepoints: Vec<Savepoint>,
}

/// Savepoint information
#[derive(Debug, Clone)]
pub struct Savepoint {
    /// Savepoint name
    pub name: String,
    /// Sequence at savepoint
    pub sequence: u64,
    /// Created timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl TransactionJournalEntry {
    /// Create a new transaction journal entry
    pub fn new(tx_id: Uuid, session_id: Uuid, node_id: NodeId, start_lsn: u64) -> Self {
        Self {
            tx_id,
            session_id,
            node_id,
            started_at: chrono::Utc::now(),
            start_lsn,
            entries: Vec::new(),
            current_sequence: 0,
            active: true,
            has_mutations: false,
            savepoints: Vec::new(),
        }
    }

    /// Add an entry to the journal
    pub fn add_entry(&mut self, entry: JournalEntry) {
        if entry.statement_type.is_mutation() {
            self.has_mutations = true;
        }
        self.current_sequence = entry.sequence;
        self.entries.push(entry);
    }

    /// Create a savepoint
    pub fn create_savepoint(&mut self, name: String) {
        self.savepoints.push(Savepoint {
            name,
            sequence: self.current_sequence,
            created_at: chrono::Utc::now(),
        });
    }

    /// Rollback to savepoint
    pub fn rollback_to_savepoint(&mut self, name: &str) -> Option<u64> {
        if let Some(idx) = self.savepoints.iter().position(|s| s.name == name) {
            let savepoint = &self.savepoints[idx];
            let sequence = savepoint.sequence;

            // Truncate entries after savepoint
            self.entries.retain(|e| e.sequence <= sequence);

            // Remove later savepoints
            self.savepoints.truncate(idx + 1);

            Some(sequence)
        } else {
            None
        }
    }

    /// Get entries for replay
    pub fn entries_for_replay(&self) -> Vec<&JournalEntry> {
        self.entries.iter().collect()
    }

    /// Get only mutation entries
    pub fn mutation_entries(&self) -> Vec<&JournalEntry> {
        self.entries
            .iter()
            .filter(|e| e.statement_type.is_mutation())
            .collect()
    }

    /// Calculate total size of journal
    pub fn total_size(&self) -> usize {
        self.entries
            .iter()
            .map(|e| e.statement.len() + estimate_params_size(&e.parameters))
            .sum()
    }
}

fn estimate_params_size(params: &[JournalValue]) -> usize {
    params
        .iter()
        .map(|p| match p {
            JournalValue::Null => 1,
            JournalValue::Bool(_) => 1,
            JournalValue::Int64(_) => 8,
            JournalValue::Float64(_) => 8,
            JournalValue::Text(s) => s.len(),
            JournalValue::Bytes(b) => b.len(),
            JournalValue::Array(a) => estimate_params_size(a),
        })
        .sum()
}

/// Fast unique transaction id for the auto-commit data path.
///
/// The data path journals every write as its own single-statement
/// transaction, so `Uuid::new_v4()` used to draw from the OS RNG once per
/// write. The id is never persisted, serialized, or put on the wire: it is
/// only a `HashMap` key plus a value formatted into log lines and replay
/// error strings (`failover_replay.rs`, `replay/mod.rs`, admin
/// `/api/replay`). The *type* and the rendered format therefore stay a
/// v4-shaped `Uuid`, but the entropy source does not need to be the OS RNG
/// on every call: we draw one random v4 UUID per process and stamp a
/// monotonic counter into its low 64 bits, restoring the RFC 4122 variant
/// bits (the version nibble lives in the high half and is preserved). A
/// collision needs either 2^62 ids inside one process or a repeat of the
/// per-process 60-bit random base.
pub fn next_auto_commit_tx_id() -> Uuid {
    static BASE: OnceLock<u128> = OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let base = *BASE.get_or_init(|| Uuid::new_v4().as_u128());
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    // RFC 4122 variant is the top two bits of the low half.
    let low = (n & 0x3fff_ffff_ffff_ffff) | 0x8000_0000_0000_0000;
    Uuid::from_u128((base & (u128::MAX << 64)) | low as u128)
}

/// Insertion-ordered store of transaction journals.
///
/// The auto-commit data path hits the global `max_journals` cap constantly,
/// so eviction runs on the write hot path. A bare `HashMap` forced an
/// O(n log n) sort of every retained journal to find the oldest ones; the
/// `order` index makes eviction O(k log n) in the number actually removed.
/// Insertion order is the eviction order, which matches `started_at` order
/// (the journal is built immediately before the lock is taken).
#[derive(Debug, Default)]
struct JournalStore {
    /// tx_id -> (insertion sequence, journal)
    entries: HashMap<Uuid, (u64, TransactionJournalEntry)>,
    /// insertion sequence -> tx_id, iterated oldest-first for eviction
    order: BTreeMap<u64, Uuid>,
    /// Next insertion sequence to hand out.
    next_seq: u64,
}

impl JournalStore {
    /// Insert (or replace) a journal, returning a mutable handle to it.
    fn insert(
        &mut self,
        tx_id: Uuid,
        journal: TransactionJournalEntry,
    ) -> &mut TransactionJournalEntry {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        if let Some((old_seq, _)) = self.entries.insert(tx_id, (seq, journal)) {
            self.order.remove(&old_seq);
        }
        self.order.insert(seq, tx_id);
        &mut self
            .entries
            .get_mut(&tx_id)
            .expect("journal just inserted")
            .1
    }

    fn remove(&mut self, tx_id: &Uuid) {
        if let Some((seq, _)) = self.entries.remove(tx_id) {
            self.order.remove(&seq);
        }
    }

    fn get(&self, tx_id: &Uuid) -> Option<&TransactionJournalEntry> {
        self.entries.get(tx_id).map(|(_, j)| j)
    }

    fn get_mut(&mut self, tx_id: &Uuid) -> Option<&mut TransactionJournalEntry> {
        self.entries.get_mut(tx_id).map(|(_, j)| j)
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn values(&self) -> impl Iterator<Item = &TransactionJournalEntry> {
        self.entries.values().map(|(_, j)| j)
    }

    fn iter(&self) -> impl Iterator<Item = (&Uuid, &TransactionJournalEntry)> {
        self.entries.iter().map(|(tx_id, (_, j))| (tx_id, j))
    }

    /// Evict the oldest journals (insertion order) until at most
    /// `target_len` remain.
    fn evict_oldest(&mut self, target_len: usize) {
        while self.entries.len() > target_len {
            let Some((_, tx_id)) = self.order.pop_first() else {
                break;
            };
            self.entries.remove(&tx_id);
        }
    }
}

/// Transaction Journal Manager
pub struct TransactionJournal {
    /// Active transaction journals, insertion-ordered for O(k) eviction
    journals: Arc<RwLock<JournalStore>>,
    /// Maximum entries per journal
    max_entries: usize,
    /// Maximum journal size (bytes)
    max_size: usize,
    /// Global cap on the number of retained transaction journals. The data-path
    /// write journaling records each write as its own auto-commit transaction
    /// (a fresh tx_id, begin + log, never committed), so without a global bound
    /// the map grows by one entry per write forever — an unbounded leak of the
    /// full SQL of every write. When the cap is reached the oldest journals
    /// (by start time) are evicted; replay only consults recent history.
    max_journals: usize,
    /// Whether journaling is enabled
    enabled: bool,
}

impl TransactionJournal {
    /// Create a new transaction journal manager
    pub fn new() -> Self {
        Self {
            journals: Arc::new(RwLock::new(JournalStore::default())),
            max_entries: 10000,
            max_size: 64 * 1024 * 1024, // 64MB
            max_journals: 50_000,
            enabled: true,
        }
    }

    /// Configure maximum entries
    pub fn with_max_entries(mut self, max: usize) -> Self {
        self.max_entries = max;
        self
    }

    /// Configure the global cap on retained journals.
    pub fn with_max_journals(mut self, max: usize) -> Self {
        self.max_journals = max.max(1);
        self
    }

    /// Enforce the global cap under the write lock: when it is reached, evict
    /// the oldest journals down to 90% of the cap in one pass. Eviction walks
    /// the insertion-order index, so it costs O(k log n) in the number removed
    /// rather than an O(n log n) sort of every retained journal.
    fn enforce_cap_locked(&self, journals: &mut JournalStore) {
        if journals.len() >= self.max_journals {
            let target = (self.max_journals * 9 / 10).max(1);
            journals.evict_oldest(target);
        }
    }

    /// Configure maximum size
    pub fn with_max_size(mut self, max: usize) -> Self {
        self.max_size = max;
        self
    }

    /// Enable or disable journaling
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Collect every journal entry across every active transaction
    /// whose `timestamp` falls within the inclusive window
    /// `[from, to]`. Results are sorted in timestamp order so the
    /// caller can replay them chronologically regardless of which
    /// transaction they came from.
    ///
    /// Used by the time-travel replay engine (`src/replay/`) to
    /// reconstruct "what happened at the source between these two
    /// timestamps" against a staging target.
    pub async fn entries_in_window(
        &self,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> Vec<(Uuid, JournalEntry)> {
        let journals = self.journals.read().await;
        let mut out: Vec<(Uuid, JournalEntry)> = Vec::new();
        for (tx_id, j) in journals.iter() {
            for entry in &j.entries {
                if entry.timestamp >= from && entry.timestamp <= to {
                    out.push((*tx_id, entry.clone()));
                }
            }
        }
        out.sort_by_key(|(_, e)| e.timestamp);
        out
    }

    /// Start journaling a transaction
    pub async fn begin_transaction(
        &self,
        tx_id: Uuid,
        session_id: Uuid,
        node_id: NodeId,
        start_lsn: u64,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let journal = TransactionJournalEntry::new(tx_id, session_id, node_id, start_lsn);
        let mut journals = self.journals.write().await;
        // Bound total retained journals: the data path never commits (each write
        // is its own tx_id), so without this the map grows forever. Evict down
        // to 90% of the cap in one pass to amortize the walk.
        self.enforce_cap_locked(&mut journals);
        journals.insert(tx_id, journal);
        drop(journals);

        tracing::debug!("Started journaling transaction {:?}", tx_id);
        Ok(())
    }

    /// Begin a transaction and log its first statement under a **single**
    /// write-lock acquisition.
    ///
    /// Behaviourally identical to `begin_transaction` followed by
    /// `log_statement` (same eviction, same limit checks with the same error
    /// text, same sequence numbering), but takes the global journal lock once
    /// instead of twice. Used by the auto-commit data path in `server.rs`,
    /// which records every write as its own single-statement transaction;
    /// explicit multi-statement transactions keep using `begin_transaction` +
    /// `log_statement`.
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_and_log(
        &self,
        tx_id: Uuid,
        session_id: Uuid,
        node_id: NodeId,
        start_lsn: u64,
        statement: String,
        parameters: Vec<JournalValue>,
        result_checksum: Option<u64>,
        rows_affected: Option<u64>,
        duration_ms: u64,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        // Everything that does not need the lock is done outside it.
        let statement_type = StatementType::from_sql(&statement);
        let new_journal = TransactionJournalEntry::new(tx_id, session_id, node_id, start_lsn);

        let mut journals = self.journals.write().await;
        self.enforce_cap_locked(&mut journals);
        let journal = journals.insert(tx_id, new_journal);

        // Same limit checks `log_statement` applies (a fresh journal is empty,
        // so these only fire for degenerate max_entries / max_size settings).
        if journal.entries.len() >= self.max_entries {
            return Err(ProxyError::Internal(
                "Transaction journal entries limit exceeded".to_string(),
            ));
        }
        if journal.total_size() >= self.max_size {
            return Err(ProxyError::Internal(
                "Transaction journal size limit exceeded".to_string(),
            ));
        }

        let sequence = journal.current_sequence + 1;
        journal.add_entry(JournalEntry {
            sequence,
            statement,
            parameters,
            result_checksum,
            rows_affected,
            timestamp: chrono::Utc::now(),
            statement_type,
            duration_ms,
        });
        drop(journals);

        tracing::debug!("Started journaling transaction {:?}", tx_id);
        Ok(())
    }

    /// Log a statement
    pub async fn log_statement(
        &self,
        tx_id: Uuid,
        statement: String,
        parameters: Vec<JournalValue>,
        result_checksum: Option<u64>,
        rows_affected: Option<u64>,
        duration_ms: u64,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mut journals = self.journals.write().await;
        let journal = journals.get_mut(&tx_id).ok_or_else(|| {
            ProxyError::Internal(format!("No journal for transaction {:?}", tx_id))
        })?;

        // Check limits
        if journal.entries.len() >= self.max_entries {
            return Err(ProxyError::Internal(
                "Transaction journal entries limit exceeded".to_string(),
            ));
        }

        if journal.total_size() >= self.max_size {
            return Err(ProxyError::Internal(
                "Transaction journal size limit exceeded".to_string(),
            ));
        }

        let sequence = journal.current_sequence + 1;
        let statement_type = StatementType::from_sql(&statement);

        let entry = JournalEntry {
            sequence,
            statement,
            parameters,
            result_checksum,
            rows_affected,
            timestamp: chrono::Utc::now(),
            statement_type,
            duration_ms,
        };

        journal.add_entry(entry);

        Ok(())
    }

    /// Create a savepoint
    pub async fn create_savepoint(&self, tx_id: Uuid, name: String) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mut journals = self.journals.write().await;
        let journal = journals.get_mut(&tx_id).ok_or_else(|| {
            ProxyError::Internal(format!("No journal for transaction {:?}", tx_id))
        })?;

        journal.create_savepoint(name);
        Ok(())
    }

    /// Rollback to savepoint
    pub async fn rollback_to_savepoint(&self, tx_id: Uuid, name: &str) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mut journals = self.journals.write().await;
        let journal = journals.get_mut(&tx_id).ok_or_else(|| {
            ProxyError::Internal(format!("No journal for transaction {:?}", tx_id))
        })?;

        journal
            .rollback_to_savepoint(name)
            .ok_or_else(|| ProxyError::Internal(format!("Savepoint '{}' not found", name)))?;

        Ok(())
    }

    /// Commit transaction (clear journal)
    pub async fn commit_transaction(&self, tx_id: Uuid) -> Result<()> {
        self.journals.write().await.remove(&tx_id);
        tracing::debug!("Committed and cleared journal for transaction {:?}", tx_id);
        Ok(())
    }

    /// Rollback transaction (clear journal)
    pub async fn rollback_transaction(&self, tx_id: Uuid) -> Result<()> {
        self.journals.write().await.remove(&tx_id);
        tracing::debug!(
            "Rolled back and cleared journal for transaction {:?}",
            tx_id
        );
        Ok(())
    }

    /// Get journal for a transaction (for replay)
    pub async fn get_journal(&self, tx_id: &Uuid) -> Option<TransactionJournalEntry> {
        self.journals.read().await.get(tx_id).cloned()
    }

    /// Get active transaction count
    pub async fn active_count(&self) -> usize {
        self.journals.read().await.len()
    }

    /// Get statistics
    pub async fn stats(&self) -> JournalStats {
        let journals = self.journals.read().await;
        let total_entries: usize = journals.values().map(|j| j.entries.len()).sum();
        let total_size: usize = journals.values().map(|j| j.total_size()).sum();

        JournalStats {
            active_transactions: journals.len(),
            total_entries,
            total_size_bytes: total_size,
            enabled: self.enabled,
        }
    }

    /// Get all active transaction journals (for failover replay)
    pub async fn get_all_active(&self) -> Vec<TransactionJournalEntry> {
        self.journals.read().await.values().cloned().collect()
    }

    /// Get the maximum start LSN across all active transactions
    /// Used to determine how far the standby needs to catch up
    pub async fn get_max_start_lsn(&self) -> Option<u64> {
        let journals = self.journals.read().await;
        journals.values().map(|j| j.start_lsn).max()
    }

    /// Test-only: number of entries in the insertion-order index. Used to
    /// prove the index does not leak slots for committed transactions.
    #[cfg(test)]
    async fn order_index_len(&self) -> usize {
        self.journals.read().await.order.len()
    }

    /// Get transactions that started on a specific node
    /// Useful for replaying only transactions affected by a node failure
    pub async fn get_transactions_for_node(&self, node_id: NodeId) -> Vec<TransactionJournalEntry> {
        self.journals
            .read()
            .await
            .values()
            .filter(|j| j.node_id == node_id)
            .cloned()
            .collect()
    }
}

impl Default for TransactionJournal {
    fn default() -> Self {
        Self::new()
    }
}

/// Journal statistics
#[derive(Debug, Clone)]
pub struct JournalStats {
    /// Number of active transactions being journaled
    pub active_transactions: usize,
    /// Total journal entries across all transactions
    pub total_entries: usize,
    /// Total size of journals in bytes
    pub total_size_bytes: usize,
    /// Whether journaling is enabled
    pub enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statement_type_detection() {
        assert_eq!(
            StatementType::from_sql("SELECT * FROM users"),
            StatementType::Select
        );
        assert_eq!(
            StatementType::from_sql("INSERT INTO users VALUES (1)"),
            StatementType::Insert
        );
        assert_eq!(
            StatementType::from_sql("UPDATE users SET name = 'x'"),
            StatementType::Update
        );
        assert_eq!(
            StatementType::from_sql("DELETE FROM users"),
            StatementType::Delete
        );
        assert_eq!(
            StatementType::from_sql("CREATE TABLE foo (id INT)"),
            StatementType::Ddl
        );
        assert_eq!(StatementType::from_sql("BEGIN"), StatementType::Transaction);
        assert_eq!(
            StatementType::from_sql("SET search_path = public"),
            StatementType::Set
        );
    }

    #[test]
    fn test_statement_type_properties() {
        assert!(StatementType::Select.is_read_only());
        assert!(!StatementType::Insert.is_read_only());

        assert!(StatementType::Insert.is_mutation());
        assert!(StatementType::Update.is_mutation());
        assert!(!StatementType::Select.is_mutation());
    }

    #[tokio::test]
    async fn test_journal_lifecycle() {
        let journal = TransactionJournal::new();
        let tx_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let node_id = NodeId::new();

        // Begin transaction
        journal
            .begin_transaction(tx_id, session_id, node_id, 0)
            .await
            .unwrap();

        // Log statements
        journal
            .log_statement(
                tx_id,
                "SELECT * FROM users".to_string(),
                vec![],
                Some(12345),
                None,
                10,
            )
            .await
            .unwrap();

        journal
            .log_statement(
                tx_id,
                "INSERT INTO users (name) VALUES ($1)".to_string(),
                vec![JournalValue::Text("test".to_string())],
                None,
                Some(1),
                5,
            )
            .await
            .unwrap();

        // Check journal
        let j = journal.get_journal(&tx_id).await.unwrap();
        assert_eq!(j.entries.len(), 2);
        assert!(j.has_mutations);

        // Commit
        journal.commit_transaction(tx_id).await.unwrap();
        assert!(journal.get_journal(&tx_id).await.is_none());
    }

    #[tokio::test]
    async fn test_savepoints() {
        let journal = TransactionJournal::new();
        let tx_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let node_id = NodeId::new();

        journal
            .begin_transaction(tx_id, session_id, node_id, 0)
            .await
            .unwrap();

        // Log some statements
        for i in 0..3 {
            journal
                .log_statement(
                    tx_id,
                    format!("INSERT INTO t VALUES ({})", i),
                    vec![],
                    None,
                    Some(1),
                    1,
                )
                .await
                .unwrap();
        }

        // Create savepoint
        journal
            .create_savepoint(tx_id, "sp1".to_string())
            .await
            .unwrap();

        // Log more
        for i in 3..5 {
            journal
                .log_statement(
                    tx_id,
                    format!("INSERT INTO t VALUES ({})", i),
                    vec![],
                    None,
                    Some(1),
                    1,
                )
                .await
                .unwrap();
        }

        let j = journal.get_journal(&tx_id).await.unwrap();
        assert_eq!(j.entries.len(), 5);

        // Rollback to savepoint
        journal.rollback_to_savepoint(tx_id, "sp1").await.unwrap();

        let j = journal.get_journal(&tx_id).await.unwrap();
        assert_eq!(j.entries.len(), 3);
    }

    #[tokio::test]
    async fn test_stats() {
        let journal = TransactionJournal::new();
        let tx_id = Uuid::new_v4();
        let session_id = Uuid::new_v4();
        let node_id = NodeId::new();

        journal
            .begin_transaction(tx_id, session_id, node_id, 0)
            .await
            .unwrap();
        journal
            .log_statement(tx_id, "SELECT 1".to_string(), vec![], None, None, 1)
            .await
            .unwrap();

        let stats = journal.stats().await;
        assert_eq!(stats.active_transactions, 1);
        assert_eq!(stats.total_entries, 1);
        assert!(stats.enabled);
    }

    /// The global journal cap must bound the map even when transactions are
    /// never committed (the data-path auto-commit journaling pattern), evicting
    /// the oldest journals rather than growing without limit.
    #[tokio::test]
    async fn global_cap_evicts_oldest_journals() {
        let journal = TransactionJournal::new().with_max_journals(10);
        let node_id = NodeId::new();
        // Begin far more single-statement transactions than the cap, never
        // committing any — mirrors journal_write on the query path.
        for _ in 0..100 {
            let tx = Uuid::new_v4();
            journal
                .begin_transaction(tx, Uuid::new_v4(), node_id, 0)
                .await
                .unwrap();
            journal
                .log_statement(
                    tx,
                    "INSERT INTO t VALUES (1)".to_string(),
                    vec![],
                    None,
                    None,
                    1,
                )
                .await
                .unwrap();
        }
        // Bounded at the cap (not 100).
        let stats = journal.stats().await;
        assert!(
            stats.active_transactions <= 10,
            "journal map must stay within the cap, got {}",
            stats.active_transactions
        );
    }

    /// `begin_and_log` must produce exactly the journal that
    /// `begin_transaction` + `log_statement` produce (one lock acquisition
    /// instead of two is the only difference).
    #[tokio::test]
    async fn begin_and_log_matches_begin_then_log() {
        let node_id = NodeId::new();
        let session_id = Uuid::new_v4();
        let sql = "INSERT INTO t (a) VALUES ($1)".to_string();

        let two_step = TransactionJournal::new();
        let tx_two = Uuid::new_v4();
        two_step
            .begin_transaction(tx_two, session_id, node_id, 7)
            .await
            .unwrap();
        two_step
            .log_statement(
                tx_two,
                sql.clone(),
                vec![JournalValue::Int64(1)],
                Some(42),
                Some(1),
                3,
            )
            .await
            .unwrap();

        let fused = TransactionJournal::new();
        let tx_one = Uuid::new_v4();
        fused
            .begin_and_log(
                tx_one,
                session_id,
                node_id,
                7,
                sql.clone(),
                vec![JournalValue::Int64(1)],
                Some(42),
                Some(1),
                3,
            )
            .await
            .unwrap();

        let a = two_step.get_journal(&tx_two).await.unwrap();
        let b = fused.get_journal(&tx_one).await.unwrap();

        assert_eq!(a.session_id, b.session_id);
        assert_eq!(a.node_id, b.node_id);
        assert_eq!(a.start_lsn, b.start_lsn);
        assert_eq!(a.active, b.active);
        assert_eq!(a.has_mutations, b.has_mutations);
        assert!(b.has_mutations, "INSERT must mark the journal as mutating");
        assert_eq!(a.current_sequence, b.current_sequence);
        assert_eq!(a.entries.len(), b.entries.len());
        assert_eq!(a.entries[0].sequence, b.entries[0].sequence);
        assert_eq!(a.entries[0].statement, b.entries[0].statement);
        assert_eq!(a.entries[0].statement_type, b.entries[0].statement_type);
        assert_eq!(a.entries[0].result_checksum, b.entries[0].result_checksum);
        assert_eq!(a.entries[0].rows_affected, b.entries[0].rows_affected);
        assert_eq!(a.entries[0].duration_ms, b.entries[0].duration_ms);
        assert_eq!(a.entries[0].parameters.len(), b.entries[0].parameters.len());
    }

    /// `begin_and_log` is a no-op (Ok) when journaling is disabled, exactly
    /// like the two-step path.
    #[tokio::test]
    async fn begin_and_log_respects_disabled_journal() {
        let mut journal = TransactionJournal::new();
        journal.set_enabled(false);
        let tx = Uuid::new_v4();
        journal
            .begin_and_log(
                tx,
                Uuid::new_v4(),
                NodeId::new(),
                0,
                "INSERT INTO t VALUES (1)".to_string(),
                vec![],
                None,
                None,
                0,
            )
            .await
            .unwrap();
        assert!(journal.get_journal(&tx).await.is_none());
        assert_eq!(journal.active_count().await, 0);
    }

    /// `begin_and_log` must honour the global cap the same way the two-step
    /// path does (the auto-commit data path never commits).
    #[tokio::test]
    async fn begin_and_log_enforces_global_cap() {
        let journal = TransactionJournal::new().with_max_journals(10);
        let node_id = NodeId::new();
        for _ in 0..100 {
            journal
                .begin_and_log(
                    next_auto_commit_tx_id(),
                    Uuid::new_v4(),
                    node_id,
                    0,
                    "INSERT INTO t VALUES (1)".to_string(),
                    vec![],
                    None,
                    None,
                    0,
                )
                .await
                .unwrap();
        }
        assert!(
            journal.active_count().await <= 10,
            "journal map must stay within the cap"
        );
    }

    /// Eviction must drop the *oldest* journals first, deterministically —
    /// including when many journals are created inside the same clock tick
    /// (the auto-commit write path). The previous `started_at` sort broke
    /// timestamp ties in random `HashMap` iteration order and could evict the
    /// newest journals; the insertion-order index cannot.
    #[tokio::test]
    async fn eviction_drops_oldest_first_even_on_timestamp_ties() {
        let journal = TransactionJournal::new().with_max_journals(10);
        let node_id = NodeId::new();
        let mut ids = Vec::new();
        for _ in 0..12 {
            let tx = next_auto_commit_tx_id();
            ids.push(tx);
            journal
                .begin_and_log(
                    tx,
                    Uuid::new_v4(),
                    node_id,
                    0,
                    "INSERT INTO t VALUES (1)".to_string(),
                    vec![],
                    None,
                    None,
                    0,
                )
                .await
                .unwrap();
        }
        // Cap 10, evicting to 9 on each overflow: the two oldest are gone and
        // every later journal survives.
        for (i, tx) in ids.iter().enumerate().take(2) {
            assert!(
                journal.get_journal(tx).await.is_none(),
                "journal {} is among the oldest and must have been evicted",
                i
            );
        }
        for (i, tx) in ids.iter().enumerate().skip(2) {
            assert!(
                journal.get_journal(tx).await.is_some(),
                "journal {} is recent and must have been retained",
                i
            );
        }
    }

    /// Committing (or rolling back) must release the insertion-order slot too,
    /// otherwise the order index would leak one entry per transaction.
    #[tokio::test]
    async fn commit_and_rollback_release_order_index_slots() {
        let journal = TransactionJournal::new();
        let node_id = NodeId::new();
        for i in 0..200 {
            let tx = Uuid::new_v4();
            journal
                .begin_transaction(tx, Uuid::new_v4(), node_id, 0)
                .await
                .unwrap();
            journal
                .log_statement(tx, "SELECT 1".to_string(), vec![], None, None, 0)
                .await
                .unwrap();
            if i % 2 == 0 {
                journal.commit_transaction(tx).await.unwrap();
            } else {
                journal.rollback_transaction(tx).await.unwrap();
            }
        }
        assert_eq!(journal.active_count().await, 0);
        assert_eq!(
            journal.order_index_len().await,
            0,
            "order index must not retain slots for finished transactions"
        );
    }

    /// The auto-commit id source must stay unique and keep the v4 UUID shape
    /// (the id is rendered into replay logs and error strings).
    #[test]
    fn auto_commit_tx_ids_are_unique_and_v4_shaped() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10_000 {
            let id = next_auto_commit_tx_id();
            assert_eq!(id.get_version_num(), 4, "must render as a v4 UUID");
            assert_eq!(
                id.as_bytes()[8] & 0xc0,
                0x80,
                "must carry the RFC 4122 variant bits"
            );
            assert!(seen.insert(id), "auto-commit tx ids must be unique");
        }
    }
}
