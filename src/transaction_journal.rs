//! Transaction Journal - TR (Transaction Replay)
//!
//! Logs all statements within a transaction for replay after failover.
//! Enables Oracle-grade TAF+TAC merged functionality.

use super::{NodeId, ProxyError, Result};
use crate::protocol::starts_with_ci;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;
use uuid::Uuid;

/// Journal entry for a single statement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    /// Entry sequence number
    pub sequence: u64,
    /// SQL statement text
    pub statement: String,
    /// Bound parameters
    pub parameters: Vec<JournalValue>,
    /// Parameter type OIDs the client declared in its `Parse` (`0` = let the
    /// backend infer). Empty for the simple protocol or an undeclared `Parse`.
    #[serde(default)]
    pub param_types: Vec<u32>,
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
    /// The backend's own verdict on this statement as the proxy relayed it
    /// (TR-07). Library callers that log without observing a response leave
    /// it `Unobserved`.
    #[serde(default)]
    pub outcome: StatementOutcome,
    /// Which wire protocol carried the statement.
    #[serde(default)]
    pub protocol: WireProtocol,
}

/// Serializable parameter value.
///
/// The capture path (TR-07) records extended-protocol `Bind` values
/// byte-for-byte: `Text` for a text-format value that is valid UTF-8,
/// `TextRaw` for a text-format value that is not, `Binary` for a
/// binary-format value. Replay sends each back in the same format, so arrays,
/// binary-encoded types and anything else the client bound reproduce exactly.
/// The typed variants (`Bool`/`Int64`/`Float64`/`Bytes`/`Array`) remain for
/// library callers that journal already-decoded values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JournalValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(Vec<u8>),
    Array(Vec<JournalValue>),
    /// Text-format (`0`) parameter bytes that were not valid UTF-8.
    TextRaw(Vec<u8>),
    /// Binary-format (`1`) parameter bytes, verbatim.
    Binary(Vec<u8>),
}

/// Backend outcome of one journaled statement, as observed by the proxy.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum StatementOutcome {
    /// No response was observed (library-recorded entry).
    #[default]
    Unobserved,
    /// The backend answered `CommandComplete` with this tag.
    Succeeded { tag: String },
    /// The backend answered `ErrorResponse`. Boxed so the enum (and every
    /// `JournalEntry`) stays small on the paths that move and drop entries.
    Failed(Box<StatementFailure>),
}

/// The `ErrorResponse` behind `StatementOutcome::Failed`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementFailure {
    pub sqlstate: String,
    pub message: String,
}

impl StatementOutcome {
    /// Rows affected as parsed from the command tag (`INSERT 0 5` → 5,
    /// `UPDATE 3` → 3); `None` when unobserved, failed or tag-less.
    pub fn rows_affected(&self) -> Option<u64> {
        match self {
            StatementOutcome::Succeeded { tag } => {
                tag.rsplit(' ').next().and_then(|n| n.parse::<u64>().ok())
            }
            _ => None,
        }
    }
}

/// Which PostgreSQL wire protocol carried a statement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireProtocol {
    /// `Query` message (simple protocol); the text may hold several statements.
    #[default]
    Simple,
    /// `Parse`/`Bind`/`Execute` (extended protocol); one statement per entry.
    Extended,
}

/// Where a journaled transaction came from (TR-07 source identity).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentity {
    /// Client socket address as accepted by the proxy.
    pub client_addr: String,
    /// Startup-packet user.
    pub user: String,
    /// Startup-packet database.
    pub database: String,
    /// Backend node (`host:port`) that executed the transaction.
    pub backend: String,
    /// Tenant id when multi-tenancy assigned one to the session.
    pub tenant: Option<String>,
}

/// Statement type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Determine statement type from SQL.
    ///
    /// Runs on every journaled statement, so it must not allocate: the
    /// prefix tests are ASCII case-insensitive comparisons on the trimmed
    /// input instead of an uppercased copy. Keyword semantics are unchanged
    /// (bare prefix match, no word boundary), matching the original
    /// `to_uppercase().starts_with(..)` classification.
    pub fn from_sql(sql: &str) -> Self {
        let s = sql.trim();
        if starts_with_ci(s, "SELECT") {
            StatementType::Select
        } else if starts_with_ci(s, "INSERT") {
            StatementType::Insert
        } else if starts_with_ci(s, "UPDATE") {
            StatementType::Update
        } else if starts_with_ci(s, "DELETE") {
            StatementType::Delete
        } else if starts_with_ci(s, "CREATE")
            || starts_with_ci(s, "ALTER")
            || starts_with_ci(s, "DROP")
        {
            StatementType::Ddl
        } else if starts_with_ci(s, "BEGIN")
            || starts_with_ci(s, "COMMIT")
            || starts_with_ci(s, "ROLLBACK")
            || starts_with_ci(s, "SAVEPOINT")
        {
            StatementType::Transaction
        } else if starts_with_ci(s, "SET") {
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Source identity (client, user, database, backend, tenant).
    #[serde(default)]
    pub source: SourceIdentity,
    /// Global commit order assigned by the journal when the backend reported
    /// the commit (TR-07). `None` while the transaction is active. The order is
    /// the order in which this proxy observed the commit responses — a total
    /// order over everything that went through this proxy, not the backend's
    /// WAL order for commits that raced on different sessions.
    #[serde(default)]
    pub commit_seq: Option<u64>,
    /// When the commit was observed.
    #[serde(default)]
    pub committed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The backend's command tag that closed the transaction (`COMMIT`, or
    /// the tag of the last statement of an implicit/auto-commit transaction).
    /// `Cow` so the common `COMMIT` costs no allocation per commit.
    #[serde(default)]
    pub commit_tag: Option<std::borrow::Cow<'static, str>>,
    /// Set when some effect of this transaction could not be captured (a
    /// `COPY ... FROM STDIN`, a statement over `journal.max_statement_bytes`,
    /// a per-journal cap). Committed-history replay refuses such a
    /// transaction instead of applying it partially.
    #[serde(default)]
    pub incomplete_reason: Option<String>,
}

/// Savepoint information
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            source: SourceIdentity::default(),
            commit_seq: None,
            committed_at: None,
            commit_tag: None,
            incomplete_reason: None,
        }
    }

    /// Attach the source identity.
    pub fn with_source(mut self, source: SourceIdentity) -> Self {
        self.source = source;
        self
    }

    /// Mark the transaction as not fully captured (first reason wins).
    pub fn mark_incomplete(&mut self, reason: impl Into<String>) {
        if self.incomplete_reason.is_none() {
            self.incomplete_reason = Some(reason.into());
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
            JournalValue::TextRaw(b) | JournalValue::Binary(b) => b.len(),
        })
        .sum()
}

/// Everything `TransactionJournal::log_entry` needs to append one statement.
#[derive(Debug, Clone, PartialEq)]
pub struct NewEntry {
    pub statement: String,
    pub parameters: Vec<JournalValue>,
    pub param_types: Vec<u32>,
    pub result_checksum: Option<u64>,
    pub rows_affected: Option<u64>,
    pub duration_ms: u64,
    pub outcome: StatementOutcome,
    pub protocol: WireProtocol,
}

impl NewEntry {
    /// Materialize the entry with its sequence number and a timestamp of now.
    pub fn into_journal_entry(self, sequence: u64) -> JournalEntry {
        JournalEntry {
            sequence,
            statement_type: StatementType::from_sql(&self.statement),
            statement: self.statement,
            parameters: self.parameters,
            param_types: self.param_types,
            result_checksum: self.result_checksum,
            rows_affected: self.rows_affected,
            timestamp: chrono::Utc::now(),
            duration_ms: self.duration_ms,
            outcome: self.outcome,
            protocol: self.protocol,
        }
    }
}

/// Where committed transactions go once the journal has ordered them (TR-07).
///
/// The in-memory committed store is always kept; a sink additionally receives
/// every committed transaction, e.g. the segmented on-disk journal. `append`
/// must not block the caller: it returns `false` when it could not accept the
/// record (queue full, writer gone), and the journal counts that as a dropped
/// record so `coverage` can report the gap honestly.
pub trait JournalSink: Send + Sync {
    /// Hand one committed transaction to the sink.
    fn append(&self, tx: &Arc<TransactionJournalEntry>) -> bool;
    /// Whether records this sink accepted survive a process restart.
    fn durable(&self) -> bool;
}

/// Committed transactions in commit order, bounded by count and bytes.
/// Lives inside `JournalStore` so a commit (take from active, push here) is
/// one lock acquisition, the same as the pre-TR-07 write path paid.
#[derive(Debug, Default)]
struct CommittedStore {
    /// `(transaction, its byte size)` — the size is computed once at push so
    /// eviction never re-walks entries.
    txs: VecDeque<(Arc<TransactionJournalEntry>, usize)>,
    bytes: usize,
    entries: usize,
}

impl CommittedStore {
    /// Push one committed transaction and evict the oldest past the caps.
    /// The evicted transactions are returned (no allocation for the usual
    /// single eviction) so the caller drops them after releasing the lock.
    fn push(
        &mut self,
        tx: Arc<TransactionJournalEntry>,
        size: usize,
        max_count: usize,
        max_bytes: usize,
    ) -> Evicted {
        self.bytes += size;
        self.entries += tx.entries.len();
        self.txs.push_back((tx, size));
        let mut evicted = Evicted::default();
        while self.txs.len() > max_count.max(1) || (self.bytes > max_bytes && self.txs.len() > 1) {
            match self.txs.pop_front() {
                Some((old, size)) => {
                    self.bytes = self.bytes.saturating_sub(size);
                    self.entries = self.entries.saturating_sub(old.entries.len());
                    evicted.push(old);
                }
                None => break,
            }
        }
        evicted
    }

    fn iter(&self) -> impl Iterator<Item = &Arc<TransactionJournalEntry>> {
        self.txs.iter().map(|(t, _)| t)
    }
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
    /// Committed transactions in commit order (TR-07).
    committed: CommittedStore,
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

    fn take(&mut self, tx_id: &Uuid) -> Option<TransactionJournalEntry> {
        let (seq, journal) = self.entries.remove(tx_id)?;
        self.order.remove(&seq);
        Some(journal)
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

/// Commit metadata computed before the journal lock is taken.
struct CommitStamp {
    at: chrono::DateTime<chrono::Utc>,
    tag: std::borrow::Cow<'static, str>,
}

impl CommitStamp {
    fn now(tag: &str) -> Self {
        let tag = if tag == "COMMIT" {
            std::borrow::Cow::Borrowed("COMMIT")
        } else {
            std::borrow::Cow::Owned(tag.to_string())
        };
        Self {
            at: chrono::Utc::now(),
            tag,
        }
    }
}

/// Transactions evicted by one push: the first without allocating (the
/// usual case is exactly one), any further ones in a `Vec`.
#[derive(Default)]
struct Evicted {
    first: Option<Arc<TransactionJournalEntry>>,
    rest: Vec<Arc<TransactionJournalEntry>>,
}

impl Evicted {
    fn push(&mut self, tx: Arc<TransactionJournalEntry>) {
        if self.first.is_none() {
            self.first = Some(tx);
        } else {
            self.rest.push(tx);
        }
    }
}

/// What a commit produced under the lock; finished outside it.
struct Committed {
    seq: u64,
    tx: Arc<TransactionJournalEntry>,
    evicted: Evicted,
}

/// Transaction Journal Manager
pub struct TransactionJournal {
    /// Active transaction journals, insertion-ordered for O(k) eviction
    journals: Arc<RwLock<JournalStore>>,
    /// Maximum entries per journal
    max_entries: usize,
    /// Maximum journal size (bytes)
    max_size: usize,
    /// Global cap on the number of retained *active* transaction journals.
    /// Since TR-07 the data path journals real transactions and commits or
    /// rolls them back, so this cap only bites when many sessions hold long
    /// open transactions (or a library caller never commits). When it is
    /// reached the oldest journals (by insertion) are evicted.
    max_journals: usize,
    /// Whether journaling is enabled
    enabled: bool,
    /// Cap on retained committed transactions (count).
    max_committed: usize,
    /// Cap on retained committed bytes (statement text + parameters).
    max_committed_bytes: usize,
    /// Next commit sequence to assign (monotonic, starts at 1; a durable sink
    /// restores it across restarts via `set_next_commit_seq`).
    next_commit_seq: AtomicU64,
    /// Optional sink (durable journal) fed on every commit.
    sink: Option<Arc<dyn JournalSink>>,
    /// Committed transactions observed since process start.
    committed_total: AtomicU64,
    /// Rolled-back transactions observed since process start.
    rolled_back_total: AtomicU64,
    /// Committed transactions the sink could not accept.
    dropped_total: AtomicU64,
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
            max_committed: 50_000,
            max_committed_bytes: 256 * 1024 * 1024,
            next_commit_seq: AtomicU64::new(1),
            sink: None,
            committed_total: AtomicU64::new(0),
            rolled_back_total: AtomicU64::new(0),
            dropped_total: AtomicU64::new(0),
        }
    }

    /// Configure the cap on retained committed transactions.
    pub fn with_max_committed(mut self, max: usize) -> Self {
        self.max_committed = max.max(1);
        self
    }

    /// Configure the cap on retained committed bytes.
    pub fn with_max_committed_bytes(mut self, max: usize) -> Self {
        self.max_committed_bytes = max.max(1);
        self
    }

    /// Attach a sink that receives every committed transaction.
    pub fn with_sink(mut self, sink: Arc<dyn JournalSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    /// Whether committed transactions are handed to a durable sink.
    pub fn is_durable(&self) -> bool {
        self.sink.as_ref().map(|s| s.durable()).unwrap_or(false)
    }

    /// Continue commit numbering after `seq` (restart recovery).
    pub fn set_next_commit_seq(&self, next: u64) {
        self.next_commit_seq.store(next.max(1), Ordering::SeqCst);
    }

    /// Highest commit sequence assigned so far (`0` = none).
    pub fn commit_seq_high(&self) -> u64 {
        self.next_commit_seq
            .load(Ordering::SeqCst)
            .saturating_sub(1)
    }

    /// Reload committed transactions recovered from a durable sink, in the
    /// order given (oldest first). Their `commit_seq` values are kept and
    /// numbering continues after the highest one. Does not feed the sink.
    pub async fn load_committed(&self, txs: Vec<TransactionJournalEntry>) {
        let mut store = self.journals.write().await;
        let mut high = self.commit_seq_high();
        for tx in txs {
            if let Some(seq) = tx.commit_seq {
                high = high.max(seq);
            }
            let size = tx.total_size();
            let _ = store.committed.push(
                Arc::new(tx),
                size,
                self.max_committed,
                self.max_committed_bytes,
            );
        }
        drop(store);
        self.set_next_commit_seq(high + 1);
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

    /// Collect every statement of every **committed** transaction whose
    /// `timestamp` falls within the inclusive window `[from, to]`, sorted in
    /// timestamp order regardless of which transaction they came from.
    ///
    /// Used by the best-effort time-window replay (`src/replay/`). Since
    /// TR-07 the source is the committed store: statements of transactions
    /// that are still open, were rolled back or failed are not included.
    pub async fn entries_in_window(
        &self,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> Vec<(Uuid, JournalEntry)> {
        // Collect under the read lock ONLY; the sort runs after the guard is
        // dropped so a wide window cannot stall live journal writers (O-04).
        let mut out: Vec<(Uuid, JournalEntry)> = {
            let store = self.journals.read().await;
            let mut out: Vec<(Uuid, JournalEntry)> = Vec::new();
            for j in store.committed.iter() {
                for entry in &j.entries {
                    if entry.timestamp >= from && entry.timestamp <= to {
                        out.push((j.tx_id, entry.clone()));
                    }
                }
            }
            out
        };
        out.sort_by_key(|(_, e)| e.timestamp);
        out
    }

    /// Committed transactions whose commit was observed inside the inclusive
    /// window `[from, to]`, in commit order (TR-07 committed history).
    pub async fn committed_in_window(
        &self,
        from: chrono::DateTime<chrono::Utc>,
        to: chrono::DateTime<chrono::Utc>,
    ) -> Vec<Arc<TransactionJournalEntry>> {
        let store = self.journals.read().await;
        store
            .committed
            .iter()
            .filter(|t| {
                t.committed_at
                    .map(|c| c >= from && c <= to)
                    .unwrap_or(false)
            })
            .cloned()
            .collect()
    }

    /// Committed transactions with `commit_seq > after`, in commit order.
    pub async fn committed_after(&self, after: u64) -> Vec<Arc<TransactionJournalEntry>> {
        let store = self.journals.read().await;
        store
            .committed
            .iter()
            .filter(|t| t.commit_seq.map(|s| s > after).unwrap_or(false))
            .cloned()
            .collect()
    }

    /// Order and retain one committed transaction under the caller's write
    /// guard (one lock acquisition per commit). Returns the sequence, the
    /// shared record for the sink and what the caps evicted.
    fn push_committed_locked(
        &self,
        store: &mut JournalStore,
        mut tx: TransactionJournalEntry,
        stamp: CommitStamp,
    ) -> Committed {
        let seq = self.next_commit_seq.fetch_add(1, Ordering::SeqCst);
        tx.commit_seq = Some(seq);
        tx.committed_at = Some(stamp.at);
        tx.commit_tag = Some(stamp.tag);
        tx.active = false;
        let size = tx.total_size();
        let tx = Arc::new(tx);
        let evicted = store.committed.push(
            tx.clone(),
            size,
            self.max_committed,
            self.max_committed_bytes,
        );
        Committed { seq, tx, evicted }
    }

    /// Count the commit, hand the record to the durable sink and drop what the
    /// caps evicted — all outside the lock (the sink never blocks).
    fn after_commit(&self, c: Committed) -> u64 {
        self.committed_total.fetch_add(1, Ordering::Relaxed);
        if let Some(sink) = self.sink.as_ref() {
            if !sink.append(&c.tx) {
                self.dropped_total.fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(c.evicted);
        c.seq
    }

    /// Record a transaction that committed without an active phase (an
    /// auto-commit statement or an implicit multi-statement transaction),
    /// already populated with its entries. Empty transactions are dropped:
    /// there is nothing to replay. Returns the commit sequence.
    pub async fn record_committed(
        &self,
        tx: TransactionJournalEntry,
        commit_tag: &str,
    ) -> Option<u64> {
        if !self.enabled || tx.entries.is_empty() {
            return None;
        }
        let stamp = CommitStamp::now(commit_tag);
        let committed = {
            let mut store = self.journals.write().await;
            self.push_committed_locked(&mut store, tx, stamp)
        };
        Some(self.after_commit(committed))
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

    /// `begin_transaction` with the TR-07 source identity attached.
    pub async fn begin_transaction_with_source(
        &self,
        tx_id: Uuid,
        session_id: Uuid,
        node_id: NodeId,
        start_lsn: u64,
        source: SourceIdentity,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let journal =
            TransactionJournalEntry::new(tx_id, session_id, node_id, start_lsn).with_source(source);
        let mut journals = self.journals.write().await;
        self.enforce_cap_locked(&mut journals);
        journals.insert(tx_id, journal);
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
            param_types: Vec::new(),
            result_checksum,
            rows_affected,
            timestamp: chrono::Utc::now(),
            statement_type,
            duration_ms,
            outcome: StatementOutcome::Unobserved,
            protocol: WireProtocol::Simple,
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
        self.log_entry(
            tx_id,
            NewEntry {
                statement,
                parameters,
                param_types: Vec::new(),
                result_checksum,
                rows_affected,
                duration_ms,
                outcome: StatementOutcome::Unobserved,
                protocol: WireProtocol::Simple,
            },
        )
        .await
    }

    /// Log a statement with its full TR-07 capture (parameter types, observed
    /// outcome, wire protocol).
    pub async fn log_entry(&self, tx_id: Uuid, new: NewEntry) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let NewEntry {
            statement,
            parameters,
            param_types,
            result_checksum,
            rows_affected,
            duration_ms,
            outcome,
            protocol,
        } = new;

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
            param_types,
            result_checksum,
            rows_affected,
            timestamp: chrono::Utc::now(),
            statement_type,
            duration_ms,
            outcome,
            protocol,
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

    /// Commit transaction: move its journal from the active set into the
    /// committed store (commit order assigned here) and hand it to the sink.
    /// A transaction with no entries is simply dropped.
    pub async fn commit_transaction(&self, tx_id: Uuid) -> Result<()> {
        self.commit_transaction_with_tag(tx_id, "COMMIT").await?;
        Ok(())
    }

    /// `commit_transaction` with the backend's closing command tag. Returns the
    /// commit sequence, or `None` when nothing was retained (unknown tx or
    /// no entries).
    pub async fn commit_transaction_with_tag(
        &self,
        tx_id: Uuid,
        commit_tag: &str,
    ) -> Result<Option<u64>> {
        // Everything that can be prepared before the lock is prepared before it.
        let stamp = CommitStamp::now(commit_tag);
        let committed = {
            let mut journals = self.journals.write().await;
            match journals.take(&tx_id) {
                None => return Ok(None),
                Some(tx) if tx.entries.is_empty() => return Ok(None),
                Some(tx) => self.push_committed_locked(&mut journals, tx, stamp),
            }
        };
        tracing::debug!("Committed journal for transaction {:?}", tx_id);
        Ok(Some(self.after_commit(committed)))
    }

    /// Rollback transaction (clear journal)
    pub async fn rollback_transaction(&self, tx_id: Uuid) -> Result<()> {
        let removed = self.journals.write().await.take(&tx_id).is_some();
        if removed {
            self.rolled_back_total.fetch_add(1, Ordering::Relaxed);
        }
        tracing::debug!(
            "Rolled back and cleared journal for transaction {:?}",
            tx_id
        );
        Ok(())
    }

    /// Mark an active transaction as not fully captured (see
    /// `TransactionJournalEntry::incomplete_reason`).
    pub async fn mark_incomplete(&self, tx_id: Uuid, reason: &str) {
        if let Some(j) = self.journals.write().await.get_mut(&tx_id) {
            j.mark_incomplete(reason);
        }
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
        let active_transactions = journals.len();
        let total_entries: usize = journals.values().map(|j| j.entries.len()).sum();
        let total_size: usize = journals.values().map(|j| j.total_size()).sum();
        let committed = &journals.committed;

        JournalStats {
            active_transactions,
            total_entries,
            total_size_bytes: total_size,
            max_journals: self.max_journals,
            enabled: self.enabled,
            committed_transactions: committed.txs.len(),
            committed_entries: committed.entries,
            committed_bytes: committed.bytes,
            max_committed: self.max_committed,
            max_committed_bytes: self.max_committed_bytes,
            commit_seq_high: self.commit_seq_high(),
            committed_total: self.committed_total.load(Ordering::Relaxed),
            rolled_back_total: self.rolled_back_total.load(Ordering::Relaxed),
            dropped_total: self.dropped_total.load(Ordering::Relaxed),
            durable: self.is_durable(),
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
    /// Global cap on retained active journals; at the cap the oldest are
    /// evicted first (`[journal] max_active_transactions`).
    pub max_journals: usize,
    /// Whether journaling is enabled
    pub enabled: bool,
    /// Committed transactions currently retained (TR-07).
    pub committed_transactions: usize,
    /// Statements across the retained committed transactions.
    pub committed_entries: usize,
    /// Bytes across the retained committed transactions.
    pub committed_bytes: usize,
    /// `[journal] max_committed_transactions`.
    pub max_committed: usize,
    /// `[journal] max_committed_bytes`.
    pub max_committed_bytes: usize,
    /// Highest commit sequence assigned (`0` = none yet).
    pub commit_seq_high: u64,
    /// Commits observed since process start.
    pub committed_total: u64,
    /// Rollbacks observed since process start.
    pub rolled_back_total: u64,
    /// Committed transactions the durable sink could not accept.
    pub dropped_total: u64,
    /// Committed transactions are handed to a durable sink.
    pub durable: bool,
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
    fn test_statement_type_detection_case_and_whitespace() {
        // Mixed case and surrounding whitespace classify exactly as the
        // uppercased-copy implementation did.
        assert_eq!(
            StatementType::from_sql("  select 1  "),
            StatementType::Select
        );
        assert_eq!(
            StatementType::from_sql("\n\tInSeRt into t values (1)"),
            StatementType::Insert
        );
        assert_eq!(
            StatementType::from_sql("alter table t add c int"),
            StatementType::Ddl
        );
        assert_eq!(StatementType::from_sql("drop table t"), StatementType::Ddl);
        assert_eq!(
            StatementType::from_sql("commit"),
            StatementType::Transaction
        );
        assert_eq!(
            StatementType::from_sql("Rollback"),
            StatementType::Transaction
        );
        assert_eq!(
            StatementType::from_sql("savepoint sp1"),
            StatementType::Transaction
        );
        assert_eq!(
            StatementType::from_sql("set local x = 1"),
            StatementType::Set
        );
        assert_eq!(
            StatementType::from_sql("EXPLAIN ANALYZE SELECT 1"),
            StatementType::Other
        );
        assert_eq!(StatementType::from_sql(""), StatementType::Other);
        assert_eq!(StatementType::from_sql("   "), StatementType::Other);
        // Prefix shorter than the keyword must not panic or match.
        assert_eq!(StatementType::from_sql("SEL"), StatementType::Other);
        // Non-ASCII bytes are compared verbatim, never case-folded.
        assert_eq!(StatementType::from_sql("ßELECT 1"), StatementType::Other);
        assert_eq!(
            StatementType::from_sql("select 'ünïcode'"),
            StatementType::Select
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
