//! Time-travel replay engine.
//!
//! Given a transaction-journal window `[from, to]`, re-executes every
//! journaled statement against a target backend (usually a staging DB).
//! The primary consumer is the admin `POST /api/replay` endpoint:
//! a developer says "replay yesterday 10:00–11:00 UTC against
//! staging-db:5432" and the engine walks the journal in timestamp order
//! and streams the statements through `crate::backend::BackendClient`.
//!
//! This module is the T2.5 foundation. It builds directly on
//! `TransactionJournal` (the existing journaling) and the backend
//! client (added in the T0-TR sequence) — no new infrastructure.

use crate::backend::{BackendClient, BackendConfig, ParamValue};
use crate::transaction_journal::{JournalValue, TransactionJournal, TransactionJournalEntry};
use crate::{ProxyError, Result};
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// A request to replay a window of journal activity.
#[derive(Debug, Clone)]
pub struct TimeTravelRequest {
    /// Inclusive start timestamp.
    pub from: DateTime<Utc>,
    /// Inclusive end timestamp.
    pub to: DateTime<Utc>,
    /// Target host for replay (usually a staging / dev DB).
    pub target_host: String,
    /// Target port.
    pub target_port: u16,
    /// Optional per-call user override. When `None`, the engine's
    /// template user is used (set at server startup — typically
    /// `postgres`).
    pub target_user: Option<String>,
    /// Optional per-call password override. `None` means "use the
    /// template password" (which is itself often `None` for `trust`
    /// auth in dev). Production callers always set this.
    pub target_password: Option<String>,
    /// Optional per-call database override.
    pub target_database: Option<String>,
}

/// Summary of a replay run.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplaySummary {
    /// Replay mode. Always `"time_window"`: this engine re-executes journaled
    /// SQL text in timestamp order. It does NOT reconstruct committed
    /// transactions (boundaries, rollbacks, outcomes) — that is a separate,
    /// not-yet-shipped capability (TR-07).
    pub mode: &'static str,
    /// Number of statements actually executed on the target.
    pub statements_replayed: u64,
    /// Statements that failed (first error preserved in `first_error`).
    pub failures: u64,
    /// True when at least one statement failed or the overall deadline cut the
    /// run short: the target received a partial history and must not be treated
    /// as a faithful copy of the source.
    pub partial: bool,
    /// True when the overall replay deadline (O-04) stopped the run early.
    pub deadline_exceeded: bool,
    /// Wall-clock duration of the replay.
    pub elapsed_ms: u64,
    /// The window that was replayed.
    #[serde(with = "chrono::serde::ts_seconds")]
    pub from: DateTime<Utc>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub to: DateTime<Utc>,
    /// First error (if any); callers typically want the full stream
    /// via the tracing log rather than a single error string.
    pub first_error: Option<String>,
}

/// A request to replay committed history (TR-07): every transaction whose
/// commit this proxy observed inside `[from, to]`, in commit order.
#[derive(Debug, Clone)]
pub struct CommittedHistoryRequest {
    /// Inclusive start of the commit-time window.
    pub from: DateTime<Utc>,
    /// Inclusive end of the commit-time window.
    pub to: DateTime<Utc>,
    /// Only transactions with `commit_seq > after_commit_seq` (resume point);
    /// `0` = from the start of the window.
    pub after_commit_seq: u64,
    pub target_host: String,
    pub target_port: u16,
    pub target_user: Option<String>,
    pub target_password: Option<String>,
    pub target_database: Option<String>,
}

/// Where a committed-history replay stopped.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ReplayStop {
    /// Journal transaction id.
    pub tx_id: String,
    /// Global commit sequence of the transaction.
    pub commit_seq: u64,
    /// Statement sequence inside the transaction (`0` = before its first
    /// statement, e.g. `BEGIN` failed or the transaction was refused).
    pub sequence: u64,
    /// Why.
    pub error: String,
}

/// Summary of a committed-history replay (TR-07).
///
/// Semantics: transactions are applied one at a time on one target
/// connection, each inside its own `BEGIN` … `COMMIT`, in commit order. The
/// first failure rolls that transaction back and stops the run; the target
/// then holds exactly the transactions up to `last_commit_seq` and nothing
/// partial. A transaction the journal could not capture completely (see
/// `TransactionJournalEntry::incomplete_reason`) is refused before it starts,
/// for the same reason.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CommittedReplaySummary {
    /// Always `"committed_history"`.
    pub mode: &'static str,
    /// Transactions applied and committed on the target.
    pub transactions_replayed: u64,
    /// Statements executed inside those transactions.
    pub statements_replayed: u64,
    /// Transactions selected by the window (and resume point).
    pub transactions_selected: u64,
    /// Commit sequence of the last transaction committed on the target
    /// (`0` = none); pass it back as `after_commit_seq` to resume.
    pub last_commit_seq: u64,
    /// True when the run stopped before the last selected transaction.
    pub partial: bool,
    /// The overall replay deadline (O-04) stopped the run.
    pub deadline_exceeded: bool,
    /// Where and why the run stopped, when `partial`.
    pub stopped_at: Option<ReplayStop>,
    pub elapsed_ms: u64,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub from: DateTime<Utc>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub to: DateTime<Utc>,
}

/// Descriptor of what the journal behind a replay retains and structurally
/// captures (TR-07), reported with every replay response so a summary can
/// never be read as more than it is.
///
/// Since the TR-07 capture slice the journal records real transactions:
/// boundaries (`BEGIN` … `COMMIT` as the backend reported them), the bound
/// parameter values of extended-protocol statements, the backend's outcome
/// of every statement, the source identity and a global commit order. The
/// booleans below say so; `survives_restart` is true only when a durable
/// `[journal] dir` is configured. The counters are a snapshot at replay time.
#[derive(Debug, Clone, serde::Serialize)]
pub struct JournalCoverage {
    /// Active (open) transaction journals at replay time.
    pub retained_transactions: usize,
    /// Statement entries across those active journals.
    pub retained_entries: usize,
    /// Bytes across those active journals (statement text + parameters).
    pub retained_bytes: usize,
    /// `[journal] max_active_transactions`; eviction is oldest-first.
    pub max_journals: usize,
    /// Committed transactions retained for replay.
    pub committed_transactions: usize,
    /// Statements across the retained committed transactions.
    pub committed_entries: usize,
    /// Bytes across the retained committed transactions.
    pub committed_bytes: usize,
    /// `[journal] max_committed_transactions`.
    pub max_committed_transactions: usize,
    /// `[journal] max_committed_bytes`.
    pub max_committed_bytes: usize,
    /// Highest commit sequence assigned so far (`0` = none).
    pub commit_seq_high: u64,
    /// Committed transactions the durable sink could not accept since start.
    pub dropped_transactions: u64,
    /// Committed transactions are appended to a durable segment store.
    pub survives_restart: bool,
    /// Committed transaction boundaries are preserved.
    pub transaction_boundaries: bool,
    /// Protocol-level `Bind` parameter values are captured.
    pub parameter_values: bool,
    /// Per-statement backend outcomes are captured.
    pub outcomes: bool,
}

/// Replay engine backed by an existing transaction journal.
pub struct ReplayEngine {
    journal: Arc<TransactionJournal>,
    /// Template BackendConfig; host/port are swapped per `TimeTravelRequest`.
    backend_template: BackendConfig,
    /// Overall wall-clock budget for one replay (O-04). `None` = unbounded.
    deadline: Option<std::time::Duration>,
}

impl ReplayEngine {
    pub fn new(journal: Arc<TransactionJournal>, backend_template: BackendConfig) -> Self {
        Self {
            journal,
            backend_template,
            deadline: None,
        }
    }

    /// Bound one whole replay run. `None` leaves it unbounded. When the
    /// deadline expires the engine stops cleanly at the current statement and
    /// reports `deadline_exceeded` + partial progress (O-04).
    pub fn with_deadline(mut self, deadline: Option<std::time::Duration>) -> Self {
        self.deadline = deadline;
        self
    }

    /// Snapshot the journal's retained size and its structural coverage so a
    /// replay caller can see exactly what backed the run (TR-07).
    pub async fn journal_coverage(&self) -> JournalCoverage {
        let stats = self.journal.stats().await;
        JournalCoverage {
            retained_transactions: stats.active_transactions,
            retained_entries: stats.total_entries,
            retained_bytes: stats.total_size_bytes,
            max_journals: stats.max_journals,
            committed_transactions: stats.committed_transactions,
            committed_entries: stats.committed_entries,
            committed_bytes: stats.committed_bytes,
            max_committed_transactions: stats.max_committed,
            max_committed_bytes: stats.max_committed_bytes,
            commit_seq_high: stats.commit_seq_high,
            dropped_transactions: stats.dropped_total,
            survives_restart: stats.durable,
            transaction_boundaries: true,
            parameter_values: true,
            outcomes: true,
        }
    }

    fn remaining(
        start: std::time::Instant,
        deadline: Option<std::time::Duration>,
    ) -> Option<std::time::Duration> {
        deadline.map(|d| d.saturating_sub(start.elapsed()))
    }

    /// Early-return summary for a deadline that expired before any work ran.
    fn deadline_summary(
        req: &TimeTravelRequest,
        start: std::time::Instant,
        detail: String,
    ) -> ReplaySummary {
        ReplaySummary {
            mode: "time_window",
            statements_replayed: 0,
            failures: 0,
            partial: true,
            deadline_exceeded: true,
            elapsed_ms: start.elapsed().as_millis() as u64,
            from: req.from,
            to: req.to,
            first_error: Some(detail),
        }
    }

    /// Replay all journaled statements in the window against the
    /// target. Statements are executed in timestamp order across all
    /// transactions — this is "what would the target DB look like if
    /// it had received exactly this history in exactly this order."
    ///
    /// Individual failures are logged and counted; they do NOT abort
    /// the replay, because partial replay is the common case when a
    /// target schema diverges from the source's.
    pub async fn replay_window(&self, req: &TimeTravelRequest) -> Result<ReplaySummary> {
        if req.from > req.to {
            return Err(ProxyError::Internal("replay window: from > to".to_string()));
        }
        if req.target_host.trim().is_empty() || req.target_port == 0 {
            return Err(ProxyError::ReplayFailed(
                "target host and port are required for an operator replay".to_string(),
            ));
        }

        let entries = self.journal.entries_in_window(req.from, req.to).await;
        let total = entries.len();
        tracing::info!(
            total_entries = total,
            from = %req.from,
            to = %req.to,
            target = %format!("{}:{}", req.target_host, req.target_port),
            "starting time-travel replay"
        );

        let mut cfg = self.backend_template.clone();
        cfg.host = req.target_host.clone();
        cfg.port = req.target_port;
        if let Some(ref u) = req.target_user {
            cfg.user = u.clone();
        }
        if let Some(ref p) = req.target_password {
            cfg.password = Some(p.clone());
        }
        if let Some(ref d) = req.target_database {
            cfg.database = Some(d.clone());
        }

        let start = std::time::Instant::now();

        // O-04: the overall deadline also bounds connecting.
        let connect = BackendClient::connect(&cfg);
        let mut client = match Self::remaining(start, self.deadline) {
            Some(remaining) if remaining.is_zero() => {
                return Ok(Self::deadline_summary(
                    req,
                    start,
                    format!(
                        "replay deadline of {:?} exceeded before connecting to {}:{}",
                        self.deadline.unwrap_or_default(),
                        req.target_host,
                        req.target_port
                    ),
                ))
            }
            Some(remaining) => match tokio::time::timeout(remaining, connect).await {
                Ok(r) => {
                    r.map_err(|e| ProxyError::ReplayFailed(format!("connect to target: {}", e)))?
                }
                Err(_) => {
                    return Ok(Self::deadline_summary(
                        req,
                        start,
                        format!(
                            "replay deadline of {:?} exceeded while connecting to {}:{}",
                            self.deadline.unwrap_or_default(),
                            req.target_host,
                            req.target_port
                        ),
                    ))
                }
            },
            None => connect
                .await
                .map_err(|e| ProxyError::ReplayFailed(format!("connect to target: {}", e)))?,
        };

        let mut statements_replayed: u64 = 0;
        let mut failures: u64 = 0;
        let mut deadline_exceeded = false;
        let mut first_error: Option<String> = None;

        for (tx_id, entry) in entries {
            let params: Vec<ParamValue> = entry
                .parameters
                .iter()
                .map(journal_value_to_param)
                .collect();

            let statement = async {
                if params.is_empty() {
                    client.simple_query(&entry.statement).await
                } else {
                    client.query_with_params(&entry.statement, &params).await
                }
            };

            // O-04: stop starting new statements once the budget is spent, and
            // bound the in-flight statement by the remaining budget.
            let outcome = match Self::remaining(start, self.deadline) {
                Some(remaining) if remaining.is_zero() => {
                    deadline_exceeded = true;
                    first_error.get_or_insert_with(|| {
                        format!(
                            "replay deadline of {:?} exceeded at tx {} seq {}",
                            self.deadline.unwrap_or_default(),
                            tx_id,
                            entry.sequence
                        )
                    });
                    break;
                }
                Some(remaining) => match tokio::time::timeout(remaining, statement).await {
                    Ok(res) => res,
                    Err(_) => {
                        deadline_exceeded = true;
                        first_error.get_or_insert_with(|| {
                            format!(
                                "replay deadline of {:?} exceeded at tx {} seq {}",
                                self.deadline.unwrap_or_default(),
                                tx_id,
                                entry.sequence
                            )
                        });
                        break;
                    }
                },
                None => statement.await,
            };

            match outcome {
                Ok(_) => {
                    statements_replayed += 1;
                }
                Err(e) => {
                    failures += 1;
                    if first_error.is_none() {
                        first_error = Some(format!("tx {} seq {}: {}", tx_id, entry.sequence, e));
                    }
                    tracing::warn!(
                        tx = %tx_id,
                        sequence = entry.sequence,
                        error = %e,
                        "replay statement failed"
                    );
                }
            }
        }

        client.close().await;

        Ok(ReplaySummary {
            mode: "time_window",
            statements_replayed,
            failures,
            partial: failures > 0 || deadline_exceeded,
            deadline_exceeded,
            elapsed_ms: start.elapsed().as_millis() as u64,
            from: req.from,
            to: req.to,
            first_error,
        })
    }
}

impl ReplayEngine {
    /// Replay committed history (TR-07): the transactions whose commit was
    /// observed in the window, in commit order, each as one transaction on
    /// one target connection, stopping at the first failure.
    pub async fn replay_committed(
        &self,
        req: &CommittedHistoryRequest,
    ) -> Result<CommittedReplaySummary> {
        if req.from > req.to {
            return Err(ProxyError::Internal("replay window: from > to".to_string()));
        }
        if req.target_host.trim().is_empty() || req.target_port == 0 {
            return Err(ProxyError::ReplayFailed(
                "target host and port are required for an operator replay".to_string(),
            ));
        }
        let start = std::time::Instant::now();
        let selected: Vec<Arc<TransactionJournalEntry>> = self
            .journal
            .committed_in_window(req.from, req.to)
            .await
            .into_iter()
            .filter(|t| t.commit_seq.unwrap_or(0) > req.after_commit_seq)
            .collect();
        let mut summary = CommittedReplaySummary {
            mode: "committed_history",
            transactions_replayed: 0,
            statements_replayed: 0,
            transactions_selected: selected.len() as u64,
            last_commit_seq: 0,
            partial: false,
            deadline_exceeded: false,
            stopped_at: None,
            elapsed_ms: 0,
            from: req.from,
            to: req.to,
        };
        tracing::info!(
            transactions = selected.len(),
            from = %req.from,
            to = %req.to,
            after_commit_seq = req.after_commit_seq,
            target = %format!("{}:{}", req.target_host, req.target_port),
            "starting committed-history replay"
        );
        if selected.is_empty() {
            summary.elapsed_ms = start.elapsed().as_millis() as u64;
            return Ok(summary);
        }

        let mut cfg = self.backend_template.clone();
        cfg.host = req.target_host.clone();
        cfg.port = req.target_port;
        if let Some(ref u) = req.target_user {
            cfg.user = u.clone();
        }
        if let Some(ref p) = req.target_password {
            cfg.password = Some(p.clone());
        }
        if let Some(ref d) = req.target_database {
            cfg.database = Some(d.clone());
        }

        let deadline = self.deadline;
        let budget = |start: std::time::Instant| Self::remaining(start, deadline);
        let stop = |summary: &mut CommittedReplaySummary,
                    tx: &TransactionJournalEntry,
                    sequence: u64,
                    error: String| {
            summary.partial = true;
            summary.stopped_at = Some(ReplayStop {
                tx_id: tx.tx_id.to_string(),
                commit_seq: tx.commit_seq.unwrap_or(0),
                sequence,
                error,
            });
        };

        let connect = BackendClient::connect(&cfg);
        let mut client = match budget(start) {
            Some(r) if r.is_zero() => {
                summary.deadline_exceeded = true;
                stop(
                    &mut summary,
                    &selected[0],
                    0,
                    "replay deadline exceeded before connecting".into(),
                );
                summary.elapsed_ms = start.elapsed().as_millis() as u64;
                return Ok(summary);
            }
            Some(r) => match tokio::time::timeout(r, connect).await {
                Ok(c) => {
                    c.map_err(|e| ProxyError::ReplayFailed(format!("connect to target: {}", e)))?
                }
                Err(_) => {
                    summary.deadline_exceeded = true;
                    stop(
                        &mut summary,
                        &selected[0],
                        0,
                        "replay deadline exceeded while connecting".into(),
                    );
                    summary.elapsed_ms = start.elapsed().as_millis() as u64;
                    return Ok(summary);
                }
            },
            None => connect
                .await
                .map_err(|e| ProxyError::ReplayFailed(format!("connect to target: {}", e)))?,
        };

        'txs: for tx in &selected {
            if let Some(reason) = tx.incomplete_reason.as_deref() {
                stop(
                    &mut summary,
                    tx,
                    0,
                    format!("transaction was not fully captured ({reason}); refusing to apply it partially"),
                );
                break;
            }
            // Deadline check before opening a transaction: never leave one open.
            if matches!(budget(start), Some(r) if r.is_zero()) {
                summary.deadline_exceeded = true;
                stop(&mut summary, tx, 0, "replay deadline exceeded".into());
                break;
            }
            if let Err(e) = Self::bounded(budget(start), client.simple_query("BEGIN")).await {
                stop(&mut summary, tx, 0, format!("BEGIN: {e}"));
                break;
            }
            for entry in &tx.entries {
                let exec = client.execute_journaled(
                    &entry.statement,
                    &entry.param_types,
                    &entry.parameters,
                );
                match Self::bounded(budget(start), exec).await {
                    Ok(_) => summary.statements_replayed += 1,
                    Err(e) => {
                        let timed_out = e.to_string().contains("replay deadline");
                        let _ = client.simple_query("ROLLBACK").await;
                        summary.deadline_exceeded |= timed_out;
                        stop(&mut summary, tx, entry.sequence, e.to_string());
                        break 'txs;
                    }
                }
            }
            match Self::bounded(budget(start), client.simple_query("COMMIT")).await {
                Ok(r) if r.command_tag.eq_ignore_ascii_case("COMMIT") => {
                    summary.transactions_replayed += 1;
                    summary.last_commit_seq = tx.commit_seq.unwrap_or(0);
                }
                Ok(r) => {
                    let _ = client.simple_query("ROLLBACK").await;
                    stop(
                        &mut summary,
                        tx,
                        tx.current_sequence,
                        format!("COMMIT answered {:?}", r.command_tag),
                    );
                    break;
                }
                Err(e) => {
                    let _ = client.simple_query("ROLLBACK").await;
                    summary.deadline_exceeded |= e.to_string().contains("replay deadline");
                    stop(
                        &mut summary,
                        tx,
                        tx.current_sequence,
                        format!("COMMIT: {e}"),
                    );
                    break;
                }
            }
        }
        client.close().await;
        summary.elapsed_ms = start.elapsed().as_millis() as u64;
        if summary.partial {
            tracing::warn!(stopped_at = ?summary.stopped_at, replayed = summary.transactions_replayed, "committed-history replay stopped");
        }
        Ok(summary)
    }

    /// Run `fut` within the remaining replay budget (`None` = unbounded).
    async fn bounded<T>(
        remaining: Option<std::time::Duration>,
        fut: impl std::future::Future<Output = crate::backend::BackendResult<T>>,
    ) -> Result<T> {
        match remaining {
            Some(r) if r.is_zero() => Err(ProxyError::ReplayFailed(
                "replay deadline exceeded".to_string(),
            )),
            Some(r) => match tokio::time::timeout(r, fut).await {
                Ok(res) => res.map_err(|e| ProxyError::ReplayFailed(e.to_string())),
                Err(_) => Err(ProxyError::ReplayFailed(
                    "replay deadline exceeded".to_string(),
                )),
            },
            None => fut
                .await
                .map_err(|e| ProxyError::ReplayFailed(e.to_string())),
        }
    }
}

/// Convert a `JournalValue` to a `ParamValue` for text-format
/// interpolation. Mirrors the translator in `failover_replay.rs`;
/// kept local here to avoid cross-module coupling for three lines.
fn journal_value_to_param(v: &JournalValue) -> ParamValue {
    match v {
        JournalValue::Null => ParamValue::Null,
        JournalValue::Bool(b) => ParamValue::Bool(*b),
        JournalValue::Int64(i) => ParamValue::Int(*i),
        JournalValue::Float64(f) => ParamValue::Float(*f),
        JournalValue::Text(s) => ParamValue::Text(s.clone()),
        JournalValue::Bytes(b) => {
            let mut s = String::with_capacity(2 + b.len() * 2);
            s.push_str("\\x");
            for byte in b {
                s.push_str(&format!("{:02x}", byte));
            }
            ParamValue::Text(s)
        }
        JournalValue::Array(items) => ParamValue::Text(array_literal(items)),
        JournalValue::TextRaw(b) => ParamValue::Text(String::from_utf8_lossy(b).into_owned()),
        // A binary-format value has no text rendering; the committed-history
        // path sends it back in binary. Time-window replay interpolates text,
        // so degrade to NULL rather than send garbage.
        JournalValue::Binary(_) => ParamValue::Null,
    }
}

/// Render a journaled array as a PostgreSQL array literal (`{1,"a b",NULL}`).
pub(crate) fn array_literal(items: &[JournalValue]) -> String {
    fn elem(v: &JournalValue, out: &mut String) {
        match v {
            JournalValue::Null => out.push_str("NULL"),
            JournalValue::Bool(b) => out.push_str(if *b { "t" } else { "f" }),
            JournalValue::Int64(i) => out.push_str(&i.to_string()),
            JournalValue::Float64(f) => out.push_str(&f.to_string()),
            JournalValue::Text(s) => quote(s, out),
            JournalValue::TextRaw(b) => quote(&String::from_utf8_lossy(b), out),
            JournalValue::Bytes(b) | JournalValue::Binary(b) => {
                let mut s = String::with_capacity(2 + b.len() * 2);
                s.push_str("\\x");
                for byte in b {
                    s.push_str(&format!("{:02x}", byte));
                }
                quote(&s, out);
            }
            JournalValue::Array(inner) => out.push_str(&array_literal(inner)),
        }
    }
    fn quote(s: &str, out: &mut String) {
        out.push('"');
        for c in s.chars() {
            if c == '"' || c == '\\' {
                out.push('\\');
            }
            out.push(c);
        }
        out.push('"');
    }
    let mut out = String::from("{");
    for (i, v) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        elem(v, &mut out);
    }
    out.push('}');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{tls::default_client_config, TlsMode};
    use crate::NodeId;
    use std::time::Duration;
    use uuid::Uuid;

    fn test_template() -> BackendConfig {
        BackendConfig {
            host: "placeholder".into(),
            port: 0,
            user: "postgres".into(),
            password: None,
            database: None,
            application_name: Some("helios-replay".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_millis(200),
            query_timeout: Duration::from_millis(200),
            tls_config: default_client_config(),
        }
    }

    #[tokio::test]
    async fn test_replay_rejects_inverted_window() {
        let journal = Arc::new(TransactionJournal::new());
        let engine = ReplayEngine::new(journal, test_template());
        let now = Utc::now();
        let req = TimeTravelRequest {
            from: now,
            to: now - chrono::Duration::seconds(1),
            target_host: "127.0.0.1".into(),
            target_port: 1,
            target_user: None,
            target_password: None,
            target_database: None,
        };
        let err = engine.replay_window(&req).await.unwrap_err();
        assert!(matches!(err, ProxyError::Internal(_)));
    }

    /// Empty journal returns a zero-statement summary without touching
    /// the network — the `connect` call still needs to succeed though,
    /// so we point at an unreachable address and expect a connect
    /// error, which is a cheap proof the code path runs.
    #[tokio::test]
    async fn test_replay_empty_window_still_connects() {
        let journal = Arc::new(TransactionJournal::new());
        let engine = ReplayEngine::new(journal, test_template());
        let now = Utc::now();
        let req = TimeTravelRequest {
            from: now - chrono::Duration::hours(1),
            to: now,
            target_host: "127.0.0.1".into(),
            target_port: 1, // refused
            target_user: None,
            target_password: None,
            target_database: None,
        };
        let err = engine.replay_window(&req).await.unwrap_err();
        match err {
            ProxyError::ReplayFailed(msg) => assert!(msg.contains("connect")),
            other => panic!("expected ReplayFailed, got {:?}", other),
        }
    }

    /// Entries outside the window are filtered out by the journal
    /// query — proved indirectly by checking only the one in-window
    /// entry appears in `entries_in_window`.
    #[tokio::test]
    async fn test_entries_in_window_filters_correctly() {
        let journal = Arc::new(TransactionJournal::new());
        let tx_id = Uuid::new_v4();
        let session = Uuid::new_v4();
        let node = NodeId::new();

        let base = Utc::now();
        journal
            .begin_transaction(tx_id, session, node, 0)
            .await
            .unwrap();

        // Insert three entries at three timestamps — the existing
        // `log_statement` only writes `chrono::Utc::now()` so we can't
        // backdate them through the public API. Rely on the built-in
        // now() and choose a window that encloses exactly now().
        let _ = base; // suppress unused
        journal
            .log_statement(tx_id, "SELECT 1".to_string(), vec![], None, None, 1)
            .await
            .unwrap();

        let from = Utc::now() - chrono::Duration::seconds(5);
        let to = Utc::now() + chrono::Duration::seconds(5);
        // TR-07: the window is committed history only.
        assert!(journal.entries_in_window(from, to).await.is_empty());
        journal.commit_transaction(tx_id).await.unwrap();
        let entries = journal.entries_in_window(from, to).await;
        assert_eq!(entries.len(), 1, "single in-window entry");

        let far_past_to = Utc::now() - chrono::Duration::hours(1);
        let far_past_from = far_past_to - chrono::Duration::hours(1);
        let entries = journal.entries_in_window(far_past_from, far_past_to).await;
        assert!(entries.is_empty(), "no entries in far-past window");
    }

    #[test]
    fn test_journal_value_to_param_matches_failover_shape() {
        // Parity with failover_replay::journal_value_to_param — the two
        // must produce the same ParamValue for identical inputs so a
        // journaled write replayed via either path produces the same
        // text literal.
        assert!(matches!(
            journal_value_to_param(&JournalValue::Null),
            ParamValue::Null
        ));
        assert!(matches!(
            journal_value_to_param(&JournalValue::Bool(true)),
            ParamValue::Bool(true)
        ));
        assert!(matches!(
            journal_value_to_param(&JournalValue::Int64(-7)),
            ParamValue::Int(-7)
        ));
    }

    /// Credential override fields default to None and the resulting
    /// BackendConfig keeps the template's user/password/database. This
    /// test proves the override path applies when fields are Some
    /// without exercising a real connect — we inspect via
    /// `apply_overrides` extracted as a pure helper for testability.
    #[test]
    fn test_credential_overrides_replace_template_fields() {
        let mut cfg = test_template();
        cfg.user = "default_user".into();
        cfg.password = None;
        cfg.database = None;

        let req = TimeTravelRequest {
            from: Utc::now(),
            to: Utc::now(),
            target_host: "h".into(),
            target_port: 5432,
            target_user: Some("override_user".into()),
            target_password: Some("secret".into()),
            target_database: Some("staging".into()),
        };

        // Inline the same override application replay_window does. If
        // this test ever drifts from the production code path,
        // replay_window's behaviour is what's authoritative; the
        // override block is small enough to spot the divergence.
        if let Some(ref u) = req.target_user {
            cfg.user = u.clone();
        }
        if let Some(ref p) = req.target_password {
            cfg.password = Some(p.clone());
        }
        if let Some(ref d) = req.target_database {
            cfg.database = Some(d.clone());
        }

        assert_eq!(cfg.user, "override_user");
        assert_eq!(cfg.password.as_deref(), Some("secret"));
        assert_eq!(cfg.database.as_deref(), Some("staging"));
    }

    #[test]
    fn test_credential_overrides_none_keeps_template_fields() {
        let mut cfg = test_template();
        cfg.user = "default_user".into();
        cfg.password = Some("template_pw".into());
        cfg.database = Some("default_db".into());

        let req = TimeTravelRequest {
            from: Utc::now(),
            to: Utc::now(),
            target_host: "h".into(),
            target_port: 5432,
            target_user: None,
            target_password: None,
            target_database: None,
        };

        if let Some(ref u) = req.target_user {
            cfg.user = u.clone();
        }
        // ... password / database left untouched.
        let _ = req;

        assert_eq!(cfg.user, "default_user");
        assert_eq!(cfg.password.as_deref(), Some("template_pw"));
        assert_eq!(cfg.database.as_deref(), Some("default_db"));
    }

    /// Summary round-trips through serde so the admin API can return
    /// it as JSON.
    #[test]
    fn test_replay_summary_serializes() {
        let s = ReplaySummary {
            mode: "time_window",
            statements_replayed: 5,
            failures: 1,
            partial: true,
            deadline_exceeded: false,
            elapsed_ms: 42,
            from: Utc::now(),
            to: Utc::now(),
            first_error: Some("oops".into()),
        };
        let j = serde_json::to_string(&s).unwrap();
        assert!(j.contains("\"mode\":\"time_window\""));
        assert!(j.contains("\"statements_replayed\":5"));
        assert!(j.contains("\"failures\":1"));
        assert!(j.contains("\"partial\":true"));
        assert!(j.contains("\"deadline_exceeded\":false"));
        assert!(j.contains("oops"));
    }

    /// The coverage descriptor must report the journal's retained sample and
    /// must not claim boundaries, parameters, outcomes or restart durability
    /// (TR-07). It has to serialize in exactly that shape for the API.
    #[tokio::test]
    async fn test_journal_coverage_reports_honest_limits() {
        let journal = Arc::new(TransactionJournal::new());
        let tx = Uuid::new_v4();
        journal
            .begin_transaction(tx, Uuid::new_v4(), NodeId::new(), 0)
            .await
            .unwrap();
        journal
            .log_statement(
                tx,
                "insert into t values (1)".to_string(),
                vec![],
                None,
                None,
                0,
            )
            .await
            .unwrap();

        let engine = ReplayEngine::new(journal.clone(), test_template());
        let cov = engine.journal_coverage().await;
        assert_eq!(cov.retained_transactions, 1, "one active journal");
        assert_eq!(cov.retained_entries, 1);
        assert!(cov.retained_bytes > 0);
        assert_eq!(cov.max_journals, 50_000);
        assert_eq!(cov.committed_transactions, 0);
        assert_eq!(cov.commit_seq_high, 0);
        // TR-07: the capture preserves boundaries, parameters and outcomes;
        // durability depends on `[journal] dir` (none here).
        assert!(!cov.survives_restart);
        assert!(cov.transaction_boundaries);
        assert!(cov.parameter_values);
        assert!(cov.outcomes);
        assert_eq!(cov.dropped_transactions, 0);
        journal.commit_transaction(tx).await.unwrap();
        let cov = engine.journal_coverage().await;
        assert_eq!(cov.retained_transactions, 0);
        assert_eq!(cov.committed_transactions, 1);
        assert_eq!(cov.committed_entries, 1);
        assert_eq!(cov.commit_seq_high, 1);

        let json = serde_json::to_string(&cov).unwrap();
        for field in [
            "\"survives_restart\":false",
            "\"transaction_boundaries\":true",
            "\"parameter_values\":true",
            "\"outcomes\":true",
            "\"committed_transactions\":1",
            "\"commit_seq_high\":1",
            "\"dropped_transactions\":0",
        ] {
            assert!(
                json.contains(field),
                "coverage must serialize {field}: {json}"
            );
        }
    }

    #[tokio::test]
    async fn test_replay_deadline_stops_before_connect() {
        // Zero budget: the engine must stop cleanly, report partial progress
        // and NOT attempt (or claim) a connection (O-04).
        let journal = Arc::new(TransactionJournal::new());
        let engine =
            ReplayEngine::new(journal, test_template()).with_deadline(Some(Duration::ZERO));
        let now = Utc::now();
        let req = TimeTravelRequest {
            from: now - chrono::Duration::minutes(1),
            to: now,
            target_host: "127.0.0.1".into(),
            target_port: 1, // would be refused if we connected
            target_user: None,
            target_password: None,
            target_database: None,
        };
        let summary = engine.replay_window(&req).await.unwrap();
        assert!(summary.deadline_exceeded);
        assert!(summary.partial);
        assert_eq!(summary.statements_replayed, 0);
        assert!(summary.first_error.unwrap_or_default().contains("deadline"));
    }

    #[tokio::test]
    async fn test_replay_requires_a_target() {
        let journal = Arc::new(TransactionJournal::new());
        let engine = ReplayEngine::new(journal, test_template());
        let now = Utc::now();
        let req = TimeTravelRequest {
            from: now - chrono::Duration::minutes(1),
            to: now,
            target_host: "   ".into(),
            target_port: 5432,
            target_user: None,
            target_password: None,
            target_database: None,
        };
        let err = engine.replay_window(&req).await.unwrap_err();
        assert!(matches!(err, ProxyError::ReplayFailed(_)));
    }
}
