//! TR-07 capture: turn what a session sends and what the backend answers into
//! real transactions for the recovery journal.
//!
//! The data path used to journal every write as its own never-committed,
//! text-only, synthetic auto-commit transaction, after the response had been
//! relayed and without looking at it. This module replaces that with a small
//! per-session state machine:
//!
//! * the forward path **registers** what it is about to send (a simple-query
//!   string, or the `Parse`/`Bind`/`Execute` messages of an extended batch —
//!   including the bound parameter values, byte for byte);
//! * the relay **observes** the backend's response (each `CommandComplete`
//!   tag, any `ErrorResponse`, and the `ReadyForQuery` status byte);
//! * [`SessionCapture::observe`] reconciles the two into journal operations:
//!   open a transaction on `BEGIN`, log each statement the backend actually
//!   completed with its outcome, apply `SAVEPOINT` / `ROLLBACK TO`, commit or
//!   roll back on the backend's own closing tag, and record an auto-commit
//!   statement (or an implicit multi-statement transaction) as one committed
//!   transaction.
//!
//! Only the backend's verdict decides what is committed: a statement is
//! journaled when its `CommandComplete` was seen, a transaction is committed
//! when the tag that closed it was `COMMIT`. A `COMMIT` issued in an aborted
//! transaction returns `ROLLBACK` and is treated as one.
//!
//! What is captured is the set of statements that changed data: DML, DDL and
//! the control statements needed to reproduce savepoint structure. Reads are
//! not journaled (a read with side effects — `nextval`, `set_config`, a
//! volatile function — is therefore not reproduced; see the docs). A
//! `COPY ... FROM STDIN` and an `EXECUTE` of a session-scoped prepared
//! statement cannot be reproduced from the journal; the transaction is kept
//! but marked incomplete so committed-history replay refuses it rather than
//! apply it partially.
//!
//! The state machine is pure (no I/O, no async) so every transition is unit
//! tested; [`apply_ops`] applies the resulting operations to the journal.

use crate::protocol::contains_ci;
use crate::transaction_journal::{
    next_auto_commit_tx_id, JournalValue, NewEntry, SourceIdentity, StatementOutcome,
    TransactionJournal, TransactionJournalEntry, WireProtocol,
};
use crate::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// Transaction-control statement, as far as the journal cares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    /// `BEGIN` / `START TRANSACTION`.
    Begin,
    /// `COMMIT` / `END`.
    Commit,
    /// `ROLLBACK` / `ABORT` (whole transaction).
    Rollback,
    /// `SAVEPOINT name`.
    Savepoint(String),
    /// `RELEASE [SAVEPOINT] name`.
    Release(String),
    /// `ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] name`.
    RollbackTo(String),
    /// `PREPARE TRANSACTION 'gid'` — ends the local transaction without
    /// committing it; its later `COMMIT PREPARED` can come from any session,
    /// so two-phase transactions are not tracked as committed history.
    PrepareTransaction,
    /// `COMMIT PREPARED` / `ROLLBACK PREPARED`: no effect on this session's
    /// transaction state.
    Passive,
}

/// What a statement means to the journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StmtKind {
    /// Not journaled.
    Read,
    /// Data or schema change: journaled with its outcome.
    Write,
    /// Transaction control: drives the state machine.
    Control(Control),
    /// `COPY ... FROM`: executed, but its data never passes through the
    /// journal. Journaled and marks the transaction incomplete.
    Copy,
    /// `EXECUTE name`: a session-scoped prepared statement the journal
    /// cannot reproduce on another connection. Journaled and marks the
    /// transaction incomplete.
    Opaque,
}

impl StmtKind {
    fn is_journaled(&self) -> bool {
        matches!(self, StmtKind::Write | StmtKind::Copy | StmtKind::Opaque)
    }
    fn incomplete_reason(&self) -> Option<&'static str> {
        match self {
            StmtKind::Copy => Some("COPY ... FROM data is not captured by the journal"),
            StmtKind::Opaque => {
                Some("EXECUTE of a session-scoped prepared statement cannot be reproduced")
            }
            _ => None,
        }
    }
}

/// Skip leading whitespace and SQL comments (`-- …` and `/* … */`).
fn skip_leading_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix("--") {
            s = match rest.find('\n') {
                Some(i) => &rest[i + 1..],
                None => "",
            };
        } else if let Some(rest) = s.strip_prefix("/*") {
            s = match rest.find("*/") {
                Some(i) => &rest[i + 2..],
                None => "",
            };
        } else {
            return s;
        }
    }
}

/// The first SQL word (ASCII letters/digits/underscore) and the remainder.
fn first_word(s: &str) -> (&str, &str) {
    let end = s
        .bytes()
        .position(|b| !(b.is_ascii_alphanumeric() || b == b'_'))
        .unwrap_or(s.len());
    (&s[..end], &s[end..])
}

fn word_eq(word: &str, kw: &str) -> bool {
    word.len() == kw.len() && word.eq_ignore_ascii_case(kw)
}

/// Savepoint name: the next identifier, optionally quoted, after `rest`.
fn savepoint_name(rest: &str) -> String {
    let rest = rest.trim_start();
    if let Some(q) = rest.strip_prefix('"') {
        let end = q.find('"').unwrap_or(q.len());
        return q[..end].to_string();
    }
    let (w, _) = first_word(rest);
    w.to_string()
}

/// Classify one statement (no interior `;`).
fn classify_single(sql: &str) -> StmtKind {
    let s = skip_leading_comments(sql);
    let (w, rest) = first_word(s);
    let (w2, rest2) = first_word(rest.trim_start());
    let upper = |kw: &str| word_eq(w, kw);
    if upper("BEGIN") || upper("START") {
        return StmtKind::Control(Control::Begin);
    }
    if upper("COMMIT") || upper("END") {
        return if word_eq(w2, "PREPARED") {
            StmtKind::Control(Control::Passive)
        } else {
            StmtKind::Control(Control::Commit)
        };
    }
    if upper("ROLLBACK") || upper("ABORT") {
        if word_eq(w2, "PREPARED") {
            return StmtKind::Control(Control::Passive);
        }
        // ROLLBACK [WORK|TRANSACTION] TO [SAVEPOINT] name
        let mut r = rest;
        let mut w2 = w2;
        let mut rest2 = rest2;
        if word_eq(w2, "WORK") || word_eq(w2, "TRANSACTION") {
            r = rest2;
            let (a, b) = first_word(r.trim_start());
            w2 = a;
            rest2 = b;
        }
        let _ = r;
        if word_eq(w2, "TO") {
            let after = rest2.trim_start();
            let (w3, rest3) = first_word(after);
            let name = if word_eq(w3, "SAVEPOINT") {
                savepoint_name(rest3)
            } else {
                savepoint_name(after)
            };
            return StmtKind::Control(Control::RollbackTo(name));
        }
        return StmtKind::Control(Control::Rollback);
    }
    if upper("SAVEPOINT") {
        return StmtKind::Control(Control::Savepoint(savepoint_name(rest)));
    }
    if upper("RELEASE") {
        let name = if word_eq(w2, "SAVEPOINT") {
            savepoint_name(rest2)
        } else {
            savepoint_name(rest)
        };
        return StmtKind::Control(Control::Release(name));
    }
    if upper("PREPARE") {
        return if word_eq(w2, "TRANSACTION") {
            StmtKind::Control(Control::PrepareTransaction)
        } else {
            // PREPARE name AS ... defines a session-scoped statement; the
            // definition itself changes no data.
            StmtKind::Read
        };
    }
    if upper("EXECUTE") {
        return StmtKind::Opaque;
    }
    if upper("COPY") {
        // COPY ... FROM loads rows; COPY ... TO only reads.
        return if contains_ci(s, " FROM ") || contains_ci(s, " FROM\n") || contains_ci(s, " FROM\t")
        {
            StmtKind::Copy
        } else {
            StmtKind::Read
        };
    }
    if upper("WITH") {
        return if contains_ci(s, "INSERT")
            || contains_ci(s, "UPDATE")
            || contains_ci(s, "DELETE")
            || contains_ci(s, "MERGE")
        {
            StmtKind::Write
        } else {
            StmtKind::Read
        };
    }
    const WRITES: [&str; 17] = [
        "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "DROP", "ALTER", "TRUNCATE", "GRANT",
        "REVOKE", "REFRESH", "COMMENT", "SECURITY", "IMPORT", "CALL", "DO", "REASSIGN",
    ];
    if WRITES.iter().any(|kw| upper(kw)) {
        return StmtKind::Write;
    }
    StmtKind::Read
}

/// Split a simple-query string into statements on `;` outside quotes,
/// dollar-quoted strings and comments. Returns the string itself when it
/// holds a single statement.
pub fn split_statements(sql: &str) -> Vec<&str> {
    let b = sql.as_bytes();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < b.len() {
        match b[i] {
            b'\'' | b'"' => {
                let q = b[i];
                i += 1;
                while i < b.len() {
                    if b[i] == q {
                        if i + 1 < b.len() && b[i + 1] == q {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            b'-' if i + 1 < b.len() && b[i + 1] == b'-' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < b.len() && b[i + 1] == b'*' => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 1;
            }
            b'$' => {
                // $tag$ ... $tag$
                let tag_end = b[i + 1..]
                    .iter()
                    .position(|&c| c == b'$')
                    .map(|p| i + 1 + p);
                if let Some(te) = tag_end {
                    let tag = &b[i..=te];
                    if tag[1..tag.len() - 1]
                        .iter()
                        .all(|c| c.is_ascii_alphanumeric() || *c == b'_')
                    {
                        let mut j = te + 1;
                        let mut closed = false;
                        while j + tag.len() <= b.len() {
                            if &b[j..j + tag.len()] == tag {
                                i = j + tag.len() - 1;
                                closed = true;
                                break;
                            }
                            j += 1;
                        }
                        if !closed {
                            i = b.len();
                        }
                    }
                }
            }
            b';' => {
                let piece = sql[start..i].trim();
                if !piece.is_empty() {
                    out.push(piece);
                }
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let tail = sql[start.min(sql.len())..].trim();
    if !tail.is_empty() {
        out.push(tail);
    }
    if out.is_empty() {
        out.push(sql.trim());
    }
    out
}

/// Classify a statement string. A multi-statement string classifies as the
/// strongest of its parts (`Copy` > `Opaque` > `Write` > `Read`); control
/// statements inside a multi-statement string are not modelled individually —
/// the backend's `ReadyForQuery` status and closing tag decide the
/// transaction transition instead, and the string is journaled whole.
pub fn classify(sql: &str) -> (StmtKind, bool) {
    let parts = split_statements(sql);
    if parts.len() <= 1 {
        return (
            classify_single(parts.first().copied().unwrap_or(sql)),
            false,
        );
    }
    let mut kind = StmtKind::Read;
    for p in parts {
        let k = classify_single(p);
        kind = match (kind, k) {
            (StmtKind::Copy, _) | (_, StmtKind::Copy) => StmtKind::Copy,
            (StmtKind::Opaque, _) | (_, StmtKind::Opaque) => StmtKind::Opaque,
            (StmtKind::Write, _) | (_, StmtKind::Write) => StmtKind::Write,
            (StmtKind::Control(c), _) | (_, StmtKind::Control(c)) => StmtKind::Control(c),
            (StmtKind::Read, StmtKind::Read) => StmtKind::Read,
        };
    }
    // A multi-statement string that is only control (e.g. "BEGIN; COMMIT")
    // is handled by the status reconciliation, not per control.
    let kind = match kind {
        StmtKind::Control(_) => StmtKind::Read,
        k => k,
    };
    (kind, true)
}

/// One `CommandComplete`-class completion the backend produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Completion {
    /// `CommandComplete` with this tag.
    Tag(String),
    /// `PortalSuspended` (an `Execute` with a row limit did not finish).
    Suspended,
    /// `EmptyQueryResponse`.
    Empty,
}

/// What the relay observed for one request/response cycle.
#[derive(Debug, Clone, Default)]
pub struct ResponseOutcome {
    /// `ReadyForQuery` status byte (`I` / `T` / `E`).
    pub status: u8,
    /// Completions in the order the backend sent them.
    pub completions: Vec<Completion>,
    /// `(sqlstate, message)` of the first `ErrorResponse`, if any.
    pub error: Option<(String, String)>,
}

impl ResponseOutcome {
    /// A status-only observation (nothing was registered for this cycle).
    pub fn status_only(status: u8) -> Self {
        Self {
            status,
            completions: Vec::new(),
            error: None,
        }
    }

    fn last_tag(&self) -> Option<&str> {
        self.completions.iter().rev().find_map(|c| match c {
            Completion::Tag(t) => Some(t.as_str()),
            _ => None,
        })
    }
}

/// A prepared statement the session parsed (extended protocol).
#[derive(Debug)]
struct Prepared {
    /// Statement text; `None` for a read (never journaled, so never copied).
    sql: Option<Arc<str>>,
    kind: StmtKind,
    multi: bool,
    param_types: Vec<u32>,
}

/// A bound portal awaiting `Execute`.
#[derive(Debug)]
struct Portal {
    prepared: Arc<Prepared>,
    params: Vec<JournalValue>,
    /// An `Execute` has already been issued on this portal in this
    /// transaction: a further `Execute` continues the same statement
    /// (after `PortalSuspended`) and must not be journaled twice.
    executed: bool,
}

/// A statement sent to the backend, awaiting its outcome.
#[derive(Debug)]
struct Pending {
    sql: Arc<str>,
    kind: StmtKind,
    multi: bool,
    params: Vec<JournalValue>,
    param_types: Vec<u32>,
    protocol: WireProtocol,
    /// Index among the `Execute`s of the batch (simple protocol: 0).
    exec_index: usize,
    started: Instant,
    /// The statement exceeded `max_statement_bytes`; its text was not kept.
    oversize: bool,
}

#[derive(Debug)]
struct OpenTx {
    tx_id: Uuid,
    aborted: bool,
}

/// Journal operation produced by [`SessionCapture::observe`].
#[derive(Debug, Clone, PartialEq)]
pub enum JournalOp {
    /// Open an active journal for an explicit transaction.
    Begin { tx_id: Uuid },
    /// Append a completed statement to an active journal.
    Log { tx_id: Uuid, entry: NewEntry },
    /// Record a savepoint.
    Savepoint { tx_id: Uuid, name: String },
    /// Roll the journal back to a savepoint (truncates later entries).
    RollbackTo { tx_id: Uuid, name: String },
    /// Mark the active journal incomplete.
    Incomplete { tx_id: Uuid, reason: String },
    /// The backend committed the transaction with this tag.
    Commit { tx_id: Uuid, tag: String },
    /// The backend rolled the transaction back (or it was lost).
    Rollback { tx_id: Uuid },
    /// An auto-commit statement or an implicit multi-statement transaction
    /// committed: record it as one committed transaction.
    AutoCommit {
        entries: Vec<NewEntry>,
        tag: String,
        incomplete: Option<String>,
    },
}

/// Per-session capture state.
#[derive(Debug)]
pub struct SessionCapture {
    prev_status: u8,
    open: Option<OpenTx>,
    prepared: HashMap<String, Arc<Prepared>>,
    unnamed: Option<Arc<Prepared>>,
    portals: HashMap<String, Portal>,
    pending: Vec<Pending>,
    execs: usize,
    max_statement_bytes: usize,
    /// Operations produced by a synchronous `discard` (a synthesized
    /// `ReadyForQuery`), emitted ahead of the next `observe`.
    deferred: Vec<JournalOp>,
    /// The explicit transaction being journaled for this session, built
    /// here and handed to the shared journal only when it commits (see
    /// [`apply_ops`]).
    active: Option<TransactionJournalEntry>,
}

impl Default for SessionCapture {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

impl NewEntry {
    fn from_pending(p: &Pending, outcome: StatementOutcome) -> Self {
        NewEntry {
            statement: if p.oversize {
                String::new()
            } else {
                p.sql.to_string()
            },
            parameters: p.params.clone(),
            param_types: p.param_types.clone(),
            result_checksum: None,
            rows_affected: outcome.rows_affected(),
            duration_ms: p.started.elapsed().as_millis() as u64,
            outcome,
            protocol: p.protocol,
        }
    }
}

enum Resolved {
    Ok(String),
    Failed(String, String),
    Unresolved,
}

impl SessionCapture {
    /// `max_statement_bytes`: a statement text longer than this is journaled
    /// as an empty statement and marks its transaction incomplete.
    pub fn new(max_statement_bytes: usize) -> Self {
        Self {
            prev_status: b'I',
            open: None,
            prepared: HashMap::new(),
            unnamed: None,
            portals: HashMap::new(),
            pending: Vec::new(),
            execs: 0,
            max_statement_bytes: max_statement_bytes.max(1),
            deferred: Vec::new(),
            active: None,
        }
    }

    /// Whether an explicit transaction is being captured.
    pub fn in_transaction(&self) -> bool {
        self.open.is_some()
    }

    /// Whether anything registered in this cycle needs its outcome.
    pub fn armed(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Start a request/response cycle (a simple query, or an extended batch
    /// up to its `Sync`). Any stale registration is dropped: the client loop
    /// is strictly request → response per session, so a leftover means the
    /// previous cycle's response was synthesized without observing it.
    pub fn begin_cycle(&mut self) {
        self.pending.clear();
        self.execs = 0;
    }

    /// Register a simple-query string. Returns `true` when its outcome must
    /// be observed (it is a write, a COPY, an EXECUTE or transaction control).
    pub fn register_simple(&mut self, sql: &str) -> bool {
        let (kind, multi) = classify(sql);
        if kind == StmtKind::Read {
            return false;
        }
        let oversize = sql.len() > self.max_statement_bytes;
        self.pending.push(Pending {
            sql: if oversize {
                Arc::from("")
            } else {
                Arc::from(sql)
            },
            kind,
            multi,
            params: Vec::new(),
            param_types: Vec::new(),
            protocol: WireProtocol::Simple,
            exec_index: 0,
            started: Instant::now(),
            oversize,
        });
        true
    }

    /// Register a `Parse` (`name`, statement text, declared parameter OIDs).
    pub fn note_parse(&mut self, name: &str, sql: &str, param_types: Vec<u32>) {
        let (kind, multi) = classify(sql);
        let prepared = Arc::new(Prepared {
            sql: if kind == StmtKind::Read {
                None
            } else if sql.len() > self.max_statement_bytes {
                Some(Arc::from(""))
            } else {
                Some(Arc::from(sql))
            },
            kind,
            multi,
            param_types,
        });
        if name.is_empty() {
            self.unnamed = Some(prepared);
        } else {
            self.prepared.insert(name.to_string(), prepared);
        }
    }

    /// Register a `Bind` from its raw payload. Parameter values of a
    /// journaled statement are copied byte for byte with their format.
    pub fn note_bind(&mut self, payload: &[u8]) {
        let Some((portal, stmt, rest)) = bind_head(payload) else {
            return;
        };
        let prepared = if stmt.is_empty() {
            self.unnamed.clone()
        } else {
            self.prepared.get(stmt).cloned()
        };
        let Some(prepared) = prepared else {
            self.portals.remove(portal);
            return;
        };
        if !prepared.kind.is_journaled() && !matches!(prepared.kind, StmtKind::Control(_)) {
            self.portals.remove(portal);
            return;
        }
        let params = bind_params(rest).unwrap_or_default();
        self.portals.insert(
            portal.to_string(),
            Portal {
                prepared,
                params,
                executed: false,
            },
        );
    }

    /// Register an `Execute` of `portal`. Returns `true` when its outcome
    /// must be observed.
    pub fn note_execute(&mut self, portal: &str) -> bool {
        let exec_index = self.execs;
        self.execs += 1;
        let Some(p) = self.portals.get_mut(portal) else {
            return false;
        };
        if p.executed {
            // Continuation of a suspended portal: same statement, not a new one.
            return false;
        }
        p.executed = true;
        let prepared = p.prepared.clone();
        let sql = prepared.sql.clone().unwrap_or_else(|| Arc::from(""));
        let oversize = prepared.sql.as_deref() == Some("") && prepared.kind.is_journaled();
        self.pending.push(Pending {
            sql,
            kind: prepared.kind.clone(),
            multi: prepared.multi,
            params: std::mem::take(&mut p.params),
            param_types: prepared.param_types.clone(),
            protocol: WireProtocol::Extended,
            exec_index,
            started: Instant::now(),
            oversize,
        });
        true
    }

    /// Register a `Close` (`'S'` statement / `'P'` portal).
    pub fn note_close(&mut self, payload: &[u8]) {
        let Some(&kind) = payload.first() else {
            return;
        };
        let rest = &payload[1..];
        let end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
        let name = std::str::from_utf8(&rest[..end]).unwrap_or("");
        match kind {
            b'S' => {
                if name.is_empty() {
                    self.unnamed = None;
                } else {
                    self.prepared.remove(name);
                }
            }
            b'P' => {
                self.portals.remove(name);
            }
            _ => {}
        }
    }

    fn resolve(p: &Pending, outcome: &ResponseOutcome) -> Resolved {
        match p.protocol {
            WireProtocol::Simple => match &outcome.error {
                Some((code, msg)) => Resolved::Failed(code.clone(), msg.clone()),
                None => Resolved::Ok(outcome.last_tag().unwrap_or("").to_string()),
            },
            WireProtocol::Extended => match outcome.completions.get(p.exec_index) {
                Some(Completion::Tag(t)) => Resolved::Ok(t.clone()),
                Some(Completion::Suspended) => Resolved::Ok("SUSPENDED".to_string()),
                Some(Completion::Empty) => Resolved::Unresolved,
                None => match &outcome.error {
                    Some((code, msg)) if p.exec_index == outcome.completions.len() => {
                        Resolved::Failed(code.clone(), msg.clone())
                    }
                    _ => Resolved::Unresolved,
                },
            },
        }
    }

    /// Reconcile the registered statements with the backend's response and
    /// return the journal operations to apply. Also clears the cycle.
    pub fn observe(&mut self, outcome: &ResponseOutcome) -> Vec<JournalOp> {
        let mut ops = std::mem::take(&mut self.deferred);
        let pending = std::mem::take(&mut self.pending);
        self.execs = 0;
        let mut implicit: Vec<NewEntry> = Vec::new();
        let mut implicit_incomplete: Option<String> = None;

        for p in &pending {
            match Self::resolve(p, outcome) {
                Resolved::Unresolved => {}
                Resolved::Failed(code, msg) => {
                    if let Some(o) = self.open.as_mut() {
                        o.aborted = true;
                        // A failed multi-statement string may have executed
                        // earlier sub-statements that stay in effect after a
                        // later ROLLBACK TO SAVEPOINT; they were not journaled.
                        if p.multi && !outcome.completions.is_empty() {
                            ops.push(JournalOp::Incomplete {
                                tx_id: o.tx_id,
                                reason: format!(
                                    "multi-statement string failed after {} sub-statement(s) \
                                     ({}: {})",
                                    outcome.completions.len(),
                                    code,
                                    msg
                                ),
                            });
                        }
                    }
                }
                Resolved::Ok(tag) => match &p.kind {
                    StmtKind::Read => {}
                    StmtKind::Control(c) => self.apply_control(c, &tag, &mut ops, &mut implicit),
                    StmtKind::Write | StmtKind::Copy | StmtKind::Opaque => {
                        let entry = NewEntry::from_pending(p, StatementOutcome::Succeeded { tag });
                        let reason = if p.oversize {
                            Some("statement text exceeds journal.max_statement_bytes".to_string())
                        } else {
                            p.kind.incomplete_reason().map(str::to_string)
                        };
                        if let Some(o) = self.open.as_ref() {
                            ops.push(JournalOp::Log {
                                tx_id: o.tx_id,
                                entry,
                            });
                            if let Some(r) = reason {
                                ops.push(JournalOp::Incomplete {
                                    tx_id: o.tx_id,
                                    reason: r,
                                });
                            }
                        } else {
                            implicit.push(entry);
                            if implicit_incomplete.is_none() {
                                implicit_incomplete = reason;
                            }
                        }
                    }
                },
            }
        }

        // Reconcile with what the backend says the session state is.
        match outcome.status {
            b'I' => {
                if let Some(o) = self.open.take() {
                    let committed = outcome.error.is_none()
                        && outcome
                            .last_tag()
                            .map(|t| t.eq_ignore_ascii_case("COMMIT"))
                            .unwrap_or(false);
                    if committed {
                        ops.push(JournalOp::Commit {
                            tx_id: o.tx_id,
                            tag: "COMMIT".to_string(),
                        });
                    } else {
                        ops.push(JournalOp::Rollback { tx_id: o.tx_id });
                    }
                }
                if !implicit.is_empty() && outcome.error.is_none() {
                    let tag = outcome.last_tag().unwrap_or("").to_string();
                    ops.push(JournalOp::AutoCommit {
                        entries: implicit,
                        tag,
                        incomplete: implicit_incomplete,
                    });
                }
                self.portals.clear();
            }
            b'T' => {
                if self.open.is_none() {
                    let tx_id = next_auto_commit_tx_id();
                    self.open = Some(OpenTx {
                        tx_id,
                        aborted: false,
                    });
                    ops.push(JournalOp::Begin { tx_id });
                    for entry in implicit.drain(..) {
                        ops.push(JournalOp::Log { tx_id, entry });
                    }
                    if let Some(r) = implicit_incomplete.take() {
                        ops.push(JournalOp::Incomplete { tx_id, reason: r });
                    }
                } else if !implicit.is_empty() {
                    // Cannot happen (writes go to the open tx), kept for safety.
                    let tx_id = self.open.as_ref().map(|o| o.tx_id).unwrap_or_default();
                    for entry in implicit.drain(..) {
                        ops.push(JournalOp::Log { tx_id, entry });
                    }
                }
            }
            b'E' => {
                match self.open.as_mut() {
                    Some(o) => o.aborted = true,
                    None => {
                        // The backend is in a failed transaction we never saw
                        // open: track it so its eventual end is observed.
                        let tx_id = next_auto_commit_tx_id();
                        self.open = Some(OpenTx {
                            tx_id,
                            aborted: true,
                        });
                        ops.push(JournalOp::Begin { tx_id });
                    }
                }
            }
            _ => {}
        }
        self.prev_status = outcome.status;
        ops
    }

    fn apply_control(
        &mut self,
        c: &Control,
        tag: &str,
        ops: &mut Vec<JournalOp>,
        implicit: &mut Vec<NewEntry>,
    ) {
        match c {
            Control::Begin => {
                if self.open.is_none() {
                    let tx_id = next_auto_commit_tx_id();
                    self.open = Some(OpenTx {
                        tx_id,
                        aborted: false,
                    });
                    ops.push(JournalOp::Begin { tx_id });
                    // Statements that preceded BEGIN in the same implicit
                    // transaction block become part of the explicit one.
                    for entry in implicit.drain(..) {
                        ops.push(JournalOp::Log { tx_id, entry });
                    }
                }
            }
            Control::Commit => {
                if let Some(o) = self.open.take() {
                    if tag.eq_ignore_ascii_case("COMMIT") && !o.aborted {
                        ops.push(JournalOp::Commit {
                            tx_id: o.tx_id,
                            tag: tag.to_string(),
                        });
                    } else {
                        ops.push(JournalOp::Rollback { tx_id: o.tx_id });
                    }
                }
            }
            Control::Rollback | Control::PrepareTransaction => {
                if let Some(o) = self.open.take() {
                    ops.push(JournalOp::Rollback { tx_id: o.tx_id });
                }
            }
            Control::Savepoint(name) => {
                if let Some(o) = self.open.as_ref() {
                    ops.push(JournalOp::Savepoint {
                        tx_id: o.tx_id,
                        name: name.clone(),
                    });
                }
            }
            Control::RollbackTo(name) => {
                if let Some(o) = self.open.as_mut() {
                    o.aborted = false;
                    ops.push(JournalOp::RollbackTo {
                        tx_id: o.tx_id,
                        name: name.clone(),
                    });
                }
            }
            Control::Release(_) | Control::Passive => {}
        }
    }

    /// The proxy synthesized a `ReadyForQuery` (aborted-transaction
    /// emulation, failover error path): nothing registered in this cycle
    /// ran. Synchronous; a resulting rollback is deferred to the next
    /// `observe`/`close` (it only removes an active journal, so delaying it
    /// is harmless).
    pub fn discard(&mut self, status: u8) {
        self.pending.clear();
        self.execs = 0;
        self.prev_status = status;
        if status == b'I' {
            self.portals.clear();
            if let Some(o) = self.open.take() {
                self.deferred.push(JournalOp::Rollback { tx_id: o.tx_id });
            }
        }
    }

    /// Whether `discard` left operations waiting for the next `observe`.
    pub fn has_deferred(&self) -> bool {
        !self.deferred.is_empty()
    }

    /// Operations deferred by `discard`, for a caller that wants to apply
    /// them without waiting for the next response.
    pub fn take_deferred(&mut self) -> Vec<JournalOp> {
        std::mem::take(&mut self.deferred)
    }

    /// The session ended: an open transaction is rolled back by the backend.
    /// The session-local transaction journal, for [`apply_ops`].
    pub fn active_mut(&mut self) -> &mut Option<TransactionJournalEntry> {
        &mut self.active
    }

    pub fn close(&mut self) -> Vec<JournalOp> {
        self.pending.clear();
        self.portals.clear();
        let mut ops = std::mem::take(&mut self.deferred);
        if let Some(o) = self.open.take() {
            ops.push(JournalOp::Rollback { tx_id: o.tx_id });
        }
        ops
    }
}

/// `(portal, statement, rest)` of a `Bind` payload.
fn bind_head(payload: &[u8]) -> Option<(&str, &str, &[u8])> {
    let p_end = payload.iter().position(|&b| b == 0)?;
    let portal = std::str::from_utf8(&payload[..p_end]).ok()?;
    let rest = &payload[p_end + 1..];
    let s_end = rest.iter().position(|&b| b == 0)?;
    let stmt = std::str::from_utf8(&rest[..s_end]).ok()?;
    Some((portal, stmt, &rest[s_end + 1..]))
}

/// Parameter values of a `Bind` payload after the two names, as
/// `JournalValue`s carrying their wire format.
fn bind_params(mut b: &[u8]) -> Option<Vec<JournalValue>> {
    fn u16_at(b: &mut &[u8]) -> Option<u16> {
        if b.len() < 2 {
            return None;
        }
        let v = u16::from_be_bytes([b[0], b[1]]);
        *b = &b[2..];
        Some(v)
    }
    fn i32_at(b: &mut &[u8]) -> Option<i32> {
        if b.len() < 4 {
            return None;
        }
        let v = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        *b = &b[4..];
        Some(v)
    }
    let nf = u16_at(&mut b)? as usize;
    let mut formats = Vec::with_capacity(nf);
    for _ in 0..nf {
        formats.push(u16_at(&mut b)? as i16);
    }
    let nv = u16_at(&mut b)? as usize;
    let mut out = Vec::with_capacity(nv);
    for i in 0..nv {
        let len = i32_at(&mut b)?;
        let format = match formats.len() {
            0 => 0,
            1 => formats[0],
            _ => *formats.get(i)?,
        };
        if len < 0 {
            out.push(JournalValue::Null);
            continue;
        }
        let len = len as usize;
        if b.len() < len {
            return None;
        }
        let bytes = &b[..len];
        b = &b[len..];
        out.push(if format == 1 {
            JournalValue::Binary(bytes.to_vec())
        } else {
            match std::str::from_utf8(bytes) {
                Ok(s) => JournalValue::Text(s.to_string()),
                Err(_) => JournalValue::TextRaw(bytes.to_vec()),
            }
        });
    }
    Some(out)
}

/// A stable `NodeId` for a backend address, so journals of one backend share
/// an id across sessions (the failover controller selects journals by node).
pub fn node_id_for_backend(addr: &str) -> NodeId {
    use std::hash::{Hash, Hasher};
    let mut h1 = std::collections::hash_map::DefaultHasher::new();
    addr.hash(&mut h1);
    let mut h2 = std::collections::hash_map::DefaultHasher::new();
    (addr, 0x9e37_79b9u32).hash(&mut h2);
    let raw = ((h1.finish() as u128) << 64) | h2.finish() as u128;
    // Stamp RFC 4122 version 4 / variant bits so it renders like every other id.
    let v = (raw & !(0xf000u128 << 64) & !(0xc000_0000_0000_0000u128))
        | (0x4000u128 << 64)
        | 0x8000_0000_0000_0000u128;
    NodeId(Uuid::from_u128(v))
}

/// Counters returned by [`apply_ops`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Applied {
    pub committed: u64,
    pub rolled_back: u64,
    pub statements: u64,
}

/// Apply capture operations for one session.
///
/// An explicit transaction is built in the session's own `local` slot
/// (`SessionCapture::active_mut`) — begin, every logged statement, savepoints
/// and incomplete marks touch nothing shared — and reaches the journal's
/// committed store exactly once, when the backend reports the commit.
/// Auto-commit statements go straight to the committed store. Nothing here
/// awaits: the journal's commit path is a short sync mutex, so the caller
/// runs this under the session's capture lock. (Taking the shared async
/// journal lock per statement parked every writer behind the scheduler's
/// wake-up latency under concurrency: −40 % committed TPS at 16 clients in
/// the P-01 harness, 2026-09-21.)
pub fn apply_ops(
    journal: &TransactionJournal,
    ops: Vec<JournalOp>,
    session_id: Uuid,
    node_id: NodeId,
    source: &SourceIdentity,
    local: &mut Option<TransactionJournalEntry>,
) -> Applied {
    let mut applied = Applied::default();
    for op in ops {
        match op {
            JournalOp::Begin { tx_id } => {
                if local.take().is_some() {
                    // Cannot happen (the capture closes one transaction before
                    // opening the next); treat a leftover as rolled back.
                    journal.local_end();
                    journal.note_rollback();
                }
                *local = Some(
                    TransactionJournalEntry::new(tx_id, session_id, node_id, 0)
                        .with_source(source.clone()),
                );
                journal.local_begin();
            }
            JournalOp::Log { tx_id, entry } => {
                applied.statements += 1;
                if let Some(tx) = local.as_mut().filter(|t| t.tx_id == tx_id) {
                    if let Err(e) = journal.append_entry(tx, entry) {
                        tx.mark_incomplete(format!("journal cap: {}", e));
                    }
                }
            }
            JournalOp::Savepoint { tx_id, name } => {
                if let Some(tx) = local.as_mut().filter(|t| t.tx_id == tx_id) {
                    tx.create_savepoint(name);
                }
            }
            JournalOp::RollbackTo { tx_id, name } => {
                if let Some(tx) = local.as_mut().filter(|t| t.tx_id == tx_id) {
                    if tx.rollback_to_savepoint(&name).is_none() {
                        tx.mark_incomplete(format!(
                            "ROLLBACK TO unknown journal savepoint {:?}",
                            name
                        ));
                    }
                }
            }
            JournalOp::Incomplete { tx_id, reason } => {
                if let Some(tx) = local.as_mut().filter(|t| t.tx_id == tx_id) {
                    tx.mark_incomplete(reason);
                }
            }
            JournalOp::Commit { tx_id, tag } => {
                if let Some(tx) = local.take_if(|t| t.tx_id == tx_id) {
                    journal.local_end();
                    if journal.record_committed_sync(tx, &tag).is_some() {
                        applied.committed += 1;
                    }
                }
            }
            JournalOp::Rollback { tx_id } => {
                if local.take_if(|t| t.tx_id == tx_id).is_some() {
                    journal.local_end();
                    journal.note_rollback();
                }
                applied.rolled_back += 1;
            }
            JournalOp::AutoCommit {
                entries,
                tag,
                incomplete,
            } => {
                let mut tx =
                    TransactionJournalEntry::new(next_auto_commit_tx_id(), session_id, node_id, 0)
                        .with_source(source.clone());
                let mut seq = 0u64;
                for e in entries {
                    seq += 1;
                    applied.statements += 1;
                    tx.add_entry(e.into_journal_entry(seq));
                }
                if let Some(r) = incomplete {
                    tx.mark_incomplete(r);
                }
                if journal.record_committed_sync(tx, &tag).is_some() {
                    applied.committed += 1;
                }
            }
        }
    }
    applied
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_journal::StatementType;

    fn ok(status: u8, tags: &[&str]) -> ResponseOutcome {
        ResponseOutcome {
            status,
            completions: tags
                .iter()
                .map(|t| Completion::Tag(t.to_string()))
                .collect(),
            error: None,
        }
    }
    fn failed(status: u8, done: &[&str]) -> ResponseOutcome {
        ResponseOutcome {
            status,
            completions: done
                .iter()
                .map(|t| Completion::Tag(t.to_string()))
                .collect(),
            error: Some(("23505".into(), "duplicate key".into())),
        }
    }
    fn simple(c: &mut SessionCapture, sql: &str, o: ResponseOutcome) -> Vec<JournalOp> {
        c.begin_cycle();
        c.register_simple(sql);
        c.observe(&o)
    }

    #[test]
    fn classify_covers_control_and_writes() {
        use Control::*;
        assert_eq!(classify("BEGIN").0, StmtKind::Control(Begin));
        assert_eq!(classify("start transaction").0, StmtKind::Control(Begin));
        assert_eq!(classify("COMMIT").0, StmtKind::Control(Commit));
        assert_eq!(classify("end;").0, StmtKind::Control(Commit));
        assert_eq!(
            classify("COMMIT PREPARED 'x'").0,
            StmtKind::Control(Passive)
        );
        assert_eq!(classify("ROLLBACK").0, StmtKind::Control(Rollback));
        assert_eq!(classify("abort").0, StmtKind::Control(Rollback));
        assert_eq!(
            classify("ROLLBACK TO SAVEPOINT s1").0,
            StmtKind::Control(RollbackTo("s1".into()))
        );
        assert_eq!(
            classify("rollback work to \"S 2\"").0,
            StmtKind::Control(RollbackTo("S 2".into()))
        );
        assert_eq!(
            classify("ROLLBACK TO s3").0,
            StmtKind::Control(RollbackTo("s3".into()))
        );
        assert_eq!(
            classify("SAVEPOINT sp").0,
            StmtKind::Control(Savepoint("sp".into()))
        );
        assert_eq!(
            classify("RELEASE SAVEPOINT sp").0,
            StmtKind::Control(Release("sp".into()))
        );
        assert_eq!(
            classify("PREPARE TRANSACTION 'g'").0,
            StmtKind::Control(PrepareTransaction)
        );
        assert_eq!(
            classify("PREPARE p AS INSERT INTO t VALUES ($1)").0,
            StmtKind::Read
        );
        assert_eq!(classify("EXECUTE p(1)").0, StmtKind::Opaque);
        assert_eq!(classify("COPY t FROM STDIN").0, StmtKind::Copy);
        assert_eq!(classify("COPY t TO STDOUT").0, StmtKind::Read);
        assert_eq!(
            classify("/* c */ -- x\n insert into t values (1)").0,
            StmtKind::Write
        );
        assert_eq!(
            classify("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x").0,
            StmtKind::Write
        );
        assert_eq!(
            classify("WITH x AS (SELECT 1) SELECT * FROM x").0,
            StmtKind::Read
        );
        assert_eq!(classify("SELECT nextval('s')").0, StmtKind::Read);
        assert_eq!(classify("SET search_path = a").0, StmtKind::Read);
        assert_eq!(classify("VACUUM t").0, StmtKind::Read);
        assert_eq!(classify("CALL p()").0, StmtKind::Write);
        assert_eq!(classify("").0, StmtKind::Read);
    }

    #[test]
    fn split_statements_respects_quotes_and_dollar_strings() {
        assert_eq!(split_statements("select 1"), vec!["select 1"]);
        assert_eq!(
            split_statements("begin; insert into t values ('a;b'); commit;"),
            vec!["begin", "insert into t values ('a;b')", "commit"]
        );
        assert_eq!(
            split_statements("do $$ begin perform 1; end $$; select 2"),
            vec!["do $$ begin perform 1; end $$", "select 2"]
        );
        assert_eq!(
            split_statements("select 1 -- ; not\n; select 2 /* ; */"),
            vec!["select 1 -- ; not", "select 2 /* ; */"]
        );
        assert_eq!(
            classify("begin; insert into t values (1); commit"),
            (StmtKind::Write, true)
        );
        assert_eq!(classify("begin; commit"), (StmtKind::Read, true));
        assert_eq!(classify("select 1; select 2"), (StmtKind::Read, true));
    }

    #[test]
    fn auto_commit_write_is_one_committed_transaction() {
        let mut c = SessionCapture::default();
        let ops = simple(
            &mut c,
            "insert into t values (1)",
            ok(b'I', &["INSERT 0 1"]),
        );
        match &ops[..] {
            [JournalOp::AutoCommit {
                entries,
                tag,
                incomplete,
            }] => {
                assert_eq!(entries.len(), 1);
                assert_eq!(tag, "INSERT 0 1");
                assert!(incomplete.is_none());
                assert_eq!(entries[0].rows_affected, Some(1));
                assert_eq!(
                    entries[0].outcome,
                    StatementOutcome::Succeeded {
                        tag: "INSERT 0 1".into()
                    }
                );
            }
            other => panic!("unexpected ops {:?}", other),
        }
        assert!(!c.in_transaction());
    }

    #[test]
    fn reads_are_not_registered_and_failed_auto_commit_is_dropped() {
        let mut c = SessionCapture::default();
        c.begin_cycle();
        assert!(!c.register_simple("select * from t"));
        assert!(c.observe(&ok(b'I', &["SELECT 3"])).is_empty());
        assert!(simple(&mut c, "insert into t values (1)", failed(b'I', &[])).is_empty());
    }

    #[test]
    fn explicit_transaction_commit_and_rollback() {
        let mut c = SessionCapture::default();
        let b = simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"]));
        let tx = match &b[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        assert!(c.in_transaction());
        let l = simple(&mut c, "update t set a = 1", ok(b'T', &["UPDATE 2"]));
        assert!(matches!(&l[..], [JournalOp::Log { tx_id, .. }] if *tx_id == tx));
        // Reads inside the transaction produce nothing.
        c.begin_cycle();
        assert!(!c.register_simple("select 1"));
        assert!(c.observe(&ok(b'T', &["SELECT 1"])).is_empty());
        let cm = simple(&mut c, "COMMIT", ok(b'I', &["COMMIT"]));
        assert_eq!(
            cm,
            vec![JournalOp::Commit {
                tx_id: tx,
                tag: "COMMIT".into()
            }]
        );
        assert!(!c.in_transaction());

        let b = simple(&mut c, "begin", ok(b'T', &["BEGIN"]));
        let tx = match &b[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        simple(&mut c, "delete from t", ok(b'T', &["DELETE 1"]));
        let rb = simple(&mut c, "rollback", ok(b'I', &["ROLLBACK"]));
        assert_eq!(rb, vec![JournalOp::Rollback { tx_id: tx }]);
    }

    #[test]
    fn failed_statement_aborts_and_commit_becomes_rollback() {
        let mut c = SessionCapture::default();
        let tx = match &simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"]))[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        simple(
            &mut c,
            "insert into t values (1)",
            ok(b'T', &["INSERT 0 1"]),
        );
        let f = simple(&mut c, "insert into t values (1)", failed(b'E', &[]));
        assert!(f.is_empty(), "a failed statement is not journaled: {:?}", f);
        // PostgreSQL answers ROLLBACK to a COMMIT in an aborted transaction.
        let cm = simple(&mut c, "COMMIT", ok(b'I', &["ROLLBACK"]));
        assert_eq!(cm, vec![JournalOp::Rollback { tx_id: tx }]);
    }

    #[test]
    fn savepoint_rollback_truncates_and_transaction_still_commits() {
        let mut c = SessionCapture::default();
        let tx = match &simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"]))[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        simple(
            &mut c,
            "insert into t values ('a')",
            ok(b'T', &["INSERT 0 1"]),
        );
        let sp = simple(&mut c, "SAVEPOINT s", ok(b'T', &["SAVEPOINT"]));
        assert_eq!(
            sp,
            vec![JournalOp::Savepoint {
                tx_id: tx,
                name: "s".into()
            }]
        );
        assert!(simple(&mut c, "insert into t values ('b')", failed(b'E', &[])).is_empty());
        let rt = simple(&mut c, "ROLLBACK TO SAVEPOINT s", ok(b'T', &["ROLLBACK"]));
        assert_eq!(
            rt,
            vec![JournalOp::RollbackTo {
                tx_id: tx,
                name: "s".into()
            }]
        );
        simple(
            &mut c,
            "insert into t values ('c')",
            ok(b'T', &["INSERT 0 1"]),
        );
        let cm = simple(&mut c, "COMMIT", ok(b'I', &["COMMIT"]));
        assert!(matches!(&cm[..], [JournalOp::Commit { tx_id, .. }] if *tx_id == tx));
    }

    #[test]
    fn multi_statement_strings_use_status_and_closing_tag() {
        let mut c = SessionCapture::default();
        // BEGIN; INSERT; COMMIT in one string from idle → one committed tx.
        let ops = simple(
            &mut c,
            "begin; insert into t values (1); commit",
            ok(b'I', &["BEGIN", "INSERT 0 1", "COMMIT"]),
        );
        assert!(
            matches!(&ops[..], [JournalOp::AutoCommit { entries, tag, .. }] if entries.len() == 1 && tag == "COMMIT")
        );
        // BEGIN; INSERT (left open) → Begin + Log via status reconciliation.
        let ops = simple(
            &mut c,
            "begin; insert into t values (2)",
            ok(b'T', &["BEGIN", "INSERT 0 1"]),
        );
        let tx = match &ops[..] {
            [JournalOp::Begin { tx_id }, JournalOp::Log { tx_id: t2, .. }] if tx_id == t2 => *tx_id,
            o => panic!("{:?}", o),
        };
        // INSERT; COMMIT closes it: status I + last tag COMMIT.
        let ops = simple(
            &mut c,
            "insert into t values (3); commit",
            ok(b'I', &["INSERT 0 1", "COMMIT"]),
        );
        assert!(
            matches!(&ops[..], [JournalOp::Log { .. }, JournalOp::Commit { tx_id, .. }] if *tx_id == tx)
        );
        // A failing string from idle commits nothing.
        assert!(simple(
            &mut c,
            "insert into t values (4); insert into t values (4)",
            failed(b'I', &["INSERT 0 1"])
        )
        .is_empty());
    }

    #[test]
    fn extended_batch_maps_completions_to_executes() {
        let mut c = SessionCapture::default();
        c.begin_cycle();
        c.note_parse("", "select $1::int", vec![23]);
        c.note_bind(&bind_payload("", "", &[0], &[Some(b"1")]));
        assert!(!c.note_execute(""), "read execute is not armed");
        c.note_parse("w", "insert into t values ($1, $2)", vec![23, 0]);
        c.note_bind(&bind_payload(
            "p1",
            "w",
            &[0, 1],
            &[Some(b"7"), Some(b"\x00\x00\x00\x01")],
        ));
        assert!(c.note_execute("p1"));
        let ops = c.observe(&ok(b'I', &["SELECT 1", "INSERT 0 1"]));
        match &ops[..] {
            [JournalOp::AutoCommit { entries, .. }] => {
                let e = &entries[0];
                assert_eq!(e.statement, "insert into t values ($1, $2)");
                assert_eq!(e.param_types, vec![23, 0]);
                assert_eq!(
                    e.parameters,
                    vec![
                        JournalValue::Text("7".into()),
                        JournalValue::Binary(vec![0, 0, 0, 1])
                    ]
                );
                assert_eq!(e.protocol, WireProtocol::Extended);
                assert_eq!(
                    e.outcome,
                    StatementOutcome::Succeeded {
                        tag: "INSERT 0 1".into()
                    }
                );
            }
            o => panic!("{:?}", o),
        }
    }

    #[test]
    fn extended_error_mid_batch_journals_nothing_after_it() {
        // Idle: the implicit transaction is rolled back entirely.
        let mut c = SessionCapture::default();
        c.begin_cycle();
        c.note_parse("", "insert into t values ($1)", vec![]);
        c.note_bind(&bind_payload("", "", &[], &[Some(b"1")]));
        c.note_execute("");
        c.note_bind(&bind_payload("", "", &[], &[Some(b"1")]));
        c.note_execute("");
        let ops = c.observe(&failed(b'I', &["INSERT 0 1"]));
        assert!(ops.is_empty(), "{:?}", ops);

        // Inside an explicit transaction: the first is logged, the second
        // failed, the third never ran.
        let tx = match &simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"]))[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        c.begin_cycle();
        c.note_parse("", "insert into t values ($1)", vec![]);
        for v in [b"1", b"2", b"3"] {
            c.note_bind(&bind_payload("", "", &[], &[Some(v)]));
            c.note_execute("");
        }
        let ops = c.observe(&failed(b'E', &["INSERT 0 1"]));
        assert_eq!(ops.len(), 1);
        assert!(
            matches!(&ops[0], JournalOp::Log { tx_id: t, entry } if *t == tx && entry.parameters == vec![JournalValue::Text("1".into())])
        );
        let cm = simple(&mut c, "COMMIT", ok(b'I', &["ROLLBACK"]));
        assert_eq!(cm, vec![JournalOp::Rollback { tx_id: tx }]);
    }

    #[test]
    fn suspended_portal_continuation_is_not_journaled_twice() {
        let mut c = SessionCapture::default();
        let tx = match &simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"]))[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        c.begin_cycle();
        c.note_parse(
            "",
            "insert into t select generate_series(1,10) returning *",
            vec![],
        );
        c.note_bind(&bind_payload("cur", "", &[], &[]));
        assert!(c.note_execute("cur"));
        let ops = c.observe(&ResponseOutcome {
            status: b'T',
            completions: vec![Completion::Suspended],
            error: None,
        });
        assert!(matches!(&ops[..], [JournalOp::Log { tx_id: t, .. }] if *t == tx));
        c.begin_cycle();
        assert!(!c.note_execute("cur"), "continuation must not re-journal");
        assert!(c.observe(&ok(b'T', &["INSERT 0 10"])).is_empty());
    }

    #[test]
    fn copy_and_execute_mark_incomplete() {
        let mut c = SessionCapture::default();
        let ops = simple(&mut c, "COPY t FROM STDIN", ok(b'I', &["COPY 5"]));
        assert!(matches!(
            &ops[..],
            [JournalOp::AutoCommit {
                incomplete: Some(_),
                ..
            }]
        ));
        let tx = match &simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"]))[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        let ops = simple(&mut c, "EXECUTE p(1)", ok(b'T', &["INSERT 0 1"]));
        assert!(
            matches!(&ops[..], [JournalOp::Log { .. }, JournalOp::Incomplete { tx_id: t, .. }] if *t == tx)
        );
    }

    #[test]
    fn oversize_statement_marks_incomplete() {
        let mut c = SessionCapture::new(16);
        let ops = simple(
            &mut c,
            "insert into t values ('0123456789abcdef')",
            ok(b'I', &["INSERT 0 1"]),
        );
        match &ops[..] {
            [JournalOp::AutoCommit {
                entries,
                incomplete,
                ..
            }] => {
                assert_eq!(entries[0].statement, "");
                assert!(incomplete
                    .as_deref()
                    .unwrap_or("")
                    .contains("max_statement_bytes"));
            }
            o => panic!("{:?}", o),
        }
    }

    #[test]
    fn synthetic_ready_and_close_roll_back_open_transactions() {
        let mut c = SessionCapture::default();
        let tx = match &simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"]))[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        c.begin_cycle();
        c.register_simple("insert into t values (1)");
        c.discard(b'T');
        assert!(c.take_deferred().is_empty(), "still in the transaction");
        assert!(c.in_transaction());
        c.discard(b'I');
        assert!(!c.in_transaction());
        // The deferred rollback rides along with the next observation.
        let ops = c.observe(&ResponseOutcome::status_only(b'I'));
        assert_eq!(ops, vec![JournalOp::Rollback { tx_id: tx }]);
        let tx = match &simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"]))[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        assert_eq!(c.close(), vec![JournalOp::Rollback { tx_id: tx }]);
    }

    #[test]
    fn unseen_transaction_state_is_reconciled_from_status() {
        // A failed transaction the capture never saw open (e.g. opened by a
        // path that did not register) is tracked so its end is observed.
        let mut c = SessionCapture::default();
        c.begin_cycle();
        let ops = c.observe(&ResponseOutcome::status_only(b'E'));
        assert!(matches!(&ops[..], [JournalOp::Begin { .. }]));
        assert!(c.in_transaction());
        let ops = simple(&mut c, "ROLLBACK", ok(b'I', &["ROLLBACK"]));
        assert!(matches!(&ops[..], [JournalOp::Rollback { .. }]));
        assert!(!c.in_transaction());
        // Two-phase: PREPARE TRANSACTION ends tracking without a commit.
        let tx = match &simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"]))[..] {
            [JournalOp::Begin { tx_id }] => *tx_id,
            o => panic!("{:?}", o),
        };
        simple(
            &mut c,
            "insert into t values (1)",
            ok(b'T', &["INSERT 0 1"]),
        );
        let ops = simple(
            &mut c,
            "PREPARE TRANSACTION 'g'",
            ok(b'I', &["PREPARE TRANSACTION"]),
        );
        assert_eq!(ops, vec![JournalOp::Rollback { tx_id: tx }]);
    }

    #[tokio::test]
    async fn apply_ops_produces_committed_history_with_savepoint_truncation() {
        let journal = TransactionJournal::new();
        let mut c = SessionCapture::default();
        let sid = Uuid::new_v4();
        let node = NodeId::new();
        let src = SourceIdentity {
            client_addr: "127.0.0.1:5".into(),
            user: "app".into(),
            database: "db".into(),
            backend: "pg:5432".into(),
            tenant: None,
        };
        let mut all = Vec::new();
        all.extend(simple(&mut c, "BEGIN", ok(b'T', &["BEGIN"])));
        all.extend(simple(
            &mut c,
            "insert into t values ('a')",
            ok(b'T', &["INSERT 0 1"]),
        ));
        all.extend(simple(&mut c, "SAVEPOINT s", ok(b'T', &["SAVEPOINT"])));
        all.extend(simple(
            &mut c,
            "insert into t values ('b')",
            ok(b'T', &["INSERT 0 1"]),
        ));
        all.extend(simple(&mut c, "ROLLBACK TO s", ok(b'T', &["ROLLBACK"])));
        all.extend(simple(
            &mut c,
            "insert into t values ('c')",
            ok(b'T', &["INSERT 0 1"]),
        ));
        all.extend(simple(&mut c, "COMMIT", ok(b'I', &["COMMIT"])));
        let mut local = None;
        let applied = apply_ops(&journal, all, sid, node, &src, &mut local);
        assert_eq!(applied.committed, 1);
        assert_eq!(journal.active_count().await, 0);
        let committed = journal.committed_after(0).await;
        assert_eq!(committed.len(), 1);
        let tx = &committed[0];
        assert_eq!(tx.commit_seq, Some(1));
        assert_eq!(tx.commit_tag.as_deref(), Some("COMMIT"));
        assert_eq!(tx.source, src);
        assert!(tx.incomplete_reason.is_none());
        let stmts: Vec<&str> = tx.entries.iter().map(|e| e.statement.as_str()).collect();
        assert_eq!(
            stmts,
            vec!["insert into t values ('a')", "insert into t values ('c')"]
        );
        assert!(tx
            .entries
            .iter()
            .all(|e| e.statement_type == StatementType::Insert));
        assert!(tx
            .entries
            .iter()
            .all(|e| matches!(e.outcome, StatementOutcome::Succeeded { .. })));

        // An auto-commit write follows in commit order.
        let ops = simple(&mut c, "delete from t", ok(b'I', &["DELETE 2"]));
        let applied = apply_ops(&journal, ops, sid, node, &src, &mut local);
        assert_eq!(applied.committed, 1);
        let committed = journal.committed_after(1).await;
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].commit_seq, Some(2));
        assert_eq!(committed[0].entries[0].rows_affected, Some(2));
        let stats = journal.stats().await;
        assert_eq!(stats.committed_transactions, 2);
        assert_eq!(stats.commit_seq_high, 2);
        assert_eq!(stats.committed_total, 2);
    }

    /// Build a `Bind` payload: portal, statement, param formats, values.
    fn bind_payload(
        portal: &str,
        stmt: &str,
        formats: &[i16],
        values: &[Option<&[u8]>],
    ) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(portal.as_bytes());
        b.push(0);
        b.extend_from_slice(stmt.as_bytes());
        b.push(0);
        b.extend_from_slice(&(formats.len() as u16).to_be_bytes());
        for f in formats {
            b.extend_from_slice(&f.to_be_bytes());
        }
        b.extend_from_slice(&(values.len() as u16).to_be_bytes());
        for v in values {
            match v {
                None => b.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(x) => {
                    b.extend_from_slice(&(x.len() as i32).to_be_bytes());
                    b.extend_from_slice(x);
                }
            }
        }
        b.extend_from_slice(&0u16.to_be_bytes());
        b
    }

    #[test]
    fn node_id_for_backend_is_stable_and_distinct() {
        assert_eq!(node_id_for_backend("a:1"), node_id_for_backend("a:1"));
        assert_ne!(node_id_for_backend("a:1"), node_id_for_backend("a:2"));
        assert_eq!(node_id_for_backend("a:1").0.get_version_num(), 4);
    }

    #[test]
    fn bind_params_decode_formats_and_nulls() {
        let p = bind_payload("", "", &[1], &[Some(b"\x01"), None]);
        let (_, _, rest) = bind_head(&p).unwrap();
        assert_eq!(
            bind_params(rest).unwrap(),
            vec![JournalValue::Binary(vec![1]), JournalValue::Null]
        );
        let p = bind_payload("", "", &[], &[Some(&[0xff, 0xfe])]);
        let (_, _, rest) = bind_head(&p).unwrap();
        assert_eq!(
            bind_params(rest).unwrap(),
            vec![JournalValue::TextRaw(vec![0xff, 0xfe])]
        );
        assert!(bind_params(&[0, 0, 0]).is_none());
    }
}
