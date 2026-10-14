//! Extended-protocol `SET` tracking (Sprinter d3a4c04c28be, 75c0c5eceac3).
//!
//! JDBC, Npgsql and tokio-postgres send a `SET` as its own extended-protocol
//! cycle: Parse, Bind, Execute, Sync. JDBC with autocommit off puts the
//! transaction's `BEGIN` in front of its first statement, in the same cycle,
//! and its `autosave` puts a `SAVEPOINT` there. The cycle's session-level
//! `SET`s and transaction-control statements are read in order, the way
//! PostgreSQL runs them: statements up to a Sync share one transaction unless
//! they control it. Outside an explicit transaction a `SET` applies when that
//! transaction commits; inside one it is deferred to the transaction's end.
//! `COMMIT`, `ROLLBACK`, savepoints and their chained and prepared forms
//! settle deferred settings the way the simple protocol does, whichever
//! protocol deferred them. Offloaded reads mirror the result, and
//! transaction replay restores it.
//!
//! When the cycle failed, the proxy cannot see where. It considers every
//! statement the error could have come from, keeps those that end in the
//! transaction status the ReadyForQuery shows, and uses the outcome they
//! agree on. A `CALL` or `DO` outside a transaction may commit part of the
//! cycle, and either case counts. When they disagree, or the cycle cannot be
//! read statement by statement (a statement the proxy has no record of, an
//! Execute of a portal not bound just before it, a cycle still copying), the
//! settings it may have changed are unknown. Inside a transaction that stays
//! open, that taints the transaction: if it commits, the session's settings
//! become unknown. Otherwise they become unknown at once. A session with
//! unknown settings keeps its reads home, and transaction replay refuses to
//! re-home it rather than restore part of its state.
//!
//! Separately, a cycle after which the proxy's record of the session's
//! prepared statements may no longer match the backend's keeps the
//! session's reads home.

/// What a Bind bound, as far as session settings are concerned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Bound {
    /// A statement the proxy has no record of.
    Unknown,
    /// A session-level `SET`, `RESET` or `DISCARD ALL` (not `SET LOCAL`,
    /// `SET TRANSACTION`, `SET CONSTRAINTS` or another `DISCARD`, whose
    /// effects end with the transaction or are not settings).
    Set(String),
    /// A transaction-control statement.
    Tx(TxVerb, String),
    /// `CALL` or `DO`: outside a transaction block the procedure may commit.
    CommitPoint,
    /// Anything else.
    Other,
}

/// What a transaction-control statement does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TxVerb {
    /// `BEGIN` / `START TRANSACTION`.
    Begin,
    /// `COMMIT` / `END` (a failed transaction's COMMIT rolls back).
    Commit,
    /// `ROLLBACK` / `ABORT`.
    Rollback,
    /// `COMMIT AND CHAIN`: ends the transaction and starts the next.
    CommitChain,
    /// `ROLLBACK AND CHAIN`.
    RollbackChain,
    /// `PREPARE TRANSACTION`.
    Prepare,
    /// `SAVEPOINT`.
    Savepoint,
    /// `RELEASE [SAVEPOINT]`.
    Release,
    /// `ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT]`.
    RollbackTo,
    /// A statement that does not lex: PostgreSQL rejects it.
    Opaque,
}

/// A Flush or Sync boundary of the cycle, with what the response showed.
#[derive(Debug, Clone, Copy)]
pub(super) struct ExtSetBoundary {
    /// A Sync (else a Flush).
    pub(super) sync: bool,
    /// The boundary's batch reached a backend.
    pub(super) forwarded: bool,
    /// The transaction status (`I`, `T` or `E`) before the cycle, and in
    /// its ReadyForQuery (Sync only).
    pub(super) before: u8,
    pub(super) after: u8,
    /// The response carried an error (Sync only). An error answering a
    /// Flush never reaches the Sync's response.
    pub(super) failed: bool,
    /// The cycle is still copying: its ReadyForQuery has not arrived, so
    /// `after` is stale (Sync only).
    pub(super) copying: bool,
}

/// What a cycle boundary means for the session.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ExtSetOutcome {
    /// The proxy's record of the session's prepared statements may no
    /// longer match the backend's: keep its reads home.
    pub(super) pin: bool,
    /// What the cycle did to the session's settings, in order.
    pub(super) ops: Vec<SettingsOp>,
}

/// One step of what a cycle did to the session's settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SettingsOp {
    /// A SET-like statement committed outside an explicit transaction.
    Apply(String),
    /// A SET-like statement ran inside the open transaction: apply it if
    /// the transaction commits.
    Defer(String),
    /// A `SAVEPOINT`, `RELEASE` or `ROLLBACK TO` ran.
    Savepoint(String),
    /// The open transaction ended: committed, rolled back, or `None` when
    /// the proxy cannot tell.
    End(Option<bool>),
    /// Inside the transaction, still open: settings (`settings`) or
    /// savepoints (`savepoints`) may have changed unseen.
    Taint { settings: bool, savepoints: bool },
    /// Settings may have changed unseen, outside a transaction or across
    /// its end.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    Set(String),
    Tx(TxVerb, String),
    CommitPoint,
    /// One or more consecutive other statements.
    Other,
}

/// The statements a cycle remembers (consecutive other statements count
/// once); a cycle with more is one the proxy does not read statement by
/// statement. It bounds the work and memory per cycle; past it the proxy is
/// only more conservative.
const MAX_STEPS: usize = 16;

/// `CALL`s and `DO`s a cycle is read with: each may or may not commit, so
/// the outcomes considered double with each one.
const MAX_COMMIT_POINTS: usize = 4;

/// One extended-protocol cycle (Sync to Sync), as far as settings are
/// concerned.
#[derive(Debug, Default)]
pub(super) struct ExtSetCycle {
    binds: u32,
    /// The cycle's statements in Bind order (the first `MAX_STEPS`).
    steps: Vec<Step>,
    /// It bound a SET-like, transaction-control, `CALL` or `DO` statement.
    relevant: bool,
    /// It bound more than `MAX_STEPS` statements.
    overflow: bool,
    /// It bound a statement the proxy has no record of.
    unknown: bool,
    /// The portal of the last Bind, and whether that Bind still awaits its
    /// Execute.
    portal: Vec<u8>,
    awaiting_execute: bool,
    /// An Execute that did not run, once, the portal bound just before it.
    unpaired: bool,
    /// The cycle Parsed a named statement that is SET-like or replaced one:
    /// if the Parse failed (the backend keeps the old definition) or never
    /// reached the backend, the proxy's record of it is wrong.
    named_set_parsed: bool,
    /// Likewise for the unnamed statement; a failed unnamed Parse leaves no
    /// unnamed statement behind, so only a cycle that never reached the
    /// backend matters.
    unnamed_set_parsed: bool,
    /// A Flush sent part of the cycle already.
    flushed: bool,
}

/// One way the cycle may have run: the step that failed (with, for a
/// `CALL`/`DO`, whether it committed first), and which `CALL`s and `DO`s
/// committed.
#[derive(Clone, Copy)]
struct Scenario {
    fail: Option<(usize, bool)>,
    commits: u32,
}

impl ExtSetCycle {
    /// A Parse: `named` statement or the unnamed one; `set_like` its new
    /// SQL; `replaced_set_like` the definition it replaces.
    pub(super) fn on_parse(&mut self, named: bool, set_like: bool, replaced_set_like: bool) {
        if set_like || replaced_set_like {
            if named {
                self.named_set_parsed = true;
            } else {
                self.unnamed_set_parsed = true;
            }
        }
    }

    /// A Bind of `portal` to `bound`.
    pub(super) fn on_bind(&mut self, portal: &[u8], bound: Bound) {
        self.binds += 1;
        if self.awaiting_execute {
            self.unpaired = true;
        }
        self.portal.clear();
        self.portal.extend_from_slice(portal);
        self.awaiting_execute = true;
        let step = match bound {
            Bound::Unknown => {
                self.unknown = true;
                return;
            }
            Bound::Other if self.steps.last() == Some(&Step::Other) => return,
            Bound::Other => Step::Other,
            Bound::Set(sql) => Step::Set(sql),
            Bound::Tx(verb, sql) => Step::Tx(verb, sql),
            Bound::CommitPoint => Step::CommitPoint,
        };
        self.relevant |= step != Step::Other;
        if self.steps.len() < MAX_STEPS {
            self.steps.push(step);
        } else {
            self.overflow = true;
        }
    }

    /// An Execute of `portal`.
    pub(super) fn on_execute(&mut self, portal: &[u8]) {
        if !self.awaiting_execute || self.portal != portal {
            self.unpaired = true;
        }
        self.awaiting_execute = false;
    }

    /// A cycle is open: something was bound or flushed since the last Sync.
    pub(super) fn is_open(&self) -> bool {
        self.binds > 0 || self.flushed
    }

    /// The cycle reached a Flush or Sync boundary. A Sync ends the cycle; a
    /// Flush only notes that part of it is running.
    pub(super) fn at_boundary(&mut self, b: ExtSetBoundary) -> ExtSetOutcome {
        if !b.sync {
            let pin = !b.forwarded && (self.named_set_parsed || self.unnamed_set_parsed);
            self.flushed = true;
            return ExtSetOutcome {
                pin,
                ops: Vec::new(),
            };
        }
        let pin = (self.named_set_parsed && (!b.forwarded || b.failed))
            || (self.unnamed_set_parsed && !b.forwarded);
        let ops = if b.forwarded {
            self.settle(b)
        } else if self.flushed {
            // Part of the cycle ran, its Sync never did.
            self.abandon(b.before)
        } else {
            Vec::new()
        };
        self.reset();
        ExtSetOutcome { pin, ops }
    }

    /// The client left the cycle without a Sync (a simple Query after a
    /// Flush, or after extended messages it never synced): what ran of it
    /// can no longer be told apart.
    pub(super) fn interrupt(&mut self, before: u8) -> Vec<SettingsOp> {
        let ops = self.abandon(before);
        self.reset();
        ops
    }

    fn sets(&self) -> bool {
        self.unknown || self.overflow || self.steps.iter().any(|s| matches!(s, Step::Set(_)))
    }

    /// The cycle may have ended a transaction (or committed part of an
    /// implicit one).
    fn may_end(&self) -> bool {
        self.unknown
            || self.overflow
            || self.steps.iter().any(|s| {
                matches!(
                    s,
                    Step::CommitPoint
                        | Step::Tx(
                            TxVerb::Commit
                                | TxVerb::Rollback
                                | TxVerb::CommitChain
                                | TxVerb::RollbackChain
                                | TxVerb::Prepare
                                | TxVerb::Opaque,
                            _
                        )
                )
            })
    }

    fn savepoints(&self) -> bool {
        self.steps.iter().any(|s| {
            matches!(
                s,
                Step::Tx(TxVerb::Savepoint | TxVerb::Release | TxVerb::RollbackTo, _)
            )
        })
    }

    /// A cycle the proxy saw only part of the outcome of.
    fn abandon(&self, before: u8) -> Vec<SettingsOp> {
        if !self.relevant && !self.unknown && !self.overflow {
            return Vec::new();
        }
        let (sets, may_end) = (self.sets(), self.may_end());
        if before == b'I' {
            if sets {
                vec![SettingsOp::Unknown]
            } else {
                Vec::new()
            }
        } else if may_end {
            vec![if sets {
                SettingsOp::Unknown
            } else {
                SettingsOp::End(None)
            }]
        } else if sets || self.savepoints() {
            vec![SettingsOp::Taint {
                settings: sets,
                savepoints: self.savepoints(),
            }]
        } else {
            Vec::new()
        }
    }

    /// A forwarded cycle's effect at its Sync.
    fn settle(&self, b: ExtSetBoundary) -> Vec<SettingsOp> {
        let open_before = b.before != b'I';
        let open_after = b.after != b'I';
        if !self.relevant && !self.unknown && !self.overflow {
            // Statements that touch neither settings nor the transaction.
            return if open_before && !open_after && !b.copying {
                vec![SettingsOp::End(None)]
            } else {
                Vec::new()
            };
        }
        let exact =
            !(self.unknown || self.overflow || self.unpaired || self.awaiting_execute || b.copying);
        if exact {
            if let Some(ops) = self.simulate(b) {
                return ops;
            }
        }
        self.fallback(b)
    }

    /// The conservative reading of a cycle the proxy cannot read statement
    /// by statement.
    fn fallback(&self, b: ExtSetBoundary) -> Vec<SettingsOp> {
        use SettingsOp::{End, Taint, Unknown};
        let open_before = b.before != b'I';
        let open_after = b.after != b'I';
        let (sets, may_end, savepoints) = (self.sets(), self.may_end(), self.savepoints());
        if b.copying {
            // No ReadyForQuery yet: the statuses say nothing, and the drain
            // to it settles a transaction that ended unseen.
            return self.abandon(b.before);
        }
        if open_before && (may_end || !open_after) {
            // The transaction may have ended inside the cycle.
            return vec![if sets { Unknown } else { End(None) }];
        }
        if !sets {
            return if savepoints && open_after {
                vec![Taint {
                    settings: false,
                    savepoints: true,
                }]
            } else {
                Vec::new()
            };
        }
        if !may_end && !open_before && !open_after && b.failed {
            // The cycle's transaction failed, and its SETs with it: nothing
            // in it could commit part of it.
            Vec::new()
        } else if !may_end && open_after {
            vec![Taint {
                settings: true,
                savepoints,
            }]
        } else {
            vec![Unknown]
        }
    }

    /// Run the cycle's statements in every way consistent with the
    /// response, and return what they agree on. `None` when no way is.
    fn simulate(&self, b: ExtSetBoundary) -> Option<Vec<SettingsOp>> {
        let commit_points = self
            .steps
            .iter()
            .filter(|s| matches!(s, Step::CommitPoint))
            .count();
        if commit_points > MAX_COMMIT_POINTS {
            return None;
        }
        // After a Flush an error may not reach the Sync's response.
        let may_succeed = !b.failed;
        let may_fail = b.failed || self.flushed;
        let mut outcomes: Vec<Vec<SettingsOp>> = Vec::new();
        let mut consider = |s: Scenario| {
            if let Some(ops) = self.run(b, s) {
                if !outcomes.contains(&ops) {
                    outcomes.push(ops);
                }
            }
        };
        for commits in 0..(1u32 << commit_points) {
            if may_succeed {
                consider(Scenario {
                    fail: None,
                    commits,
                });
            }
            if may_fail {
                for (i, step) in self.steps.iter().enumerate() {
                    if matches!(
                        step,
                        Step::Tx(TxVerb::Begin | TxVerb::Rollback | TxVerb::RollbackChain, _)
                    ) {
                        // These do not fail once bound.
                        continue;
                    }
                    consider(Scenario {
                        fail: Some((i, false)),
                        commits,
                    });
                    if *step == Step::CommitPoint {
                        consider(Scenario {
                            fail: Some((i, true)),
                            commits,
                        });
                    }
                }
            }
        }
        let first = outcomes.first()?;
        if outcomes.len() == 1 {
            return Some(first.clone());
        }
        // Keep what every outcome starts with; the rest is in doubt.
        let common = (0..first.len())
            .take_while(|&i| outcomes.iter().all(|o| o.get(i) == first.get(i)))
            .count();
        let mut ops = first[..common].to_vec();
        let tails = || outcomes.iter().flat_map(|o| o[common..].iter());
        if tails().all(|op| matches!(op, SettingsOp::End(_))) {
            ops.push(SettingsOp::End(None));
        } else if b.after == b'I'
            || tails().any(|op| {
                matches!(
                    op,
                    SettingsOp::End(_) | SettingsOp::Apply(_) | SettingsOp::Unknown
                )
            })
        {
            ops.push(SettingsOp::Unknown);
        } else {
            ops.push(SettingsOp::Taint {
                settings: tails().any(|op| {
                    matches!(
                        op,
                        SettingsOp::Defer(_) | SettingsOp::Taint { settings: true, .. }
                    )
                }),
                savepoints: tails().any(|op| {
                    matches!(
                        op,
                        SettingsOp::Savepoint(_)
                            | SettingsOp::Taint {
                                savepoints: true,
                                ..
                            }
                    )
                }),
            });
        }
        Some(ops)
    }

    /// The settings operations of one scenario, or `None` when it cannot
    /// have happened or does not end in the status the response showed.
    fn run(&self, b: ExtSetBoundary, s: Scenario) -> Option<Vec<SettingsOp>> {
        use SettingsOp::{Apply, Defer, End, Savepoint};
        // An explicit transaction block is open; it failed; the SETs of the
        // cycle's own (implicit) transaction so far.
        let mut open = b.before != b'I';
        let mut aborted = b.before == b'E';
        let mut implicit: Vec<&str> = Vec::new();
        let mut ops = Vec::new();
        let mut commit_point = 0;
        for (i, step) in self.steps.iter().enumerate() {
            let failing = s.fail.map(|(f, _)| f) == Some(i);
            let runs_when_aborted = matches!(
                step,
                Step::Tx(
                    TxVerb::Commit
                        | TxVerb::Rollback
                        | TxVerb::CommitChain
                        | TxVerb::RollbackChain
                        | TxVerb::RollbackTo,
                    _
                )
            );
            if aborted && !runs_when_aborted && !failing {
                // It would fail ("current transaction is aborted").
                return None;
            }
            if failing {
                match step {
                    // A failed COMMIT or PREPARE TRANSACTION rolls back.
                    Step::Tx(TxVerb::Commit | TxVerb::Prepare, _) if open && !aborted => {
                        ops.push(End(Some(false)));
                        open = false;
                    }
                    // What a failed COMMIT AND CHAIN leaves is not modelled.
                    Step::Tx(TxVerb::CommitChain, _) => return None,
                    _ if open => aborted = true,
                    // A procedure that committed before it failed.
                    Step::CommitPoint if s.fail == Some((i, true)) => {
                        ops.extend(implicit.drain(..).map(|q| Apply(q.to_string())));
                    }
                    _ => implicit.clear(),
                }
                // PostgreSQL skips the rest of the cycle.
                break;
            }
            match step {
                Step::Other => {}
                Step::CommitPoint => {
                    if !open && s.commits & (1 << commit_point) != 0 {
                        ops.extend(implicit.drain(..).map(|q| Apply(q.to_string())));
                    }
                    commit_point += 1;
                }
                Step::Set(sql) if open => ops.push(Defer(sql.clone())),
                Step::Set(sql) => implicit.push(sql),
                Step::Tx(verb, sql) => match verb {
                    TxVerb::Begin => {
                        if !open {
                            // The block takes in what the cycle ran before.
                            open = true;
                            ops.extend(implicit.drain(..).map(|q| Defer(q.to_string())));
                        }
                    }
                    TxVerb::Commit | TxVerb::Rollback | TxVerb::Prepare if open => {
                        ops.push(match verb {
                            TxVerb::Commit if !aborted => End(Some(true)),
                            TxVerb::Prepare => End(None),
                            _ => End(Some(false)),
                        });
                        open = false;
                        aborted = false;
                    }
                    TxVerb::Commit => {
                        ops.extend(implicit.drain(..).map(|q| Apply(q.to_string())));
                    }
                    TxVerb::Rollback => implicit.clear(),
                    TxVerb::CommitChain | TxVerb::RollbackChain if open => {
                        ops.push(End(Some(*verb == TxVerb::CommitChain && !aborted)));
                        aborted = false;
                    }
                    TxVerb::Savepoint | TxVerb::Release | TxVerb::RollbackTo if open => {
                        ops.push(Savepoint(sql.clone()));
                        aborted = false;
                    }
                    // Outside a transaction block these are errors, and an
                    // unlexable statement always is.
                    _ => return None,
                },
            }
        }
        if !open {
            // The cycle's own transaction commits at the Sync.
            ops.extend(implicit.drain(..).map(|q| Apply(q.to_string())));
        } else if aborted {
            // A failed transaction recovers only by ROLLBACK TO a savepoint
            // taken before these SETs, which undoes them: they never apply.
            while matches!(ops.last(), Some(Defer(_))) {
                ops.pop();
            }
        }
        let status = match (open, aborted) {
            (false, _) => b'I',
            (true, false) => b'T',
            (true, true) => b'E',
        };
        (status == b.after).then_some(ops)
    }

    fn reset(&mut self) {
        self.binds = 0;
        self.steps.clear();
        self.relevant = false;
        self.overflow = false;
        self.unknown = false;
        self.portal.clear();
        self.awaiting_execute = false;
        self.unpaired = false;
        self.named_set_parsed = false;
        self.unnamed_set_parsed = false;
        self.flushed = false;
    }
}
