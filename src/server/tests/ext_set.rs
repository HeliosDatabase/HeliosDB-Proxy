//! Extended-protocol SET tracking: the per-cycle outcome (Sprinter
//! d3a4c04c28be, 75c0c5eceac3). The client loop feeds every Parse, Bind,
//! Execute and boundary of a cycle; the cycle's statements are run in order
//! under every failure the response allows, and only what all of those
//! agree on is taken; anything else makes the settings unknown.
use super::*;
use SettingsOp::{Apply, Defer, End, Savepoint, Taint, Unknown};

const SET: &str = "SET search_path TO t2";
const SET2: &str = "SET work_mem = '1MB'";

fn set() -> Bound {
    Bound::Set(SET.to_string())
}

fn set2() -> Bound {
    Bound::Set(SET2.to_string())
}

fn tx(sql: &str) -> Bound {
    ProxyServer::tr_bound(sql.as_bytes())
}

fn sync(before: u8, after: u8, failed: bool) -> ExtSetBoundary {
    ExtSetBoundary {
        sync: true,
        forwarded: true,
        before,
        after,
        failed,
        copying: false,
        completed: None,
        simple: false,
    }
}

fn ok_sync() -> ExtSetBoundary {
    sync(b'I', b'I', false)
}

fn simple_sync(before: u8, after: u8, failed: bool) -> ExtSetBoundary {
    ExtSetBoundary {
        simple: true,
        ..sync(before, after, failed)
    }
}

fn flush(forwarded: bool, before: u8) -> ExtSetBoundary {
    ExtSetBoundary {
        sync: false,
        forwarded,
        before,
        after: before,
        failed: false,
        copying: false,
        completed: None,
        simple: false,
    }
}

/// `b` with the cycle's completion count.
fn counted(b: ExtSetBoundary, completed: u32) -> ExtSetBoundary {
    ExtSetBoundary {
        completed: Some(completed),
        ..b
    }
}

fn ops(ops: Vec<SettingsOp>) -> ExtSetOutcome {
    ExtSetOutcome { pin: false, ops }
}

fn pinned() -> ExtSetOutcome {
    ExtSetOutcome {
        pin: true,
        ops: Vec::new(),
    }
}

fn apply(sql: &str) -> SettingsOp {
    Apply(sql.to_string())
}

fn defer(sql: &str) -> SettingsOp {
    Defer(sql.to_string())
}

fn savepoint(sql: &str) -> SettingsOp {
    Savepoint(sql.to_string())
}

/// Bind and Execute `bound` on the unnamed portal.
fn run(c: &mut ExtSetCycle, bound: Bound) {
    c.on_bind(b"", bound);
    c.on_execute(b"");
}

/// Parse(unnamed SET) Bind Execute — the JDBC / Npgsql / tokio-postgres shape.
fn lone_set(c: &mut ExtSetCycle) {
    c.on_parse(false, true, false);
    run(c, set());
}

fn cycle(feed: impl FnOnce(&mut ExtSetCycle), b: ExtSetBoundary) -> ExtSetOutcome {
    let mut c = ExtSetCycle::default();
    feed(&mut c);
    c.at_boundary(b)
}

/// One cycle running `stmts` in order.
fn stmts(stmts: &[Bound], b: ExtSetBoundary) -> ExtSetOutcome {
    cycle(
        |c| {
            for s in stmts {
                run(c, s.clone());
            }
        },
        b,
    )
}

#[test]
fn a_lone_set_in_autocommit_is_applied() {
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(c.at_boundary(ok_sync()), ops(vec![apply(SET)]));
    // The cycle ended: the next one starts clean.
    c.on_parse(false, false, false);
    run(&mut c, Bound::Other);
    assert_eq!(c.at_boundary(ok_sync()), ops(vec![]));
}

#[test]
fn a_named_set_bound_again_later_is_applied() {
    let mut c = ExtSetCycle::default();
    c.on_parse(true, true, false);
    assert_eq!(c.at_boundary(ok_sync()), ops(vec![]));
    c.on_bind(b"p", set());
    c.on_execute(b"p");
    assert_eq!(c.at_boundary(ok_sync()), ops(vec![apply(SET)]));
}

#[test]
fn a_failed_set_changed_nothing() {
    for (before, after) in [(b'I', b'I'), (b'T', b'E'), (b'E', b'E')] {
        assert_eq!(
            cycle(lone_set, sync(before, after, true)),
            ops(vec![]),
            "{} -> {}",
            before as char,
            after as char
        );
    }
    // Two SETs in a failed implicit transaction: whichever failed, both go.
    assert_eq!(stmts(&[set(), set2()], sync(b'I', b'I', true)), ops(vec![]));
}

#[test]
fn sets_inside_a_transaction_are_deferred_in_order() {
    assert_eq!(
        cycle(lone_set, sync(b'T', b'T', false)),
        ops(vec![defer(SET)])
    );
    // JDBC with autocommit off: BEGIN in front of the transaction's first
    // statement, in the same cycle; BEGIN takes in what ran before it.
    for list in [
        vec![tx("BEGIN"), set()],
        vec![set(), tx("BEGIN")],
        vec![Bound::Other, tx("BEGIN"), set(), Bound::Other],
    ] {
        assert_eq!(
            stmts(&list, sync(b'I', b'T', false)),
            ops(vec![defer(SET)]),
            "{list:?}"
        );
    }
    assert_eq!(
        stmts(&[tx("BEGIN"), set()], sync(b'I', b'E', true)),
        ops(vec![])
    );
    assert_eq!(
        stmts(&[set(), set2()], sync(b'T', b'T', false)),
        ops(vec![defer(SET), defer(SET2)])
    );
    // pgjdbc autosave: a SAVEPOINT in front of every statement.
    assert_eq!(
        stmts(
            &[tx("SAVEPOINT PGJDBC_AUTOSAVE"), set()],
            sync(b'T', b'T', false)
        ),
        ops(vec![savepoint("SAVEPOINT PGJDBC_AUTOSAVE"), defer(SET)])
    );
    // Several SETs and other statements in autocommit all commit at the Sync.
    assert_eq!(
        stmts(&[set(), Bound::Other, set2()], ok_sync()),
        ops(vec![apply(SET), apply(SET2)])
    );
}

#[test]
fn transaction_control_settles_deferred_settings() {
    let cases: [(&str, &[&str], ExtSetBoundary, Vec<SettingsOp>); 18] = [
        (
            "commit",
            &["COMMIT"],
            sync(b'T', b'I', false),
            vec![End(Some(true))],
        ),
        (
            "end",
            &["END WORK"],
            sync(b'T', b'I', false),
            vec![End(Some(true))],
        ),
        (
            "failed commit rolls back",
            &["COMMIT"],
            sync(b'T', b'I', true),
            vec![End(Some(false))],
        ),
        (
            "commit of a failed transaction",
            &["COMMIT"],
            sync(b'E', b'I', false),
            vec![End(Some(false))],
        ),
        (
            "rollback",
            &["ROLLBACK"],
            sync(b'T', b'I', false),
            vec![End(Some(false))],
        ),
        (
            "abort",
            &["/* c */ ABORT"],
            sync(b'E', b'I', false),
            vec![End(Some(false))],
        ),
        (
            "commit and chain",
            &["COMMIT AND CHAIN"],
            sync(b'T', b'T', false),
            vec![End(Some(true))],
        ),
        (
            "rollback and chain",
            &["ROLLBACK AND CHAIN"],
            sync(b'T', b'T', false),
            vec![End(Some(false))],
        ),
        (
            // Settings made in a prepared transaction persist, as at COMMIT.
            "prepare transaction",
            &["PREPARE TRANSACTION 'x'"],
            sync(b'T', b'I', false),
            vec![End(Some(true))],
        ),
        (
            "savepoint",
            &["SAVEPOINT a"],
            sync(b'T', b'T', false),
            vec![savepoint("SAVEPOINT a")],
        ),
        (
            "rollback to recovers a failed transaction",
            &["ROLLBACK TO a"],
            sync(b'E', b'T', false),
            vec![savepoint("ROLLBACK TO a")],
        ),
        (
            "failed release",
            &["RELEASE a"],
            sync(b'T', b'E', true),
            vec![],
        ),
        (
            "prepare in a failed transaction rolls back",
            &["PREPARE TRANSACTION 'x'"],
            sync(b'E', b'I', false),
            vec![End(Some(false))],
        ),
        ("begin", &["BEGIN"], sync(b'I', b'T', false), vec![]),
        (
            "commit outside a transaction",
            &["COMMIT"],
            ok_sync(),
            vec![],
        ),
        (
            "a statement then a commit",
            &["UPDATE t SET a = 1", "COMMIT"],
            sync(b'T', b'I', false),
            vec![End(Some(true))],
        ),
        (
            // The UPDATE failing would leave 'E': the COMMIT failed.
            "a statement then a failed commit",
            &["UPDATE t SET a = 1", "COMMIT"],
            sync(b'T', b'I', true),
            vec![End(Some(false))],
        ),
        (
            "commit then begin",
            &["COMMIT", "BEGIN"],
            sync(b'T', b'T', false),
            vec![End(Some(true))],
        ),
    ];
    for (what, sqls, boundary, expected) in cases {
        let list: Vec<Bound> = sqls.iter().map(|s| tx(s)).collect();
        assert_eq!(stmts(&list, boundary), ops(expected), "{what}");
    }
    // A failed savepoint statement among others may or may not have run.
    assert_eq!(
        stmts(&[tx("SAVEPOINT a"), Bound::Other], sync(b'T', b'E', true)),
        ops(vec![Taint {
            settings: false,
            savepoints: true
        }])
    );
    // SET then COMMIT, and the other way round.
    assert_eq!(
        stmts(&[set(), tx("COMMIT")], sync(b'T', b'I', false)),
        ops(vec![defer(SET), End(Some(true))])
    );
    assert_eq!(
        stmts(&[tx("COMMIT"), set()], sync(b'T', b'I', false)),
        ops(vec![End(Some(true)), apply(SET)])
    );
    // The transaction ended in a cycle with nothing the proxy can read it
    // from.
    assert_eq!(
        stmts(&[Bound::Other], sync(b'T', b'I', false)),
        ops(vec![End(None)])
    );
}

#[test]
fn sets_before_a_procedure_are_unknown() {
    // PostgreSQL refuses transaction control in a procedure called after
    // other statements (2D000), but another PG-wire backend may let it
    // commit or roll back the SETs before it.
    for call in [&b"CALL p()"[..], b"/* t */ DO $$BEGIN COMMIT; END$$"] {
        assert_eq!(ProxyServer::tr_bound(call), Bound::CommitPoint);
    }
    let call = Bound::CommitPoint;
    let set_call = [set(), call.clone(), Bound::Other];
    assert_eq!(stmts(&set_call, sync(b'I', b'I', true)), ops(vec![Unknown]));
    assert_eq!(stmts(&set_call, ok_sync()), ops(vec![Unknown]));
    // Inside a transaction block, or with nothing before it, it cannot.
    assert_eq!(stmts(&set_call, sync(b'T', b'E', true)), ops(vec![]));
    assert_eq!(stmts(&[call, set()], ok_sync()), ops(vec![apply(SET)]));
}

#[test]
fn a_transaction_the_cycle_opens_and_rolls_back_leaves_nothing() {
    // Whether nothing ran or the COMMIT failed, nothing changed.
    let whole = [tx("BEGIN"), set(), Bound::Other, tx("COMMIT")];
    assert_eq!(stmts(&whole, sync(b'I', b'I', true)), ops(vec![]));
    assert_eq!(stmts(&whole, simple_sync(b'I', b'I', true)), ops(vec![]));
    for (end, after) in [("ROLLBACK", b'I'), ("ROLLBACK AND CHAIN", b'T')] {
        assert_eq!(
            stmts(&[tx("BEGIN"), set(), tx(end)], sync(b'I', after, false)),
            ops(vec![]),
            "{end}"
        );
    }
    // Committed, it stands.
    assert_eq!(
        stmts(&[tx("BEGIN"), set(), tx("COMMIT")], ok_sync()),
        ops(vec![defer(SET), End(Some(true))])
    );
    // A transaction open before the cycle still ends visibly.
    assert_eq!(
        stmts(&[set(), tx("ROLLBACK")], sync(b'T', b'I', false)),
        ops(vec![defer(SET), End(Some(false))])
    );
}

#[test]
fn a_prepared_set_config_outlives_a_rollback() {
    let prepared = [tx("BEGIN"), Bound::Untracked, tx("ROLLBACK")];
    assert_eq!(stmts(&prepared, ok_sync()), ops(vec![Unknown]));
    // Inside a transaction open before the cycle, at once.
    assert_eq!(
        stmts(&[Bound::Untracked], sync(b'T', b'T', false)),
        ops(vec![Unknown])
    );
    // If it never ran, nothing.
    assert_eq!(
        stmts(&[Bound::Untracked], sync(b'I', b'I', true)),
        ops(vec![])
    );
}

#[test]
fn a_simple_query_is_parsed_whole() {
    // An extended cycle can fail at its COMMIT's Bind (still 'E', the
    // savepoint taken) or at the SAVEPOINT; a simple query either ran
    // nothing or failed while a statement ran, so the SAVEPOINT failed.
    let list = [tx("SAVEPOINT a"), tx("COMMIT")];
    assert_eq!(
        stmts(&list, sync(b'T', b'E', true)),
        ops(vec![Taint {
            settings: false,
            savepoints: true
        }])
    );
    assert_eq!(stmts(&list, simple_sync(b'T', b'E', true)), ops(vec![]));
    // DISCARD ALL in a multi-statement simple query (an implicit block)
    // fails; alone it applies.
    let discard = ProxyServer::tr_bound(b"DISCARD ALL");
    assert_eq!(
        stmts(
            &[discard.clone(), Bound::Other],
            simple_sync(b'I', b'I', true)
        ),
        ops(vec![])
    );
    assert_eq!(
        stmts(&[discard], simple_sync(b'I', b'I', false)),
        ops(vec![apply("DISCARD ALL")])
    );
}

#[test]
fn the_statement_limit_is_honoured() {
    let mut c = ExtSetCycle::with_limit(1);
    run(&mut c, set());
    run(&mut c, set2());
    assert_eq!(c.at_boundary(ok_sync()), ops(vec![Unknown]));
    c.set_limit(2);
    run(&mut c, set());
    run(&mut c, set2());
    assert_eq!(c.at_boundary(ok_sync()), ops(vec![apply(SET), apply(SET2)]));
}

#[test]
fn discard_all_commits_at_once_and_only_first() {
    let discard = || ProxyServer::tr_bound(b"DISCARD ALL");
    assert_eq!(discard(), Bound::DiscardAll("DISCARD ALL".into()));
    assert_eq!(
        stmts(&[discard()], ok_sync()),
        ops(vec![apply("DISCARD ALL")])
    );
    // First in the cycle it committed before a later error could undo it,
    // unless it was the statement that failed: the proxy cannot tell.
    assert_eq!(
        stmts(&[discard(), Bound::Other], sync(b'I', b'I', true)),
        ops(vec![Unknown])
    );
    // Inside a transaction block it fails.
    assert_eq!(stmts(&[discard()], sync(b'T', b'E', true)), ops(vec![]));
    // After a COMMIT the next transaction is fresh: it runs there too.
    assert_eq!(
        stmts(&[tx("COMMIT"), discard()], sync(b'T', b'I', false)),
        ops(vec![End(Some(true)), apply("DISCARD ALL")])
    );
}

#[test]
fn an_error_after_the_last_statement_is_considered() {
    // A Parse or Describe after the COMMIT's Execute can fail after the
    // COMMIT succeeded, or the COMMIT itself failed.
    let mut c = ExtSetCycle::default();
    run(&mut c, tx("COMMIT"));
    c.on_other_message();
    assert_eq!(c.at_boundary(sync(b'T', b'I', true)), ops(vec![End(None)]));
    // Without such a message only the COMMIT can have failed.
    assert_eq!(
        stmts(&[tx("COMMIT")], sync(b'T', b'I', true)),
        ops(vec![End(Some(false))])
    );
}

#[test]
fn a_flushed_cycle_considers_an_unseen_error() {
    // An error answering the Flush never reaches the Sync's response.
    let mut c = ExtSetCycle::default();
    run(&mut c, tx("COMMIT"));
    assert_eq!(c.at_boundary(flush(true, b'T')), ops(vec![]));
    assert_eq!(c.at_boundary(sync(b'T', b'I', false)), ops(vec![End(None)]));
    // A ROLLBACK is a rollback however it went.
    let mut c = ExtSetCycle::default();
    run(&mut c, tx("ROLLBACK"));
    assert_eq!(c.at_boundary(flush(true, b'T')), ops(vec![]));
    assert_eq!(
        c.at_boundary(sync(b'T', b'I', false)),
        ops(vec![End(Some(false))])
    );
    // A SET in autocommit: applied or not.
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(c.at_boundary(flush(true, b'I')), ops(vec![]));
    assert_eq!(c.at_boundary(ok_sync()), ops(vec![Unknown]));
    // Inside a transaction an error would have left 'E'.
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(c.at_boundary(flush(true, b'T')), ops(vec![]));
    assert_eq!(
        c.at_boundary(sync(b'T', b'T', false)),
        ops(vec![defer(SET)])
    );
}

#[test]
fn a_counted_cycle_places_the_failure_exactly() {
    // pgjdbc autosave: [SAVEPOINT, failing INSERT]. Uncounted, the SAVEPOINT
    // may have failed too, and the savepoints are in doubt.
    let autosave = [tx("SAVEPOINT PGJDBC_AUTOSAVE"), Bound::Other];
    assert_eq!(
        stmts(&autosave, sync(b'T', b'E', true)),
        ops(vec![Taint {
            settings: false,
            savepoints: true
        }])
    );
    // One statement completed: the INSERT failed, the SAVEPOINT stands.
    assert_eq!(
        stmts(&autosave, counted(sync(b'T', b'E', true), 1)),
        ops(vec![savepoint("SAVEPOINT PGJDBC_AUTOSAVE")])
    );
    // None did: the SAVEPOINT failed.
    assert_eq!(
        stmts(&autosave, counted(sync(b'T', b'E', true), 0)),
        ops(vec![])
    );
    // A Parse after the COMMIT failed: the COMMIT completed first.
    for (completed, end) in [(1, true), (0, false)] {
        let mut c = ExtSetCycle::default();
        run(&mut c, tx("COMMIT"));
        c.on_other_message();
        assert_eq!(
            c.at_boundary(counted(sync(b'T', b'I', true), completed)),
            ops(vec![End(Some(end))]),
            "{completed}"
        );
    }
    // Consecutive other statements count one each: three completed means
    // the second SAVEPOINT failed, not a message after it.
    assert_eq!(
        stmts(
            &[
                tx("SAVEPOINT a"),
                Bound::Other,
                Bound::Other,
                tx("SAVEPOINT b")
            ],
            counted(sync(b'T', b'E', true), 3)
        ),
        ops(vec![savepoint("SAVEPOINT a")])
    );
    // A simple query is parsed whole: the statement after the completed ones
    // failed while it ran.
    assert_eq!(
        stmts(
            &[tx("BEGIN"), set(), tx("COMMIT"), set2()],
            counted(simple_sync(b'I', b'I', true), 3)
        ),
        ops(vec![defer(SET), End(Some(true))])
    );
}

#[test]
fn a_counted_flush_output_settles_the_cycle() {
    // psycopg pipeline: a SET answered at a Flush, then the Sync. With every
    // response counted, an error at the Flush is no longer unseen.
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(c.at_boundary(flush(true, b'I')), ops(vec![]));
    assert_eq!(c.at_boundary(counted(ok_sync(), 1)), ops(vec![apply(SET)]));
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(c.at_boundary(flush(true, b'I')), ops(vec![]));
    assert_eq!(
        c.at_boundary(counted(sync(b'I', b'I', true), 0)),
        ops(vec![])
    );
    // A count that does not fit the cycle is not trusted.
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(c.at_boundary(flush(true, b'I')), ops(vec![]));
    assert_eq!(c.at_boundary(counted(ok_sync(), 2)), ops(vec![Unknown]));
    assert_eq!(
        stmts(&[set()], counted(sync(b'I', b'I', true), 2)),
        ops(vec![])
    );
}

#[test]
fn cycles_the_proxy_cannot_read_make_settings_unknown() {
    type Case = (
        &'static str,
        fn(&mut ExtSetCycle),
        ExtSetBoundary,
        Vec<SettingsOp>,
    );
    let cases: [Case; 6] = [
        (
            "Execute of another portal",
            |c| {
                c.on_bind(b"a", set());
                c.on_execute(b"holdcur");
            },
            ok_sync(),
            vec![Unknown],
        ),
        (
            "SET bound, never executed",
            |c| c.on_bind(b"", set()),
            ok_sync(),
            vec![Unknown],
        ),
        (
            "a statement the proxy has no record of",
            |c| run(c, Bound::Unknown),
            ok_sync(),
            vec![Unknown],
        ),
        (
            "a SET bound inside a transaction, never executed",
            |c| c.on_bind(b"p", set()),
            sync(b'T', b'T', false),
            vec![Taint {
                settings: true,
                savepoints: false,
            }],
        ),
        (
            "more statements than the cycle remembers",
            |c| {
                for _ in 0..9 {
                    run(c, tx("SAVEPOINT a"));
                    run(c, tx("RELEASE a"));
                }
            },
            sync(b'T', b'T', false),
            vec![Unknown],
        ),
        (
            "a statement that does not lex",
            |c| run(c, tx("ROLLBACK TO 'open")),
            sync(b'T', b'T', false),
            vec![End(None)],
        ),
    ];
    for (what, feed, boundary, expected) in cases {
        assert_eq!(cycle(feed, boundary), ops(expected), "{what}");
    }
    // Simulated outcomes that disagree on the settings: unknown after the
    // transaction's end, a taint while it stays open.
    assert_eq!(
        stmts(
            &[set(), tx("SAVEPOINT a"), Bound::Other],
            sync(b'T', b'E', true)
        ),
        ops(vec![Taint {
            settings: true,
            savepoints: true
        }])
    );
}

#[test]
fn a_copying_cycle_leaves_the_transaction_end_to_the_drain() {
    let copying = |before| ExtSetBoundary {
        copying: true,
        ..sync(before, before, false)
    };
    // The statuses are stale until the drain; a transaction end in the cycle
    // settles the deferred SETs as unknown (ROLLBACK; BEGIN; COPY).
    assert_eq!(
        stmts(&[tx("ROLLBACK"), tx("BEGIN"), Bound::Other], copying(b'T')),
        ops(vec![End(None)])
    );
    assert_eq!(stmts(&[Bound::Other], copying(b'T')), ops(vec![]));
    assert_eq!(
        stmts(&[Bound::Other, set()], copying(b'T')),
        ops(vec![Taint {
            settings: true,
            savepoints: false
        }])
    );
    assert_eq!(
        stmts(&[Bound::Other, set()], copying(b'I')),
        ops(vec![Unknown])
    );
}

#[test]
fn an_interrupted_cycle_settles_conservatively() {
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(c.at_boundary(flush(true, b'T')), ops(vec![]));
    assert!(c.is_open());
    // The unnamed SET's Parse may not have run either: the pin stays.
    assert_eq!(
        c.interrupt(b'T'),
        ExtSetOutcome {
            pin: true,
            ops: vec![Taint {
                settings: true,
                savepoints: false
            }]
        }
    );
    assert!(!c.is_open());
    run(&mut c, tx("COMMIT"));
    assert_eq!(c.interrupt(b'T'), ops(vec![End(None)]));
    run(&mut c, set());
    assert_eq!(c.interrupt(b'I'), ops(vec![Unknown]));
    run(&mut c, Bound::Other);
    assert_eq!(c.interrupt(b'I'), ops(vec![]));
}

#[test]
fn a_stale_statement_record_pins() {
    // A named SET-like Parse in a failed cycle (e.g. 42P05: the backend
    // keeps the old definition while the proxy recorded the new one).
    let mut c = ExtSetCycle::default();
    c.on_parse(true, true, false);
    assert_eq!(c.at_boundary(sync(b'I', b'I', true)), pinned());
    // A named Parse replacing a SET-like statement, never forwarded.
    let mut c = ExtSetCycle::default();
    c.on_parse(true, false, true);
    assert_eq!(
        c.at_boundary(ExtSetBoundary {
            forwarded: false,
            ..ok_sync()
        }),
        pinned()
    );
    // An unnamed SET-like Parse never forwarded (rate limit, open circuit).
    let mut c = ExtSetCycle::default();
    c.on_parse(false, true, false);
    assert_eq!(c.at_boundary(flush(false, b'I')), pinned());
    // A failed unnamed Parse leaves no unnamed statement: nothing to pin.
    let mut c = ExtSetCycle::default();
    c.on_parse(false, true, false);
    assert_eq!(c.at_boundary(sync(b'I', b'I', true)), ops(vec![]));
    // A cycle that never reached a backend changed no setting.
    let mut c = ExtSetCycle::default();
    lone_set(&mut c);
    assert_eq!(
        c.at_boundary(ExtSetBoundary {
            forwarded: false,
            ..sync(b'T', b'T', false)
        }),
        pinned()
    );
}

#[test]
fn bound_statements_are_classified_from_raw_bytes() {
    use TxVerb::*;
    for (sql, expected) in [
        (&b"SET a = 1"[..], Some(None)),
        (b"SET x = 'a;b'", Some(None)),
        (b"/* c */ RESET ALL", Some(None)),
        (
            b"SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
            Some(None),
        ),
        (b"BEGIN", Some(Some(Begin))),
        (
            b"start transaction isolation level serializable",
            Some(Some(Begin)),
        ),
        (b"-- c\nCOMMIT", Some(Some(Commit))),
        (b"END WORK", Some(Some(Commit))),
        (b"COMMIT AND NO CHAIN", Some(Some(Commit))),
        (b"COMMIT AND CHAIN", Some(Some(CommitChain))),
        (b"ROLLBACK", Some(Some(Rollback))),
        (b"ABORT TRANSACTION", Some(Some(Rollback))),
        (b"ROLLBACK AND CHAIN", Some(Some(RollbackChain))),
        (b"ROLLBACK TO s", Some(Some(RollbackTo))),
        (b"rollback work to savepoint s", Some(Some(RollbackTo))),
        (b"SAVEPOINT s", Some(Some(Savepoint))),
        (b"RELEASE SAVEPOINT s", Some(Some(Release))),
        (b"PREPARE TRANSACTION 'x'", Some(Some(Prepare))),
        (b"ROLLBACK TO 'open", Some(Some(Opaque))),
    ] {
        let got = ProxyServer::tr_bound(sql);
        let text = String::from_utf8_lossy(sql).to_string();
        match expected {
            Some(None) => assert_eq!(got, Bound::Set(text.clone()), "{text}"),
            Some(Some(v)) => {
                let keep = matches!(v, Savepoint | Release | RollbackTo);
                let kept = if keep { text.clone() } else { String::new() };
                assert_eq!(got, Bound::Tx(v, kept), "{text}")
            }
            None => unreachable!(),
        }
    }
    // Statements whose effects end with the transaction, are not settings,
    // or are not this session's transaction control.
    for sql in [
        &b"SET LOCAL app.tenant = '42'"[..],
        b"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        b"SET CONSTRAINTS ALL DEFERRED",
        b"DISCARD TEMP",
        b"DISCARD PLANS",
        b"PREPARE q AS SELECT 1",
        b"COMMIT PREPARED 'x'",
        b"ROLLBACK PREPARED 'x'",
        b"SELECT 1",
        b"BEGINNING",
        b"/* COMMIT */ SELECT 1",
        b"SELECT set_config('request.jwt.claims', $1, true)",
        b"select set_config ( 'a' , f(1, 2) , TRUE )",
        b"SELECT set_config('a', 'b', 't')",
        b"SELECT 'set_config(1, 2, false)'",
    ] {
        assert_eq!(
            ProxyServer::tr_bound(sql),
            Bound::Other,
            "{}",
            String::from_utf8_lossy(sql)
        );
    }
    // set_config(..., false) or an is_local the proxy cannot read: a
    // session-level change it cannot record.
    for sql in [
        "SELECT set_config('search_path', '', false)",
        "SELECT set_config('a', 'x,y', is_local)",
        "SELECT 1, set_config('a', 'b', true), set_config('c', 'd', false)",
        "SELECT set_config('a', $$'$$, false) -- ', true)",
        "SELECT set_config('a', E'x\\'', true)",
    ] {
        // Applied, an empty SET is one the proxy cannot record.
        assert_eq!(
            ProxyServer::tr_bound(sql.as_bytes()),
            Bound::Set(String::new()),
            "{sql}"
        );
    }
    // A prepared set_config: EXECUTE sets it later, whatever the
    // transaction does; a non-UTF-8 savepoint name could collide.
    assert_eq!(
        ProxyServer::tr_bound(b"PREPARE p AS SELECT set_config('a', 'b', false)"),
        Bound::Untracked
    );
    assert_eq!(ProxyServer::tr_bound(b"SAVEPOINT a\xe9"), Bound::Unknown);
    // A non-UTF-8 session SET: its shape is known, its text is not.
    assert_eq!(
        ProxyServer::tr_bound(b"SET application_name = 'caf\xe9'"),
        Bound::Set(String::new())
    );
    assert_eq!(
        ProxyServer::tr_bound(b"DISCARD ALL"),
        Bound::DiscardAll("DISCARD ALL".into())
    );
    // Not UTF-8: the statement's shape still counts.
    assert_eq!(
        ProxyServer::tr_bound(b"COMMIT /* \xff */"),
        Bound::Tx(TxVerb::Commit, String::new())
    );
    // (PostgreSQL rejects this one anyway: unterminated.)
    assert_eq!(
        ProxyServer::tr_bound(b"SET a = '\xff"),
        Bound::Set(String::new())
    );
    // Text that does not lex at all is unknown.
    assert_eq!(ProxyServer::tr_bound(b"SET 'open"), Bound::Unknown);
}

#[test]
fn set_like_statements_are_recognised_from_raw_bytes() {
    for (sql, set_like) in [
        (&b"SET search_path TO a"[..], true),
        (b"/* jdbc */ set application_name = 'x'", true),
        (b"-- c\rRESET ALL", true),
        (b"DISCARD ALL", true),
        (b"SELECT 1", false),
        (b"SETTINGS", false),
        (b"/* SET */ SELECT 1", false),
        (b"\xff\xfe SET a = 1", false),
    ] {
        assert_eq!(
            ProxyServer::tr_is_set_like_bytes(sql),
            set_like,
            "{:?}",
            String::from_utf8_lossy(sql)
        );
    }
    let parse = |name: &str, sql: &str| {
        let mut payload = name.as_bytes().to_vec();
        payload.push(0);
        payload.extend_from_slice(sql.as_bytes());
        payload.extend_from_slice(&[0, 0, 0]);
        payload
    };
    assert_eq!(
        ProxyServer::parse_payload_sql_bytes(&parse("s1", "SET a = 1")),
        Some(&b"SET a = 1"[..])
    );
    assert_eq!(
        ProxyServer::parse_payload_sql_bytes(&parse("", "SELECT 1")),
        Some(&b"SELECT 1"[..])
    );
    assert_eq!(ProxyServer::leading_cstring(b"portal\0\0\0\0\0"), b"portal");
    assert_eq!(ProxyServer::leading_cstring(b"\0rest"), b"");
}
