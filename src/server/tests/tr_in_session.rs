use super::*;
use crate::config::TrMode;
use crate::protocol::QueryMessage;

const MODES: [TrMode; 4] = [
    TrMode::None,
    TrMode::Session,
    TrMode::Select,
    TrMode::Transaction,
];
const PHASES: [FaultPhase; 2] = [FaultPhase::NotDelivered, FaultPhase::OutcomeUnknown];
const KINDS: [StmtKind; 5] = [
    StmtKind::Read,
    StmtKind::Write,
    StmtKind::Commit,
    StmtKind::Control,
    StmtKind::Other,
];

/// Every cell of the decision table must satisfy the hard rules.
#[test]
fn tr_decide_exhaustive_invariants() {
    use TrAction::*;
    let mut cells = 0;
    for mode in MODES {
        for phase in PHASES {
            for in_tx in [false, true] {
                for has_writes in [false, true] {
                    for replayable in [false, true] {
                        for kind in KINDS {
                            cells += 1;
                            let a = ProxyServer::tr_decide(
                                mode, phase, in_tx, has_writes, replayable, kind,
                            );
                            let ctx = format!(
                                        "{mode:?}/{phase:?}/in_tx={in_tx}/writes={has_writes}/replayable={replayable}/{kind:?} -> {a:?}"
                                    );
                            // none: always one error, then close.
                            if mode == TrMode::None {
                                assert_eq!(a, CloseWithError("57P01"), "{ctx}");
                                continue;
                            }
                            assert!(!matches!(a, CloseWithError(_)), "{ctx}");
                            // A statement that never ran, outside a
                            // transaction, is always just re-run.
                            if phase == FaultPhase::NotDelivered && !in_tx {
                                assert_eq!(a, Reexecute, "{ctx}");
                            }
                            // Never double-apply: an autocommit write/
                            // opaque statement with unknown outcome is
                            // never re-executed.
                            if phase == FaultPhase::OutcomeUnknown
                                && !in_tx
                                && matches!(kind, StmtKind::Write | StmtKind::Other)
                            {
                                assert_eq!(a, ErrorAndContinue("08007"), "{ctx}");
                            }
                            // A COMMIT with unknown outcome is never retried.
                            if phase == FaultPhase::OutcomeUnknown
                                && in_tx
                                && kind == StmtKind::Commit
                            {
                                assert_eq!(a, ErrorAndContinue("08007"), "{ctx}");
                            }
                            // Replay only ever happens inside a
                            // transaction that is recorded & replayable.
                            if a == ReplayThenReexecute {
                                assert!(in_tx && replayable, "{ctx}");
                                assert_ne!(mode, TrMode::Session, "{ctx}");
                                if mode == TrMode::Select {
                                    assert!(!has_writes, "{ctx}");
                                }
                            }
                            // Inside a transaction the ONLY transparent
                            // outcome is a replay (the tx died with the
                            // old backend; a bare re-execution would run
                            // in autocommit).
                            if in_tx {
                                assert_ne!(a, Reexecute, "{ctx}");
                            }
                            // session mode never replays anything.
                            if mode == TrMode::Session {
                                assert!(
                                    matches!(a, ErrorAndContinue(_))
                                        || (phase == FaultPhase::NotDelivered && !in_tx),
                                    "{ctx}"
                                );
                            }
                            // SQLSTATE follows the phase.
                            if let ErrorAndContinue(code) = a {
                                match phase {
                                    FaultPhase::NotDelivered => {
                                        assert_eq!(code, "57P01", "{ctx}")
                                    }
                                    FaultPhase::OutcomeUnknown => {
                                        assert_eq!(code, "08007", "{ctx}")
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(cells, 4 * 2 * 2 * 2 * 2 * 5);
}

/// Spot rows straight from the specification table.
#[test]
fn tr_decide_spec_rows() {
    use FaultPhase::*;
    use StmtKind::*;
    use TrAction::*;
    let d = ProxyServer::tr_decide;
    // none
    assert_eq!(
        d(TrMode::None, OutcomeUnknown, true, true, true, Read),
        CloseWithError("57P01")
    );
    // session: not-delivered outside tx -> re-execute (any kind)
    assert_eq!(
        d(TrMode::Session, NotDelivered, false, false, false, Write),
        Reexecute
    );
    assert_eq!(
        d(TrMode::Session, NotDelivered, false, false, false, Commit),
        Reexecute
    );
    // session: not-delivered inside tx -> 57P01; unknown -> 08007
    assert_eq!(
        d(TrMode::Session, NotDelivered, true, true, true, Write),
        ErrorAndContinue("57P01")
    );
    assert_eq!(
        d(TrMode::Session, OutcomeUnknown, false, false, false, Read),
        ErrorAndContinue("08007")
    );
    assert_eq!(
        d(TrMode::Session, OutcomeUnknown, true, false, true, Read),
        ErrorAndContinue("08007")
    );
    // select: unknown-outcome read outside tx -> re-execute; write -> 08007
    assert_eq!(
        d(TrMode::Select, OutcomeUnknown, false, false, false, Read),
        Reexecute
    );
    assert_eq!(
        d(TrMode::Select, OutcomeUnknown, false, false, false, Control),
        Reexecute
    );
    assert_eq!(
        d(TrMode::Select, OutcomeUnknown, false, false, false, Write),
        ErrorAndContinue("08007")
    );
    // select: read-only replayable tx -> replay; tx with writes -> error
    assert_eq!(
        d(TrMode::Select, OutcomeUnknown, true, false, true, Read),
        ReplayThenReexecute
    );
    assert_eq!(
        d(TrMode::Select, NotDelivered, true, false, true, Write),
        ReplayThenReexecute
    );
    assert_eq!(
        d(TrMode::Select, OutcomeUnknown, true, true, true, Read),
        ErrorAndContinue("08007")
    );
    assert_eq!(
        d(TrMode::Select, NotDelivered, true, true, true, Read),
        ErrorAndContinue("57P01")
    );
    assert_eq!(
        d(TrMode::Select, OutcomeUnknown, true, false, false, Read),
        ErrorAndContinue("08007")
    );
    // transaction: replay uncommitted tx then re-execute (writes included)
    assert_eq!(
        d(TrMode::Transaction, NotDelivered, true, true, true, Write),
        ReplayThenReexecute
    );
    assert_eq!(
        d(TrMode::Transaction, OutcomeUnknown, true, true, true, Write),
        ReplayThenReexecute
    );
    assert_eq!(
        d(TrMode::Transaction, NotDelivered, true, true, true, Commit),
        ReplayThenReexecute
    );
    // transaction: COMMIT with unknown outcome -> 08007, never retried
    assert_eq!(
        d(
            TrMode::Transaction,
            OutcomeUnknown,
            true,
            true,
            true,
            Commit
        ),
        ErrorAndContinue("08007")
    );
    // transaction: autocommit write unknown -> 08007
    assert_eq!(
        d(
            TrMode::Transaction,
            OutcomeUnknown,
            false,
            false,
            false,
            Write
        ),
        ErrorAndContinue("08007")
    );
    // transaction: non-replayable (over cap) degrades to session behaviour
    assert_eq!(
        d(TrMode::Transaction, NotDelivered, true, true, false, Write),
        ErrorAndContinue("57P01")
    );
    assert_eq!(
        d(TrMode::Transaction, OutcomeUnknown, true, true, false, Read),
        ErrorAndContinue("08007")
    );
}

#[test]
fn tr_classify_table() {
    use StmtKind::*;
    let c = |sql: &str| ProxyServer::tr_classify(sql, &TrReadPolicy::default());
    assert_eq!(c("SELECT 1"), Read);
    assert_eq!(c("  select * from t where x = 'a;b' "), Write);
    assert_eq!(c("SELECT count(*) FROM t;"), Read);
    assert_eq!(c("SELECT CASE WHEN x THEN 1 END FROM t"), Read);
    assert_eq!(c("SELECT * INTO t2 FROM t"), Other);
    assert_eq!(c("SELECT nextval('s')"), Other);
    assert_eq!(c("SELECT pg_sleep(0.5), 42"), Read);
    // TR-03: a read is re-executable only if every call is a known
    // side-effect-free built-in. Anything else may already have run.
    assert_eq!(c("SELECT audit_side_effect()"), Other);
    assert_eq!(c("SELECT my_schema.my_udf(1)"), Other);
    assert_eq!(c("SELECT lower(name), count(*) FROM t"), Read);
    assert_eq!(c("SELECT pg_catalog.upper('x')"), Read);
    assert_eq!(c("SELECT \"MyFn\"(1)"), Other);
    assert_eq!(c("SELECT currval('s')"), Other);
    assert_eq!(c("SELECT pg_notify('c', 'p')"), Other);
    assert_eq!(c("SELECT set_config('a', 'b', false)"), Other);
    assert_eq!(c("SELECT pg_advisory_lock(1)"), Other);
    assert_eq!(c("SELECT random(), now(), gen_random_uuid()"), Read);
    assert_eq!(c("SELECT 'f(x)' AS s, $$g()$$ FROM t"), Read); // calls only in literals
    assert_eq!(
        c("WITH x AS (SELECT audit_side_effect()) SELECT * FROM x"),
        Other
    );
    assert_eq!(
        c("SELECT * FROM t WHERE id IN (1, 2) AND EXISTS (SELECT 1)"),
        Read
    );
    assert_eq!(c("SELECT CAST(x AS int), COALESCE(a, b) FROM t"), Read);
    // Operator-listed UDFs become eligible.
    let policy = TrReadPolicy::from_config(&["Audit_Side_Effect".to_string()]);
    assert_eq!(
        ProxyServer::tr_classify("SELECT audit_side_effect()", &policy),
        Read
    );
    assert_eq!(
        ProxyServer::tr_classify("SELECT other_udf()", &policy),
        Other
    );
    assert_eq!(c("SHOW application_name"), Read);
    assert_eq!(c("VALUES (1)"), Read);
    assert_eq!(c("TABLE t"), Read);
    assert_eq!(c("WITH x AS (SELECT 1) SELECT * FROM x"), Read);
    assert_eq!(
        c("WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d"),
        Write
    );
    assert_eq!(c("with u as (update t set v=1) select 1"), Write);
    assert_eq!(c("EXPLAIN SELECT 1"), Read);
    assert_eq!(c("EXPLAIN ANALYZE DELETE FROM t"), Other);
    assert_eq!(c("COPY t TO STDOUT"), Read);
    assert_eq!(c("COPY t FROM STDIN"), Write);
    assert_eq!(c("INSERT INTO t VALUES (1)"), Write);
    assert_eq!(c("update t set v = 2"), Write);
    assert_eq!(c("DELETE FROM t"), Write);
    assert_eq!(
        c("MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE"),
        Write
    );
    assert_eq!(c("CREATE TABLE x(i int)"), Write);
    assert_eq!(c("CALL p()"), Write);
    assert_eq!(c("DO $$ BEGIN END $$"), Write);
    assert_eq!(c("EXECUTE p(1)"), Write);
    assert_eq!(c("COMMIT"), Commit);
    assert_eq!(c("commit;"), Commit);
    assert_eq!(c("END"), Commit);
    assert_eq!(c("END TRANSACTION"), Commit);
    assert_eq!(c("COMMIT PREPARED 'x'"), Commit);
    assert_eq!(c("PREPARE TRANSACTION 'x'"), Commit);
    assert_eq!(c("PREPARE p AS SELECT 1"), Other); // server-side prepare, not a commit
    assert_eq!(c("INSERT INTO t VALUES (1); COMMIT"), Commit); // multi-stmt that may commit
    assert_eq!(
        c("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)"),
        Write
    );
    assert_eq!(c("BEGIN"), Control);
    assert_eq!(c("BEGIN; INSERT INTO t VALUES (1)"), Write);
    assert_eq!(c("START TRANSACTION"), Control);
    assert_eq!(c("SAVEPOINT s1"), Control);
    assert_eq!(c("RELEASE SAVEPOINT s1"), Control);
    assert_eq!(c("ROLLBACK"), Control);
    assert_eq!(c("ROLLBACK TO SAVEPOINT s1"), Control);
    assert_eq!(c("ABORT"), Control);
    assert_eq!(c("SET application_name = 'x'"), Control);
    assert_eq!(c("RESET ALL"), Control);
    assert_eq!(c("DISCARD ALL"), Control);
    assert_eq!(c(""), Control);
    assert_eq!(c("   ;  "), Control);
    assert_eq!(c("SETTINGS"), Other);
    assert_eq!(c("LISTEN ch"), Other);
    assert_eq!(c("NOTIFY ch"), Other);
    assert_eq!(c("LOCK TABLE t"), Other);
    assert_eq!(c("FETCH 10 FROM c"), Other);
}

#[test]
fn tr_ends_transaction_recognises_single_statement_ends_only() {
    let f = ProxyServer::tr_ends_transaction;
    for s in [
        "ROLLBACK",
        "rollback;",
        "ABORT",
        "COMMIT",
        "END",
        "COMMIT WORK",
        "END TRANSACTION",
        " Rollback Work ; ",
        "/* app=shop */ ROLLBACK",
        "ROLLBACK /* app=shop */",
        "-- note\rCOMMIT",
    ] {
        assert!(f(s), "{s}");
    }
    for s in [
        "ROLLBACK TO SAVEPOINT a",
        "ROLLBACK PREPARED 'x'",
        "COMMIT PREPARED 'x'",
        "COMMIT AND CHAIN",
        "ROLLBACK; SELECT 1",
        "SELECT 1",
        "",
        "ENDING",
        "/* ROLLBACK */ SELECT 1",
        "ROLLBACK /* x */ TO SAVEPOINT a",
    ] {
        assert!(!f(s), "{s}");
    }
}

#[test]
fn tr_session_set_tracking_filters() {
    let set = ProxyServer::tr_is_session_set;
    assert!(set("SET application_name = 'tr-f3'"));
    assert!(set("set search_path to a, b;"));
    assert!(set("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"));
    assert!(set("SET ROLE readonly"));
    assert!(set("RESET application_name"));
    assert!(!set("SET LOCAL statement_timeout = 1"));
    assert!(!set("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
    assert!(!set("SET CONSTRAINTS ALL DEFERRED"));
    assert!(!set("SET a = 1; SET b = 2"));
    assert!(!set("SELECT set_config('a','b',false)"));
    assert!(!set("SETTINGS"));
    assert!(set("SET search_path TO app /* app=shop */"));
    assert!(!set("SET /* x */ LOCAL work_mem = 1"));
    let all = ProxyServer::tr_reset_all_op;
    assert_eq!(all("RESET ALL"), Some(GucOp::ResetAll));
    assert_eq!(all("discard all;"), Some(GucOp::DiscardAll));
    assert_eq!(all("RESET /* c */ ALL /* d */"), Some(GucOp::ResetAll));
    assert_eq!(all("RESET application_name"), None);
    assert_eq!(all("DISCARD PLANS"), None);
    // A comment that says "all" is not ALL.
    assert_eq!(all("RESET work_mem /* all */"), None);
    assert_eq!(all("DISCARD PLANS -- all"), None);
}

#[test]
fn starts_with_word_ci_requires_boundary() {
    assert!(ProxyServer::starts_with_word_ci("SET x", "SET"));
    assert!(ProxyServer::starts_with_word_ci("set", "SET"));
    assert!(ProxyServer::starts_with_word_ci("END;", "END"));
    assert!(!ProxyServer::starts_with_word_ci("SETTINGS", "SET"));
    assert!(!ProxyServer::starts_with_word_ci("SE", "SET"));
}

#[test]
fn parse_msg_sql_extracts_query_from_encoded_parse() {
    let mut p = vec![b'P', 0, 0, 0, 0];
    p.extend_from_slice(&cstr("ps1"));
    p.extend_from_slice(&cstr("SELECT 42"));
    p.extend_from_slice(&[0, 0]);
    assert_eq!(ProxyServer::parse_msg_sql(&p), Some("SELECT 42"));
    assert_eq!(ProxyServer::parse_msg_sql(&p[..3]), None);
    let mut reg: HashMap<String, bytes::Bytes> = HashMap::new();
    reg.insert("ps1".to_string(), bytes::Bytes::from(p));
    let refs = vec!["ps1".to_string()];
    assert_eq!(
        ProxyServer::tr_extended_sql(None, &refs, &reg),
        Some("SELECT 42")
    );
    assert_eq!(
        ProxyServer::tr_extended_sql(Some("SELECT 1"), &refs, &reg),
        Some("SELECT 1")
    );
    assert_eq!(
        ProxyServer::tr_extended_sql(None, &["nope".to_string()], &reg),
        None
    );
}

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut f = vec![tag];
    f.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    f.extend_from_slice(body);
    f
}

#[test]
fn tr_commit_classification_guards_every_query_statement() {
    for sql in [
        "/* audit */ COMMIT",
        "-- ignored\nEND WORK",
        "PREPARE/* nested /* x */ */TRANSACTION 'tx'",
        "SELECT ';COMMIT'; COMMIT PREPARED 'tx'",
        "COMMIT AND CHAIN",
        "ROLLBACK; INSERT INTO t VALUES (1)",
        "SELECT $x$COMMIT$x$;END",
    ] {
        let kind = ProxyServer::tr_classify(sql, &TrReadPolicy::default());
        assert_eq!(kind, StmtKind::Commit, "{sql}");
        assert_eq!(
            ProxyServer::tr_decide(
                TrMode::Transaction,
                FaultPhase::OutcomeUnknown,
                true,
                true,
                true,
                kind
            ),
            TrAction::ErrorAndContinue("08007")
        );
    }
}

#[test]
fn tr_visible_response_progress_never_reexecutes() {
    for action in [
        TrAction::Reexecute,
        TrAction::ReplayThenReexecute,
        TrAction::ErrorAndContinue("08007"),
        TrAction::CloseWithError("57P01"),
    ] {
        for raw in [false, true] {
            for terminal in [false, true] {
                let progress = ResponseProgress {
                    bytes: 7,
                    raw,
                    terminal,
                };
                let guarded = ProxyServer::tr_response_action(action, progress, false);
                assert!(!matches!(
                    guarded,
                    TrAction::Reexecute | TrAction::ReplayThenReexecute
                ));
                if raw || terminal {
                    assert_eq!(guarded, TrAction::CloseIncompleteResponse);
                }
                // Independent of the byte count: an unfinished frame or a
                // published terminal frame forbids injection even if this
                // call recorded no bytes of its own.
                let zero = ResponseProgress {
                    bytes: 0,
                    raw,
                    terminal,
                };
                if raw || terminal {
                    assert_eq!(
                        ProxyServer::tr_response_action(action, zero, false),
                        TrAction::CloseIncompleteResponse
                    );
                }
            }
        }
        // Rows published without a terminal frame: re-execution is
        // refused (it would append a second result), but the session can
        // still be told what happened.
        let published_rows = ResponseProgress {
            bytes: 7,
            raw: false,
            terminal: false,
        };
        let guarded = ProxyServer::tr_response_action(action, published_rows, false);
        assert_ne!(guarded, TrAction::CloseIncompleteResponse);
        assert!(!matches!(
            guarded,
            TrAction::Reexecute | TrAction::ReplayThenReexecute
        ));
        // Backend-watch output may be a raw prefix from an earlier
        // Flush, even if this call has not sent any response bytes.
        assert_eq!(
            ProxyServer::tr_response_action(action, ResponseProgress::default(), true),
            TrAction::CloseIncompleteResponse
        );
        assert_eq!(
            ProxyServer::tr_response_action(action, ResponseProgress::default(), false),
            action
        );
    }
}

#[tokio::test]
async fn tr_stream_fault_retains_visible_rows_and_terminal_frames() {
    for (bytes, terminal) in [
        (Vec::new(), false),
        (
            [frame(b'T', b"description"), frame(b'D', b"row")].concat(),
            false,
        ),
        (
            [
                frame(b'T', b"description"),
                frame(b'D', b"row"),
                frame(b'C', b"SELECT 1\0"),
            ]
            .concat(),
            true,
        ),
        (frame(b'E', b"SERROR\0CXX000\0Mfailed\0\0"), true),
    ] {
        for capture in [false, true] {
            #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
            if capture {
                continue;
            }
            let (mut backend, mut peer) = pair().await;
            let (client, mut recipient) = pair().await;
            let mut client = ClientStream::Plain(client);
            let session = make_test_session();
            let server = ProxyServer::new(test_config()).unwrap();
            let sent = bytes.clone();
            let feed = tokio::spawn(async move {
                peer.write_all(&sent).await.unwrap();
            });
            let receive = tokio::spawn(async move {
                let mut received = Vec::new();
                recipient.read_to_end(&mut received).await.unwrap();
                received
            });
            let failure = if capture {
                #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
                {
                    ProxyServer::stream_until_ready_capture(
                        &mut client,
                        &mut backend,
                        &session,
                        RelayLimits {
                            client_write_timeout: Duration::from_secs(1),
                            backend_read_timeout: Duration::from_secs(1),
                            max_frame_bytes: usize::MAX,
                            response_timeout: None,
                            observation_bytes: usize::MAX,
                            hold_bytes: 0,
                        },
                        16,
                        &server.state.metrics,
                    )
                    .await
                    .unwrap_err()
                }
                #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
                {
                    unreachable!()
                }
            } else {
                ProxyServer::stream_until_ready(&mut client, &mut backend, &session, &server.state)
                    .await
                    .unwrap_err()
            };
            drop(client);
            feed.await.unwrap();
            assert_eq!(receive.await.unwrap(), bytes);
            assert_eq!(failure.progress.bytes, bytes.len() as u64);
            assert_eq!(failure.progress.terminal, terminal);
            assert!(!failure.progress.raw);
            let mut fault = None;
            BackendFault::set_response(&mut fault, "backend", &failure);
            assert_eq!(fault.unwrap().progress.bytes, bytes.len() as u64);
        }
    }
}

#[test]
fn tr_extended_execute_identity_and_commit_boundaries() {
    fn parse(name: &str, sql: &str) -> Vec<u8> {
        frame(b'P', &[cstr(name), cstr(sql), vec![0, 0]].concat())
    }
    fn bind(portal: &str, name: &str) -> Vec<u8> {
        // Binary int32 parameter and binary results. Safety inspection
        // must leave these opaque bytes intact, including embedded NULs.
        frame(
            b'B',
            &[
                cstr(portal),
                cstr(name),
                vec![0, 1, 0, 1, 0, 1, 0, 0, 0, 4, 0, 0, 0, 7, 0, 1, 0, 1],
            ]
            .concat(),
        )
    }
    fn execute(portal: &str) -> Vec<u8> {
        frame(b'E', &[cstr(portal), vec![0; 4]].concat())
    }
    let classify = |frames: Vec<Vec<u8>>| {
        ProxyServer::tr_extended_kind(&frames.concat(), None, 256, &TrReadPolicy::default())
    };
    for end in ["COMMIT", "END", "PREPARE TRANSACTION 'x'", "ROLLBACK"] {
        assert_eq!(
            classify(vec![
                parse("a", "SELECT 1"),
                bind("a", "a"),
                execute("a"),
                parse("b", end),
                bind("b", "b"),
                execute("b"),
                frame(b'S', &[])
            ]),
            StmtKind::Commit,
            "{end}"
        );
    }
    // A portal owns the definition at Bind time, even after the unnamed
    // statement is replaced. Looking up the last Parse would miss COMMIT.
    assert_eq!(
        classify(vec![
            parse("", "COMMIT"),
            bind("saved", ""),
            parse("", "SELECT 1"),
            execute("saved")
        ]),
        StmtKind::Commit
    );
    assert_eq!(
        classify(vec![
            parse("", "SELECT 1"),
            bind("saved", ""),
            parse("", "COMMIT"),
            execute("saved")
        ]),
        StmtKind::Read
    );
    // The unexecuted Parse isn't sufficient evidence of the executed SQL.
    assert_eq!(
        classify(vec![parse("read", "SELECT 1"), execute("older")]),
        StmtKind::Commit
    );
    assert_eq!(
        classify(vec![bind("p", "older"), execute("p")]),
        StmtKind::Commit
    );
    assert_eq!(
        classify(vec![parse("s", "SELECT $1"), bind("p", "s"), execute("p")]),
        StmtKind::Read
    );
    assert_eq!(
        classify(vec![
            parse("s", "SELECT 1"),
            bind("p", "s"),
            frame(b'C', b"Pp\0"),
            execute("p")
        ]),
        StmtKind::Commit
    );
    let held = parse("", "COMMIT");
    let batch = [bind("", ""), execute(""), frame(b'S', &[])].concat();
    assert_eq!(
        ProxyServer::tr_extended_kind(&batch, Some(&held), 256, &TrReadPolicy::default()),
        StmtKind::Commit
    );
    let unnamed_read = [parse("", "SELECT 1"), bind("", ""), execute("")].concat();
    assert_eq!(
        ProxyServer::tr_extended_kind(&unnamed_read, None, 0, &TrReadPolicy::default()),
        StmtKind::Read
    );
    let named_read = [parse("s", "SELECT 1"), bind("p", "s"), execute("p")].concat();
    assert_eq!(
        ProxyServer::tr_extended_kind(&named_read, None, 0, &TrReadPolicy::default()),
        StmtKind::Commit
    );
    assert_eq!(
        ProxyServer::tr_extended_kind(&named_read, None, 1, &TrReadPolicy::default()),
        StmtKind::Read
    );
    let too_many_portals = [
        parse("s", "SELECT 1"),
        bind("p", "s"),
        bind("q", "s"),
        execute("q"),
    ]
    .concat();
    assert_eq!(
        ProxyServer::tr_extended_kind(&too_many_portals, None, 1, &TrReadPolicy::default()),
        StmtKind::Commit
    );
    // Every truncated header/body is opaque; a complete prefix that
    // already Executes COMMIT must remain so, even before final Sync.
    let wire = [held, batch].concat();
    let commit_end = wire.len() - 5;
    for offset in commit_end..=wire.len() {
        assert_eq!(
            ProxyServer::tr_extended_kind(&wire[..offset], None, 256, &TrReadPolicy::default()),
            StmtKind::Commit
        );
    }
    for bytes in [
        vec![b'E'],
        vec![b'E', 0, 0, 0, 3],
        vec![b'P', 255, 255, 255, 255],
    ] {
        assert_eq!(
            ProxyServer::tr_extended_kind(&bytes, None, 256, &TrReadPolicy::default()),
            StmtKind::Commit
        );
    }
}

/// A transport that accepts exactly `limit` bytes, then errors, stalls,
/// or returns zero. This reproduces write_all losing its partial count.
struct PrefixWriter {
    received: Vec<u8>,
    limit: usize,
    failure: u8,
}

impl tokio::io::AsyncWrite for PrefixWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        use std::task::Poll;
        let count = bytes.len().min(self.limit - self.received.len());
        if count == 0 {
            return match self.failure {
                b'T' => Poll::Pending,
                b'Z' => Poll::Ready(Ok(0)),
                _ => Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
            };
        }
        self.received.extend_from_slice(&bytes[..count]);
        Poll::Ready(Ok(count))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn tr_partial_extended_writes_never_authorize_commit_reexecution() {
    let mut batch = Vec::new();
    for sql in ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"] {
        batch.extend(frame(b'P', &[cstr(""), cstr(sql), vec![0, 0]].concat()));
        batch.extend(frame(b'B', &[0; 8]));
        batch.extend(frame(b'E', &[0; 5]));
    }
    batch.extend(frame(b'S', &[]));
    let kind = ProxyServer::tr_extended_kind(&batch, None, 16, &TrReadPolicy::default());
    assert_eq!(kind, StmtKind::Commit);
    // Every possible byte offset includes each frontend frame boundary
    // and a complete COMMIT Execute followed by an incomplete final Sync.
    for limit in 0..batch.len() {
        let mut writer = PrefixWriter {
            received: Vec::new(),
            limit,
            failure: b'E',
        };
        let (_, phase) = ProxyServer::tr_write_batch(&mut writer, &batch, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(writer.received, batch[..limit]);
        for in_tx in [false, true] {
            assert_eq!(
                ProxyServer::tr_decide(TrMode::Transaction, phase, in_tx, true, true, kind),
                TrAction::ErrorAndContinue("08007"),
                "offset={limit}, in_tx={in_tx}"
            );
        }
    }
    for failure in *b"TZ" {
        let mut writer = PrefixWriter {
            received: Vec::new(),
            limit: batch.len() - 5,
            failure,
        };
        let (_, phase) = ProxyServer::tr_write_batch(&mut writer, &batch, Duration::from_millis(5))
            .await
            .unwrap_err();
        assert_eq!(phase, FaultPhase::OutcomeUnknown);
        assert_eq!(writer.received, batch[..batch.len() - 5]);
    }
    let mut writer = PrefixWriter {
        received: Vec::new(),
        limit: batch.len(),
        failure: b'E',
    };
    assert!(
        ProxyServer::tr_write_batch(&mut writer, &batch, Duration::from_secs(1))
            .await
            .is_ok()
    );
    assert_eq!(writer.received, batch);
    // A real pre-dispatch connection failure remains safe to retry.
    assert_eq!(
        ProxyServer::tr_decide(
            TrMode::Transaction,
            FaultPhase::NotDelivered,
            false,
            false,
            false,
            kind
        ),
        TrAction::Reexecute
    );
}

/// One response per call (the replay/restore paths only ever have ONE
/// response outstanding — statement, drain, statement, drain), with
/// the status byte and error flag reported.
#[tokio::test]
async fn drain_until_ready_reports_status_and_errors() {
    let (mut a, mut b) = tokio::io::duplex(4096);
    let mut wire = frame(b'C', &cstr("INSERT 0 1"));
    wire.extend_from_slice(&frame(b'Z', b"T"));
    b.write_all(&wire).await.unwrap();
    let (status, err) =
        ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX, None)
            .await
            .unwrap();
    assert_eq!((status, err), (b'T', false));
    // Error frames are reported, not fatal.
    let mut wire = frame(b'E', &[b'S', 0, b'C', 0, 0]);
    wire.extend_from_slice(&frame(b'Z', b"E"));
    b.write_all(&wire).await.unwrap();
    let (status, err) =
        ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX, None)
            .await
            .unwrap();
    assert_eq!((status, err), (b'E', true));
    // A COPY-in request cannot be satisfied during a replay.
    b.write_all(&frame(b'G', &[0, 0, 0])).await.unwrap();
    assert!(
        ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX, None)
            .await
            .is_err()
    );
    // EOF is an error.
    drop(b);
    assert!(
        ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX, None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn drain_until_ready_times_out_on_silent_backend() {
    let (mut a, _b) = tokio::io::duplex(64);
    let r =
        ProxyServer::drain_until_ready(&mut a, Duration::from_millis(50), usize::MAX, None).await;
    assert!(matches!(r, Err(ProxyError::Network(_))));
}

#[tokio::test]
async fn tr_run_discard_and_restore_session_state() {
    let (mut a, mut b) = tokio::io::duplex(4096);
    // Backend: answers the first statement OK, rejects the second —
    // one response per received Query, like a real server.
    let backend = tokio::spawn(async move {
        let mut seen = Vec::new();
        for i in 0..2 {
            let mut hdr = [0u8; 5];
            b.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
            let mut body = vec![0u8; len - 4];
            b.read_exact(&mut body).await.unwrap();
            seen.push((
                hdr[0],
                crate::protocol::query_text(&body).unwrap().to_string(),
            ));
            let mut out = if i == 0 {
                frame(b'C', &cstr("SET"))
            } else {
                frame(b'E', &[b'C', 0, 0])
            };
            out.extend_from_slice(&frame(b'Z', b"I"));
            b.write_all(&out).await.unwrap();
        }
        seen
    });
    let gucs = vec!["SET a = 1".to_string(), "SET b = 2".to_string()];
    let r = ProxyServer::tr_restore_session_state(
        &mut a,
        &gucs,
        Duration::from_secs(5),
        Duration::from_secs(5),
        usize::MAX,
    )
    .await;
    assert!(matches!(r, Err(ProxyError::Protocol(_))), "{r:?}");
    // Both Query frames reached the backend, in order.
    let seen = backend.await.unwrap();
    assert_eq!(
        seen,
        vec![
            (b'Q', "SET a = 1".to_string()),
            (b'Q', "SET b = 2".to_string())
        ]
    );
    // Empty list restores nothing and succeeds.
    let (mut a2, _b2) = tokio::io::duplex(64);
    assert_eq!(
        ProxyServer::tr_restore_session_state(
            &mut a2,
            &[],
            Duration::from_secs(1),
            Duration::from_secs(1),
            usize::MAX,
        )
        .await
        .unwrap(),
        0
    );
}

/// Fake PG backend: answers every simple Query with `CommandComplete` +
/// `ReadyForQuery('T')`; a query containing "boom" gets ErrorResponse +
/// RFQ('E'). Received query texts are pushed to `seen`.
async fn fake_backend(listener: tokio::net::TcpListener, seen: Arc<std::sync::Mutex<Vec<String>>>) {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut buf = BytesMut::with_capacity(4096);
    loop {
        buf.reserve(4096);
        match sock.read_buf(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        while buf.len() >= 5 {
            let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            if buf.len() < len + 1 {
                break;
            }
            let f = buf.split_to(len + 1);
            if f[0] == b'Q' {
                let q = crate::protocol::query_text(&f[5..])
                    .unwrap_or("")
                    .to_string();
                let fail = q.contains("boom");
                seen.lock().unwrap().push(q);
                let mut out = if fail {
                    frame(b'E', &[b'C', b'4', b'2', b'0', b'0', b'0', 0, 0])
                } else {
                    frame(b'C', &cstr("OK"))
                };
                out.extend_from_slice(&frame(b'Z', if fail { b"E" } else { b"T" }));
                sock.write_all(&out).await.unwrap();
            }
        }
    }
}

fn simple_log(sql: &str) -> StatementLog {
    StatementLog {
        sql: sql.to_string(),
        params: Vec::new(),
        result_checksum: None,
        executed_at: chrono::Utc::now(),
        extended: None,
    }
}

#[tokio::test]
async fn tr_replay_transaction_replays_in_order_and_reports_rejections() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    tokio::spawn(fake_backend(listener, seen.clone()));
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    conns.insert(
        addr.clone(),
        BackendConn::new(TcpStream::connect(&addr).await.unwrap()),
    );
    let registry: HashMap<String, bytes::Bytes> = HashMap::new();
    let entries = vec![
        simple_log("BEGIN"),
        simple_log("INSERT INTO t VALUES (1)"),
        simple_log("SAVEPOINT a"),
    ];
    let r =
        ProxyServer::tr_replay_transaction(&mut conns, &addr, &entries, &registry, &state).await;
    assert!(r.is_ok());
    assert_eq!(
        *seen.lock().unwrap(),
        vec!["BEGIN", "INSERT INTO t VALUES (1)", "SAVEPOINT a"]
    );
    // A rejected statement stops the replay with a Statement failure.
    let entries = vec![
        simple_log("BEGIN"),
        simple_log("INSERT boom"),
        simple_log("SELECT 1"),
    ];
    let r =
        ProxyServer::tr_replay_transaction(&mut conns, &addr, &entries, &registry, &state).await;
    match r {
        Err(ReplayFailure::Statement(d)) => assert!(d.contains("2/3"), "{d}"),
        _ => panic!("expected statement failure"),
    }
    assert_eq!(
        seen.lock().unwrap().len(),
        5,
        "replay stopped at the failure"
    );
    // A missing connection is a Backend failure.
    let r =
        ProxyServer::tr_replay_transaction(&mut conns, "127.0.0.1:1", &entries, &registry, &state)
            .await;
    assert!(matches!(r, Err(ReplayFailure::Backend(_))));
}

fn qmsg(sql: &str) -> Message {
    QueryMessage {
        query: sql.to_string(),
    }
    .encode()
}

/// Drive the recorder through a transaction: BEGIN/INSERT/SET are
/// recorded (with write tracking), the statement cap marks the
/// transaction non-replayable (+ metric), COMMIT releases the record
/// and promotes transaction-scoped SETs, and the SET cap stops tracking.
/// A transaction that ends in ROLLBACK discards its session state, so
/// SETs made inside it must never be restored on the replacement backend.
/// `ROLLBACK; <DML>` is the trap: for replay safety it classifies as a
/// possible commit (the trailing statement commits in autocommit), and
/// reusing that classification for the GUC decision would restore
/// settings the database threw away.
#[tokio::test]
async fn tr_recorder_does_not_promote_gucs_of_a_rolled_back_transaction() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Transaction);

    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SET work_mem = '512MB'"), &session, &state).await;
    assert_eq!(
        tr.pending_tx_gucs,
        vec![GucOp::Set("SET work_mem = '512MB'".to_string())]
    );

    // Ends the transaction as a ROLLBACK, while the trailing statement
    // commits on its own — `StmtKind::Commit`, but nothing of the
    // transaction's own session state survived.
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ProxyServer::tr_after_simple(
        &mut tr,
        &qmsg("ROLLBACK; INSERT INTO t VALUES (1)"),
        &session,
        &state,
    )
    .await;
    assert!(
        tr.gucs.is_empty(),
        "a rolled-back transaction's SETs must not be restored: {:?}",
        tr.gucs
    );
    assert!(tr.pending_tx_gucs.is_empty(), "pending set must be dropped");

    // The same shape ending in a real COMMIT still promotes.
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SET work_mem = '64MB'"), &session, &state).await;
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("COMMIT"), &session, &state).await;
    assert_eq!(tr.gucs, vec!["SET work_mem = '64MB'"]);
}

/// TR-04: the three harness scenarios that 1.6.1 failed. GUC changes
/// inside a transaction are deferred and follow PostgreSQL's own
/// rollback semantics, including savepoints; variables are keyed by
/// name so a repeated SET does not consume another cap slot.
#[tokio::test]
async fn tr_recorder_session_gucs_are_transactional_and_savepoint_scoped() {
    let mut config = test_config();
    config.limits.tr_max_session_set_statements = 1;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Session);
    async fn step(
        tr: &mut TrSession,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        status: u8,
        sql: &str,
    ) {
        ProxyServer::note_ready_for_query(session, status, false);
        ProxyServer::tr_after_simple(tr, &qmsg(sql), session, state).await;
    }

    // guc_cap: a repeated SET of one variable replaces, cap 1 is enough
    // and the LATEST value is what gets restored.
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        "SET application_name = 'first'",
    )
    .await;
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        "SET application_name = 'latest'",
    )
    .await;
    assert_eq!(tr.gucs, vec!["SET application_name = 'latest'"]);
    assert!(!tr.guc_cap_hit);
    // A second DISTINCT variable exceeds the cap; the failover is then
    // refused rather than restoring a partial set.
    step(&mut tr, &session, &state, b'I', "SET work_mem = '64MB'").await;
    assert!(tr.guc_cap_hit);
    assert!(ProxyServer::tr_restore_preflight(&tr).is_err());
    step(&mut tr, &session, &state, b'I', "RESET ALL").await;
    assert!(tr.gucs.is_empty() && !tr.guc_cap_hit);
    assert!(ProxyServer::tr_restore_preflight(&tr).is_ok());

    // guc_savepoint: a SET after a savepoint that is rolled back to is
    // undone; the base value committed before the transaction survives.
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        "SET application_name = 'base'",
    )
    .await;
    step(&mut tr, &session, &state, b'T', "BEGIN").await;
    step(&mut tr, &session, &state, b'T', "SAVEPOINT s").await;
    step(
        &mut tr,
        &session,
        &state,
        b'T',
        "SET application_name = 'undone'",
    )
    .await;
    step(&mut tr, &session, &state, b'T', "ROLLBACK TO s").await;
    step(&mut tr, &session, &state, b'I', "COMMIT").await;
    assert_eq!(tr.gucs, vec!["SET application_name = 'base'"]);
    assert!(tr.pending_tx_gucs.is_empty() && tr.tx_savepoints.is_empty());

    // RELEASE keeps the SET made after the savepoint.
    step(&mut tr, &session, &state, b'T', "BEGIN").await;
    step(&mut tr, &session, &state, b'T', "SAVEPOINT \"S2\"").await;
    step(
        &mut tr,
        &session,
        &state,
        b'T',
        "SET application_name = 'kept'",
    )
    .await;
    step(&mut tr, &session, &state, b'T', "RELEASE SAVEPOINT \"S2\"").await;
    step(&mut tr, &session, &state, b'I', "COMMIT").await;
    assert_eq!(tr.gucs, vec!["SET application_name = 'kept'"]);

    // guc_reset_rollback: RESET ALL inside a rolled-back transaction
    // is undone — the committed value is still restored.
    step(&mut tr, &session, &state, b'T', "BEGIN").await;
    step(&mut tr, &session, &state, b'T', "RESET ALL").await;
    assert_eq!(tr.pending_tx_gucs, vec![GucOp::ResetAll]);
    assert_eq!(tr.gucs, vec!["SET application_name = 'kept'"], "deferred");
    step(&mut tr, &session, &state, b'I', "ROLLBACK").await;
    assert_eq!(tr.gucs, vec!["SET application_name = 'kept'"]);
    // ...and applied when the transaction commits.
    step(&mut tr, &session, &state, b'T', "BEGIN").await;
    step(&mut tr, &session, &state, b'T', "RESET application_name").await;
    step(&mut tr, &session, &state, b'I', "COMMIT").await;
    assert!(tr.gucs.is_empty());
}

/// 75c0c5eceac3: extended-protocol cycles settle deferred settings,
/// whichever protocol deferred them; a cycle the proxy cannot read makes the
/// session's settings unknown, which keeps its reads home and refuses its
/// failover until `DISCARD ALL`.
#[tokio::test]
async fn tr_extended_cycles_settle_deferred_settings() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Session);
    let ext = |tr: &mut TrSession, op: Option<SettingsOp>, idle_after_sync: bool| {
        ProxyServer::tr_ext_set_outcome(
            tr,
            ExtSetOutcome {
                pin: false,
                ops: op.into_iter().collect(),
            },
            idle_after_sync,
            &session,
            &state,
        )
    };
    let untracked = || {
        state
            .metrics
            .tr
            .session_set_untracked
            .load(Ordering::Relaxed)
    };
    async fn simple(
        tr: &mut TrSession,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        status: u8,
        sql: &str,
    ) {
        ProxyServer::note_ready_for_query(session, status, false);
        ProxyServer::tr_after_simple(tr, &qmsg(sql), session, state).await;
    }
    let app = |v: &str| format!("SET application_name = '{v}'");

    // Mixed protocol: a simple-protocol SET inside a transaction that an
    // extended-protocol COMMIT ends is promoted...
    simple(&mut tr, &session, &state, b'T', "BEGIN").await;
    simple(&mut tr, &session, &state, b'T', &app("mixed")).await;
    assert_eq!(tr.pending_tx_gucs.len(), 1);
    ProxyServer::note_ready_for_query(&session, b'I', false);
    assert!(!ext(&mut tr, Some(SettingsOp::End(Some(true))), true));
    tr.prev_status = b'I'; // the client loop's baseline after the cycle
    assert_eq!(tr.gucs, vec![app("mixed")]);
    assert!(!ProxyServer::tr_tx_settings_open(&tr));
    // ...and one an extended-protocol ROLLBACK ends is dropped.
    simple(&mut tr, &session, &state, b'T', "BEGIN").await;
    simple(&mut tr, &session, &state, b'T', &app("dropped")).await;
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ext(&mut tr, Some(SettingsOp::End(Some(false))), true);
    tr.prev_status = b'I';
    assert_eq!(tr.gucs, vec![app("mixed")]);
    assert!(!ProxyServer::tr_tx_settings_open(&tr));

    // Extended-protocol SETs and savepoints inside a transaction, ended by
    // a simple-protocol COMMIT: ROLLBACK WORK TO undoes the later SET.
    simple(&mut tr, &session, &state, b'T', "BEGIN").await;
    ext(&mut tr, Some(SettingsOp::Defer(app("kept"))), false);
    ext(
        &mut tr,
        Some(SettingsOp::Savepoint("SAVEPOINT s".into())),
        false,
    );
    ext(
        &mut tr,
        Some(SettingsOp::Defer("/* tag */ SET work_mem = '1MB'".into())),
        false,
    );
    ext(
        &mut tr,
        Some(SettingsOp::Savepoint("ROLLBACK WORK TO SAVEPOINT s".into())),
        false,
    );
    assert_eq!(tr.pending_tx_gucs, vec![GucOp::Set(app("kept"))]);
    tr.prev_status = b'T';
    simple(&mut tr, &session, &state, b'I', "COMMIT").await;
    assert_eq!(tr.gucs, vec![app("kept")]);
    assert!(!tr.gucs_unknown && untracked() == 0);

    // A tainted transaction that rolls back leaves the settings known...
    simple(&mut tr, &session, &state, b'T', "BEGIN").await;
    ext(
        &mut tr,
        Some(SettingsOp::Taint {
            settings: true,
            savepoints: false,
        }),
        false,
    );
    simple(&mut tr, &session, &state, b'I', "ROLLBACK").await;
    assert!(!tr.gucs_unknown && ProxyServer::tr_restore_preflight(&tr).is_ok());
    // ...one that commits does not: reads stay home, the failover is refused.
    simple(&mut tr, &session, &state, b'T', "BEGIN").await;
    ext(
        &mut tr,
        Some(SettingsOp::Taint {
            settings: true,
            savepoints: false,
        }),
        false,
    );
    assert!(!session.reads_pinned.load(Ordering::Relaxed));
    simple(&mut tr, &session, &state, b'I', "COMMIT").await;
    assert!(tr.gucs_unknown);
    assert!(session.reads_pinned.load(Ordering::Relaxed));
    assert_eq!(untracked(), 1);
    let refused = ProxyServer::tr_restore_preflight(&tr).unwrap_err();
    assert!(refused.to_string().contains("cannot follow"), "{refused}");
    // Unknown again: counted once per session.
    ext(&mut tr, Some(SettingsOp::Unknown), true);
    assert_eq!(untracked(), 1);
    // DISCARD ALL resets every setting: known again.
    ext(&mut tr, Some(SettingsOp::Apply("DISCARD ALL".into())), true);
    assert!(!tr.gucs_unknown && tr.gucs.is_empty());
    assert!(ProxyServer::tr_restore_preflight(&tr).is_ok());

    // A ROLLBACK TO a savepoint the proxy has no record of undid an unknown
    // share of the deferred SETs.
    simple(&mut tr, &session, &state, b'T', "BEGIN").await;
    ext(&mut tr, Some(SettingsOp::Defer(app("maybe"))), false);
    ext(
        &mut tr,
        Some(SettingsOp::Savepoint("ROLLBACK TO unseen".into())),
        false,
    );
    assert!(tr.tx_gucs_unknown);
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ext(&mut tr, Some(SettingsOp::End(Some(true))), true);
    tr.prev_status = b'I';
    assert!(tr.gucs_unknown);
    ext(&mut tr, Some(SettingsOp::Apply("DISCARD ALL".into())), true);

    // Unseen savepoints with deferred SETs pending taint the transaction;
    // without any they only forget the savepoints.
    simple(&mut tr, &session, &state, b'T', "BEGIN").await;
    ext(
        &mut tr,
        Some(SettingsOp::Savepoint("SAVEPOINT a".into())),
        false,
    );
    let lost = Some(SettingsOp::Taint {
        settings: false,
        savepoints: true,
    });
    ext(&mut tr, lost, false);
    assert!(tr.tx_savepoints.is_empty() && !tr.tx_gucs_unknown);
    ext(&mut tr, Some(SettingsOp::Defer(app("x"))), false);
    ext(
        &mut tr,
        Some(SettingsOp::Taint {
            settings: false,
            savepoints: true,
        }),
        false,
    );
    assert!(tr.tx_gucs_unknown);
    simple(&mut tr, &session, &state, b'I', "ROLLBACK").await;
    assert!(!tr.gucs_unknown);

    // The transaction ended where no cycle showed it end: an idle
    // ReadyForQuery with deferred SETs still pending settles them as
    // unknown, after an extended cycle or a simple statement alike.
    simple(&mut tr, &session, &state, b'T', "BEGIN").await;
    simple(&mut tr, &session, &state, b'T', &app("lost")).await;
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ext(&mut tr, None, true);
    assert!(tr.gucs_unknown && !ProxyServer::tr_tx_settings_open(&tr));
    ext(&mut tr, Some(SettingsOp::Apply("DISCARD ALL".into())), true);
    tr.prev_status = b'I';
    tr.pending_tx_gucs.push(GucOp::Set(app("stale")));
    simple(&mut tr, &session, &state, b'I', "SELECT 1").await;
    assert!(tr.gucs_unknown && tr.pending_tx_gucs.is_empty());
}

/// 72494e4aaebf: a multi-statement simple query is read statement by
/// statement, like an extended-protocol cycle: a pool reset forgets the
/// previous user's settings, a failed batch changes nothing, a chained COMMIT
/// applies the transaction's SETs.
#[tokio::test]
async fn tr_simple_queries_are_read_statement_by_statement() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Session);
    async fn step(
        tr: &mut TrSession,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        status: u8,
        failed: bool,
        sql: &str,
    ) {
        ProxyServer::note_ready_for_query(session, status, failed);
        ProxyServer::tr_after_simple(tr, &qmsg(sql), session, state).await;
    }
    let tracked = |tr: &TrSession, needle: &str| tr.gucs.iter().any(|g| g.contains(needle));
    let search_path = "SET search_path TO tenant_a";

    // Npgsql's pool reset when it keeps prepared statements.
    step(&mut tr, &session, &state, b'I', false, search_path).await;
    step(&mut tr, &session, &state, b'I', false, "SET ROLE tenant_a").await;
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;CLOSE ALL;UNLISTEN *;\
         SELECT pg_advisory_unlock_all();DISCARD SEQUENCES;DISCARD TEMP",
    )
    .await;
    assert!(!tracked(&tr, "search_path"), "{:?}", tr.gucs);
    assert!(
        tracked(&tr, "SESSION AUTHORIZATION DEFAULT"),
        "{:?}",
        tr.gucs
    );
    assert!(!tr.gucs_unknown);
    // asyncpg's.
    step(&mut tr, &session, &state, b'I', false, search_path).await;
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        "SELECT pg_advisory_unlock_all(); CLOSE ALL; UNLISTEN *; RESET ALL;",
    )
    .await;
    assert!(!tracked(&tr, "search_path"), "{:?}", tr.gucs);
    // A SET batched with a read; a quoted ';' is part of a value.
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        "SET work_mem = '8MB'; SELECT 1",
    )
    .await;
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        "SET application_name = 'a;b'",
    )
    .await;
    assert!(
        tracked(&tr, "8MB") && tracked(&tr, "'a;b'"),
        "{:?}",
        tr.gucs
    );
    // A failed batch: its implicit transaction rolled back with the SET.
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        true,
        "SET work_mem = '64MB'; SELECT 1/0",
    )
    .await;
    assert!(
        tracked(&tr, "8MB") && !tracked(&tr, "64MB"),
        "{:?}",
        tr.gucs
    );
    // A batch that is a whole transaction.
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        "BEGIN; SET work_mem = '32MB'; COMMIT",
    )
    .await;
    assert!(tracked(&tr, "32MB"), "{:?}", tr.gucs);
    // COMMIT AND CHAIN applies; the chained transaction's ROLLBACK keeps it.
    step(&mut tr, &session, &state, b'T', false, "BEGIN").await;
    step(
        &mut tr,
        &session,
        &state,
        b'T',
        false,
        "SET statement_timeout = '5s'",
    )
    .await;
    step(&mut tr, &session, &state, b'T', false, "COMMIT AND CHAIN").await;
    step(&mut tr, &session, &state, b'I', false, "ROLLBACK").await;
    assert!(tracked(&tr, "statement_timeout"), "{:?}", tr.gucs);
    // A transaction that ended on a statement no reading explains: unknown.
    step(&mut tr, &session, &state, b'T', false, "BEGIN").await;
    step(
        &mut tr,
        &session,
        &state,
        b'T',
        false,
        "SET lock_timeout = '1s'",
    )
    .await;
    step(&mut tr, &session, &state, b'I', false, "SELECT 1").await;
    assert!(tr.gucs_unknown);
    step(&mut tr, &session, &state, b'I', false, "DISCARD ALL").await;
    assert!(!tr.gucs_unknown && tr.gucs.is_empty());
    // A query that does not lex ran nothing...
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        true,
        "SET a = 'open; SELECT 1",
    )
    .await;
    step(&mut tr, &session, &state, b'I', true, "SET a = 'open").await;
    assert!(!tr.gucs_unknown && tr.gucs.is_empty());
    // ...but one whose statements depend on the session's backslash reading
    // may have run any of them.
    step(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        "SET application_name = 'x\\'; SET work_mem = '1MB'; --'",
    )
    .await;
    assert!(tr.gucs_unknown);
}

/// PostgreSQL's SET/RESET SESSION AUTHORIZATION also ends a SET ROLE: the
/// restore set must not bring the role back, and a role set after it must be
/// restored after it. Two pool resets on one connection, each followed by
/// another tenant's SET ROLE, used to restore the session authorization last,
/// switching the role off (the second tenant ran as the pool user).
#[tokio::test]
async fn tr_session_authorization_ends_the_role() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Session);
    let reset = "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;CLOSE ALL";
    for sql in ["SET ROLE tenant_a", reset, "SET ROLE tenant_b"] {
        ProxyServer::note_ready_for_query(&session, b'I', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg(sql), &session, &state).await;
    }
    assert_eq!(
        tr.gucs,
        vec!["SET SESSION AUTHORIZATION DEFAULT", "SET ROLE tenant_b"]
    );
    for sql in [reset, "SET ROLE tenant_c"] {
        ProxyServer::tr_after_simple(&mut tr, &qmsg(sql), &session, &state).await;
    }
    assert_eq!(
        tr.gucs,
        vec!["SET SESSION AUTHORIZATION DEFAULT", "SET ROLE tenant_c"]
    );
    ProxyServer::tr_after_simple(
        &mut tr,
        &qmsg("RESET SESSION AUTHORIZATION"),
        &session,
        &state,
    )
    .await;
    assert!(tr.gucs.is_empty(), "{:?}", tr.gucs);
}

/// Simple queries: a transaction the query opens and rolls back, a SQL
/// function body, `set_config`, text that is not UTF-8, a query that yields
/// for COPY, and a response the proxy answered itself.
#[tokio::test]
async fn tr_simple_query_edge_cases() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Session);
    let tracked = |tr: &TrSession, needle: &str| tr.gucs.iter().any(|g| g.contains(needle));
    async fn send(
        tr: &mut TrSession,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        status: u8,
        failed: bool,
        msg: Message,
    ) {
        ProxyServer::note_ready_for_query(session, status, failed);
        ProxyServer::tr_after_simple(tr, &msg, session, state).await;
    }
    let raw = |bytes: &[u8]| Message {
        msg_type: crate::protocol::MessageType::Query,
        payload: bytes::BytesMut::from(bytes),
    };

    // A whole transaction in one query whose COMMIT failed: nothing changed,
    // nothing unknown.
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        true,
        qmsg("BEGIN; SET search_path TO app; INSERT INTO t VALUES (1); COMMIT"),
    )
    .await;
    assert!(!tr.gucs_unknown && tr.gucs.is_empty(), "{:?}", tr.gucs);
    // A SQL-standard function body's own `;`s.
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        qmsg(
            "SET work_mem = '2MB'; CREATE FUNCTION f() RETURNS int LANGUAGE sql \
         BEGIN ATOMIC SELECT 1; END",
        ),
    )
    .await;
    assert!(!tr.gucs_unknown && tracked(&tr, "2MB"), "{:?}", tr.gucs);
    // set_config: local is fine, session-level cannot be recorded.
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        qmsg("select set_config('role', 'anon', true)"),
    )
    .await;
    assert!(!tr.gucs_unknown);
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        qmsg("select set_config('search_path', '', false)"),
    )
    .await;
    assert!(tr.gucs_unknown);
    send(&mut tr, &session, &state, b'I', false, qmsg("DISCARD ALL")).await;
    assert!(!tr.gucs_unknown && tr.gucs.is_empty());
    // A prepared set_config survives the rollback of its transaction.
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        qmsg("BEGIN; PREPARE p AS SELECT set_config('a', 'b', false); ROLLBACK"),
    )
    .await;
    assert!(tr.gucs_unknown);
    send(&mut tr, &session, &state, b'I', false, qmsg("DISCARD ALL")).await;
    // A `begin` parameter is no function body: the SET after it counts.
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        qmsg(
            "CREATE FUNCTION f(begin date) RETURNS int LANGUAGE sql AS $$SELECT 1$$; \
              SET search_path = app",
        ),
    )
    .await;
    assert!(tracked(&tr, "search_path = app"), "{:?}", tr.gucs);
    send(&mut tr, &session, &state, b'I', false, qmsg("DISCARD ALL")).await;

    // Not UTF-8 (LATIN1): a SET inside a transaction that rolls back is no
    // loss; one that applies cannot be recorded.
    send(&mut tr, &session, &state, b'T', false, qmsg("BEGIN")).await;
    send(
        &mut tr,
        &session,
        &state,
        b'T',
        false,
        raw(b"SET application_name = 'caf\xe9'\0"),
    )
    .await;
    send(&mut tr, &session, &state, b'I', false, qmsg("ROLLBACK")).await;
    assert!(!tr.gucs_unknown);
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        raw(b"SET application_name = 'caf\xe9'\0"),
    )
    .await;
    assert!(tr.gucs_unknown);
    send(&mut tr, &session, &state, b'I', false, qmsg("DISCARD ALL")).await;
    // ...while its other statements are read as usual.
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        raw(b"SELECT 'caf\xe9'; SET work_mem = '4MB'\0"),
    )
    .await;
    // (In encodings whose trail bytes can be `;` the lossy split cannot be
    // trusted where a SET is involved.)
    assert!(tr.gucs_unknown, "{:?}", tr.gucs);
    send(&mut tr, &session, &state, b'I', false, qmsg("DISCARD ALL")).await;
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        raw(b"SELECT 'caf\xe9'; SELECT 2\0"),
    )
    .await;
    assert!(!tr.gucs_unknown);
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        raw(b"SELECT 1; SET application_name = 'caf\xe9'\0"),
    )
    .await;
    assert!(tr.gucs_unknown);
    send(&mut tr, &session, &state, b'I', false, qmsg("DISCARD ALL")).await;

    // A query the proxy answered itself (the aborted-transaction emulation)
    // changed nothing.
    tr.skip_settings = true;
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        qmsg("SET work_mem = '1GB'"),
    )
    .await;
    assert!(!tracked(&tr, "1GB"));
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        qmsg("SET work_mem = '8MB'"),
    )
    .await;
    assert!(tracked(&tr, "8MB"));

    // A query that yields for COPY is read at the drain.
    session.copy_in_progress.store(true, Ordering::Relaxed);
    send(
        &mut tr,
        &session,
        &state,
        b'I',
        false,
        qmsg("SET work_mem = '16MB'; COPY t FROM STDIN"),
    )
    .await;
    assert!(!tracked(&tr, "16MB"), "not before the drain");
    session.copy_in_progress.store(false, Ordering::Relaxed);
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ProxyServer::tr_after_copy_drain(&mut tr, &session, &state).await;
    assert!(
        tracked(&tr, "16MB") && tr.copy_query.is_none(),
        "{:?}",
        tr.gucs
    );
}

/// Savepoint names are read like PostgreSQL reads them (comments aside,
/// quoted names whole), and the tracked savepoints are capped by `[limits]
/// tr_max_tracked_savepoints`.
#[test]
fn tr_savepoint_names_are_lexed_and_capped() {
    let set = |v: &str| GucOp::Set(format!("SET application_name = '{v}'"));
    let note = ProxyServer::tr_note_savepoint;
    let mut tr = TrSession::new(TrMode::Session);
    // A comment inside ROLLBACK TO.
    tr.pending_tx_gucs = vec![set("a")];
    note(&mut tr, "SAVEPOINT s", 8);
    tr.pending_tx_gucs.push(set("b"));
    note(&mut tr, "ROLLBACK /* undo */ TO s", 8);
    assert_eq!(tr.pending_tx_gucs, vec![set("a")]);
    // Quoted names with spaces are whole names.
    let mut tr = TrSession::new(TrMode::Session);
    note(&mut tr, "SAVEPOINT \"my a\"", 8);
    tr.pending_tx_gucs.push(set("x"));
    note(&mut tr, "SAVEPOINT \"my b\"", 8);
    note(&mut tr, "ROLLBACK WORK TO \"my a\"", 8);
    assert!(tr.pending_tx_gucs.is_empty(), "x came after \"my a\"");
    // A savepoint the proxy has no record of, with SETs pending.
    tr.pending_tx_gucs.push(set("y"));
    note(&mut tr, "ROLLBACK TRANSACTION TO SAVEPOINT nowhere", 8);
    assert!(tr.tx_gucs_unknown);
    // Past the cap the records start over; a ROLLBACK TO a dropped one
    // makes the transaction's settings unknown.
    let mut tr = TrSession::new(TrMode::Session);
    for n in ["a", "b", "c"] {
        note(&mut tr, &format!("SAVEPOINT {n}"), 2);
        tr.pending_tx_gucs.push(set(n));
    }
    assert_eq!(tr.tx_savepoints.len(), 1);
    note(&mut tr, "ROLLBACK TO c", 2);
    assert_eq!(tr.pending_tx_gucs, vec![set("a"), set("b")]);
    assert!(!tr.tx_gucs_unknown);
    note(&mut tr, "ROLLBACK TO a", 2);
    assert!(tr.tx_gucs_unknown);
}

#[test]
fn tr_guc_op_sees_through_leading_comments() {
    let op = ProxyServer::tr_guc_op;
    assert_eq!(
        op("/* app=shop */ SET search_path TO tenant1"),
        Some(GucOp::Set("SET search_path TO tenant1".to_string()))
    );
    assert_eq!(
        op("-- note\n  set work_mem = '64MB';"),
        Some(GucOp::Set("set work_mem = '64MB';".to_string()))
    );
    assert_eq!(
        op("/* a /* nested */ b */ RESET work_mem"),
        Some(GucOp::Reset("work_mem".to_string()))
    );
    assert_eq!(op("/* x */ RESET ALL"), Some(GucOp::ResetAll));
    assert_eq!(op("DISCARD ALL"), Some(GucOp::DiscardAll));
    assert_eq!(
        op("RESET /* c */ search_path"),
        Some(GucOp::Reset("search_path".to_string()))
    );
    assert_eq!(
        op("RESET work_mem /* all */"),
        Some(GucOp::Reset("work_mem".to_string()))
    );
    assert_eq!(
        op("-- t\rSET search_path TO t2"),
        Some(GucOp::Set("SET search_path TO t2".to_string()))
    );
    for none in [
        "SET LOCAL work_mem = '1MB'",
        "/* x */ SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        "DISCARD PLANS",
        "SET a = 1; SET b = 2",
        "SELECT 1",
        "/* unterminated SET a = 1",
    ] {
        assert_eq!(op(none), None, "{none:?}");
    }
    let like = |sql: &str| ProxyServer::tr_is_set_like_bytes(sql.as_bytes());
    assert!(like("/* app=shop */ set search_path TO x"));
    assert!(like("-- c\nDISCARD ALL"));
    assert!(!like("SELECT 'SET a = 1'"));
    assert!(!like("SETTINGS"));
}

/// sqlcommenter tags every statement with a leading comment: the tracker
/// must still see the SETs, RESETs and savepoints behind it, or offloaded
/// reads (and a TR re-home) would run with the default settings.
#[tokio::test]
async fn tr_recorder_tracks_gucs_behind_leading_comments() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Session);
    async fn step(
        tr: &mut TrSession,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        status: u8,
        sql: &str,
    ) {
        ProxyServer::note_ready_for_query(session, status, false);
        ProxyServer::tr_after_simple(tr, &qmsg(sql), session, state).await;
    }
    let s = (&session, &state);
    step(
        &mut tr,
        s.0,
        s.1,
        b'I',
        "/* app=shop */ SET search_path TO tenant1",
    )
    .await;
    step(
        &mut tr,
        s.0,
        s.1,
        b'I',
        "/* a */ -- b\n SET work_mem = '64MB'",
    )
    .await;
    assert_eq!(
        tr.gucs,
        vec!["SET search_path TO tenant1", "SET work_mem = '64MB'"]
    );
    let epoch = tr.guc_epoch;
    step(&mut tr, s.0, s.1, b'I', "/* app=shop */ RESET work_mem").await;
    assert_eq!(tr.gucs, vec!["SET search_path TO tenant1"]);
    assert!(tr.guc_epoch > epoch);

    // A savepoint behind a comment still scopes the SETs made after it.
    step(&mut tr, s.0, s.1, b'T', "BEGIN").await;
    step(&mut tr, s.0, s.1, b'T', "/* c */ SAVEPOINT s1").await;
    step(
        &mut tr,
        s.0,
        s.1,
        b'T',
        "/* c */ SET search_path TO tenant2",
    )
    .await;
    step(&mut tr, s.0, s.1, b'T', "/* c */ ROLLBACK TO SAVEPOINT s1").await;
    step(&mut tr, s.0, s.1, b'I', "COMMIT").await;
    assert_eq!(tr.gucs, vec!["SET search_path TO tenant1"]);

    step(&mut tr, s.0, s.1, b'I', "/* app=shop */ RESET ALL").await;
    assert!(tr.gucs.is_empty());
}

/// PostgreSQL's `RESET ALL` keeps the role and the session authorization;
/// `DISCARD ALL` resets them too. Both always fit under the cap. (The role is
/// set after the session authorization, which would otherwise end it.)
#[tokio::test]
async fn tr_recorder_reset_all_keeps_the_role() {
    let mut config = test_config();
    config.limits.tr_max_session_set_statements = 1;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Session);
    async fn step(
        tr: &mut TrSession,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        sql: &str,
    ) {
        ProxyServer::note_ready_for_query(session, b'I', false);
        ProxyServer::tr_after_simple(tr, &qmsg(sql), session, state).await;
    }
    for sql in [
        "SET work_mem = '64MB'",
        "SET SESSION AUTHORIZATION app",
        "SET ROLE tenant_a",
    ] {
        step(&mut tr, &session, &state, sql).await;
    }
    assert_eq!(
        tr.gucs,
        vec![
            "SET work_mem = '64MB'",
            "SET SESSION AUTHORIZATION app",
            "SET ROLE tenant_a"
        ],
        "the role and the session authorization fit past the cap"
    );
    assert!(!tr.guc_cap_hit);
    step(&mut tr, &session, &state, "SET search_path TO x").await;
    assert!(tr.guc_cap_hit);
    assert!(ProxyServer::tr_sets_role(&tr.gucs));

    step(&mut tr, &session, &state, "RESET ALL").await;
    assert_eq!(
        tr.gucs,
        vec!["SET SESSION AUTHORIZATION app", "SET ROLE tenant_a"]
    );
    assert!(!tr.guc_cap_hit);

    step(&mut tr, &session, &state, "RESET ROLE").await;
    assert_eq!(tr.gucs, vec!["SET SESSION AUTHORIZATION app"]);

    step(&mut tr, &session, &state, "DISCARD ALL").await;
    assert!(tr.gucs.is_empty());
}

/// A SERIALIZABLE default isolation level (either spelling) is flagged: a hot
/// standby refuses such reads, so they stay on the primary.
#[tokio::test]
async fn tr_recorder_flags_a_serializable_default() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Session);
    for (sql, serializable) in [
        ("SET default_transaction_isolation = 'serializable'", true),
        (
            "SET default_transaction_isolation = 'read committed'",
            false,
        ),
        (
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            true,
        ),
        ("RESET ALL", false),
        (
            "/* jdbc */ SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
            true,
        ),
        ("DISCARD ALL", false),
    ] {
        ProxyServer::note_ready_for_query(&session, b'I', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg(sql), &session, &state).await;
        assert_eq!(tr.serializable_default, serializable, "{sql}");
    }
}

#[test]
fn tr_guc_name_identifies_the_variable() {
    let n = |sql: &str| ProxyServer::tr_guc_name(sql);
    assert_eq!(
        n("RESET /* c */ search_path").as_deref(),
        Some("search_path")
    );
    assert_eq!(n("SET app.tenant = 'a'").as_deref(), Some("app.tenant"));
    assert_eq!(
        n("SET app.tenant/* c */= 'a'").as_deref(),
        Some("app.tenant")
    );
    assert_eq!(
        n("SET search_path TO app /* sqlcommenter='x' */").as_deref(),
        Some("search_path")
    );
    assert_eq!(n("SET /* c */ ROLE x").as_deref(), Some("role"));
    assert_eq!(
        n("SET application_name = 'x'").as_deref(),
        Some("application_name")
    );
    assert_eq!(
        n("set Application_Name to 'x';").as_deref(),
        Some("application_name")
    );
    assert_eq!(
        n("SET SESSION work_mem = '1MB'").as_deref(),
        Some("work_mem")
    );
    assert_eq!(n("SET TIME ZONE 'UTC'").as_deref(), Some("timezone"));
    assert_eq!(n("SET timezone TO 'UTC'").as_deref(), Some("timezone"));
    assert_eq!(n("SET SCHEMA 'public'").as_deref(), Some("search_path"));
    assert_eq!(n("SET NAMES 'UTF8'").as_deref(), Some("client_encoding"));
    assert_eq!(n("SET ROLE readonly").as_deref(), Some("role"));
    assert_eq!(
        n("SET SESSION AUTHORIZATION bob").as_deref(),
        Some("session_authorization")
    );
    assert_eq!(
        n("RESET application_name").as_deref(),
        Some("application_name")
    );
    assert_eq!(n("RESET ROLE").as_deref(), Some("role"));
    assert_eq!(n("SET \"Quoted.Var\" = 1").as_deref(), Some("quoted.var"));
    assert_eq!(n("RESET ALL"), None);
    assert_eq!(n("SELECT 1"), None);
}

/// TR-06: the observation digest is order-sensitive over the frames the
/// client sees, ignores asynchronous frames, and reports overflow
/// instead of a digest once the byte budget is exceeded.
#[test]
fn observation_digest_is_ordered_bounded_and_ignores_async_frames() {
    let t = frame(b'T', b"desc");
    let d1 = frame(b'D', b"row1");
    let d2 = frame(b'D', b"row2");
    let c = frame(b'C', b"SELECT 2\0");
    let digest = |frames: &[&[u8]], cap: usize| {
        let mut o = Observation::new(cap);
        for f in frames {
            o.note(f);
        }
        o.finish()
    };
    let a = digest(&[&t, &d1, &d2, &c], usize::MAX).unwrap();
    assert_eq!(
        a,
        digest(&[&t, &d1, &d2, &c], usize::MAX).unwrap(),
        "deterministic"
    );
    assert_ne!(
        a,
        digest(&[&t, &d2, &d1, &c], usize::MAX).unwrap(),
        "order matters"
    );
    assert_ne!(
        a,
        digest(&[&t, &d1, &c], usize::MAX).unwrap(),
        "row count matters"
    );
    // Notices, parameter status and notifications are not observed.
    let n = frame(b'N', b"SNOTICE\0\0");
    let s_ = frame(b'S', b"application_name\0x\0");
    let a_ = frame(b'A', b"\0\0\0\x01chan\0\0");
    assert_eq!(
        a,
        digest(&[&n, &t, &s_, &d1, &a_, &d2, &c], usize::MAX).unwrap()
    );
    // Budget: the total of observed frame bytes must fit.
    let total = t.len() + d1.len() + d2.len() + c.len();
    assert!(digest(&[&t, &d1, &d2, &c], total).is_some());
    assert!(
        digest(&[&t, &d1, &d2, &c], total - 1).is_none(),
        "over budget"
    );
    assert_ne!(a, 0, "zero is reserved for none");
}

/// A statement executed inside a recorded transaction carries the
/// response digest the relay published; a response over the budget
/// makes the transaction non-replayable; the opening BEGIN carries none.
#[tokio::test]
async fn tr_recorder_attaches_observations_and_refuses_unverifiable() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Transaction);

    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
    // The relay observed the SELECT's response.
    let mut o = Observation::new(usize::MAX);
    o.note(&frame(b'T', b"d"));
    o.note(&frame(b'D', b"r"));
    o.note(&frame(b'C', b"SELECT 1\0"));
    let d = o.finish().unwrap();
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::note_observation(&session, Some(&o));
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &state).await;
    {
        let ts = session.tx_state.read().await;
        assert_eq!(ts.statements.len(), 2);
        assert_eq!(
            ts.statements[0].result_checksum, None,
            "BEGIN: nothing to verify"
        );
        assert_eq!(ts.statements[1].result_checksum, Some(d));
        assert!(!ts.non_replayable);
    }
    // Over-budget response: unverifiable, so the transaction is dropped.
    let mut big = Observation::new(4);
    big.note(&frame(b'D', b"too large"));
    assert!(big.finish().is_none());
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::note_observation(&session, Some(&big));
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 2"), &session, &state).await;
    let ts = session.tx_state.read().await;
    assert!(ts.non_replayable && ts.statements.is_empty());
}

/// Snapshot-pinning isolation levels are never replayed: explicitly on
/// BEGIN/START, via SET TRANSACTION, or inherited from a tracked
/// `default_transaction_isolation`.
#[test]
fn tr_snapshot_sensitive_transactions_are_detected() {
    let f = |sql: &str, gucs: &[&str]| {
        let g: Vec<String> = gucs.iter().map(|x| x.to_string()).collect();
        ProxyServer::tr_snapshot_sensitive(sql, &g)
    };
    assert!(f("BEGIN ISOLATION LEVEL SERIALIZABLE", &[]));
    assert!(f("begin isolation level repeatable read", &[]));
    assert!(f(
        "START TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY",
        &[]
    ));
    assert!(f("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[]));
    assert!(f("SET TRANSACTION SNAPSHOT '00000003-0000001B-1'", &[]));
    assert!(f(
        "BEGIN",
        &["SET default_transaction_isolation = 'serializable'"]
    ));
    assert!(!f(
        "BEGIN",
        &["SET default_transaction_isolation = 'read committed'"]
    ));
    assert!(!f("BEGIN", &["SET application_name = 'x'"]));
    assert!(!f("BEGIN ISOLATION LEVEL READ COMMITTED", &[]));
    assert!(!f(
        "SELECT 1",
        &["SET default_transaction_isolation = 'serializable'"]
    ));
    assert!(!f("SET application_name = 'serializable'", &[]));
}

/// Replay verifies each statement against the digest the client saw:
/// a divergent result is a protocol error (surfaced as 40001 + ROLLBACK
/// by the caller); a matching one passes.
#[tokio::test]
async fn tr_run_discard_verifies_the_observed_result() {
    use tokio::io::AsyncReadExt as _;
    use tokio::io::AsyncWriteExt as _;
    async fn backend(mut b: tokio::io::DuplexStream, frames: Vec<u8>) {
        let mut q = vec![0u8; 5];
        b.read_exact(&mut q).await.unwrap();
        let len = u32::from_be_bytes([q[1], q[2], q[3], q[4]]) as usize;
        let mut rest = vec![0u8; len - 4];
        b.read_exact(&mut rest).await.unwrap();
        b.write_all(&frames).await.unwrap();
    }
    let good = [
        frame(b'T', b"d"),
        frame(b'D', b"r"),
        frame(b'C', b"SELECT 1\0"),
    ]
    .concat();
    let mut want = Observation::new(usize::MAX);
    want.note(&frame(b'T', b"d"));
    want.note(&frame(b'D', b"r"));
    want.note(&frame(b'C', b"SELECT 1\0"));
    let want = want.finish().unwrap();
    let rfq = frame(b'Z', b"T");

    let (mut a, b) = tokio::io::duplex(4096);
    tokio::spawn(backend(b, [good.clone(), rfq.clone()].concat()));
    let r = ProxyServer::tr_run_discard(
        &mut a,
        "SELECT 1",
        Duration::from_secs(1),
        Duration::from_secs(1),
        usize::MAX,
        Some((want, usize::MAX)),
    )
    .await;
    assert_eq!(r.unwrap(), b'T', "matching observation replays");

    let diverged = [
        frame(b'T', b"d"),
        frame(b'D', b"OTHER"),
        frame(b'C', b"SELECT 1\0"),
    ]
    .concat();
    let (mut a2, b2) = tokio::io::duplex(4096);
    tokio::spawn(backend(b2, [diverged, rfq.clone()].concat()));
    let r = ProxyServer::tr_run_discard(
        &mut a2,
        "SELECT 1",
        Duration::from_secs(1),
        Duration::from_secs(1),
        usize::MAX,
        Some((want, usize::MAX)),
    )
    .await;
    assert!(matches!(r, Err(ProxyError::Protocol(_))), "{r:?}");

    // No expectation recorded (e.g. the BEGIN): nothing is verified.
    let (mut a3, b3) = tokio::io::duplex(4096);
    tokio::spawn(backend(b3, [good, rfq].concat()));
    assert!(ProxyServer::tr_run_discard(
        &mut a3,
        "SELECT 1",
        Duration::from_secs(1),
        Duration::from_secs(1),
        usize::MAX,
        None,
    )
    .await
    .is_ok());
}

/// The whole-response deadline shrinks the per-read budget and refuses
/// once passed; disabled it leaves the per-read timeout alone.
#[test]
fn read_budget_honours_the_response_deadline() {
    let per = Duration::from_secs(30);
    assert_eq!(read_budget(per, None).unwrap(), per);
    let soon = tokio::time::Instant::now() + Duration::from_millis(200);
    assert!(read_budget(per, Some(soon)).unwrap() <= Duration::from_millis(200));
    let past = tokio::time::Instant::now() - Duration::from_millis(1);
    assert!(matches!(
        read_budget(per, Some(past)),
        Err(ProxyError::Network(_))
    ));
}

/// A recovery deadline already in the past fails the primary wait
/// immediately instead of polling for `write_timeout_secs`.
#[tokio::test]
async fn select_primary_until_respects_a_passed_deadline() {
    let mut config = test_config();
    config.write_timeout_secs = 60;
    // With every primary disabled only the deadline can end the wait.
    for n in &mut config.nodes {
        n.enabled = false;
    }
    let server = ProxyServer::new(config.clone()).unwrap();
    let session = make_test_session();
    let started = std::time::Instant::now();
    let r = ProxyServer::select_primary_until(
        &session,
        &server.state,
        &config,
        tokio::time::Instant::now(),
    )
    .await;
    assert!(matches!(r, Err(ProxyError::NoHealthyNodes)), "{r:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn advance_health_applies_success_threshold_on_recovery() {
    // Healthy nodes stay healthy on the first success.
    assert_eq!(
        ProxyServer::advance_health(true, 0, 0, true, 3, 2),
        (true, 1, 0)
    );
    // An unhealthy node needs `success_threshold` consecutive successes.
    assert_eq!(
        ProxyServer::advance_health(false, 0, 3, true, 3, 2),
        (false, 1, 0)
    );
    assert_eq!(
        ProxyServer::advance_health(false, 1, 0, true, 3, 2),
        (true, 2, 0)
    );
    // Failures count toward `failure_threshold` and reset successes.
    assert_eq!(
        ProxyServer::advance_health(true, 5, 0, false, 3, 2),
        (true, 0, 1)
    );
    assert_eq!(
        ProxyServer::advance_health(true, 5, 2, false, 3, 2),
        (false, 0, 3)
    );
}

#[cfg(feature = "lag-routing")]
#[test]
fn lag_policy_treats_unknown_as_fresh_unless_strict() {
    // Known lag over the threshold is excluded (when a threshold is set).
    assert!(ProxyServer::lag_excludes_standby(Some(100), 10, false));
    assert!(!ProxyServer::lag_excludes_standby(Some(5), 10, false));
    assert!(!ProxyServer::lag_excludes_standby(Some(100), 0, false));
    // Unknown lag: allowed by default, excluded under the strict policy.
    assert!(!ProxyServer::lag_excludes_standby(None, 10, false));
    assert!(ProxyServer::lag_excludes_standby(None, 10, true));
}

#[test]
fn parse_pg_lsn_roundtrip() {
    assert_eq!(parse_pg_lsn("0/0"), Some(0));
    assert_eq!(parse_pg_lsn("0/16B3748"), Some(0x016B_3748));
    assert_eq!(parse_pg_lsn("1/0"), Some(1 << 32));
    assert_eq!(
        parse_pg_lsn("16/B374D848"),
        Some((0x16 << 32) | 0xB374_D848)
    );
    assert_eq!(parse_pg_lsn("not-an-lsn"), None);
    assert_eq!(parse_pg_lsn(""), None);
}

#[test]
fn pick_read_node_honors_each_strategy() {
    use crate::config::Strategy;

    // Round-robin cycles deterministically.
    let rr = |t: u64| {
        ProxyServer::pick_read_node(Strategy::RoundRobin, &[1, 1], &[0.0, 0.0], &[0, 0], t).unwrap()
    };
    assert_eq!((rr(0), rr(1), rr(2), rr(3)), (0, 1, 0, 1));

    // Weighted round-robin: 1:3 over 400 tickets.
    let mut counts = [0usize; 2];
    for t in 0..400 {
        let i = ProxyServer::pick_read_node(
            Strategy::WeightedRoundRobin,
            &[1, 3],
            &[0.0, 0.0],
            &[0, 0],
            t,
        )
        .unwrap();
        counts[i] += 1;
    }
    assert!(
        counts[1] > counts[0] * 2,
        "weighted distribution wrong: {counts:?}"
    );

    // Least-connections and latency pick the better node.
    assert_eq!(
        ProxyServer::pick_read_node(Strategy::LeastConnections, &[1, 1], &[1.0, 9.0], &[5, 1], 0),
        Some(1)
    );
    assert_eq!(
        ProxyServer::pick_read_node(Strategy::LatencyBased, &[1, 1], &[50.0, 5.0], &[0, 0], 0),
        Some(1)
    );

    // Random stays in range and samples both nodes.
    let mut seen = [false; 2];
    for t in 0..64 {
        let i = ProxyServer::pick_read_node(Strategy::Random, &[1, 1], &[0.0, 0.0], &[0, 0], t)
            .unwrap();
        assert!(i < 2);
        seen[i] = true;
    }
    assert!(seen[0] && seen[1], "random must sample both nodes");

    // Power-of-two-choices favours the unloaded node.
    let mut p2c = [0usize; 2];
    for t in 0..200 {
        let i =
            ProxyServer::pick_read_node(Strategy::PowerOfTwo, &[1, 1], &[1.0, 1.0], &[0, 20], t)
                .unwrap();
        p2c[i] += 1;
    }
    assert!(
        p2c[0] > p2c[1],
        "p2c should prefer the unloaded node: {p2c:?}"
    );
}

/// V-01: every advertised capability that the config enables must be
/// actually wired on the running state — not merely present as a struct
/// field. Deleting a construction hook (or gating it out) fails this test.
#[test]
fn enabled_capabilities_are_actually_wired() {
    let mut config = test_config();
    // Unconditional mutation keeps `mut` meaningful in minimal builds.
    config.tr_enabled = true;

    #[cfg(feature = "query-cache")]
    {
        config.cache.enabled = true;
    }
    #[cfg(feature = "routing-hints")]
    {
        config.routing_hints.enabled = true;
    }
    #[cfg(feature = "rate-limiting")]
    {
        config.rate_limit.enabled = true;
    }
    #[cfg(feature = "circuit-breaker")]
    {
        config.circuit_breaker.enabled = true;
    }
    #[cfg(feature = "query-analytics")]
    {
        config.analytics.enabled = true;
    }

    let server = ProxyServer::new(config).unwrap();
    let state = &server.state;
    // Feature-independent: a fresh server has no sessions. Keeps the
    // binding used in a no-default-features build too.
    assert!(state.sessions.is_empty(), "fresh server has sessions");

    #[cfg(feature = "pool-modes")]
    assert!(
        state.pool_manager.is_some(),
        "pool-modes compiled but not wired"
    );

    #[cfg(feature = "query-cache")]
    assert!(
        state.query_cache.is_some(),
        "query-cache enabled but not wired"
    );
    #[cfg(feature = "routing-hints")]
    assert!(
        state.hint_parser.is_some(),
        "routing-hints enabled but not wired"
    );
    #[cfg(feature = "rate-limiting")]
    assert!(
        state.rate_limiter.is_some(),
        "rate-limiting enabled but not wired"
    );
    #[cfg(feature = "circuit-breaker")]
    assert!(
        state.circuit_breaker.is_some(),
        "circuit-breaker enabled but not wired"
    );
    #[cfg(feature = "query-analytics")]
    assert!(
        state.analytics.is_some(),
        "query-analytics enabled but not wired"
    );
}

#[test]
fn authoritative_leader_requires_an_enabled_configured_node() {
    let mut config = ProxyConfig::default();
    config.add_node("primary-a:5432", "primary").unwrap();
    config.add_node("primary-b:5432", "primary").unwrap();

    let tracker = PrimaryTracker::new_standalone();
    // No provider answer yet: wait, never fall back to roles.
    assert_eq!(ProxyServer::authoritative_leader(&config, &tracker), None);

    tracker.set_primary(uuid::Uuid::new_v4(), "primary-b:5432".to_string());
    assert_eq!(
        ProxyServer::authoritative_leader(&config, &tracker).as_deref(),
        Some("primary-b:5432")
    );

    // A provider answer that is not a configured node is ignored.
    tracker.set_primary(uuid::Uuid::new_v4(), "unknown:5432".to_string());
    assert_eq!(ProxyServer::authoritative_leader(&config, &tracker), None);

    // A disabled configured node is not eligible either.
    tracker.set_primary(uuid::Uuid::new_v4(), "primary-a:5432".to_string());
    config.nodes[0].enabled = false;
    assert_eq!(ProxyServer::authoritative_leader(&config, &tracker), None);
}

#[test]
fn remember_batch_name_dedups_and_caps() {
    let mut list: Vec<String> = Vec::new();
    ProxyServer::remember_batch_name(&mut list, "s1", 3);
    ProxyServer::remember_batch_name(&mut list, "s1", 3);
    ProxyServer::remember_batch_name(&mut list, "s2", 3);
    ProxyServer::remember_batch_name(&mut list, "s3", 3);
    ProxyServer::remember_batch_name(&mut list, "s4", 3);
    assert_eq!(
        list,
        vec!["s1".to_string(), "s2".to_string(), "s3".to_string()]
    );
}

#[test]
fn severity_response_frames_fields_once() {
    let bytes = ProxyServer::create_severity_response("ERROR", "57P01", "backend down");
    assert_eq!(bytes[0], b'E');
    let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    assert_eq!(len + 1, bytes.len(), "frame length covers payload + itself");
    let payload = &bytes[5..];
    for field in [
        b"SERROR\0".as_slice(),
        b"VERROR\0".as_slice(),
        b"C57P01\0".as_slice(),
        b"Mbackend down\0".as_slice(),
    ] {
        assert!(
            payload.windows(field.len()).any(|w| w == field),
            "missing field {:?}",
            field
        );
    }
    assert_eq!(*payload.last().unwrap(), 0, "payload terminator");
}

#[tokio::test]
async fn tr_recorder_tracks_transaction_gucs_and_caps() {
    let mut config = test_config();
    config.limits.tr_max_replay_statements = 3;
    config.limits.tr_max_session_set_statements = 2;
    let server = ProxyServer::new(config).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Transaction);

    // Autocommit SET -> tracked immediately. SET LOCAL -> ignored.
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ProxyServer::tr_after_simple(
        &mut tr,
        &qmsg("SET application_name = 'x'"),
        &session,
        &state,
    )
    .await;
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SET LOCAL a = 1"), &session, &state).await;
    assert_eq!(tr.gucs, vec!["SET application_name = 'x'"]);
    // A rejected SET is not tracked.
    ProxyServer::note_ready_for_query(&session, b'I', true);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SET bogus = 1"), &session, &state).await;
    assert_eq!(tr.gucs.len(), 1);
    assert!(session.tx_state.read().await.statements.is_empty());

    // BEGIN opens the record; INSERT marks writes.
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
    {
        let ts = session.tx_state.read().await;
        assert!(ts.in_transaction && ts.tx_id.is_some());
        assert_eq!(ts.statements.len(), 1);
        assert!(!ts.has_writes && ts.read_only && !ts.non_replayable);
    }
    ProxyServer::tr_after_simple(&mut tr, &qmsg("INSERT INTO t VALUES (1)"), &session, &state)
        .await;
    // SET inside the transaction is pending until COMMIT.
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SET b = 2"), &session, &state).await;
    assert_eq!(
        tr.pending_tx_gucs,
        vec![GucOp::Set("SET b = 2".to_string())]
    );
    {
        let ts = session.tx_state.read().await;
        assert_eq!(ts.statements.len(), 3);
        assert!(ts.has_writes && !ts.read_only);
        assert_eq!(
            ts.replay_bytes,
            "BEGIN".len() + "INSERT INTO t VALUES (1)".len() + "SET b = 2".len()
        );
    }
    // Fourth statement exceeds tr_max_replay_statements = 3.
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &state).await;
    {
        let ts = session.tx_state.read().await;
        assert!(ts.non_replayable);
        assert!(ts.statements.is_empty(), "record released at the cap");
        assert!(ts.in_transaction, "still inside the transaction");
    }
    assert_eq!(
        state.metrics.tr.replay_cap_exceeded.load(Ordering::Relaxed),
        1
    );
    // COMMIT -> record released, pending SET promoted (cap 2 reached).
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("COMMIT"), &session, &state).await;
    assert!(!session.tx_state.read().await.in_transaction);
    assert_eq!(tr.gucs, vec!["SET application_name = 'x'", "SET b = 2"]);
    assert!(tr.pending_tx_gucs.is_empty());
    // Third SET hits tr_max_session_set_statements = 2 -> cap metric.
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SET c = 3"), &session, &state).await;
    assert!(tr.guc_cap_hit);
    assert_eq!(tr.gucs.len(), 2);
    assert_eq!(
        state
            .metrics
            .tr
            .session_set_cap_exceeded
            .load(Ordering::Relaxed),
        1
    );
    // RESET ALL wipes tracking and lifts the cap.
    ProxyServer::tr_after_simple(&mut tr, &qmsg("RESET ALL"), &session, &state).await;
    assert!(tr.gucs.is_empty() && !tr.guc_cap_hit);

    // A rolled-back transaction drops its pending SETs, and a failed
    // transaction ('E') is never replayable.
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SET d = 4"), &session, &state).await;
    ProxyServer::note_ready_for_query(&session, b'E', true);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("INSERT boom"), &session, &state).await;
    assert!(session.tx_state.read().await.non_replayable);
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("ROLLBACK"), &session, &state).await;
    assert!(tr.gucs.is_empty() && tr.pending_tx_gucs.is_empty());
}

/// `session` mode records no transaction statements (no lock, no
/// allocation on the in-transaction path) but still tracks SETs;
/// `none` tracks nothing.
#[tokio::test]
async fn tr_recorder_mode_gating() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Session);
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
    ProxyServer::tr_after_simple(&mut tr, &qmsg("INSERT INTO t VALUES (1)"), &session, &state)
        .await;
    assert!(session.tx_state.read().await.statements.is_empty());
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("COMMIT"), &session, &state).await;
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SET a = 1"), &session, &state).await;
    assert_eq!(tr.gucs, vec!["SET a = 1"]);

    let mut none = TrSession::new(TrMode::None);
    ProxyServer::tr_after_simple(&mut none, &qmsg("SET a = 1"), &session, &state).await;
    assert!(none.gucs.is_empty());
}

/// A tenant/rewrite transform taints the transaction: recorded text is
/// not what executed, so it must not be replayed.
#[tokio::test]
async fn tr_recorder_taint_marks_non_replayable() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Transaction);
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
    session
        .tr_replay_tainted
        .store(true, std::sync::atomic::Ordering::Relaxed);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &state).await;
    let ts = session.tx_state.read().await;
    assert!(ts.non_replayable && ts.statements.is_empty());
    assert!(!session
        .tr_replay_tainted
        .load(std::sync::atomic::Ordering::Relaxed));
}

/// Extended-protocol cycles: Flush-terminated batches accumulate and the
/// Sync closes them into one replay entry carrying the raw frames.
#[tokio::test]
async fn tr_recorder_extended_cycle_accumulates_until_sync() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Transaction);
    let registry: HashMap<String, bytes::Bytes> = HashMap::new();
    // Already inside a transaction (BEGIN recorded via simple protocol).
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
    let flush_batch = bytes::Bytes::from(
        [
            frame(b'B', &[b"\0\0".to_vec(), vec![0; 6]].concat()),
            frame(b'E', &[0; 5]),
            frame(b'H', &[]),
        ]
        .concat(),
    );
    let sync_batch = bytes::Bytes::from(frame(b'S', &[]));
    let unnamed = (
        bytes::Bytes::from(frame(
            b'P',
            &[cstr(""), cstr("INSERT INTO t VALUES ($1)"), vec![0; 2]].concat(),
        )),
        bytes::Bytes::from_static(b"sig"),
    );
    ProxyServer::tr_after_extended(
        &mut tr,
        &flush_batch,
        Some(&unnamed),
        Some("INSERT INTO t VALUES ($1)"),
        false,
        &[],
        &[],
        &registry,
        &session,
        &state,
    )
    .await;
    assert!(tr.ext_cycle.is_some());
    assert_eq!(session.tx_state.read().await.statements.len(), 1);
    ProxyServer::tr_after_extended(
        &mut tr,
        &sync_batch,
        None,
        None,
        true,
        &["s1".to_string()],
        &["s1".to_string()],
        &registry,
        &session,
        &state,
    )
    .await;
    assert!(tr.ext_cycle.is_none());
    let ts = session.tx_state.read().await;
    assert_eq!(ts.statements.len(), 2);
    assert!(ts.has_writes);
    let ext = ts.statements[1].extended.as_ref().expect("extended entry");
    assert_eq!(
        ext.frames,
        [flush_batch.as_ref(), sync_batch.as_ref()].concat()
    );
    assert_eq!(ext.unnamed_parse.as_ref(), Some(&unnamed.0));
    assert_eq!(ext.defines, vec!["s1"]);
    assert_eq!(ts.statements[1].sql, "INSERT INTO t VALUES ($1)");
    assert_eq!(
        ts.replay_bytes,
        "BEGIN".len() + flush_batch.len() + sync_batch.len() + unnamed.0.len()
    );
}

#[tokio::test]
async fn tr_recorder_does_not_retain_a_committed_transaction_prefix() {
    let server = ProxyServer::new(test_config()).unwrap();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Transaction);
    ProxyServer::note_ready_for_query(&session, b'T', false);
    for sql in ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT; BEGIN"] {
        ProxyServer::tr_after_simple(&mut tr, &qmsg(sql), &session, &server.state).await;
    }
    let ts = session.tx_state.read().await;
    assert!(ts.non_replayable);
    assert!(ts.statements.is_empty());
    assert_eq!(ts.replay_bytes, 0);
}

#[tokio::test]
async fn tr_recorder_preserves_later_held_parse_positions() {
    for first_held in [false, true] {
        let server = ProxyServer::new(test_config()).unwrap();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);
        let registry = HashMap::new();
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &server.state).await;
        let mut expected = Vec::new();
        for (index, sql) in ["SELECT 1", "SELECT 2", "SELECT 3"].iter().enumerate() {
            let parse =
                bytes::Bytes::from(frame(b'P', &[cstr(""), cstr(sql), vec![0; 2]].concat()));
            let mut frames = Vec::new();
            let held = if first_held || index > 0 {
                Some((parse.clone(), bytes::Bytes::new()))
            } else {
                frames.extend_from_slice(&parse);
                None
            };
            frames.extend_from_slice(&frame(b'B', &[0; 8]));
            frames.extend_from_slice(&frame(b'E', &[0; 5]));
            frames.extend_from_slice(&frame(if index == 2 { b'S' } else { b'H' }, &[]));
            if held.is_some() {
                expected.extend_from_slice(&parse);
            }
            expected.extend_from_slice(&frames);
            ProxyServer::tr_after_extended(
                &mut tr,
                &bytes::Bytes::from(frames),
                held.as_ref(),
                Some(sql),
                index == 2,
                &[],
                &[],
                &registry,
                &session,
                &server.state,
            )
            .await;
        }
        let ts = session.tx_state.read().await;
        assert!(!ts.non_replayable);
        assert_eq!(ts.statements.len(), 2);
        let recorded = ts.statements[1].extended.as_ref().unwrap();
        assert_eq!(
            [
                recorded.unnamed_parse.as_deref().unwrap_or(&[]),
                &recorded.frames
            ]
            .concat(),
            expected,
        );
        assert_eq!(ts.replay_bytes, "BEGIN".len() + expected.len());
    }
}

#[tokio::test]
async fn tr_recorder_bounds_open_flush_cycle_before_sync() {
    for statement_cap in [1, 256] {
        let mut config = test_config();
        config.limits.tr_max_replay_bytes = 64;
        config.limits.tr_max_replay_statements = statement_cap;
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);
        ProxyServer::note_ready_for_query(&session, b'T', false);
        ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &server.state).await;
        let batch = bytes::Bytes::from(frame(b'H', &[]));
        for _ in 0..32 {
            ProxyServer::tr_after_extended(
                &mut tr,
                &batch,
                None,
                None,
                false,
                &[],
                &[],
                &HashMap::new(),
                &session,
                &server.state,
            )
            .await;
            let retained = tr.ext_cycle.as_ref().map_or(0, |c| c.frames.len());
            assert!(retained + session.tx_state.read().await.replay_bytes <= 64);
        }
        assert!(tr.ext_cycle.is_none());
        let ts = session.tx_state.read().await;
        assert!(ts.non_replayable && ts.statements.is_empty());
        assert_eq!(ts.replay_bytes, 0);
        assert_eq!(
            server
                .state
                .metrics
                .tr
                .replay_cap_exceeded
                .load(Ordering::Relaxed),
            1
        );
    }
}

#[tokio::test]
async fn tr_recorder_retains_begin_before_first_sync_or_refuses_incomplete_history() {
    for (cap, simple_end) in [(4096, false), (1, false), (4096, true)] {
        let mut config = test_config();
        config.limits.tr_max_replay_bytes = cap;
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();
        let mut tr = TrSession::new(TrMode::Transaction);
        let parse =
            bytes::Bytes::from(frame(b'P', &[cstr(""), cstr("BEGIN"), vec![0; 2]].concat()));
        let held = (parse.clone(), bytes::Bytes::new());
        let flush = bytes::Bytes::from(
            [frame(b'B', &[0; 8]), frame(b'E', &[0; 5]), frame(b'H', &[])].concat(),
        );
        // No RFQ has exposed BEGIN yet: both the client-visible and
        // recorder status still say Idle when the Flush is retained.
        ProxyServer::note_ready_for_query(&session, b'I', false);
        ProxyServer::tr_after_extended(
            &mut tr,
            &flush,
            Some(&held),
            Some("BEGIN"),
            false,
            &[],
            &[],
            &HashMap::new(),
            &session,
            &server.state,
        )
        .await;
        ProxyServer::note_ready_for_query(&session, b'T', false);
        let sync = bytes::Bytes::from(frame(b'S', &[]));
        if simple_end {
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &server.state).await;
        } else {
            ProxyServer::tr_after_extended(
                &mut tr,
                &sync,
                None,
                None,
                true,
                &[],
                &[],
                &HashMap::new(),
                &session,
                &server.state,
            )
            .await;
        }
        let ts = session.tx_state.read().await;
        if cap == 1 || simple_end {
            assert!(ts.non_replayable && ts.statements.is_empty());
        } else {
            assert!(!ts.non_replayable);
            assert_eq!(ts.statements.len(), 1);
            let entry = ts.statements[0].extended.as_ref().unwrap();
            assert_eq!(
                [entry.unnamed_parse.as_deref().unwrap_or(&[]), &entry.frames].concat(),
                [parse.as_ref(), flush.as_ref(), sync.as_ref()].concat(),
            );
        }
        assert!(tr.ext_cycle.is_none());
        assert!(!tr.ext_cycle_dropped);
    }
}

#[tokio::test]
async fn tr_recorder_idle_flush_cap_does_not_taint_next_transaction() {
    let mut config = test_config();
    config.limits.tr_max_replay_bytes = 64;
    let server = ProxyServer::new(config).unwrap();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Transaction);
    ProxyServer::note_ready_for_query(&session, b'I', false);
    for _ in 0..16 {
        ProxyServer::tr_after_extended(
            &mut tr,
            &bytes::Bytes::from(frame(b'H', &[])),
            None,
            None,
            false,
            &[],
            &[],
            &HashMap::new(),
            &session,
            &server.state,
        )
        .await;
    }
    assert!(tr.ext_cycle_dropped);
    ProxyServer::tr_after_extended(
        &mut tr,
        &bytes::Bytes::from(frame(b'S', &[])),
        None,
        None,
        true,
        &[],
        &[],
        &HashMap::new(),
        &session,
        &server.state,
    )
    .await;
    assert!(!session.tx_state.read().await.non_replayable);
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &server.state).await;
    let ts = session.tx_state.read().await;
    assert!(!ts.non_replayable);
    assert_eq!(ts.statements.len(), 1);
}

#[test]
fn backend_fault_set_ignores_client_errors() {
    let mut slot = None;
    BackendFault::set(
        &mut slot,
        "n",
        FaultPhase::OutcomeUnknown,
        &ProxyError::Network("Client write error: x".into()),
    );
    assert!(slot.is_none());
    BackendFault::set(
        &mut slot,
        "n",
        FaultPhase::OutcomeUnknown,
        &ProxyError::Network("Backend read error: reset".into()),
    );
    let f = slot.unwrap();
    assert_eq!(
        (f.node.as_str(), f.phase),
        ("n", FaultPhase::OutcomeUnknown)
    );
}

#[test]
fn tr_metrics_snapshot_roundtrip() {
    let m = TrMetrics::default();
    m.failovers.fetch_add(2, Ordering::Relaxed);
    m.unknown_outcome_errors.fetch_add(3, Ordering::Relaxed);
    let s = m.snapshot();
    assert_eq!(s.failovers, 2);
    assert_eq!(s.unknown_outcome_errors, 3);
    assert_eq!(s.transactions_replayed, 0);
}

// ---- backend authentication on a fresh (redial/failover) connection ----

/// Read one complete frame (tag + body) from a stream.
async fn read_frame<S: AsyncReadExt + Unpin>(s: &mut S) -> (u8, Vec<u8>) {
    let mut hdr = [0u8; 5];
    s.read_exact(&mut hdr).await.unwrap();
    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; len - 4];
    s.read_exact(&mut body).await.unwrap();
    (hdr[0], body)
}

fn auth_frame(kind: u32, body: &[u8]) -> Vec<u8> {
    let mut b = kind.to_be_bytes().to_vec();
    b.extend_from_slice(body);
    frame(b'R', &b)
}

/// A SCRAM-SHA-256 backend (driven by the tested `ScramServer`)
/// accepts the proxy's client exchange; the post-auth frames are
/// returned for the caller to forward.
#[tokio::test]
async fn complete_backend_auth_completes_scram_with_credential() {
    use crate::auth_scram::{ScramServer, ScramVerifier};
    let (mut proxy_side, mut backend_side) = tokio::io::duplex(8192);
    let password = "benchpass";
    let verifier = ScramVerifier::from_password(password, b"saltsaltsaltsalt".to_vec(), 4096);
    let backend = tokio::spawn(async move {
        // AuthenticationSASL: mechanism list.
        backend_side
            .write_all(&auth_frame(10, b"SCRAM-SHA-256\0\0"))
            .await
            .unwrap();
        let (tag, body) = read_frame(&mut backend_side).await;
        assert_eq!(tag, b'p');
        let mech_end = body.iter().position(|&b| b == 0).unwrap() + 1;
        let client_first = std::str::from_utf8(&body[mech_end + 4..]).unwrap();
        let (server, server_first) =
            ScramServer::start(verifier, client_first, "serverNONCE").unwrap();
        backend_side
            .write_all(&auth_frame(11, server_first.as_bytes()))
            .await
            .unwrap();
        let (tag, body) = read_frame(&mut backend_side).await;
        assert_eq!(tag, b'p');
        let server_final = server.finish(std::str::from_utf8(&body).unwrap()).unwrap();
        let mut out = auth_frame(12, server_final.as_bytes());
        out.extend_from_slice(&auth_frame(0, b""));
        out.extend_from_slice(&frame(b'S', b"server_version\0"));
        out.extend_from_slice(&frame(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
        out.extend_from_slice(&frame(b'Z', b"I"));
        backend_side.write_all(&out).await.unwrap();
        backend_side
    });
    let forwarded =
        ProxyServer::complete_backend_auth(&mut proxy_side, 1 << 20, "bench", Some(password))
            .await
            .unwrap();
    let _ = backend.await.unwrap();
    // Only non-auth frames are handed back: ParameterStatus,
    // BackendKeyData, ReadyForQuery.
    assert_eq!(forwarded[0], b'S');
    assert!(forwarded.ends_with(&frame(b'Z', b"I")));
    assert!(!forwarded.contains(&b'R') || forwarded[0] != b'R');
    let tags: Vec<u8> = {
        let mut v = Vec::new();
        let mut off = 0;
        while off < forwarded.len() {
            v.push(forwarded[off]);
            let len = u32::from_be_bytes([
                forwarded[off + 1],
                forwarded[off + 2],
                forwarded[off + 3],
                forwarded[off + 4],
            ]) as usize;
            off += 1 + len;
        }
        v
    };
    assert_eq!(tags, vec![b'S', b'K', b'Z']);
}

/// A wrong password is rejected by the SCRAM server -> ErrorResponse
/// -> `ProxyError::Auth`.
#[tokio::test]
async fn complete_backend_auth_reports_scram_rejection() {
    use crate::auth_scram::{ScramServer, ScramVerifier};
    let (mut proxy_side, mut backend_side) = tokio::io::duplex(8192);
    let verifier = ScramVerifier::from_password("right", b"saltsaltsaltsalt".to_vec(), 4096);
    tokio::spawn(async move {
        backend_side
            .write_all(&auth_frame(10, b"SCRAM-SHA-256\0\0"))
            .await
            .unwrap();
        let (_, body) = read_frame(&mut backend_side).await;
        let mech_end = body.iter().position(|&b| b == 0).unwrap() + 1;
        let client_first = std::str::from_utf8(&body[mech_end + 4..]).unwrap();
        let (server, server_first) =
            ScramServer::start(verifier, client_first, "serverNONCE").unwrap();
        backend_side
            .write_all(&auth_frame(11, server_first.as_bytes()))
            .await
            .unwrap();
        let (_, body) = read_frame(&mut backend_side).await;
        assert!(server.finish(std::str::from_utf8(&body).unwrap()).is_err());
        let mut err = vec![b'S'];
        err.extend_from_slice(b"FATAL\0C28P01\0Mpassword authentication failed\0\0");
        backend_side.write_all(&frame(b'E', &err)).await.unwrap();
    });
    let r =
        ProxyServer::complete_backend_auth(&mut proxy_side, 1 << 20, "bench", Some("wrong")).await;
    match r {
        Err(ProxyError::Auth(m)) => {
            assert!(m.contains("password authentication failed"), "{m}")
        }
        other => panic!("expected Auth error, got {other:?}"),
    }
}

fn fatal(code: &str, message: &str) -> Vec<u8> {
    let mut body = vec![b'S'];
    body.extend_from_slice(format!("FATAL\0C{code}\0M{message}\0\0").as_bytes());
    frame(b'E', &body)
}

/// A backend at `max_connections` answers the startup packet with
/// 53300: proxy-side backend auth reports capacity, not an auth failure.
#[tokio::test]
async fn complete_backend_auth_reports_capacity_refusal() {
    let (mut proxy_side, mut backend_side) = tokio::io::duplex(8192);
    tokio::spawn(async move {
        backend_side
            .write_all(&fatal("53300", "sorry, too many clients already"))
            .await
            .unwrap();
    });
    match ProxyServer::complete_backend_auth(&mut proxy_side, 1 << 20, "bench", Some("pw")).await {
        Err(ProxyError::PoolExhausted(m)) => {
            assert!(m.contains("53300") && m.contains("too many clients"), "{m}")
        }
        other => panic!("expected PoolExhausted, got {other:?}"),
    }
}

/// Pass-through relay: a 53300 first frame is held back from the client
/// (so the caller can free idle capacity and redial) and reported as
/// capacity.
#[tokio::test]
async fn passthrough_auth_holds_back_a_capacity_refusal() {
    let server = ProxyServer::new(test_config()).unwrap();
    let (mut backend, mut backend_peer) = pair().await;
    let (client_raw, mut client_peer) = pair().await;
    let mut client = ClientStream::Plain(client_raw);
    backend_peer
        .write_all(&fatal("53300", "sorry, too many clients already"))
        .await
        .unwrap();
    let r = ProxyServer::proxy_authentication(
        &mut client,
        &mut backend,
        &server.state,
        "127.0.0.1:5432",
        &mut false,
    )
    .await;
    match r {
        Err(ProxyError::PoolExhausted(m)) => {
            assert!(m.contains("max_connections") && m.contains("53300"), "{m}")
        }
        other => panic!("expected PoolExhausted, got {other:?}"),
    }
    drop(client);
    let mut got = Vec::new();
    client_peer.read_to_end(&mut got).await.unwrap();
    assert!(
        got.is_empty(),
        "the refusal must not reach the client: {got:?}"
    );
}

/// Any other backend error is relayed unchanged and reported with its
/// SQLSTATE: only class 28 is an authentication failure.
#[tokio::test]
async fn passthrough_auth_relays_errors_and_reports_their_sqlstate() {
    for (code, is_auth) in [("28P01", true), ("3D000", false)] {
        let server = ProxyServer::new(test_config()).unwrap();
        let (mut backend, mut backend_peer) = pair().await;
        let (client_raw, mut client_peer) = pair().await;
        let mut client = ClientStream::Plain(client_raw);
        let err = fatal(code, "refused");
        backend_peer.write_all(&err).await.unwrap();
        let r = ProxyServer::proxy_authentication(
            &mut client,
            &mut backend,
            &server.state,
            "127.0.0.1:5432",
            &mut false,
        )
        .await;
        match (r, is_auth) {
            (Err(ProxyError::Auth(m)), true) => assert!(m.starts_with(code), "{m}"),
            (Err(ProxyError::Connection(m)), false) => assert!(m.contains(code), "{m}"),
            (other, _) => panic!("{code}: unexpected {other:?}"),
        }
        drop(client);
        let mut got = Vec::new();
        client_peer.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, err, "{code}: the error reaches the client unchanged");
    }
}

/// A successful startup is relayed byte for byte and the cancel key
/// is registered.
#[tokio::test]
async fn passthrough_auth_relays_a_successful_startup_unchanged() {
    let server = ProxyServer::new(test_config()).unwrap();
    let (mut backend, mut backend_peer) = pair().await;
    let (client_raw, mut client_peer) = pair().await;
    let mut client = ClientStream::Plain(client_raw);
    let mut bytes = auth_frame(0, b"");
    bytes.extend(frame(b'S', b"server_version\x0018.4\0"));
    bytes.extend(frame(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
    bytes.extend(frame(b'Z', b"I"));
    backend_peer.write_all(&bytes).await.unwrap();
    ProxyServer::proxy_authentication(
        &mut client,
        &mut backend,
        &server.state,
        "127.0.0.1:5432",
        &mut false,
    )
    .await
    .expect("startup completes");
    drop(client);
    let mut got = Vec::new();
    client_peer.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, bytes);
}

#[test]
fn error_response_fields_reads_code_and_message() {
    let (code, msg) = ProxyServer::error_response_fields(&fatal("53300", "too many clients"));
    assert_eq!((code.as_str(), msg.as_str()), ("53300", "too many clients"));
    let (code, msg) = ProxyServer::error_response_fields(&[b'E', 0, 0, 0, 4]);
    assert_eq!(code, "");
    assert!(!msg.is_empty());
}

#[cfg(feature = "query-cache")]
mod c02_cache_work {
    use super::*;
    use crate::cache::{CacheConfig, QueryCache};
    use crate::journal_capture::JournalOp;
    use crate::transaction_journal::{NewEntry, StatementOutcome, WireProtocol};

    fn entry(sql: &str) -> NewEntry {
        NewEntry {
            statement: sql.to_string(),
            parameters: Vec::new(),
            param_types: Vec::new(),
            result_checksum: None,
            rows_affected: Some(1),
            duration_ms: 0,
            outcome: StatementOutcome::Succeeded {
                tag: "UPDATE 1".into(),
            },
            protocol: WireProtocol::Extended,
        }
    }

    fn run(ops: Vec<JournalOp>, stage: &mut TxCacheStage) -> CacheWork {
        let qc = QueryCache::new(CacheConfig::default());
        ProxyServer::cache_work_from_ops(&qc, &ops, stage)
    }

    // Regression coverage uses only pre-existing APIs so the same tests run
    // against the unchanged baseline production implementation.
    fn prepared_cycle(sql: &str, tags: &[&str], error: bool) -> CacheWork {
        use crate::journal_capture::{Completion, ResponseOutcome, SessionCapture};
        let mut capture = SessionCapture::default();
        capture.begin_cycle();
        assert!(capture.register_simple(sql));
        let ops = capture.observe(&ResponseOutcome {
            status: b'I',
            completions: tags.iter().map(|s| Completion::Tag((*s).into())).collect(),
            error: error.then(|| ("42704".into(), "prepared transaction missing".into())),
        });
        run(ops, &mut TxCacheStage::default())
    }

    #[test]
    fn prepared_commit_invalidates_without_local_write_stage() {
        let work = prepared_cycle(
            "COMMIT PREPARED 'from_another_session'",
            &["COMMIT PREPARED"],
            false,
        );
        assert!(work.all, "a prepared transaction can modify unknown tables");
        assert!(work.tables.is_empty());
    }

    #[test]
    fn prepared_commit_comment_and_quoted_gid_invalidate() {
        let work = prepared_cycle(
            "COMMIT /* note */ PREPARED 'gid;with''quote'",
            &["COMMIT PREPARED"],
            false,
        );
        assert!(work.all);
    }

    #[test]
    fn preparing_transaction_drops_stage_without_publishing() {
        use crate::journal_capture::{Completion, ResponseOutcome, SessionCapture};
        let qc = QueryCache::new(CacheConfig::default());
        let mut capture = SessionCapture::default();
        let mut stage = TxCacheStage::default();
        for (sql, tag, status) in [
            ("BEGIN", "BEGIN", b'T'),
            ("UPDATE accounts SET v=1", "UPDATE 1", b'T'),
            ("PREPARE TRANSACTION 'g'", "PREPARE TRANSACTION", b'I'),
        ] {
            capture.begin_cycle();
            assert!(capture.register_simple(sql));
            let ops = capture.observe(&ResponseOutcome {
                status,
                completions: vec![Completion::Tag(tag.into())],
                error: None,
            });
            let work = ProxyServer::cache_work_from_ops(&qc, &ops, &mut stage);
            assert!(!work.all && work.tables.is_empty(), "{sql} is not a commit");
            if sql.starts_with("UPDATE") {
                assert_eq!(stage.tables, vec!["accounts"]);
            }
        }
        assert!(stage.tables.is_empty());
        assert!(!stage.unknown);
    }

    #[test]
    fn prepared_commit_requires_successful_backend_completion() {
        for (sql, tags, error) in [
            ("COMMIT PREPARED 'missing'", vec![], true),
            ("COMMIT PREPARED 'not_executed'", vec![], false),
            (
                "ROLLBACK PREPARED 'discarded'",
                vec!["ROLLBACK PREPARED"],
                false,
            ),
        ] {
            let work = prepared_cycle(sql, &tags, error);
            assert!(!work.all && work.tables.is_empty(), "{sql}");
        }
    }

    #[test]
    fn autocommit_write_invalidates_its_tables_at_once() {
        let mut stage = TxCacheStage::default();
        let w = run(
            vec![JournalOp::AutoCommit {
                entries: vec![entry("UPDATE accounts SET v = $1")],
                tag: "UPDATE 1".into(),
                incomplete: None,
            }],
            &mut stage,
        );
        assert_eq!(w.tables, vec!["accounts".to_string()]);
        assert!(!w.all);
        assert!(stage.tables.is_empty());
    }

    #[test]
    fn explicit_transaction_invalidates_at_commit_only() {
        let tx = uuid::Uuid::new_v4();
        let mut stage = TxCacheStage::default();
        let w = run(
            vec![
                JournalOp::Begin { tx_id: tx },
                JournalOp::Log {
                    tx_id: tx,
                    entry: entry("UPDATE accounts SET v = 3"),
                },
            ],
            &mut stage,
        );
        assert!(w.tables.is_empty() && !w.all, "nothing at statement time");
        assert_eq!(stage.tables, vec!["accounts".to_string()], "staged");
        let w = run(
            vec![JournalOp::Commit {
                tx_id: tx,
                tag: "COMMIT".into(),
            }],
            &mut stage,
        );
        assert_eq!(w.tables, vec!["accounts".to_string()], "commit time");
        assert!(stage.tables.is_empty());
    }

    #[test]
    fn rollback_drops_the_stage() {
        let tx = uuid::Uuid::new_v4();
        let mut stage = TxCacheStage::default();
        run(
            vec![JournalOp::Log {
                tx_id: tx,
                entry: entry("DELETE FROM accounts"),
            }],
            &mut stage,
        );
        let w = run(vec![JournalOp::Rollback { tx_id: tx }], &mut stage);
        assert!(w.tables.is_empty() && !w.all);
        assert!(stage.tables.is_empty() && !stage.unknown);
    }

    #[test]
    fn unknown_scope_invalidates_everything_at_commit() {
        let tx = uuid::Uuid::new_v4();
        for (ops, why) in [
            (
                vec![JournalOp::Log {
                    tx_id: tx,
                    entry: entry("ALTER TABLE accounts ADD COLUMN c int"),
                }],
                "DDL",
            ),
            (
                vec![
                    JournalOp::Log {
                        tx_id: tx,
                        entry: entry("COPY accounts FROM STDIN"),
                    },
                    JournalOp::Incomplete {
                        tx_id: tx,
                        reason: "COPY FROM".into(),
                    },
                ],
                "COPY",
            ),
            (
                vec![JournalOp::Log {
                    tx_id: tx,
                    entry: entry("EXECUTE upd(1)"),
                }],
                "EXECUTE",
            ),
        ] {
            let mut stage = TxCacheStage::default();
            let w = run(ops, &mut stage);
            assert!(!w.all && w.tables.is_empty(), "{why}: staged only");
            assert!(stage.unknown, "{why}: staged as unknown");
            let w = run(
                vec![JournalOp::Commit {
                    tx_id: tx,
                    tag: "COMMIT".into(),
                }],
                &mut stage,
            );
            assert!(w.all, "{why}: commit time");
        }
        let mut stage = TxCacheStage::default();
        let w = run(
            vec![JournalOp::AutoCommit {
                entries: vec![entry("TRUNCATE accounts")],
                tag: "TRUNCATE TABLE".into(),
                incomplete: None,
            }],
            &mut stage,
        );
        assert!(w.all, "autocommit TRUNCATE");
    }
}

/// Without a credential (pass-through mode) a challenge fails fast with
/// a clear error instead of a timeout; a trust backend still completes.
#[tokio::test]
async fn complete_backend_auth_without_credential() {
    let (mut proxy_side, mut backend_side) = tokio::io::duplex(4096);
    backend_side
        .write_all(&auth_frame(10, b"SCRAM-SHA-256\0\0"))
        .await
        .unwrap();
    let r = ProxyServer::complete_backend_auth(&mut proxy_side, 1 << 20, "bench", None).await;
    match r {
        Err(ProxyError::Auth(m)) => assert!(m.contains("holds no credential"), "{m}"),
        other => panic!("expected Auth error, got {other:?}"),
    }
    // Trust backend: AuthenticationOk straight away.
    let (mut p2, mut b2) = tokio::io::duplex(4096);
    let mut out = auth_frame(0, b"");
    out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
    out.extend_from_slice(&frame(b'Z', b"I"));
    b2.write_all(&out).await.unwrap();
    let fwd = ProxyServer::complete_backend_auth(&mut p2, 1 << 20, "bench", None)
        .await
        .unwrap();
    assert_eq!(fwd[0], b'K');
    assert!(fwd.ends_with(&frame(b'Z', b"I")));
}

/// Cleartext and MD5 challenges are answered from the credential.
#[tokio::test]
async fn complete_backend_auth_answers_cleartext_and_md5() {
    // Cleartext.
    let (mut p, mut b) = tokio::io::duplex(4096);
    let backend = tokio::spawn(async move {
        b.write_all(&auth_frame(3, b"")).await.unwrap();
        let (tag, body) = read_frame(&mut b).await;
        assert_eq!((tag, body.as_slice()), (b'p', &b"pw\0"[..]));
        let mut out = auth_frame(0, b"");
        out.extend_from_slice(&frame(b'Z', b"I"));
        b.write_all(&out).await.unwrap();
    });
    ProxyServer::complete_backend_auth(&mut p, 1 << 20, "u", Some("pw"))
        .await
        .unwrap();
    backend.await.unwrap();
    // MD5.
    let (mut p, mut b) = tokio::io::duplex(4096);
    let backend = tokio::spawn(async move {
        b.write_all(&auth_frame(5, &[1, 2, 3, 4])).await.unwrap();
        let (tag, body) = read_frame(&mut b).await;
        assert_eq!(tag, b'p');
        assert_eq!(
            body,
            crate::backend::auth::md5_password_response("u", "pw", &[1, 2, 3, 4])
        );
        let mut out = auth_frame(0, b"");
        out.extend_from_slice(&frame(b'Z', b"I"));
        b.write_all(&out).await.unwrap();
    });
    ProxyServer::complete_backend_auth(&mut p, 1 << 20, "u", Some("pw"))
        .await
        .unwrap();
    backend.await.unwrap();
}

/// A server whose tracker is a provider-backed authority. `[[nodes]]` keeps
/// `pg-a` as the configured primary, as it does in a static config after the
/// provider has promoted `pg-b`.
fn authoritative_server() -> (ProxyServer, Arc<PrimaryTracker>, ProxyConfig) {
    let mut config = ProxyConfig::default();
    config.add_node("pg-a:5432", "primary").unwrap();
    config.add_node("pg-b:5432", "standby").unwrap();
    let mut server = ProxyServer::new(config.clone()).unwrap();
    let tracker = Arc::new(PrimaryTracker::new_standalone());
    let state = Arc::get_mut(&mut server.state).expect("fresh server state is unshared");
    state.authoritative_topology = true;
    state.primary_tracker = tracker.clone();
    state.routing = Arc::new(crate::routing_snapshot::RoutingView::new(true));
    (server, tracker, config)
}

/// What the routing refresher does on a tracker event: set the provider's
/// leader and republish the routing snapshot.
fn promote(server: &ProxyServer, tracker: &PrimaryTracker, addr: &str) {
    tracker.set_primary(uuid::Uuid::new_v4(), addr.to_string());
    ProxyServer::publish_routing(&server.state);
}

/// Regression for Sprinter a1843fc5bd56: a NEW session opened after the
/// provider promoted another node must start on the provider's leader, not on
/// the configured (now demoted) primary.
#[tokio::test]
async fn new_session_after_promotion_selects_the_authoritative_leader() {
    let (server, tracker, config) = authoritative_server();
    let state = &server.state;

    promote(&server, &tracker, "pg-a:5432");
    let s1 = make_test_session();
    assert_eq!(
        ProxyServer::select_node(&s1, state, &config).await.unwrap(),
        "pg-a:5432"
    );

    // Promotion: the provider now reports pg-b. Config roles are unchanged.
    promote(&server, &tracker, "pg-b:5432");
    let s2 = make_test_session();
    assert_eq!(
        ProxyServer::select_node(&s2, state, &config).await.unwrap(),
        "pg-b:5432",
        "a new session must connect to the promoted leader"
    );
}

/// Regression for Sprinter 480d8084f833: a session whose home connection is
/// still open after the provider moved the leader is re-homed (so replay
/// restores its settings) instead of drifting to a fresh connection. Without
/// a valid leader, or under a static topology, nothing moves.
#[tokio::test]
async fn a_home_that_lost_the_leadership_is_reported_moved() {
    let (server, tracker, config) = authoritative_server();
    let state = &server.state;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    conns.insert(
        "pg-a:5432".to_string(),
        BackendConn::new(TcpStream::connect(&addr).await.unwrap()),
    );

    promote(&server, &tracker, "pg-a:5432");
    assert_eq!(
        ProxyServer::home_moved(&conns, Some("pg-a:5432"), state, &config),
        None
    );
    promote(&server, &tracker, "pg-b:5432");
    assert_eq!(
        ProxyServer::home_moved(&conns, Some("pg-a:5432"), state, &config).as_deref(),
        Some("pg-a:5432")
    );
    // No open connection there (it already went), or no home: nothing to move.
    assert_eq!(
        ProxyServer::home_moved(&HashMap::new(), Some("pg-a:5432"), state, &config),
        None
    );
    assert_eq!(ProxyServer::home_moved(&conns, None, state, &config), None);

    // A static topology keeps the configured roles.
    let static_server = ProxyServer::new(config.clone()).unwrap();
    assert_eq!(
        ProxyServer::home_moved(&conns, Some("pg-b:5432"), &static_server.state, &config),
        None
    );
}

/// The write path must re-route a session whose current node is the
/// configured primary once the provider has demoted it, and must not route
/// writes anywhere while the provider has no valid leader.
#[tokio::test]
async fn writes_follow_the_authoritative_leader_not_the_configured_role() {
    let (server, tracker, config) = authoritative_server();
    let state = &server.state;
    let session = make_test_session();

    promote(&server, &tracker, "pg-b:5432");
    assert!(!ProxyServer::is_write_target(state, &config, "pg-a:5432"));
    assert!(ProxyServer::is_write_target(state, &config, "pg-b:5432"));

    // A session still sitting on the demoted configured primary is moved.
    let target = ProxyServer::choose_target_node(
        true,
        None,
        Some("pg-a:5432"),
        ReadRoute::Stay,
        &session,
        state,
        &config,
    )
    .await
    .unwrap()
    .0;
    assert_eq!(target, "pg-b:5432");

    // A session already on the leader stays put.
    let target = ProxyServer::choose_target_node(
        true,
        None,
        Some("pg-b:5432"),
        ReadRoute::Stay,
        &session,
        state,
        &config,
    )
    .await
    .unwrap()
    .0;
    assert_eq!(target, "pg-b:5432");
}

/// H-01 step 3: after a promotion, reads leave the promoted node and use the
/// demoted former primary, although `[[nodes]]` still calls them standby and
/// primary.
#[tokio::test]
async fn reads_follow_the_authoritative_snapshot_after_promotion() {
    let (server, tracker, config) = authoritative_server();
    let state = &server.state;

    promote(&server, &tracker, "pg-a:5432");
    let s = make_test_session();
    assert_eq!(
        ProxyServer::select_read_node(&s, state, &config)
            .await
            .unwrap(),
        "pg-b:5432"
    );

    promote(&server, &tracker, "pg-b:5432");
    for _ in 0..4 {
        let s = make_test_session();
        assert_eq!(
            ProxyServer::select_read_node(&s, state, &config)
                .await
                .unwrap(),
            "pg-a:5432",
            "the demoted primary is the only replica now"
        );
    }
    let snapshot = state.routing.load();
    assert_eq!(snapshot.leader.as_deref(), Some("pg-b:5432"));
    assert_eq!(state.routing.leader_changes_total(), 1);
}

/// A lapsed provider authority stops writes, and reads fall back to the
/// configured replicas rather than guessing which node leads.
#[tokio::test]
async fn lost_authority_stops_writes_and_reads_use_configured_replicas() {
    let (server, tracker, config) = authoritative_server();
    let state = &server.state;
    promote(&server, &tracker, "pg-b:5432");
    tracker.clear_primary();
    ProxyServer::publish_routing(state);
    assert!(!ProxyServer::is_write_target(state, &config, "pg-a:5432"));
    assert!(!ProxyServer::is_write_target(state, &config, "pg-b:5432"));
    let s = make_test_session();
    assert_eq!(
        ProxyServer::select_read_node(&s, state, &config)
            .await
            .unwrap(),
        "pg-b:5432"
    );
    assert_eq!(
        state.routing.leader_changes_total(),
        0,
        "losing the leader is not a move"
    );
}

/// Lag is measured against the published leader, on every other node.
#[test]
fn lag_sampling_follows_the_authoritative_leader() {
    let (server, tracker, config) = authoritative_server();
    let state = &server.state;
    let (a, b) = (&config.nodes[0], &config.nodes[1]);
    assert!(ProxyServer::lag_source(state, &config).is_none());
    promote(&server, &tracker, "pg-b:5432");
    let source = ProxyServer::lag_source(state, &config);
    assert_eq!(source.map(|n| n.address()), Some("pg-b:5432"));
    assert!(ProxyServer::samples_lag(state, a, source));
    assert!(!ProxyServer::samples_lag(state, b, source));

    // Static: the configured primary is the source, the standby is sampled.
    let static_server = ProxyServer::new(config.clone()).unwrap();
    let st = &static_server.state;
    let source = ProxyServer::lag_source(st, &config);
    assert_eq!(source.map(|n| n.address()), Some("pg-a:5432"));
    assert!(!ProxyServer::samples_lag(st, a, source));
    assert!(ProxyServer::samples_lag(st, b, source));
}

/// A leader move closes the idle pooled connections to the old leader and
/// keeps the others.
#[cfg(feature = "pool-modes")]
#[tokio::test]
async fn leader_move_closes_idle_pooled_connections_to_the_old_leader() {
    use tokio::net::{TcpListener, TcpStream};
    let (mut server, tracker, _config) = authoritative_server();
    let pool = Arc::new(crate::pool::BackendIdlePool::new(4, 64));
    Arc::get_mut(&mut server.state)
        .expect("fresh server state is unshared")
        .backend_pool = Some(pool.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut peers = Vec::new();
    for node in ["pg-a:5432", "pg-a:5432", "pg-b:5432"] {
        let (client, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        peers.push(accepted.unwrap().0);
        assert!(pool.checkin(&crate::pool::pool_key(node, "u", "d"), client.unwrap()));
    }

    promote(&server, &tracker, "pg-a:5432");
    assert_eq!(
        pool.idle_count(),
        3,
        "electing a first leader evicts nothing"
    );
    promote(&server, &tracker, "pg-b:5432");
    assert_eq!(pool.idle_count(), 1);
    assert_eq!(server.state.routing.evicted_idle_total(), 2);
    assert!(pool
        .checkout(&crate::pool::pool_key("pg-b:5432", "u", "d"))
        .is_some());
}

/// Without a provider the configured role still decides (unchanged behaviour).
#[test]
fn static_topology_still_uses_configured_roles_for_write_target() {
    let mut config = ProxyConfig::default();
    config.add_node("pg-a:5432", "primary").unwrap();
    config.add_node("pg-b:5432", "standby").unwrap();
    let server = ProxyServer::new(config.clone()).unwrap();
    assert!(ProxyServer::is_write_target(
        &server.state,
        &config,
        "pg-a:5432"
    ));
    assert!(!ProxyServer::is_write_target(
        &server.state,
        &config,
        "pg-b:5432"
    ));
    assert!(!ProxyServer::is_write_target(
        &server.state,
        &config,
        "unknown:5432"
    ));
}

// ---- TR-08: statements and portals from earlier protocol cycles ----

fn tr08_parse(name: &str, sql: &str) -> Vec<u8> {
    frame(b'P', &[cstr(name), cstr(sql), vec![0, 0]].concat())
}

fn tr08_bind(portal: &str, name: &str) -> Vec<u8> {
    frame(b'B', &[cstr(portal), cstr(name), vec![0; 6]].concat())
}

fn tr08_execute(portal: &str) -> Vec<u8> {
    frame(b'E', &[cstr(portal), vec![0; 4]].concat())
}

fn tr08_acked(name: &str, sql: &str) -> (String, AckedStmt) {
    (
        name.to_string(),
        AckedStmt {
            parse: bytes::Bytes::from(tr08_parse(name, sql)),
            kind: ProxyServer::tr_parse_kind(sql, &TrReadPolicy::default()),
        },
    )
}

/// A Bind of a statement an earlier cycle prepared is classified by the
/// acknowledged SQL; anything the cycle closed, redefined after use, or the
/// session cannot vouch for stays opaque (a possible COMMIT).
#[test]
fn tr08_prior_statements_and_portals_classify_later_cycles() {
    let policy = TrReadPolicy::default();
    let acked: HashMap<String, AckedStmt> = [
        tr08_acked("r", "SELECT 1"),
        tr08_acked("w", "INSERT INTO t VALUES ($1)"),
        tr08_acked("c", "COMMIT"),
        tr08_acked("rb", "ROLLBACK"),
    ]
    .into_iter()
    .collect();
    let portals: HashMap<String, StmtKind> = [("cur".to_string(), StmtKind::Read)].into();
    let prior = PriorCycles {
        statements: &acked,
        portals: &portals,
        registry: None,
    };
    let kind = |frames: Vec<Vec<u8>>| {
        ProxyServer::tr_extended_kind_prior(&frames.concat(), None, 256, &policy, prior)
    };
    let read = vec![tr08_bind("", "r"), tr08_execute(""), frame(b'S', &[])];
    // Without the prior cycles the reference is opaque, as before TR-08.
    assert_eq!(
        ProxyServer::tr_extended_kind(&read.concat(), None, 256, &policy),
        StmtKind::Commit
    );
    assert_eq!(kind(read), StmtKind::Read);
    assert_eq!(
        kind(vec![tr08_bind("", "w"), tr08_execute("")]),
        StmtKind::Write
    );
    for end in ["c", "rb", "unknown"] {
        assert_eq!(
            kind(vec![tr08_bind("", end), tr08_execute("")]),
            StmtKind::Commit,
            "{end}"
        );
    }
    // Closed earlier in this cycle: not the acknowledged statement any more.
    assert_eq!(
        kind(vec![
            frame(b'C', b"Sr\0"),
            tr08_bind("", "r"),
            tr08_execute("")
        ]),
        StmtKind::Commit
    );
    // Used, then redefined in the same cycle: opaque.
    assert_eq!(
        kind(vec![
            tr08_bind("", "r"),
            tr08_execute(""),
            frame(b'C', b"Sr\0"),
            tr08_parse("r", "SELECT 2"),
            tr08_bind("", "r"),
            tr08_execute("")
        ]),
        StmtKind::Commit
    );
    // Redefined before any use: the cycle's own definition wins.
    assert_eq!(
        kind(vec![
            frame(b'C', b"Sw\0"),
            tr08_parse("w", "SELECT 2"),
            tr08_bind("", "w"),
            tr08_execute("")
        ]),
        StmtKind::Read
    );
    // Portals bound by earlier cycles of the transaction.
    assert_eq!(kind(vec![tr08_execute("cur")]), StmtKind::Read);
    assert_eq!(
        kind(vec![frame(b'C', b"Pcur\0"), tr08_execute("cur")]),
        StmtKind::Commit
    );
    assert_eq!(kind(vec![tr08_execute("other")]), StmtKind::Commit);

    // Re-execution re-prepares from the registry: it must hold the same Parse.
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    registry.insert("r".to_string(), acked["r"].parse.clone());
    let read = [tr08_bind("", "r"), tr08_execute("")].concat();
    let guarded = |registry: &HashMap<String, bytes::Bytes>| {
        ProxyServer::tr_extended_kind_prior(
            &read,
            None,
            256,
            &policy,
            PriorCycles {
                registry: Some(registry),
                ..prior
            },
        )
    };
    assert_eq!(guarded(&registry), StmtKind::Read);
    registry.insert(
        "r".to_string(),
        bytes::Bytes::from(tr08_parse("r", "DELETE FROM t")),
    );
    assert_eq!(guarded(&registry), StmtKind::Commit);
    registry.remove("r");
    assert_eq!(guarded(&registry), StmtKind::Commit);

    // The scan reports the outside statements (first use, once each,
    // Describe included) and the named-portal changes in wire order.
    let mut scan = CycleScan::default();
    let k = ProxyServer::tr_extended_cycle_scan(
        &[&[
            frame(b'D', b"Sw\0"),
            tr08_bind("p", "r"),
            tr08_execute("p"),
            tr08_bind("", "r"),
            tr08_execute(""),
            frame(b'C', b"Pp\0"),
        ]
        .concat()],
        None,
        256,
        &policy,
        Some(prior),
        Some(&mut scan),
    );
    assert_eq!(k, StmtKind::Read);
    assert_eq!(scan.outside, vec!["w".to_string(), "r".to_string()]);
    assert_eq!(
        scan.portal_updates,
        vec![
            ("p".to_string(), Some(StmtKind::Read)),
            ("p".to_string(), None)
        ]
    );
}

/// The acknowledged statements follow only cycles that reached
/// ReadyForQuery cleanly; anything uncertain is forgotten.
#[test]
fn tr08_definitions_follow_acknowledged_cycles_only() {
    let policy = TrReadPolicy::default();
    let p_r = tr08_parse("r", "SELECT 1");
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    registry.insert("r".to_string(), bytes::Bytes::from(p_r.clone()));
    let mut acked = HashMap::new();
    let clean = [p_r.clone(), frame(b'S', &[])].concat();
    let note = |acked: &mut HashMap<String, AckedStmt>,
                frames: &[u8],
                registry: &HashMap<String, bytes::Bytes>,
                had_error: bool| {
        ProxyServer::tr_note_definitions(acked, &[frames], registry, had_error, &policy)
    };

    note(&mut acked, &clean, &registry, false);
    assert_eq!(acked["r"].kind, StmtKind::Read);
    assert_eq!(acked["r"].parse, registry["r"]);
    // The backend may hold the old definition, the new one, or none.
    note(&mut acked, &clean, &registry, true);
    assert!(acked.is_empty());
    // The registry's Parse must be this very frame.
    note(&mut acked, &clean, &registry, false);
    let mut other = registry.clone();
    other.insert(
        "r".to_string(),
        bytes::Bytes::from(tr08_parse("r", "SELECT 2")),
    );
    note(&mut acked, &clean, &other, false);
    assert!(acked.is_empty());
    // Close forgets; Close then Parse in one cycle defines the new one.
    note(&mut acked, &clean, &registry, false);
    note(&mut acked, &frame(b'C', b"Sr\0"), &registry, false);
    assert!(acked.is_empty());
    note(
        &mut acked,
        &[frame(b'C', b"Sr\0"), p_r.clone()].concat(),
        &registry,
        false,
    );
    assert!(acked.contains_key("r"));
    // SQL that manages statements itself forgets every name.
    for sql in ["DEALLOCATE ALL", "DISCARD ALL", "PREPARE q AS SELECT 1"] {
        note(&mut acked, &clean, &registry, false);
        note(
            &mut acked,
            &[tr08_parse("", sql), tr08_bind("", ""), tr08_execute("")].concat(),
            &registry,
            false,
        );
        assert!(acked.is_empty(), "{sql}");
    }
    // An unparsable frame forgets every name.
    note(&mut acked, &clean, &registry, false);
    note(&mut acked, &[b'P', 0, 0], &registry, false);
    assert!(acked.is_empty());
}

/// Read one frontend frame (tag, body) from a test backend's socket.
async fn tr08_read_frame<R: tokio::io::AsyncRead + Unpin>(r: &mut R) -> (u8, Vec<u8>) {
    use tokio::io::AsyncReadExt;
    let mut header = [0u8; 5];
    r.read_exact(&mut header).await.unwrap();
    let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    let mut body = vec![0u8; len - 4];
    r.read_exact(&mut body).await.unwrap();
    (header[0], body)
}

/// Redefinition closes whatever the connection holds under the name,
/// re-Parses it and Syncs; a rejection is a protocol error.
#[tokio::test]
async fn tr08_redefine_closes_reparses_and_syncs() {
    let parse = tr08_parse("ins", "INSERT INTO t VALUES ($1)");
    for reject in [false, true] {
        let (mut proxy, mut backend) = tokio::io::duplex(64 * 1024);
        let fake = tokio::spawn(async move {
            let mut seen = Vec::new();
            loop {
                let f = tr08_read_frame(&mut backend).await;
                let done = f.0 == b'S';
                seen.push(f);
                if done {
                    break;
                }
            }
            let reply = if reject {
                [frame(b'E', b"SERROR\0C42601\0Mbad\0\0"), frame(b'Z', b"E")].concat()
            } else {
                [frame(b'3', &[]), frame(b'1', &[]), frame(b'Z', b"T")].concat()
            };
            backend.write_all(&reply).await.unwrap();
            seen
        });
        let r = ProxyServer::tr_redefine_statement(
            &mut proxy,
            "ins",
            &parse,
            Duration::from_secs(2),
            Duration::from_secs(2),
            1 << 20,
        )
        .await;
        let seen = fake.await.unwrap();
        assert_eq!(
            seen.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
            vec![b'C', b'P', b'S']
        );
        assert_eq!(seen[0].1, b"Sins\0");
        assert_eq!(frame(b'P', &seen[1].1), parse);
        if reject {
            assert!(matches!(r, Err(ProxyError::Protocol(_))), "{r:?}");
        } else {
            r.unwrap();
        }
    }
}

/// Replay restores an outside statement from the snapshot the cycle was
/// recorded with — not from the registry, which a later Parse may have
/// changed — before re-sending the cycle.
#[tokio::test]
async fn tr08_replay_redefines_outside_statements_from_the_snapshot() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let fake = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut seen = Vec::new();
        // Redefinition (C, P, S) then the recorded cycle (B, E, S).
        let mut syncs = 0;
        while syncs < 2 {
            let f = tr08_read_frame(&mut sock).await;
            let reply = match f.0 {
                b'C' => frame(b'3', &[]),
                b'P' => frame(b'1', &[]),
                b'B' => frame(b'2', &[]),
                b'E' => frame(b'C', b"INSERT 0 1\0"),
                b'S' => {
                    syncs += 1;
                    frame(b'Z', b"T")
                }
                other => panic!("unexpected frame {}", other as char),
            };
            seen.push(f);
            sock.write_all(&reply).await.unwrap();
        }
        seen
    });
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    conns.insert(
        addr.clone(),
        BackendConn::new(TcpStream::connect(&addr).await.unwrap()),
    );
    let acknowledged = bytes::Bytes::from(tr08_parse("ins", "INSERT INTO t VALUES ($1)"));
    // The session has since re-Parsed the name: the registry differs.
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    registry.insert(
        "ins".to_string(),
        bytes::Bytes::from(tr08_parse("ins", "DELETE FROM t")),
    );
    let cycle = [tr08_bind("", "ins"), tr08_execute(""), frame(b'S', &[])].concat();
    let entries = vec![StatementLog {
        sql: "INSERT INTO t VALUES ($1)".to_string(),
        params: Vec::new(),
        result_checksum: None,
        executed_at: chrono::Utc::now(),
        extended: Some(ExtendedBatchLog {
            frames: bytes::Bytes::from(cycle.clone()),
            unnamed_parse: None,
            defines: Vec::new(),
            refs: vec!["ins".to_string()],
            outside: vec![("ins".to_string(), acknowledged.clone())],
        }),
    }];
    let r =
        ProxyServer::tr_replay_transaction(&mut conns, &addr, &entries, &registry, &state).await;
    assert!(r.is_ok());
    let seen = fake.await.unwrap();
    assert_eq!(
        seen.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
        vec![b'C', b'P', b'S', b'B', b'E', b'S']
    );
    assert_eq!(frame(b'P', &seen[1].1), acknowledged.to_vec());
    assert!(conns[&addr].prepared.contains("ins"));
}

/// The recorder end to end: a statement prepared in its own cycle before
/// BEGIN is acknowledged; a later cycle inside the transaction that binds
/// it is recorded as replayable with the acknowledged Parse; a Parse that
/// only a Flush-chunked cycle carried is never acknowledged.
#[tokio::test]
async fn tr08_recorder_snapshots_statements_from_earlier_cycles() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Transaction);
    let ins = tr08_parse("ins", "INSERT INTO t VALUES ($1)");
    let late = tr08_parse("late", "SELECT 2");
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    registry.insert("ins".to_string(), bytes::Bytes::from(ins.clone()));
    registry.insert("late".to_string(), bytes::Bytes::from(late.clone()));
    let sync = || bytes::Bytes::from(frame(b'S', &[]));

    // Parse "ins" + Sync, outside any transaction.
    ProxyServer::note_ready_for_query(&session, b'I', false);
    let prepare = bytes::Bytes::from([ins.clone(), frame(b'S', &[])].concat());
    ProxyServer::tr_after_extended(
        &mut tr,
        &prepare,
        None,
        Some("INSERT INTO t VALUES ($1)"),
        true,
        &["ins".to_string()],
        &[],
        &registry,
        &session,
        &state,
    )
    .await;
    assert_eq!(tr.acked["ins"].kind, StmtKind::Write);

    // Parse "late" + Flush, then Sync: the Flush answer could have carried
    // an error the Sync never reports, so "late" stays unacknowledged.
    let chunk = bytes::Bytes::from([late.clone(), frame(b'H', &[])].concat());
    ProxyServer::tr_after_extended(
        &mut tr,
        &chunk,
        None,
        Some("SELECT 2"),
        false,
        &["late".to_string()],
        &[],
        &registry,
        &session,
        &state,
    )
    .await;
    ProxyServer::tr_after_extended(
        &mut tr,
        &sync(),
        None,
        None,
        true,
        &[],
        &[],
        &registry,
        &session,
        &state,
    )
    .await;
    assert!(!tr.acked.contains_key("late"));
    assert!(tr.acked.contains_key("ins"));

    // BEGIN, then Bind/Execute "ins" in a later cycle.
    ProxyServer::note_ready_for_query(&session, b'T', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
    let use_ins = bytes::Bytes::from(
        [
            tr08_bind("cur", "ins"),
            tr08_execute("cur"),
            frame(b'S', &[]),
        ]
        .concat(),
    );
    ProxyServer::tr_after_extended(
        &mut tr,
        &use_ins,
        None,
        None,
        true,
        &[],
        &["ins".to_string()],
        &registry,
        &session,
        &state,
    )
    .await;
    {
        let ts = session.tx_state.read().await;
        assert!(
            !ts.non_replayable,
            "a bound acknowledged statement is replayable"
        );
        assert!(ts.has_writes);
        let ext = ts.statements[1].extended.as_ref().expect("extended entry");
        assert_eq!(
            ext.outside,
            vec![("ins".to_string(), registry["ins"].clone())]
        );
        assert_eq!(
            ts.replay_bytes,
            "BEGIN".len() + use_ins.len() + "ins".len() + ins.len()
        );
    }
    assert_eq!(tr.tx_portals.get("cur"), Some(&StmtKind::Write));

    // A statement only a Flush-chunked cycle defined stays opaque.
    let use_late =
        bytes::Bytes::from([tr08_bind("", "late"), tr08_execute(""), frame(b'S', &[])].concat());
    ProxyServer::tr_after_extended(
        &mut tr,
        &use_late,
        None,
        None,
        true,
        &[],
        &["late".to_string()],
        &registry,
        &session,
        &state,
    )
    .await;
    assert!(session.tx_state.read().await.non_replayable);

    // DEALLOCATE forgets every name; COMMIT ends the portals.
    ProxyServer::tr_after_simple(&mut tr, &qmsg("DEALLOCATE ALL"), &session, &state).await;
    assert!(tr.acked.is_empty());
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ProxyServer::tr_after_simple(&mut tr, &qmsg("COMMIT"), &session, &state).await;
    assert!(tr.tx_portals.is_empty());
}

// ---- TR-09: holding a re-executable read until ReadyForQuery ----

/// Relay `response` (then EOF from the backend) with `hold`, through the
/// plain or the capture relay; returns what the client received and the
/// relay's result (bytes sent, or the failure's progress).
async fn tr09_relay(
    response: Vec<u8>,
    hold: usize,
    capture: bool,
) -> (Vec<u8>, std::result::Result<u64, ResponseProgress>) {
    let (mut backend, mut peer) = pair().await;
    let (client, mut recipient) = pair().await;
    let mut client = ClientStream::Plain(client);
    let session = make_test_session();
    let mut config = test_config();
    config.limits.tr_buffer_small_reads_bytes = hold;
    let server = ProxyServer::new(config).unwrap();
    let feed = tokio::spawn(async move {
        peer.write_all(&response).await.unwrap();
    });
    let receive = tokio::spawn(async move {
        let mut received = Vec::new();
        recipient.read_to_end(&mut received).await.unwrap();
        received
    });
    let r = if capture {
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        {
            ProxyServer::stream_until_ready_capture_buffered(
                &mut client,
                &mut backend,
                &mut BytesMut::new(),
                &session,
                RelayLimits {
                    hold_bytes: hold,
                    ..server.state.limits.relay()
                },
                usize::MAX,
                &server.state.metrics,
            )
            .await
            .map(|(sent, ..)| sent)
        }
        #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
        {
            unreachable!()
        }
    } else {
        ProxyServer::stream_until_ready_buffered(
            &mut client,
            &mut backend,
            &mut BytesMut::new(),
            &session,
            &server.state,
            hold,
        )
        .await
    };
    drop(client);
    feed.await.unwrap();
    (receive.await.unwrap(), r.map_err(|f| f.progress))
}

#[tokio::test]
async fn tr09_held_read_publishes_nothing_before_ready_for_query() {
    let rows = [
        frame(b'T', b"description"),
        frame(b'D', b"row one"),
        frame(b'D', b"row two"),
        frame(b'C', b"SELECT 2\0"),
    ]
    .concat();
    let whole = [rows.clone(), frame(b'Z', b"I")].concat();
    for capture in [false, true] {
        #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
        if capture {
            continue;
        }
        // Held: the backend dies before ReadyForQuery, the client saw
        // nothing and no terminal frame, so the read can be re-executed.
        let (seen, r) = tr09_relay(rows.clone(), 1024, capture).await;
        assert!(seen.is_empty(), "capture={capture}");
        let progress = r.unwrap_err();
        assert_eq!(progress.bytes, 0);
        assert!(!progress.terminal);
        assert_eq!(
            ProxyServer::tr_response_action(TrAction::Reexecute, progress, false),
            TrAction::Reexecute
        );
        // Held and complete: everything arrives, once.
        let (seen, r) = tr09_relay(whole.clone(), 1024, capture).await;
        assert_eq!(seen, whole);
        assert_eq!(r.unwrap(), whole.len() as u64);
        // Outgrown: published at the bound, then the fault keeps the usual
        // publication rule (rows and CommandComplete were shown).
        let (seen, r) = tr09_relay(rows.clone(), 8, capture).await;
        assert_eq!(seen, rows);
        let progress = r.unwrap_err();
        assert_eq!(progress.bytes, rows.len() as u64);
        assert!(progress.terminal);
        // Off: streams as before.
        let (seen, r) = tr09_relay(rows.clone(), 0, capture).await;
        assert_eq!(seen, rows);
        assert!(r.unwrap_err().terminal);
    }
}

/// Only a read the proxy may re-execute, under select/transaction, is held.
#[test]
fn tr09_hold_applies_to_reexecutable_reads_only() {
    let mut config = test_config();
    config.limits.tr_buffer_small_reads_bytes = 4096;
    let server = ProxyServer::new(config).unwrap();
    let session = |mode| {
        let mut s = make_test_session();
        Arc::get_mut(&mut s).unwrap().tr_mode = mode;
        s
    };
    for (mode, kind, want) in [
        (TrMode::Transaction, StmtKind::Read, 4096),
        (TrMode::Select, StmtKind::Read, 4096),
        (TrMode::Transaction, StmtKind::Write, 0),
        (TrMode::Transaction, StmtKind::Commit, 0),
        (TrMode::Session, StmtKind::Read, 0),
        (TrMode::None, StmtKind::Read, 0),
    ] {
        assert_eq!(
            ProxyServer::tr_hold_bytes(&session(mode), &server.state, || kind),
            want,
            "{mode:?}/{kind:?}"
        );
    }
    let off = ProxyServer::new(test_config()).unwrap();
    assert_eq!(
        ProxyServer::tr_hold_bytes(&session(TrMode::Transaction), &off.state, || {
            panic!("not classified when off")
        }),
        0
    );
}

/// TR-08 review F1: a batch the aborted-transaction emulation refuses is
/// never forwarded, so its Parses must not survive in the registry; undoing
/// them in reverse restores what the backend kept.
#[test]
fn tr08_refused_batch_registry_writes_are_undone() {
    let v = |s: &str| bytes::Bytes::from(s.to_string());
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    registry.insert("s1".into(), v("defA"));
    // Close S1 / Parse S1 defB / Parse S2 / Parse S1 defC, all refused 25P02
    // (elements evaluate in order, like the Parses arriving).
    let undo = vec![
        ("s1".to_string(), registry.insert("s1".into(), v("defB"))),
        ("s2".to_string(), registry.insert("s2".into(), v("new"))),
        ("s1".to_string(), registry.insert("s1".into(), v("defC"))),
    ];
    ProxyServer::undo_map_writes(&mut registry, undo);
    assert_eq!(registry.len(), 1);
    assert_eq!(registry["s1"], v("defA"));
}

/// TR-08 review F3/F4: a cycle that yielded for COPY acknowledges nothing
/// (the backend may never reach its later Parses), and a COPY drain that
/// ends the transaction drops the transaction's portals.
#[tokio::test]
async fn tr08_copy_cycles_acknowledge_nothing_and_end_portals() {
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let session = make_test_session();
    let mut tr = TrSession::new(TrMode::Transaction);
    let s2 = tr08_parse("s2", "SELECT 2");
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    registry.insert("s2".to_string(), bytes::Bytes::from(s2.clone()));
    ProxyServer::note_ready_for_query(&session, b'I', false);
    session
        .copy_in_progress
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let batch = bytes::Bytes::from([s2.clone(), frame(b'S', &[])].concat());
    ProxyServer::tr_after_extended(
        &mut tr,
        &batch,
        None,
        Some("SELECT 2"),
        true,
        &["s2".to_string()],
        &[],
        &registry,
        &session,
        &state,
    )
    .await;
    assert!(
        tr.acked.is_empty(),
        "a COPY-yielded cycle acknowledges no Parse"
    );
    session
        .copy_in_progress
        .store(false, std::sync::atomic::Ordering::Relaxed);
    tr.tx_portals.insert("cur".to_string(), StmtKind::Read);
    ProxyServer::note_ready_for_query(&session, b'I', false);
    ProxyServer::tr_after_copy_drain(&mut tr, &session, &state).await;
    assert!(tr.tx_portals.is_empty());
}

// ---- F2: a Close reaches only the connection its batch is routed to ----

fn f2_names(set: &std::collections::HashSet<String>) -> Vec<&str> {
    let mut names: Vec<&str> = set.iter().map(String::as_str).collect();
    names.sort_unstable();
    names
}

/// A fake backend that answers one connection's frames until a Sync: Close,
/// Parse and Bind complete, Execute reports `tag`, Flush gets no reply.
async fn f2_fake_backend(
    listener: tokio::net::TcpListener,
    tag: &'static [u8],
) -> Vec<(u8, Vec<u8>)> {
    let (mut sock, _) = listener.accept().await.unwrap();
    let mut seen = Vec::new();
    loop {
        let f = tr08_read_frame(&mut sock).await;
        let reply = match f.0 {
            b'C' => frame(b'3', &[]),
            b'P' => frame(b'1', &[]),
            b'B' => frame(b'2', &[]),
            b'E' => frame(b'C', tag),
            b'H' => Vec::new(),
            b'S' => frame(b'Z', b"I"),
            other => panic!("unexpected frame {}", other as char),
        };
        let done = f.0 == b'S';
        seen.push(f);
        sock.write_all(&reply).await.unwrap();
        if done {
            return seen;
        }
    }
}

/// Close bookkeeping: the connection that ran the batch drops the names the
/// batch left closed (but keeps one it Parsed again after the Close) and is
/// no longer provably clean; every other connection's copy of a closed name
/// turns stale.
#[tokio::test]
async fn f2_closes_prune_the_target_and_mark_other_connections_stale() {
    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    for addr in ["primary:5432", "standby:5432"] {
        let mut bc = BackendConn::new(pair().await.0);
        bc.prepared.extend(["keep", "s1", "s2"].map(String::from));
        conns.insert(addr.to_string(), bc);
    }
    // `Close s1; Parse s1 ...; Close s2; Sync` ran on the standby.
    let closes = ["s1", "s2"].map(String::from);
    let mut left_closed = Vec::new();
    ProxyServer::remember_batch_name(&mut left_closed, "s1", 8);
    ProxyServer::forget_batch_close(&mut left_closed, "s1");
    ProxyServer::remember_batch_name(&mut left_closed, "s2", 8);
    assert_eq!(left_closed, vec!["s2".to_string()]);
    ProxyServer::note_statement_closes(&mut conns, Some("standby:5432"), &closes, &left_closed);

    let standby = &conns["standby:5432"];
    assert_eq!(f2_names(&standby.prepared), ["keep", "s1"]);
    assert!(standby.stale.is_empty());
    #[cfg(feature = "pool-modes")]
    assert!(
        standby.dirty,
        "a Close may have been skipped after an error"
    );
    let primary = &conns["primary:5432"];
    assert_eq!(f2_names(&primary.prepared), ["keep"]);
    assert_eq!(f2_names(&primary.stale), ["s1", "s2"]);

    // A batch that reached no backend treats every connection as "other".
    let keep = ["keep".to_string()];
    ProxyServer::note_statement_closes(&mut conns, None, &keep, &keep);
    assert_eq!(f2_names(&conns["standby:5432"].stale), ["keep"]);
    assert!(conns["primary:5432"].prepared.is_empty());
    assert_eq!(f2_names(&conns["primary:5432"].stale), ["keep", "s1", "s2"]);
}

/// A batch routed to a connection that still holds a statement the session
/// closed through another connection: the proxy closes it there first. A
/// Bind then runs the session's current definition (re-prepared from the
/// registry), and a Parse of the name does not fail with 42P05. The client
/// sees only the batch's own reply.
#[tokio::test]
async fn f2_stale_statement_is_closed_before_the_batch_uses_it() {
    let current = tr08_parse("s1", "SELECT v FROM t WHERE id = 1");
    let bind_batch = [tr08_bind("", "s1"), tr08_execute(""), frame(b'S', &[])].concat();
    let parse_batch = [current.clone(), bind_batch.clone()].concat();
    struct Case {
        batch: Vec<u8>,
        reprepare: Vec<String>,
        defines: Vec<String>,
        /// Frame types the backend must see, in order.
        want: &'static [u8],
    }
    let cases = [
        Case {
            batch: bind_batch,
            reprepare: vec!["s1".to_string()],
            defines: Vec::new(),
            want: b"CHPHBES",
        },
        Case {
            batch: parse_batch,
            reprepare: Vec::new(),
            defines: vec!["s1".to_string()],
            want: b"CHPBES",
        },
    ];
    for case in cases {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let fake = tokio::spawn(f2_fake_backend(listener, b"SELECT 1\0"));
        let config = test_config();
        let server = ProxyServer::new(config.clone()).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        let (client_raw, mut client_peer) = pair().await;
        let mut client = ClientStream::Plain(client_raw);
        let mut conns: HashMap<String, BackendConn> = HashMap::new();
        let mut bc = BackendConn::new(TcpStream::connect(&addr).await.unwrap());
        bc.stale.insert("s1".to_string());
        conns.insert(addr.clone(), bc);
        let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
        registry.insert("s1".to_string(), bytes::Bytes::from(current.clone()));
        let mut fault = None;
        let (node, _) = ProxyServer::forward_extended_batch(
            &mut client,
            &case.batch,
            None,
            true,
            &mut conns,
            Some(addr.as_str()),
            &ReadRouting::OFF,
            &registry,
            &case.reprepare,
            &case.defines,
            None,
            0,
            &session,
            &state,
            &config,
            &mut fault,
        )
        .await
        .unwrap();
        assert_eq!(node.as_deref(), Some(addr.as_str()));

        let seen = fake.await.unwrap();
        assert_eq!(seen.iter().map(|(t, _)| *t).collect::<Vec<_>>(), case.want);
        assert_eq!(seen[0].1, b"Ss1\0");
        assert_eq!(frame(b'P', &seen[2].1), current, "the current definition");
        let bc = &conns[&addr];
        assert!(bc.prepared.contains("s1") && bc.stale.is_empty());

        let mut reply = [
            frame(b'2', &[]),
            frame(b'C', b"SELECT 1\0"),
            frame(b'Z', b"I"),
        ]
        .concat();
        if !case.defines.is_empty() {
            reply = [frame(b'1', &[]), reply].concat();
        }
        let mut got = vec![0u8; reply.len()];
        client_peer.read_exact(&mut got).await.unwrap();
        assert_eq!(got, reply);
    }
}

/// Transaction replay onto a connection that still holds a statement the
/// session closed elsewhere closes it before re-preparing the registry's
/// definition and re-sending the cycle.
#[tokio::test]
async fn f2_replay_closes_a_stale_statement_first() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let fake = tokio::spawn(f2_fake_backend(listener, b"INSERT 0 1\0"));
    let server = ProxyServer::new(test_config()).unwrap();
    let state = server.state.clone();
    let mut conns: HashMap<String, BackendConn> = HashMap::new();
    let mut bc = BackendConn::new(TcpStream::connect(&addr).await.unwrap());
    bc.stale.insert("ins".to_string());
    conns.insert(addr.clone(), bc);
    let parse = tr08_parse("ins", "INSERT INTO t VALUES (1)");
    let mut registry: HashMap<String, bytes::Bytes> = HashMap::new();
    registry.insert("ins".to_string(), bytes::Bytes::from(parse.clone()));
    let cycle = [tr08_bind("", "ins"), tr08_execute(""), frame(b'S', &[])].concat();
    let entries = vec![StatementLog {
        sql: "INSERT INTO t VALUES (1)".to_string(),
        params: Vec::new(),
        result_checksum: None,
        executed_at: chrono::Utc::now(),
        extended: Some(ExtendedBatchLog {
            frames: bytes::Bytes::from(cycle),
            unnamed_parse: None,
            defines: Vec::new(),
            refs: vec!["ins".to_string()],
            outside: Vec::new(),
        }),
    }];
    let r =
        ProxyServer::tr_replay_transaction(&mut conns, &addr, &entries, &registry, &state).await;
    assert!(r.is_ok());
    let seen = fake.await.unwrap();
    assert_eq!(seen.iter().map(|(t, _)| *t).collect::<Vec<_>>(), b"CHPHBES");
    assert_eq!(seen[0].1, b"Sins\0");
    assert_eq!(frame(b'P', &seen[2].1), parse);
    let bc = &conns[&addr];
    assert!(bc.prepared.contains("ins") && bc.stale.is_empty());
}
