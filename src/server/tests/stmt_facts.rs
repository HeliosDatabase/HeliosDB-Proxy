use super::super::stmt_fact_classifications;
use super::{ProxyServer, StmtFacts};

/// Statement table spanning every classifier branch the facts
/// memoize: reads, CTEs, all DML verbs, DDL, COPY in both
/// directions, session-state verbs, transaction control, PREPARE /
/// EXECUTE / LISTEN, multi-statement strings, leading and hint
/// comments, volatile and locking reads, `SELECT ... INTO`, and case
/// / whitespace variants.
const CASES: [&str; 48] = [
    "SELECT 1",
    "select v from t",
    "  \t\n SELECT v FROM t  ",
    "SELECT v FROM t;",
    "SELECT v FROM t ; ",
    "SELECT v FROM t WHERE into_total > 1",
    "SELECT * INTO tmp FROM t",
    "select now()",
    "SELECT random()",
    "select nextval('s')",
    "SELECT set_config('x','y',false)",
    "SELECT pg_advisory_lock(1)",
    "SELECT v FROM t FOR UPDATE",
    "SELECT v FROM t FOR SHARE",
    "WITH c AS (SELECT 1) SELECT * FROM c",
    "WITH c AS (SELECT 1) SELECT * INTO tmp FROM c",
    "VALUES (1),(2)",
    "TABLE t",
    "SHOW search_path",
    "EXPLAIN SELECT 1",
    "FETCH ALL FROM cur",
    "INSERT INTO t VALUES (1)",
    "insert into t (a) values (1)",
    "UPDATE t SET v = 1",
    "DELETE FROM t WHERE id = 1",
    "CREATE TABLE t (id int)",
    "DROP TABLE t",
    "ALTER TABLE t ADD COLUMN c int",
    "TRUNCATE t",
    "GRANT SELECT ON t TO r",
    "REVOKE SELECT ON t FROM r",
    "VACUUM ANALYZE t",
    "REINDEX TABLE t",
    "CLUSTER t",
    "COPY t FROM STDIN",
    "COPY t TO STDOUT",
    "SET search_path TO tenant_b",
    "set TimeZone = 'UTC'",
    "SET TRANSACTION READ ONLY",
    "RESET ALL",
    "DISCARD ALL",
    "PREPARE p AS SELECT 1",
    "EXECUTE p(1)",
    "LISTEN chan",
    "BEGIN",
    "COMMIT",
    "ROLLBACK TO SAVEPOINT sp1;",
    "SELECT v FROM t; UPDATE t SET v = 1",
];

/// Extra strings that are not plain statements: empty, whitespace,
/// leading block/line comments and a routing-hint comment. These
/// exercise the "leading comment masks the verb" branches.
const ODD_CASES: [&str; 7] = [
    "",
    "   ",
    "/* leading */ SELECT 1",
    "-- leading\nSELECT 1",
    "/*helios:route=primary*/ SELECT 1",
    "/*helios:route=primary*/ UPDATE t SET v = 1",
    "BEGIN; UPDATE t SET v = 1; COMMIT",
];

/// Every getter must agree with the legacy classifier it memoizes —
/// for every statement shape. This is the contract that makes
/// threading the facts through the forward path a pure optimisation
/// rather than a behaviour change.
#[test]
fn facts_agree_with_legacy_classifiers() {
    assert!(CASES.len() + ODD_CASES.len() >= 40);
    for sql in CASES.iter().chain(ODD_CASES.iter()).copied() {
        let mut f = StmtFacts::new(sql);
        assert_eq!(
            f.is_write(),
            ProxyServer::is_write_query(sql),
            "is_write mismatch for {sql:?}"
        );
        #[cfg(feature = "edge-proxy")]
        assert_eq!(
            f.has_interior_semicolon(),
            ProxyServer::stmt_has_interior_semicolon(sql),
            "has_interior_semicolon mismatch for {sql:?}"
        );
        #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
        assert_eq!(
            f.leaves_session_state(),
            ProxyServer::stmt_leaves_session_state(sql),
            "leaves_session_state mismatch for {sql:?}"
        );
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        assert_eq!(
            f.is_cacheable_read(),
            ProxyServer::is_cacheable_read_sql(sql),
            "is_cacheable_read mismatch for {sql:?}"
        );
    }
}

/// LAZINESS CONTRACT (the reason the memo is `Option`-celled rather
/// than computed up front): building the facts — the one thing the
/// forward path does for EVERY simple query — classifies nothing at
/// all. In the stock configuration `skip_clean_reset`, the query cache
/// and the edge proxy are all off, so no gate ever asks and the
/// statement is never scanned by any of these classifiers.
#[test]
fn building_facts_classifies_nothing() {
    let before = stmt_fact_classifications();

    let facts = StmtFacts::new("SELECT a, b FROM t WHERE id = 1");
    let msg = crate::protocol::QueryMessage {
        query: "INSERT INTO t VALUES (1)".to_string(),
    }
    .encode();
    let from_msg = StmtFacts::of_query(&msg);
    // Reading the borrowed text is not a classification either.
    assert_eq!(facts.sql, "SELECT a, b FROM t WHERE id = 1");
    assert_eq!(from_msg.sql, "INSERT INTO t VALUES (1)");

    assert_eq!(
        stmt_fact_classifications(),
        before,
        "constructing StmtFacts must not run any classifier"
    );
}

/// …and once a gate does ask, the answer is computed exactly once no
/// matter how many gates (or how many calls) consult it.
#[test]
fn each_fact_is_classified_at_most_once() {
    let sql = "SELECT v FROM t";
    let mut f = StmtFacts::new(sql);

    let before = stmt_fact_classifications();
    let first = f.is_write();
    let second = f.is_write();
    let third = f.is_write();
    assert_eq!(first, second);
    assert_eq!(first, third);
    assert_eq!(
        stmt_fact_classifications() - before,
        1,
        "is_write must be classified once, then memoized"
    );

    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    {
        let before = stmt_fact_classifications();
        let _ = f.leaves_session_state();
        let _ = f.leaves_session_state();
        assert_eq!(stmt_fact_classifications() - before, 1);
    }
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    {
        let before = stmt_fact_classifications();
        let _ = f.is_cacheable_read();
        let _ = f.is_cacheable_read();
        assert_eq!(stmt_fact_classifications() - before, 1);
    }
    #[cfg(feature = "edge-proxy")]
    {
        let before = stmt_fact_classifications();
        let _ = f.has_interior_semicolon();
        let _ = f.has_interior_semicolon();
        assert_eq!(stmt_fact_classifications() - before, 1);
    }
}

/// The forward path rebuilds the facts when a routing-hint strip, a
/// rewrite rule or the tenant transform replaced the SQL. The rebuild
/// must describe the NEW text — i.e. clear the memo — otherwise the
/// cache / edge / pool gates would consult facts derived from the
/// pre-rewrite string. The hint-strip case is discriminating: a
/// leading `helios:` comment masks the SELECT lead, so the fact flips.
#[test]
fn rebuilding_on_changed_sql_clears_the_memo() {
    let hinted = "/*helios:route=primary*/ SELECT v FROM t";
    let stripped = "SELECT v FROM t";

    let mut before = StmtFacts::new(hinted);
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    assert!(
        !before.is_cacheable_read(),
        "leading comment masks the SELECT lead"
    );
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    assert!(before.leaves_session_state(), "…and the neutral lead too");
    let _ = before.is_write();

    // Rebuilt exactly as `forward_simple_query` does it, on the final
    // message: a fresh value, so nothing memoized from `hinted`
    // survives.
    let msg = crate::protocol::QueryMessage {
        query: stripped.to_string(),
    }
    .encode();
    let mut after = StmtFacts::of_query(&msg);
    assert_eq!(after.sql, stripped);
    let count_before = stmt_fact_classifications();
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    assert!(
        after.is_cacheable_read(),
        "the stripped SELECT is cacheable"
    );
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    assert!(!after.leaves_session_state());
    assert!(!after.is_write());
    assert!(
        stmt_fact_classifications() > count_before,
        "the rebuilt facts must re-classify, not reuse the old answers"
    );
}

/// The multi-statement fact is exactly the interior-`;` rule the edge
/// invalidation gate used to re-derive inline.
#[cfg(feature = "edge-proxy")]
#[test]
fn interior_semicolon_only_counts_non_trailing() {
    for (sql, want) in [
        ("SELECT 1", false),
        ("SELECT 1;", false),
        ("SELECT 1 ; ", false),
        ("SELECT 1; SELECT 2", true),
        ("BEGIN; UPDATE t SET v = 1; COMMIT", true),
        ("", false),
    ] {
        assert_eq!(
            ProxyServer::stmt_has_interior_semicolon(sql),
            want,
            "{sql:?}"
        );
        assert_eq!(
            StmtFacts::new(sql).has_interior_semicolon(),
            want,
            "{sql:?}"
        );
    }
}

/// `of_query` borrows the SQL carried by a `Query` message, and a
/// payload that is not a valid query cstring falls back to the empty
/// statement — for which every classifier answers `false`, the same
/// fallback each individual call site used before the facts existed.
#[test]
fn of_query_reads_the_message_payload() {
    let msg = crate::protocol::QueryMessage {
        query: "UPDATE t SET v = 1".to_string(),
    }
    .encode();
    let mut f = StmtFacts::of_query(&msg);
    assert_eq!(f.sql, "UPDATE t SET v = 1");
    assert!(f.is_write());

    let empty =
        crate::protocol::Message::new(crate::protocol::MessageType::Query, bytes::BytesMut::new());
    let mut f = StmtFacts::of_query(&empty);
    assert_eq!(f.sql, "");
    assert!(!f.is_write());
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    assert!(!f.leaves_session_state());
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    assert!(!f.is_cacheable_read());
    #[cfg(feature = "edge-proxy")]
    assert!(!f.has_interior_semicolon());
}
