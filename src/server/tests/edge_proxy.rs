use super::ProxyServer;
use std::collections::HashMap;

/// `edge_write_needs_invalidation` with the multi-statement fact
/// derived from `sql` — exactly what the forward path passes from
/// `StmtFacts::has_interior_semicolon`.
fn ewni(is_write: bool, sql: &str) -> bool {
    ProxyServer::edge_write_needs_invalidation(
        is_write,
        sql,
        ProxyServer::stmt_has_interior_semicolon(sql),
    )
}

#[test]
fn bare_txn_control_is_exempt_from_invalidation() {
    // F10: BEGIN/START/SAVEPOINT/RELEASE/ROLLBACK change no rows —
    // an ORM's txn-per-request must not full-flush the fleet twice
    // per request.
    for sql in [
        "BEGIN",
        "begin;",
        "START TRANSACTION",
        "SAVEPOINT sp1",
        "RELEASE SAVEPOINT sp1",
        "ROLLBACK",
        "ROLLBACK TO SAVEPOINT sp1;",
    ] {
        assert!(!ewni(true, sql), "{sql:?} must be exempt");
    }
    // COMMIT keeps its conservative flush (closes the in-txn
    // write visibility window), and SET stays (GUC mitigation).
    assert!(ewni(true, "COMMIT"));
    assert!(ewni(true, "SET search_path TO tenant_b"));
}

#[test]
fn compound_txn_strings_still_invalidate() {
    // A multi-statement string may hide a write behind a
    // txn-control or SELECT lead — never exempt it.
    assert!(ewni(true, "BEGIN; UPDATE t SET v = 1; COMMIT"));
    // SELECT-leading batch with a trailing write classifies
    // is_write=false, but the interior `;` forces invalidation.
    assert!(ewni(false, "SELECT v FROM t; UPDATE t SET v = 1"));
    // A plain single SELECT does not invalidate.
    assert!(!ewni(false, "SELECT v FROM t"));
}

#[test]
fn copy_from_counts_as_write() {
    assert!(ewni(false, "COPY t FROM STDIN"));
    assert!(ProxyServer::is_edge_copy_write_sql("copy t from stdin"));
    // COPY TO exports rows — a read.
    assert!(!ewni(false, "COPY t TO STDOUT"));
}

#[test]
fn procedural_and_txn_end_trigger_invalidation() {
    // G3: SQL-level EXECUTE/CALL/DO are opaque writes — the simple path
    // must invalidate and the classifier flags them (the extended path
    // memoizes them as the empty-set wildcard via the same predicate).
    for sql in [
        "EXECUTE ins(1)",
        "execute ins(1)",
        "CALL do_write()",
        "DO $$ BEGIN PERFORM 1; END $$",
    ] {
        assert!(
            ProxyServer::is_edge_procedural_sql(sql),
            "{sql:?} is procedural"
        );
        assert!(
            ewni(false, sql),
            "{sql:?} must invalidate on the simple path"
        );
    }
    for sql in [
        "INSERT INTO t VALUES (1)",
        "SELECT 1 FROM t",
        "UPDATE t SET v=1",
    ] {
        assert!(
            !ProxyServer::is_edge_procedural_sql(sql),
            "{sql:?} is not procedural"
        );
    }

    // G2: COMMIT and its END synonym both trigger the wildcard flush
    // (closing the commit-visibility window); the openers do not.
    for sql in [
        "COMMIT",
        "commit work",
        "COMMIT PREPARED 'x'",
        "END",
        "END TRANSACTION",
        "end;",
    ] {
        assert!(ProxyServer::is_edge_txn_end_sql(sql), "{sql:?} ends a txn");
        assert!(
            ewni(false, sql),
            "{sql:?} must invalidate (commit-visibility window)"
        );
    }
    for sql in ["BEGIN", "START TRANSACTION", "SAVEPOINT s1", "ROLLBACK"] {
        assert!(
            !ProxyServer::is_edge_txn_end_sql(sql),
            "{sql:?} is not a txn-end"
        );
        assert!(
            !ewni(false, sql),
            "{sql:?} must stay exempt on the simple path"
        );
    }
}

#[test]
fn edge_meta_prunable_protects_reparsed_names() {
    // G1: at a Sync, a Closed name NOT re-Parsed this batch is prunable;
    // a name Closed then re-Parsed (Npgsql statement replacement) must be
    // RETAINED so its fresh DML metadata survives to invalidate.
    let closes = vec!["S1".to_string(), "S2".to_string()];
    let none: Vec<String> = vec![];
    assert_eq!(
        ProxyServer::edge_meta_prunable(&closes, &none),
        vec!["S1", "S2"]
    );
    // S1 re-Parsed in the same batch → excluded from pruning.
    assert_eq!(
        ProxyServer::edge_meta_prunable(&closes, &["S1".to_string()]),
        vec!["S2"]
    );
    // Every closed name re-Parsed → nothing pruned (all meta kept fresh).
    assert!(ProxyServer::edge_meta_prunable(&closes, &closes).is_empty());
}

#[test]
fn edge_dml_classifier_covers_dml_not_txn_control() {
    for sql in [
        "INSERT INTO t VALUES (1)",
        "update t set v = 1",
        "DELETE FROM t WHERE id = 1",
        "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
        "TRUNCATE t",
        "ALTER TABLE t ADD COLUMN c int",
        "COPY t FROM STDIN",
        "WITH del AS (DELETE FROM t RETURNING id) SELECT * FROM del",
    ] {
        assert!(ProxyServer::is_edge_dml_sql(sql), "{sql:?} is DML");
    }
    // Txn control / reads / plain CTE reads: NOT invalidation
    // triggers (BEGIN/COMMIT here would full-flush per txn).
    for sql in [
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SET search_path TO x",
        "SELECT v FROM t",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "COPY t TO STDOUT",
    ] {
        assert!(!ProxyServer::is_edge_dml_sql(sql), "{sql:?} is not DML");
    }
}

#[test]
fn extended_batch_tables_union_and_wildcard() {
    let mut named: HashMap<String, Option<Vec<String>>> = HashMap::new();
    named.insert("ins".into(), Some(vec!["orders".into()]));
    named.insert("upd".into(), Some(vec!["users".into()]));
    named.insert("sel".into(), None);
    named.insert("weird".into(), Some(vec![])); // unattributable DML
    let unnamed: Option<Vec<String>> = Some(vec!["events".into()]);

    // Read-only batch: no invalidation.
    assert_eq!(
        ProxyServer::edge_extended_batch_tables(&["sel".to_string()], false, &named, &unnamed),
        None
    );
    // DML batch: union of referenced statements' tables.
    let t = ProxyServer::edge_extended_batch_tables(
        &["ins".to_string(), "upd".to_string(), "ins".to_string()],
        false,
        &named,
        &unnamed,
    )
    .expect("dml");
    assert_eq!(t, vec!["orders".to_string(), "users".to_string()]);
    // Unnamed execution folds in.
    let t =
        ProxyServer::edge_extended_batch_tables(&[], true, &named, &unnamed).expect("unnamed dml");
    assert_eq!(t, vec!["events".to_string()]);
    // Any unattributable DML → wildcard (invalidate everything).
    let t = ProxyServer::edge_extended_batch_tables(
        &["ins".to_string(), "weird".to_string()],
        false,
        &named,
        &unnamed,
    )
    .expect("dml");
    assert!(t.is_empty(), "wildcard invalidation");
    // Unknown names (never Parse'd — backend errors anyway) are
    // treated as non-DML.
    assert_eq!(
        ProxyServer::edge_extended_batch_tables(&["ghost".to_string()], false, &named, &None),
        None
    );
}
