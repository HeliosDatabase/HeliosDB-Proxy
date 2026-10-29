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

/// R6-2: a string of more than one statement is read one statement at a
/// time, split at the lexer's `;`. A string of reads (and bare transaction
/// control) invalidates nothing: it used to flush every edge entry, since
/// `write_tables` gives no table set for more than one statement. One with a
/// write, or with text the backends may split differently, invalidates.
#[test]
fn multi_statement_reads_do_not_invalidate() {
    for sql in [
        "SELECT a FROM t1; SELECT b FROM t2",
        "SELECT a FROM t1; SELECT b FROM t2;",
        "SELECT 'a;b' FROM t",
        "/* x */ SELECT 1; -- y\nSELECT 2",
        "BEGIN; SELECT a FROM t; ROLLBACK",
    ] {
        assert!(!ewni(false, sql), "{sql:?}");
    }
    assert!(!ewni(true, "BEGIN; SELECT a FROM t; ROLLBACK"));
    for sql in [
        "SELECT a FROM t1; UPDATE t2 SET v = 1",
        "SELECT a FROM t1; /* job */ DELETE FROM t2",
        "SELECT a FROM t1; MERGE BRANCH fix INTO main",
        "SELECT a FROM t1; COMMIT",
        "SELECT a FROM t1; SET search_path TO b",
        "SELECT a FROM t1; \u{131}nsert \u{131}nto kv VALUES (1)",
        "SELECT E'\\'' ; UPDATE b SET v = 2",
        "SELECT 1 -- ; \n; SELECT 2",
    ] {
        assert!(ewni(false, sql), "{sql:?}");
    }
    // A string a routing hint or a plugin sent to the primary as a write
    // keeps its flush.
    assert!(ewni(true, "SELECT a FROM t1; SELECT b FROM t2"));
}

/// A statement this classifier does not list as a write, in a string of
/// more than one, flushes every edge entry: every such string used to, and
/// reading it one statement at a time left `REFRESH MATERIALIZED VIEW a;
/// REFRESH MATERIALIZED VIEW b` (or a `LOCK`, or any statement a backend
/// adds) flushing nothing. Only reads and bare transaction control are known
/// to change no rows.
#[test]
fn multi_statement_strings_flush_on_any_statement_not_known_quiet() {
    for sql in [
        "REFRESH MATERIALIZED VIEW mv_sales; REFRESH MATERIALIZED VIEW mv_users",
        "SELECT 1; REFRESH MATERIALIZED VIEW CONCURRENTLY mv",
        "SELECT 1; EXPLAIN ANALYZE UPDATE t SET v = 2",
        "SELECT 1; LOCK TABLE t IN EXCLUSIVE MODE",
        "SELECT 1; CHECKPOINT",
        "SELECT 1; (SELECT 2)",
        "SELECT 1; selection()",
    ] {
        assert!(ewni(false, sql), "{sql:?}");
    }
    for sql in [
        "SELECT 1; VALUES (1); TABLE t; SHOW search_path",
        "WITH x AS (SELECT 1) SELECT * FROM x; EXPLAIN SELECT * FROM t",
        "START TRANSACTION; SELECT 1; SAVEPOINT a; RELEASE a; ROLLBACK",
    ] {
        assert!(!ewni(false, sql), "{sql:?}");
    }
}

/// R6-2: a `;` inside a literal or a quoted identifier ends no statement, so
/// a string holding one statement is read as that statement, as it is
/// without the `;`. The raw `;` used to send it down the multi-statement
/// rule, where any statement but a read flushes: a CSV export with
/// `DELIMITER ';'` flushed its table on every edge, a `NOTIFY` whose payload
/// held a `;` every edge entry.
#[test]
fn a_semicolon_inside_a_literal_ends_no_statement() {
    for sql in [
        "COPY t TO STDOUT (FORMAT csv, DELIMITER ';')",
        "NOTIFY jobs, 'id=1;type=x'",
        "DECLARE c CURSOR FOR SELECT * FROM t WHERE s = 'a;b'",
        "LISTEN \"a;b\";",
    ] {
        assert!(ProxyServer::stmt_has_interior_semicolon(sql), "{sql:?}");
        assert!(!ewni(false, sql), "{sql:?}");
    }
    for sql in [
        "COPY t FROM STDIN (FORMAT csv, DELIMITER ';')",
        "UPDATE t SET s = 'a;b'",
        "NOTIFY jobs, 'a;b'; NOTIFY jobs, 'c'",
        "SELECT 'a;b'; REFRESH MATERIALIZED VIEW mv",
    ] {
        assert!(ewni(false, sql), "{sql:?}");
    }
    // One a routing hint or a plugin sent to the primary as a write keeps
    // its flush.
    assert!(ewni(true, "NOTIFY jobs, 'a;b'"));
}

/// The verbs the journal capture counts as writes, and an `EXPLAIN` that
/// runs what it explains, invalidate on their own too, on both protocols
/// (the extended one memoizes `is_edge_dml_sql`): `REFRESH MATERIALIZED
/// VIEW mv` left `SELECT * FROM mv` served until its TTL.
#[test]
fn refresh_and_explain_analyze_invalidate() {
    for sql in [
        "REFRESH MATERIALIZED VIEW mv",
        "/* nightly */ refresh materialized view concurrently mv",
        "COMMENT ON TABLE t IS 'x'",
        "SECURITY LABEL ON TABLE t IS 'secret'",
        "IMPORT FOREIGN SCHEMA s FROM SERVER f INTO public",
        "REASSIGN OWNED BY a TO b",
        "EXPLAIN ANALYZE DELETE FROM t",
        "EXPLAIN (ANALYSE, BUFFERS) UPDATE t SET v = 2",
    ] {
        assert!(ProxyServer::is_edge_dml_sql(sql), "{sql:?}");
        assert!(ewni(false, sql), "{sql:?}");
    }
    for sql in [
        "EXPLAIN SELECT * FROM t",
        "EXPLAIN UPDATE t SET v = 'analyze'",
        "EXPLAIN SELECT 1 -- analyze",
    ] {
        assert!(!ProxyServer::is_edge_dml_sql(sql), "{sql:?}");
        assert!(!ewni(false, sql), "{sql:?}");
    }
    assert!(crate::edge::fingerprint::write_tables("REFRESH MATERIALIZED VIEW mv").is_empty());
    assert_eq!(
        crate::edge::fingerprint::write_tables("EXPLAIN ANALYZE DELETE FROM t"),
        ["t"]
    );
}

/// A `WITH` read is no data-modifying CTE because a literal, a comment or a
/// quoted identifier holds a verb's text: each used to flush its tables on
/// every run (`[edge]`, both protocols). Backend-ambiguous text may hold a
/// verb.
#[test]
fn verbs_in_literals_and_comments_are_no_cte_writes() {
    for sql in [
        "WITH recent AS (SELECT id, op FROM audit_log WHERE op = 'delete') \
         SELECT count(*) FROM recent",
        "WITH recent AS (SELECT id FROM audit_log) -- update nightly\n\
         SELECT count(*) FROM recent",
        "WITH recent AS (SELECT \"update\" FROM audit_log) SELECT * FROM recent",
    ] {
        assert!(!ProxyServer::is_edge_dml_sql(sql), "{sql:?}");
        assert!(!ewni(false, sql), "{sql:?}");
    }
    for sql in [
        "WITH d AS (DELETE FROM t WHERE op = 'x' RETURNING id) SELECT * FROM d",
        "WITH d AS (SELECT 1) INSERT INTO t SELECT * FROM d",
        "WITH d AS (SELECT 'C:\\' AS p) SELECT * FROM d",
    ] {
        assert!(ProxyServer::is_edge_dml_sql(sql), "{sql:?}");
        assert!(ewni(false, sql), "{sql:?}");
    }
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

/// The simple-protocol write check reads the first word past leading
/// comments (the routing classifier does not), knows `MERGE` (PostgreSQL's
/// `MERGE INTO … USING …`, HeliosDB-Nano's `MERGE BRANCH`), and takes a
/// statement whose leading comment the backends end in different places
/// for a write of unknown scope. Each of these used to invalidate nothing.
#[test]
fn merge_and_writes_behind_comments_invalidate() {
    for sql in [
        "MERGE BRANCH fix INTO main",
        "MERGE INTO kv USING src ON true WHEN MATCHED THEN DELETE",
        "/* job */ UPDATE kv SET v = 2",
        "-- v2\nCREATE OR REPLACE VIEW v AS SELECT 1",
        "/* x */ SET search_path TO b",
        "/* x */ COMMIT",
        "/* a /* b */ SELECT 1",
        "-- x\rSELECT 1\nDELETE FROM kv",
    ] {
        assert!(ewni(false, sql), "{sql:?}");
    }
    for sql in [
        "/* x */ BEGIN",
        "/* x */ SELECT v FROM kv",
        "-- c\r\nSELECT 1",
    ] {
        assert!(!ewni(false, sql), "{sql:?}");
    }
    assert!(ProxyServer::is_edge_dml_sql(
        "/* job */ UPDATE kv SET v = 2"
    ));
    assert!(ProxyServer::is_edge_dml_sql(
        "-- x\rSELECT 1\nDELETE FROM kv"
    ));
    assert!(!ProxyServer::is_edge_dml_sql("/* x */ SELECT v FROM kv"));
}

/// HeliosDB-Lite matches a keyword upper-cased the Unicode way
/// (`ınsert ınto kv …`, a dotless `ı`, is an INSERT there). The edge check
/// read the first word as ASCII and flushed neither on it nor on such a
/// data-modifying CTE verb, so the cached reads of `kv` stayed served.
#[test]
fn a_keyword_lite_upper_cases_is_a_write_of_unknown_scope() {
    for sql in [
        "\u{131}nsert \u{131}nto kv values (5)",
        "/* c */ in\u{17f}ert into kv values (5)",
        "\u{131}nsert \u{131}nto kv values (5);",
    ] {
        assert!(ewni(false, sql), "{sql:?}");
        assert!(ProxyServer::is_edge_dml_sql(sql), "{sql:?}");
        assert!(
            crate::edge::fingerprint::write_tables(sql).is_empty(),
            "{sql:?}"
        );
    }
    let cte = "WITH c AS (SELECT 1) \u{131}nsert \u{131}nto kv SELECT * FROM c";
    assert!(ProxyServer::is_edge_dml_sql(cte));
    assert!(crate::edge::fingerprint::write_tables(cte).is_empty());
    // Non-ASCII text elsewhere is no verb.
    for sql in [
        "WITH c AS (SELECT 'caf\u{e9}') SELECT * FROM c",
        "SELECT '\u{131}nsert' FROM kv",
    ] {
        assert!(!ProxyServer::is_edge_dml_sql(sql), "{sql:?}");
        assert!(!ewni(false, sql), "{sql:?}");
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

/// `[cache] max_cacheable_response_bytes` is the `[cache]` key a SIGHUP
/// applies, and the relay's capture takes it from the live config: the
/// `[edge]` cache's per-entry ceiling follows it, so a response between the
/// old and the new value is stored instead of buffered and then refused as
/// oversize.
#[tokio::test]
async fn a_sighup_reload_moves_the_edge_entry_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("proxy.toml");
    let nodes = "[[nodes]]\nhost = \"pg-p\"\nport = 5432\nrole = \"primary\"\n";
    let config_with =
        |bytes: usize| format!("[cache]\nmax_cacheable_response_bytes = {bytes}\n{nodes}");
    std::fs::write(&path, config_with(16)).unwrap();
    let path = path.to_string_lossy().into_owned();
    let config = crate::config::ProxyConfig::from_file(&path).unwrap();
    let server = ProxyServer::new(config)
        .unwrap()
        .with_config_path(Some(path.clone()));
    let entry = || crate::edge::CacheEntry {
        version: 1,
        response_bytes: bytes::Bytes::from_static(&[b'x'; 24]),
        tables: vec!["t".to_string()],
        expires_at: std::time::Instant::now() + std::time::Duration::from_secs(60),
    };
    let key = || crate::edge::CacheKey::new("SELECT v FROM t", "p");
    let edge = &server.state.edge_cache;
    assert!(!edge.insert_if_fresh(key(), entry()), "24 bytes > 16");

    std::fs::write(&path, config_with(32)).unwrap();
    server.reload_config().await;
    assert_eq!(
        server
            .state
            .live_config
            .load()
            .cache
            .max_cacheable_response_bytes,
        32
    );
    assert!(edge.insert_if_fresh(key(), entry()), "24 bytes <= 32");
    assert_eq!(edge.stats().oversize_rejected, 1);
}
