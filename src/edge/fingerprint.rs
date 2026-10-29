//! Edge-cache query fingerprinting.
//!
//! Twin of the query-cache normalizer (`src/cache/normalizer.rs`) so
//! `edge-proxy` builds without the `query-cache` feature — this module
//! must NOT reference the cache modules. Both read the statement with the
//! shared quote-aware scanner in `crate::sql_text` (C-07): a `--` or `/*`
//! inside a literal is text, a comment separates the tokens around it,
//! literals are folded in statement order with their kind, and
//! dollar-quoted literals and quoted identifiers keep their case. It
//! produces the ingredients of an edge `CacheKey` plus the table set
//! invalidations target:
//!
//! - `fingerprint` — comments stripped, literals folded to `?`,
//!   whitespace collapsed, every word kept as written (HeliosDB-Nano
//!   labels an output column as written, so case variants never share a
//!   slot). Groups "the same query shape".
//! - `params_hash` — 16-hex digest of the extracted literals (kind,
//!   value and position) plus the session `database`/`user` and the
//!   sorted session variables (startup GUCs like TimeZone/options
//!   affect result bytes, so they partition the cache too). The tenant
//!   identity is ALSO carried verbatim in the edge `CacheKey`, so a
//!   64-bit hash collision can never alias tenants. Hashes only need to
//!   be stable within one process: cache keys never cross the wire —
//!   invalidation is version+table based.
//! - `tables` — schema-stripped, lowercased identifiers following
//!   FROM/JOIN/INTO/UPDATE/TABLE/COPY. An *empty* set means the
//!   analyzer couldn't attribute the statement to tables; per the
//!   coherence rules the server never caches such a read (an
//!   invalidation could not target it) and invalidates everything on
//!   such a write.

use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::sql_text;

/// Analysis of one SQL statement for the edge cache.
#[derive(Debug, Clone)]
pub struct EdgeFingerprint {
    /// Normalized query shape (literals replaced with `?`).
    pub fingerprint: String,
    /// 16-hex digest of literal values + database + user.
    pub params_hash: String,
    /// Tables referenced, schema-stripped + lowercased, deduped.
    pub tables: Vec<String>,
}

// Superset of the normalizer's pattern: COPY is edge-specific so
// `COPY t FROM ...` attributes the LOADED table (t), not just the
// source token after FROM. Extra captured tokens (e.g. `stdin`) only
// widen invalidation — safe. An `INSERT` with its optional `OR <word>` /
// `REPLACE`, `OVERWRITE`, `INTO` and `TABLE` is one keyword: HeliosDB-Nano
// and HeliosDB-Lite take `INTO` as optional (`INSERT orders SELECT …`).
// Any run of whitespace and `(` before the
// table is skipped (a parenthesized join, `FROM (orders JOIN items ON …)`),
// and so is an optional `ONLY` (`UPDATE ONLY kv`, `DELETE FROM ONLY (kv)`):
// it is a reserved word, never the table. The keyword is group 1 and the
// table group 2, read through `sql_text::table_names`, which skips a
// keyword that ends an identifier (`SELECT valid_from FROM prices`,
// `SELECT xcopy FROM t`), a subquery's `SELECT` after a `(`, and never lets
// one match swallow the next keyword. A name has up to three dotted parts,
// the table last (`benchdb.public.kv`), and a dot may have whitespace on
// either side (`app . kv`, and `app./**/kv`, whose comment the table text
// turns into a space; a Unicode space too, which HeliosDB-Nano and
// HeliosDB-Lite read as whitespace): still the table `kv`, never `app`.
// Keep the two in sync when touching either.
static TABLE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(FROM|JOIN|INTO|UPDATE|TABLE|COPY|INSERT(?:\s+OR\s+[a-zA-Z_]+|\s+REPLACE)?(?:\s+OVERWRITE)?(?:\s+INTO)?(?:\s+TABLE)?)[\s(]+(?:ONLY(?:\s+|\s*\(\s*))?([a-zA-Z_][a-zA-Z0-9_]*(?:\s*\.\s*[a-zA-Z_][a-zA-Z0-9_]*){0,2})").unwrap()
});

/// Statements whose written tables their text does not name: DDL (a view, a
/// function, a policy or a `TRUNCATE … CASCADE` changes reads of relations
/// it does not list), and `EXECUTE`, `CALL` and `DO`. The query cache's
/// `write_tables` list, without `COPY` (whose loaded table the edge cache
/// attributes) and `SELECT` (a read forced to the primary reaches the edge
/// invalidation too, and only over-invalidates its own tables).
const UNKNOWN_SCOPE: &[&str] = &[
    "CREATE", "ALTER", "DROP", "TRUNCATE", "COMMENT", "GRANT", "REVOKE", "EXECUTE", "CALL", "DO",
    "IMPORT", "SECURITY", "REFRESH", "REINDEX", "CLUSTER", "VACUUM", "LOCK",
];

/// Just the referenced table set — byte-identical to
/// `analyze(..).tables` (both extract from the comment-free text,
/// `sql_text::strip_comments`), without the literal folding, the
/// fingerprint copy, or the params hash. The write-invalidation path
/// ([`write_tables`]) discards everything else, so a bulk INSERT must not
/// pay full fingerprinting (F19).
pub fn tables_only(sql: &str) -> Vec<String> {
    let mut tables = Vec::new();
    add_tables(&mut tables, &sql_text::strip_comments(sql));
    tables
}

/// The table set a write's invalidation targets: the tables its
/// comment-free text names, or the empty set (invalidate everything) for
/// backend-ambiguous text and a string of more than one statement
/// (`sql_text::single_statement`), for a `MERGE` that is not `MERGE INTO`
/// (HeliosDB-Nano's `MERGE BRANCH src INTO dst` changes every table of the
/// branch, and the word after its `INTO` is the branch, never a table), for
/// an `INSERT` without `INTO` (HeliosDB-Nano and HeliosDB-Lite write the
/// table named next, which the extraction does not read as one), and for a
/// table match that took a word sqlparser (HeliosDB-Nano, HeliosDB-Lite)
/// reads before the target (`sql_text::skipped_target`: `UPDATE OR IGNORE
/// kv`, `INSERT INTO LOCAL kv`, which write `kv` there), for a name the
/// extraction cannot read whole where it reads a table name (a quoted
/// identifier, a name with `$` or a non-ASCII character: `UPDATE s."t" …`
/// used to name the table `s`; a quoted literal, which HeliosDB-Nano and
/// HeliosDB-Lite read as the table, where it names a table the write may
/// change), and for DDL and the other statements whose effect the text does
/// not name ([`UNKNOWN_SCOPE`]: `CREATE OR REPLACE VIEW v AS SELECT * FROM
/// t` used to invalidate `t` alone, leaving `v`'s entries served).
pub fn write_tables(sql: &str) -> Vec<String> {
    let Some((t, text)) = sql_text::single_statement(sql) else {
        return Vec::new();
    };
    if sql_text::merge_without_into(t)
        || sql_text::insert_without_into(t)
        || UNKNOWN_SCOPE
            .iter()
            .any(|verb| crate::protocol::starts_with_ci(t, verb))
    {
        return Vec::new();
    }
    let mut tables = Vec::new();
    for (keyword, table) in sql_text::table_matches(&TABLE_PATTERN, &text) {
        if sql_text::skipped_target(keyword, table) {
            return Vec::new();
        }
        add_table(&mut tables, table);
    }
    tables
}

/// Analyze one statement: normalized fingerprint, tenant-scoped
/// params hash, and the referenced table set.
///
/// `session_vars` — the session's OTHER startup/GUC parameters
/// (sorted by key by the caller for determinism), folded into the
/// hash so sessions that differ in result-affecting parameters
/// (TimeZone, options=-csearch_path, ...) never share a slot.
pub fn analyze(
    sql: &str,
    database: &str,
    user: &str,
    session_vars: &[(String, String)],
) -> EdgeFingerprint {
    analyze_with(sql, database, user, session_vars, false)
}

/// [`analyze`], with the fingerprint's whitespace and comments kept as
/// written when `verbatim` (the relay's choice once a backend reported a
/// HeliosDB `server_version`: those engines read both before parsing, so two
/// texts share a key only when they differ in literal values alone).
pub fn analyze_with(
    sql: &str,
    database: &str,
    user: &str,
    session_vars: &[(String, String)],
    verbatim: bool,
) -> EdgeFingerprint {
    // NOTE: keep this the same extraction `tables_only` uses — the
    // read key's table set and the write invalidation's table set
    // must never drift, or entries become unsweepable.
    let tables = tables_only(sql);
    let fp = if verbatim {
        sql_text::fingerprint_verbatim(sql)
    } else {
        sql_text::fingerprint(sql)
    };

    // Tenant isolation: database + user are part of the hash, so the
    // same SQL text from two tenants never shares a cache slot.
    // (`Hash for str` includes a length terminator, so ("ab","c") and
    // ("a","bc") can't collide by concatenation.)
    let mut hasher = DefaultHasher::new();
    database.hash(&mut hasher);
    user.hash(&mut hasher);
    for (k, v) in session_vars {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    fp.hash_literals(&mut hasher);
    let params_hash = format!("{:016x}", hasher.finish());

    EdgeFingerprint {
        fingerprint: fp.text,
        params_hash,
        tables,
    }
}

/// Add the table identifiers after FROM/JOIN/INTO/UPDATE/TABLE/COPY in
/// `sql` to `tables`: lowercased, schema prefix stripped, cut to the 63
/// bytes PostgreSQL keeps of a name (a read spelling a longer one and a
/// write naming the catalog's relation name one table), first-seen order,
/// deduped.
fn add_tables(tables: &mut Vec<String>, sql: &str) {
    for table in sql_text::table_names(&TABLE_PATTERN, sql) {
        add_table(tables, table);
    }
}

/// Add one captured `table` to `tables` as [`add_tables`] does.
fn add_table(tables: &mut Vec<String>, table: &str) {
    let name = table.rsplit('.').next().unwrap_or(table).trim();
    let name = sql_text::truncated_identifier(name).to_lowercase();
    if !tables.contains(&name) {
        tables.push(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_fold_into_fingerprint() {
        let a = analyze("SELECT * FROM users WHERE id = 123", "db", "u", &[]);
        assert_eq!(a.fingerprint, "SELECT * FROM users WHERE id = ?");
        assert_eq!(a.tables, vec!["users".to_string()]);
    }

    #[test]
    fn same_shape_different_params_share_fingerprint_not_hash() {
        let a = analyze("SELECT * FROM users WHERE id = 1", "db", "u", &[]);
        let b = analyze("SELECT * FROM users WHERE id = 2", "db", "u", &[]);
        assert_eq!(a.fingerprint, b.fingerprint);
        assert_ne!(a.params_hash, b.params_hash);
    }

    #[test]
    fn params_hash_is_deterministic_and_16_hex() {
        let a = analyze("SELECT * FROM t WHERE x = 'v'", "db", "u", &[]);
        let b = analyze("SELECT * FROM t WHERE x = 'v'", "db", "u", &[]);
        assert_eq!(a.params_hash, b.params_hash);
        assert_eq!(a.params_hash.len(), 16);
        assert!(a.params_hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn tenant_isolation_database_and_user_change_hash() {
        let base = analyze("SELECT * FROM users WHERE id = 1", "app", "alice", &[]);
        let other_db = analyze("SELECT * FROM users WHERE id = 1", "app2", "alice", &[]);
        let other_user = analyze("SELECT * FROM users WHERE id = 1", "app", "bob", &[]);
        assert_ne!(base.params_hash, other_db.params_hash);
        assert_ne!(base.params_hash, other_user.params_hash);
        // Shape is tenant-independent; only the hash isolates.
        assert_eq!(base.fingerprint, other_db.fingerprint);
    }

    #[test]
    fn whitespace_collapses() {
        let a = analyze("select  *\n  from   users\twhere id=1", "db", "u", &[]);
        let b = analyze("select * from users where id=1", "db", "u", &[]);
        assert_eq!(a.fingerprint, b.fingerprint);
    }

    /// Words keep their spelling: HeliosDB-Nano labels an output column as
    /// written, so `SELECT Name …` and `SELECT name …` return different
    /// RowDescriptions. Upper-casing every word gave them one edge slot, and
    /// the second client got the first one's column names.
    #[test]
    fn case_variants_never_share_a_slot() {
        for (a, b) in [
            (
                "SELECT Name FROM users WHERE id = 1",
                "SELECT name FROM users WHERE id = 1",
            ),
            (
                "SELECT name AS Total FROM users WHERE id = 1",
                "SELECT name AS total FROM users WHERE id = 1",
            ),
        ] {
            let (a, b) = (analyze(a, "db", "u", &[]), analyze(b, "db", "u", &[]));
            assert_eq!(a.params_hash, b.params_hash);
            assert_ne!(a.fingerprint, b.fingerprint);
        }
    }

    #[test]
    fn comments_and_hints_are_stripped() {
        let a = analyze(
            "/* helios:route=primary */ SELECT * FROM users -- trailing\nWHERE id = 1",
            "db",
            "u",
            &[],
        );
        assert_eq!(a.fingerprint, "SELECT * FROM users WHERE id = ?");
    }

    #[test]
    fn tables_from_join_update_insert() {
        let a = analyze(
            "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id",
            "db",
            "u",
            &[],
        );
        assert!(a.tables.contains(&"users".to_string()));
        assert!(a.tables.contains(&"orders".to_string()));

        let b = analyze("UPDATE products SET price = 10", "db", "u", &[]);
        assert_eq!(b.tables, vec!["products".to_string()]);

        let c = analyze("INSERT INTO events (id) VALUES (1)", "db", "u", &[]);
        assert_eq!(c.tables, vec!["events".to_string()]);
    }

    #[test]
    fn schema_prefix_is_stripped_and_dupes_collapse() {
        let a = analyze(
            "SELECT * FROM public.users JOIN users ON true",
            "db",
            "u",
            &[],
        );
        assert_eq!(a.tables, vec!["users".to_string()]);
    }

    /// A long name is keyed by the 63 bytes PostgreSQL keeps: a read that
    /// spells 70 bytes and a write naming the stored relation sweep one slot.
    #[test]
    fn table_names_are_truncated_like_postgresql() {
        let long = format!("t{}", "x".repeat(69));
        let stored = &long[..63];
        let read = analyze(
            &format!("SELECT v FROM {long} WHERE id = 1"),
            "db",
            "u",
            &[],
        );
        assert_eq!(read.tables, [stored]);
        assert_eq!(
            tables_only(&format!("UPDATE public.{stored} SET v = 1")),
            read.tables
        );
        assert_eq!(
            tables_only(&format!("DELETE FROM {}abc", stored.to_uppercase())),
            read.tables
        );
    }

    #[test]
    fn session_vars_partition_the_hash() {
        // Startup GUCs (TimeZone, options=-csearch_path, ...) change
        // result bytes — sessions differing in them must not share a
        // cache slot. Shape stays shared; only the hash isolates.
        let none = analyze("SELECT * FROM t WHERE id = 1", "db", "u", &[]);
        let tz_utc = analyze(
            "SELECT * FROM t WHERE id = 1",
            "db",
            "u",
            &[("TimeZone".to_string(), "UTC".to_string())],
        );
        let tz_cet = analyze(
            "SELECT * FROM t WHERE id = 1",
            "db",
            "u",
            &[("TimeZone".to_string(), "Europe/Zurich".to_string())],
        );
        assert_ne!(none.params_hash, tz_utc.params_hash);
        assert_ne!(tz_utc.params_hash, tz_cet.params_hash);
        assert_eq!(none.fingerprint, tz_utc.fingerprint);
    }

    #[test]
    fn tables_only_matches_analyze_tables() {
        // The write path uses `tables_only`; the read key uses
        // `analyze(..).tables`. They must never drift or entries
        // become unsweepable.
        let cases = [
            "SELECT * FROM users WHERE id = 5",
            "/*+ route=primary */ SELECT * FROM users -- from x\nWHERE id = 1",
            "UPDATE products SET note = 'from nowhere' WHERE id = 2",
            "INSERT INTO events (id) VALUES (1)",
            "SELECT u.*, o.* FROM public.users u JOIN orders o ON u.id = o.user_id",
            "TRUNCATE TABLE audit_log",
            "COPY staging FROM STDIN",
            "BEGIN",
            "SELECT 1",
        ];
        for sql in cases {
            assert_eq!(
                tables_only(sql),
                analyze(sql, "db", "u", &[]).tables,
                "tables_only diverged for {sql:?}"
            );
        }
    }

    /// HeliosDB-Nano's `MERGE [DATABASE] BRANCH src INTO dst` changes every
    /// table of the branch it merges into: its invalidation is the wildcard
    /// (the empty set), never the branch name after `INTO`. PostgreSQL's
    /// `MERGE INTO t USING …` still targets `t`.
    #[test]
    fn a_branch_merge_invalidates_everything() {
        for sql in [
            "MERGE BRANCH fix INTO main",
            "merge database branch fix into main",
            "  /* deploy */ MERGE BRANCH fix INTO main",
            // A Unicode space is whitespace for HeliosDB-Nano.
            "\u{a0}MERGE\u{a0}BRANCH fix INTO main",
        ] {
            assert_eq!(tables_only(sql), ["main"], "{sql}");
            assert!(write_tables(sql).is_empty(), "{sql}");
        }
        let merge = "MERGE INTO accounts a USING fixes f ON a.id = f.id WHEN MATCHED THEN DELETE";
        assert_eq!(write_tables(merge), tables_only(merge));
        assert_eq!(write_tables(merge), ["accounts"]);
        assert_eq!(write_tables("UPDATE kv SET v = 1"), ["kv"]);
    }

    /// HeliosDB-Nano and HeliosDB-Lite take `INTO` as optional in an
    /// `INSERT` and write the table named next: its invalidation is the
    /// wildcard, never the source tables alone.
    #[test]
    fn an_insert_without_into_invalidates_everything() {
        for sql in [
            "INSERT orders SELECT id, v FROM staging",
            "INSERT orders (c) VALUES ((SELECT max(c) FROM s))",
            "INSERT OR REPLACE orders SELECT * FROM staging",
        ] {
            assert!(write_tables(sql).is_empty(), "{sql}");
        }
        assert_eq!(
            write_tables("INSERT INTO orders SELECT * FROM staging"),
            ["orders", "staging"]
        );
        // Nor in a later statement: the string's scope is unknown anyway.
        assert!(write_tables("UPDATE a SET x = 1; INSERT orders SELECT * FROM staging").is_empty());
        assert_eq!(
            tables_only("WITH n AS (INSERT orders VALUES (1) RETURNING *) SELECT * FROM n"),
            ["orders", "n"]
        );
    }

    /// sqlparser (HeliosDB-Nano, HeliosDB-Lite) reads an `UPDATE`'s conflict
    /// clause and an `INSERT`'s `INTO LOCAL` before the target: the edge
    /// invalidation used to name a table `or`, `replace` or `local`, and
    /// `kv`'s edge entries stayed served. It is now the wildcard.
    #[test]
    fn a_word_sqlparser_reads_before_the_target_invalidates_everything() {
        for sql in [
            "UPDATE OR IGNORE kv SET v = 2 WHERE id = 1",
            "UPDATE REPLACE kv SET v = 2",
            "INSERT INTO LOCAL kv VALUES (1)",
            "UPDATE a SET x = 1; UPDATE OR FAIL kv SET v = 2",
        ] {
            assert!(write_tables(sql).is_empty(), "{sql}");
        }
        assert_eq!(write_tables("UPDATE orders SET v = 2"), ["orders"]);
        assert_eq!(
            write_tables("INSERT INTO TABLE local VALUES (1)"),
            ["local"]
        );
    }

    #[test]
    fn copy_attributes_the_loaded_table() {
        // `COPY t FROM STDIN` must extract t (the table the rows land
        // in); the trailing source token is harmless over-invalidation.
        let t = tables_only("COPY orders FROM STDIN WITH (FORMAT csv)");
        assert!(t.contains(&"orders".to_string()));
        let t2 = tables_only("COPY public.orders FROM '/tmp/f.csv'");
        assert!(t2.contains(&"orders".to_string()));
    }

    /// `UPDATE ONLY kv` used to attribute the write to a table `only`, so
    /// kv's edge entries were never swept.
    #[test]
    fn only_is_never_the_table() {
        for sql in [
            "UPDATE ONLY kv SET v = 1",
            "DELETE FROM ONLY kv WHERE id = 1",
            "DELETE FROM ONLY (kv) WHERE id = 1",
            "MERGE INTO ONLY kv USING kv AS s ON true WHEN MATCHED THEN DELETE",
            "SELECT * FROM ONLY kv",
        ] {
            assert_eq!(tables_only(sql), ["kv"], "{sql}");
        }
        assert_eq!(tables_only("UPDATE only_rows SET v = 1"), ["only_rows"]);
    }

    /// A schema qualifier's dot next to a comment or whitespace is still a
    /// qualifier: `app./**/kv` (the table text's `app. kv`) used to be keyed
    /// to a table `app`, so neither spelling's write swept the other's read.
    #[test]
    fn a_spaced_qualifier_dot_still_names_the_table() {
        for sql in [
            "UPDATE app./**/kv SET v = 2 WHERE id = 1",
            "SELECT v FROM app/**/.kv WHERE id = 1",
            "INSERT INTO app . kv VALUES (1)",
            "DELETE FROM app.\n  kv WHERE id = 1",
            "SELECT v FROM app.kv WHERE id = 1",
        ] {
            assert_eq!(tables_only(sql), ["kv"], "{sql}");
            assert_eq!(analyze(sql, "db", "u", &[]).tables, ["kv"], "{sql}");
        }
        assert_eq!(
            tables_only("COPY app /* c */ . kv FROM STDIN"),
            ["kv", "stdin"]
        );
    }

    /// A three-part name used to be keyed to its schema, and a parenthesized
    /// join lost its first table: writes spelled otherwise never swept them.
    #[test]
    fn three_part_names_and_parenthesized_joins_name_their_tables() {
        for sql in [
            "SELECT v FROM benchdb.public.kv WHERE id = 1",
            "UPDATE benchdb . public . kv SET v = 2",
            "COPY benchdb.public.kv FROM STDIN",
        ] {
            assert!(tables_only(sql).contains(&"kv".to_string()), "{sql}");
            assert!(!tables_only(sql).contains(&"public".to_string()), "{sql}");
        }
        assert_eq!(
            tables_only("SELECT * FROM (orders JOIN items ON orders.id = items.order_id)"),
            ["orders", "items"]
        );
        assert_eq!(
            analyze(
                "SELECT * FROM ((a JOIN b ON true) JOIN c ON true)",
                "db",
                "u",
                &[]
            )
            .tables,
            ["a", "b", "c"]
        );
        // A subquery's first word is no table: still never cached.
        assert!(tables_only("SELECT * FROM (SELECT f(1)) x").is_empty());
    }

    /// A keyword at the end of a select-list column (`valid_from`,
    /// `last_update`, `data_table`, `xcopy`) used to match, capture the real
    /// `FROM` as the table and step over the relation: the read was keyed to
    /// a table `from`, which no write to its table sweeps.
    #[test]
    fn a_keyword_ending_an_identifier_is_not_one() {
        for (sql, want) in [
            ("SELECT id, valid_from FROM prices WHERE id = 1", "prices"),
            ("SELECT valid_from FROM prices", "prices"),
            ("SELECT last_update FROM accounts", "accounts"),
            ("SELECT x, data_table FROM t", "t"),
            ("SELECT xcopy FROM t", "t"),
            ("SELECT yjoin FROM t", "t"),
            ("SELECT zinto FROM t", "t"),
            ("SELECT \u{f1}from FROM t", "t"),
        ] {
            assert_eq!(tables_only(sql), [want], "{sql}");
            assert_eq!(analyze(sql, "db", "u", &[]).tables, [want], "{sql}");
        }
        assert_eq!(
            tables_only("UPDATE prices SET valid_from = now() WHERE id = 1"),
            ["prices"]
        );
        // A keyword after any other byte still counts, and its match no
        // longer swallows the real keyword right after it.
        for sql in [
            "SELECT t.from FROM prices",
            "SELECT 1 AS from FROM prices",
            "SELECT x$from FROM prices",
            "SELECT 1from prices",
            "SELECT $$a$$FROM prices",
            "SELECT t.copy FROM prices",
        ] {
            assert!(
                tables_only(sql).contains(&"prices".to_string()),
                "{sql}: {:?}",
                tables_only(sql)
            );
        }
    }

    /// A keyword after a Unicode space (U+00A0, U+3000) is one for
    /// HeliosDB-Nano and HeliosDB-Lite, which read the space as whitespace:
    /// it used to be skipped as the end of an identifier, so a read named
    /// only its other tables (`WHERE id IN (SELECT id FROM other)` was keyed
    /// to `other` alone) and a write missed its target.
    #[test]
    fn a_keyword_after_a_unicode_space_counts() {
        for space in ['\u{a0}', '\u{3000}'] {
            for (sql, want) in [
                (format!("SELECT v{space}FROM kv WHERE id = 1"), vec!["kv"]),
                (
                    format!("SELECT v{space}FROM kv WHERE id IN (SELECT id FROM other)"),
                    vec!["kv", "other"],
                ),
                (
                    format!("SELECT v FROM a{space}JOIN kv ON true"),
                    vec!["a", "kv"],
                ),
                (format!("SELECT v FROM app{space}.kv"), vec!["kv"]),
            ] {
                assert_eq!(tables_only(&sql), want, "{sql:?}");
                assert_eq!(analyze(&sql, "db", "u", &[]).tables, want, "{sql:?}");
            }
            let insert = format!("INSERT{space}INTO kv SELECT * FROM src");
            assert_eq!(write_tables(&insert), ["kv", "src"], "{insert:?}");
        }
    }

    /// C-07: a `--` inside a literal used to truncate the shape, so these
    /// two reads shared a key (shape `... ILIKE '%A`, no params).
    #[test]
    fn comment_markers_inside_literals_do_not_merge_keys() {
        let seven = analyze(
            "SELECT * FROM posts WHERE title ILIKE '%a -- b%' AND owner_id = 7",
            "db",
            "u",
            &[],
        );
        let eight = analyze(
            "SELECT * FROM posts WHERE title ILIKE '%a -- b%' AND owner_id = 8",
            "db",
            "u",
            &[],
        );
        assert_eq!(
            seven.fingerprint,
            "SELECT * FROM posts WHERE title ILIKE ? AND owner_id = ?"
        );
        assert_ne!(
            (&seven.fingerprint, &seven.params_hash),
            (&eight.fingerprint, &eight.params_hash)
        );
        let a = analyze("SELECT v FROM t WHERE a = '1' AND b = 2", "db", "u", &[]);
        let b = analyze("SELECT v FROM t WHERE a = 2 AND b = '1'", "db", "u", &[]);
        assert_ne!(a.params_hash, b.params_hash);
    }

    #[test]
    fn tables_only_equals_analyze_tables_with_literal_markers() {
        for sql in [
            "SELECT * FROM posts WHERE title = '--' AND x IN (SELECT 1 FROM tags)",
            "SELECT * FROM a /* c */ JOIN b ON true WHERE s = '/*'",
            "UPDATE notes SET body = 'from -- here' WHERE id = 1",
            "SELECT v FROM/**/secrets",
            r"SELECT v FROM t WHERE p = 'C:\' -- FROM x",
        ] {
            assert_eq!(
                tables_only(sql),
                analyze(sql, "db", "u", &[]).tables,
                "tables_only diverged for {sql:?}"
            );
        }
        // Tables after a `--` literal are no longer cut off.
        assert_eq!(
            tables_only("SELECT * FROM posts WHERE title = '--' AND x IN (SELECT 1 FROM tags)"),
            ["posts", "tags"]
        );
    }

    /// Where HeliosDB-Nano reads comments otherwise than PostgreSQL, the edge
    /// key keeps the rest verbatim (the relay never caches such a read).
    #[test]
    fn nano_comment_readings_split_keys() {
        let key = |sql: &str| {
            let a = analyze(sql, "db", "u", &[]);
            (a.fingerprint, a.params_hash)
        };
        for (a, b) in [
            (
                "SELECT val FROM kv /* /* */ WHERE id = 2 -- */ WHERE id = 1",
                "SELECT val FROM kv WHERE id = 1",
            ),
            (
                "SELECT val FROM kv --\rWHERE id = 1",
                "SELECT val FROM kv WHERE id = 1",
            ),
            // HeliosDB-Nano cuts `AS of FROM kv` out of the first.
            (
                "SELECT val AS of FROM kv WHERE id = 1",
                "SELECT val AS  of FROM kv WHERE id = 1",
            ),
            // `\v` is whitespace: the first is `'ab'`, the second an error.
            (
                "SELECT id FROM t WHERE s = 'a'\x0b\n'b'",
                "SELECT id FROM t WHERE s = 'a'\x0b 'b'",
            ),
        ] {
            assert_ne!(key(a), key(b), "{a:?} vs {b:?}");
        }
    }

    #[test]
    fn tableless_statement_yields_empty_set() {
        // The server's coherence rule: an empty table set is never
        // cached (a table-targeted invalidation couldn't drop it).
        let a = analyze("SELECT 1", "db", "u", &[]);
        assert!(a.tables.is_empty());
        let b = analyze("SELECT now()", "db", "u", &[]);
        assert!(b.tables.is_empty());
    }

    /// On a HeliosDB backend the edge key keeps comments and whitespace:
    /// HeliosDB-Lite's `SIMILAR TO` rewrite takes the apostrophe in a comment
    /// for the start of a literal and rewrites the real literal after it, and
    /// reads `SIMILAR TO` only with one space; HeliosDB-Nano's point-lookup
    /// fast path declines a statement holding a comment (its planner folds
    /// `Post` to `post`, the fast path looks `Post` up as written), and its
    /// empty-projection rewrite matches `select from ` exactly. Each pair
    /// shares the collapsed key and must not share the verbatim one.
    #[test]
    fn verbatim_keys_keep_what_heliosdb_engines_read() {
        let key = |sql: &str, verbatim: bool| {
            let fp = analyze_with(sql, "db", "u", &[], verbatim);
            (fp.fingerprint, fp.params_hash)
        };
        for (a, b) in [
            (
                "SELECT id FROM notes /* don't */ WHERE body = 'SIMILAR TO'",
                "SELECT id FROM notes WHERE body = 'SIMILAR TO'",
            ),
            (
                "SELECT id FROM notes WHERE body SIMILAR TO 'a%'",
                "SELECT id FROM notes WHERE body SIMILAR\n  TO 'a%'",
            ),
            (
                "SELECT * FROM Post WHERE id = 1",
                "SELECT * FROM Post WHERE id = 1 /*helios:cache_ttl=30*/",
            ),
            ("SELECT FROM t", "SELECT\nFROM t"),
            // sqlparser fails the tagged spelling (its tag scanner loses the
            // end after `$a`) and reads the untagged one as `x$a`.
            (
                "SELECT id FROM kv WHERE s = $$x$a$$",
                "SELECT id FROM kv WHERE s = $ab$x$a$ab$",
            ),
        ] {
            assert_eq!(key(a, false), key(b, false), "collapsed: {a:?} vs {b:?}");
            assert_ne!(key(a, true), key(b, true), "verbatim: {a:?} vs {b:?}");
        }
        // Literal values still fold, with their kind and position.
        let one = analyze_with("SELECT v FROM kv WHERE id = 1", "db", "u", &[], true);
        let two = analyze_with("SELECT v FROM kv WHERE id = 2", "db", "u", &[], true);
        assert_eq!(one.fingerprint, "SELECT v FROM kv WHERE id = ?");
        assert_eq!(one.fingerprint, two.fingerprint);
        assert_ne!(one.params_hash, two.params_hash);
        assert_eq!(one.tables, ["kv"]);
    }

    /// R6-1 and R6-2: a write whose text is backend-ambiguous, or a string of
    /// more than one statement, invalidates every edge entry (the empty
    /// set); a `;` inside a literal or a comment ends nothing, so only the
    /// written table is invalidated.
    #[test]
    fn ambiguous_or_several_statements_invalidate_everything() {
        for sql in [
            "UPDATE a SET p = 'C:\\'; UPDATE -- c\n b SET v = 1",
            "UPDATE a SET v = 1 /* /* */ ; UPDATE b SET v = 2 -- */",
            "UPDATE a SET p = $$it's$$",
            "UPDATE a SET x = 1; TRUNCATE b",
            "UPDATE a SET x = 1; UPDATE b SET v = 2",
            "SELECT 1 AS a$b$ ; UPDATE b SET v = 2",
            // HeliosDB-Lite ends a statement at a `;` inside a comment.
            "UPDATE a SET x = 1 -- ; UPDATE b SET v = 2",
            // HeliosDB-Nano's splitter knows only ASCII dollar tags: it
            // truncates `kv`, the text names only `t`.
            "SET app.tag = $\u{e9}$ ; TRUNCATE kv ; SELECT 1 FROM t ; SET app.tag = $\u{e9}$",
        ] {
            assert!(write_tables(sql).is_empty(), "{sql:?}");
        }
        for (sql, table) in [
            ("UPDATE a SET x = 'it''s -- b'", "a"),
            ("UPDATE a SET x = 1 -- done;", "a"),
            (
                "UPDATE posts SET body = 'Hi; do you know?' WHERE id = 1",
                "posts",
            ),
            (
                "INSERT INTO comments (post_id, body) VALUES (7, 'Thanks; do you ship to Canada?')",
                "comments",
            ),
        ] {
            assert_eq!(write_tables(sql), [table], "{sql:?}");
        }
    }

    /// A write naming something the table pattern cannot read whole (a
    /// quoted identifier, a quoted literal or a name with `$` or a non-ASCII
    /// character where it reads a table name, a `U&"…"` one anywhere) used to
    /// invalidate a table `into`, `values` or a qualifier, and the written
    /// table's edge entries stayed served (`SELECT * FROM orders` after
    /// `INSERT INTO "orders" …`). DDL named only the tables its text reads
    /// (`CREATE OR REPLACE VIEW v AS SELECT * FROM t` invalidated `t`, not
    /// `v`). Both invalidate everything now.
    #[test]
    fn unreadable_names_and_ddl_invalidate_everything() {
        for sql in [
            "INSERT INTO \"orders\" (id) VALUES (1)",
            "INSERT INTO \"s\".orders VALUES (1)",
            "MERGE INTO \"t\" USING s ON true WHEN NOT MATCHED THEN INSERT VALUES (1)",
            "UPDATE s.\"t\" SET v = 1",
            "UPDATE \"t\" SET v = 1 FROM u",
            "UPDATE U&\"t\" SET v = 1 FROM u",
            "INSERT INTO caf\u{e9} VALUES (1)",
            "UPDATE t$x SET v = 1",
            "UPDATE public.'kv' SET v = 'new' WHERE id = 1",
            "DELETE FROM 'kv' WHERE id IN (SELECT id FROM t2)",
            "DELETE x FROM 'kv' WHERE id IN (SELECT id FROM u)",
            "WITH d AS (\u{131}nsert \u{131}nto kv VALUES (1) RETURNING *) SELECT * FROM d",
            "CREATE OR REPLACE VIEW v AS SELECT * FROM t",
            "-- v2\nCREATE OR REPLACE VIEW v AS SELECT * FROM t",
            "TRUNCATE TABLE a CASCADE",
            "ALTER TABLE t RENAME TO u",
            "DROP VIEW v",
        ] {
            assert!(write_tables(sql).is_empty(), "{sql:?}");
        }
        assert_eq!(write_tables("UPDATE orders SET v = 2"), ["orders"]);
        assert_eq!(write_tables("UPDATE t SET v = 'caf\u{e9}', w = $1"), ["t"]);
        // A non-ASCII column names no table: only the target is flushed
        // (every edge entry used to be).
        for (sql, table) in [
            (
                "UPDATE customers SET \u{540d}\u{524d} = 'x' WHERE id = 1",
                "customers",
            ),
            (
                "UPDATE kunden SET gr\u{f6}\u{df}e = 3 WHERE id = 7",
                "kunden",
            ),
            (
                "INSERT INTO produits (pr\u{e9}nom) VALUES ('a')",
                "produits",
            ),
            (
                "UPDATE kunden SET stra\u{df}e = 'Hauptstr. 1' WHERE id = 7",
                "kunden",
            ),
            ("UPDATE users SET ad\u{131} = 'x' WHERE id = 1", "users"),
            // A literal after the `FROM` of `IS DISTINCT FROM` or a `COPY`'s
            // source names no table: only the target is flushed.
            (
                "UPDATE orders SET status = 'shipped' WHERE id = 42 \
                 AND status IS DISTINCT FROM 'shipped'",
                "orders",
            ),
            ("COPY kv FROM '/tmp/x.csv'", "kv"),
        ] {
            assert_eq!(write_tables(sql), [table], "{sql:?}");
        }
        // A quoted column or conflict target names no table, and a query tag
        // whose quotes all close is no comment HeliosDB-Lite splits in.
        assert_eq!(
            write_tables("UPDATE accounts SET \"lastLogin\" = now() WHERE id = $1"),
            ["accounts"]
        );
        assert_eq!(
            write_tables(
                "INSERT INTO orders (\"orderId\") VALUES ($1) ON CONFLICT (\"orderId\") DO NOTHING"
            ),
            ["orders"]
        );
        assert_eq!(
            write_tables("/*application='app'*/ UPDATE t SET note = 'a;b' WHERE id = 1"),
            ["t"]
        );
        // `COPY … FROM` keeps the loaded table.
        assert!(write_tables("COPY orders FROM STDIN").contains(&"orders".to_string()));
    }
}
