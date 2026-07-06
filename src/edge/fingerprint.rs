//! Edge-cache query fingerprinting.
//!
//! Self-contained twin of the query-cache normalizer
//! (`src/cache/normalizer.rs`) so `edge-proxy` builds without the
//! `query-cache` feature — this module must NOT reference the cache
//! modules. It produces the ingredients of an edge `CacheKey` plus
//! the table set invalidations target:
//!
//! - `fingerprint` — comments/hints stripped, literals folded to `?`,
//!   whitespace collapsed, uppercased. Groups "the same query shape".
//! - `params_hash` — 16-hex digest of the extracted literal values
//!   plus the session `database`/`user` and the sorted session
//!   variables (startup GUCs like TimeZone/options affect result
//!   bytes, so they partition the cache too). The tenant identity is
//!   ALSO carried verbatim in the edge `CacheKey`, so a 64-bit hash
//!   collision can never alias tenants. Hashes only need to be stable
//!   within one process: cache keys never cross the wire —
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

// Same patterns as the query-cache normalizer, duplicated on purpose
// (feature independence). Keep the two in sync when touching either.
static STRING_LITERAL: Lazy<Regex> = Lazy::new(|| Regex::new(r#"'(?:[^'\\]|\\.)*'"#).unwrap());

static NUMBER_LITERAL: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b\d+(?:\.\d+)?(?:e[+-]?\d+)?\b").unwrap());

static WHITESPACE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());

// Superset of the normalizer's pattern: COPY is edge-specific so
// `COPY t FROM ...` attributes the LOADED table (t), not just the
// source token after FROM. Extra captured tokens (e.g. `stdin`) only
// widen invalidation — safe.
static TABLE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:FROM|JOIN|INTO|UPDATE|TABLE|COPY)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap()
});

static HINT_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"/\*[^*]*\*/").unwrap());

static COMMENT_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"--[^\n]*").unwrap());

/// Strip routing hints + `--` comments. Returns borrowed text when
/// nothing matched (the common case) — zero-copy.
fn strip_hints_and_comments(sql: &str) -> std::borrow::Cow<'_, str> {
    match HINT_PATTERN.replace_all(sql, "") {
        std::borrow::Cow::Borrowed(_) => COMMENT_PATTERN.replace_all(sql, ""),
        std::borrow::Cow::Owned(s) => match COMMENT_PATTERN.replace_all(&s, "") {
            std::borrow::Cow::Borrowed(_) => std::borrow::Cow::Owned(s),
            std::borrow::Cow::Owned(s2) => std::borrow::Cow::Owned(s2),
        },
    }
}

/// Just the referenced table set — byte-identical to
/// `analyze(..).tables` (both extract from the hint/comment-stripped
/// text BEFORE literal folding), without the literal-folding regex
/// passes, the full-buffer uppercase copy, or the params hash. The
/// write-invalidation path uses this: it discards everything else, so
/// a bulk INSERT must not pay full fingerprinting (F19).
pub fn tables_only(sql: &str) -> Vec<String> {
    extract_tables(&strip_hints_and_comments(sql))
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
    // Strip hints and comments first so neither literals inside them
    // nor hint text itself perturbs the fingerprint.
    let sql = strip_hints_and_comments(sql);

    // Extract tables before literal folding (folding can't create
    // table tokens, but keep the same order as the normalizer).
    // NOTE: keep this the same extraction `tables_only` uses — the
    // read key's table set and the write invalidation's table set
    // must never drift, or entries become unsweepable.
    let tables = extract_tables(&sql);

    // Fold literals to `?`, collecting their values for the hash.
    let mut params: Vec<String> = Vec::new();
    let sql = STRING_LITERAL.replace_all(&sql, |caps: &regex::Captures| {
        let value = caps.get(0).unwrap().as_str();
        // Store without the surrounding quotes.
        params.push(value[1..value.len() - 1].to_string());
        "?"
    });
    let sql = NUMBER_LITERAL.replace_all(&sql, |caps: &regex::Captures| {
        params.push(caps.get(0).unwrap().as_str().to_string());
        "?"
    });

    let sql = WHITESPACE.replace_all(&sql, " ");
    let fingerprint = sql.trim().to_uppercase();

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
    for p in &params {
        p.hash(&mut hasher);
    }
    let params_hash = format!("{:016x}", hasher.finish());

    EdgeFingerprint {
        fingerprint,
        params_hash,
        tables,
    }
}

/// Table identifiers after FROM/JOIN/INTO/UPDATE/TABLE, lowercased,
/// schema prefix stripped, first-seen order, deduped.
fn extract_tables(sql: &str) -> Vec<String> {
    let mut tables: Vec<String> = Vec::new();
    for cap in TABLE_PATTERN.captures_iter(sql) {
        if let Some(m) = cap.get(1) {
            let table = m.as_str().to_lowercase();
            let name = table.split('.').next_back().unwrap_or(&table);
            if !tables.iter().any(|t| t == name) {
                tables.push(name.to_string());
            }
        }
    }
    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_fold_into_fingerprint() {
        let a = analyze("SELECT * FROM users WHERE id = 123", "db", "u", &[]);
        assert_eq!(a.fingerprint, "SELECT * FROM USERS WHERE ID = ?");
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
    fn whitespace_collapses_and_case_normalizes() {
        let a = analyze("select  *\n  from   users\twhere id=1", "db", "u", &[]);
        let b = analyze("SELECT * FROM users WHERE id=1", "db", "u", &[]);
        assert_eq!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn comments_and_hints_are_stripped() {
        let a = analyze(
            "/* helios:route=primary */ SELECT * FROM users -- trailing\nWHERE id = 1",
            "db",
            "u",
            &[],
        );
        assert_eq!(a.fingerprint, "SELECT * FROM USERS WHERE ID = ?");
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

    #[test]
    fn copy_attributes_the_loaded_table() {
        // `COPY t FROM STDIN` must extract t (the table the rows land
        // in); the trailing source token is harmless over-invalidation.
        let t = tables_only("COPY orders FROM STDIN WITH (FORMAT csv)");
        assert!(t.contains(&"orders".to_string()));
        let t2 = tables_only("COPY public.orders FROM '/tmp/f.csv'");
        assert!(t2.contains(&"orders".to_string()));
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
}
