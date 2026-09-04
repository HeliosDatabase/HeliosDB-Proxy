//! SQL-injection heuristic scanner.
//!
//! Pattern-based detection — purposefully shallow. This is **not** a
//! parser. Reasons:
//!
//! - The proxy already routes parsed queries; an attacker bypasses
//!   that by stuffing payloads into string literals. Pattern
//!   matching catches the literal-stuffing case the parser by
//!   definition cannot.
//! - The signal is "this looks like a known payload shape" — useful
//!   alongside a real WAF, not a substitute for one.
//! - False positives are surface area. Each pattern is documented
//!   with the payload class it targets so operators can mute the
//!   ones they don't want.
//!
//! Returned values are pattern *labels*, not the payload itself.
//! Operators correlate against the SQL excerpt in the parent event.

/// Lower-case `sql` into `buf`, replacing whatever `buf` held.
///
/// Semantically `*buf = sql.to_lowercase()`, but a caller that keeps
/// `buf` across calls reuses its allocation instead of allocating
/// fresh each time. ASCII input (the overwhelming majority of SQL)
/// takes a byte-wise path with no temporary; non-ASCII input falls
/// back to `str::to_lowercase` verbatim — one temporary `String` plus
/// a copy into `buf` — so the full Unicode case mapping, and
/// therefore the scan verdict, is unchanged at the cost of doing
/// strictly more work than the ASCII path for that one call.
pub fn lower_into(sql: &str, buf: &mut String) {
    buf.clear();
    if sql.is_ascii() {
        buf.push_str(sql);
        buf.make_ascii_lowercase();
    } else {
        buf.push_str(&sql.to_lowercase());
    }
}

/// Scan `sql` and return the labels of every pattern that matched.
/// Empty vec = clean.
///
/// Convenience wrapper: lower-cases into a fresh buffer once and
/// delegates to [`scan_lowered`]. Hot paths that already keep a
/// scratch buffer should call [`lower_into`] + [`scan_lowered`]
/// instead so nothing is allocated per query.
///
/// Kept as public API for out-of-tree callers (this crate's own hot
/// path uses [`lower_into`] + [`scan_lowered`] directly and does not
/// call this function).
pub fn scan(sql: &str) -> Vec<String> {
    let mut lower = String::new();
    lower_into(sql, &mut lower);
    scan_lowered(&lower)
}

/// Scan an already-lower-cased statement (see [`lower_into`]) and
/// return the labels of every pattern that matched. Empty vec =
/// clean.
///
/// Every matcher is case-insensitive by construction — its needles
/// are lower-case ASCII — so one lowered view feeds all of them.
/// Passing a string that has *not* been lower-cased silently misses
/// upper-case payloads.
pub fn scan_lowered(lower: &str) -> Vec<String> {
    let mut hits = Vec::new();

    if matches_classic_or(lower) {
        hits.push("classic_or_payload".into());
    }
    if matches_union_select(lower) {
        hits.push("union_select".into());
    }
    if matches_comment_escape(lower) {
        hits.push("comment_escape".into());
    }
    if matches_stacked_queries(lower) {
        hits.push("stacked_queries".into());
    }
    if matches_time_based(lower) {
        hits.push("time_based_blind".into());
    }
    if matches_information_schema_probe(lower) {
        hits.push("information_schema_probe".into());
    }

    hits
}

/// `OR 1=1`, `OR '1'='1'`, `OR true` — the canonical authentication
/// bypass payload. Triggers on tautologies that would never appear
/// in a legitimate query a parameterised driver builds.
fn matches_classic_or(lower: &str) -> bool {
    // Match OR <token> = <same token> with optional quotes.
    // Looking for: " or 1=1", " or '1'='1'", " or \"a\"=\"a\"", " or true"
    let needles = [
        " or 1=1",
        " or 1 = 1",
        " or '1'='1'",
        " or '1' = '1'",
        " or true",
        " or true--",
        " or true#",
        "' or '1'='1",
        "\" or \"1\"=\"1",
    ];
    needles.iter().any(|n| lower.contains(n))
}

/// `UNION SELECT` payloads — extracts data from arbitrary tables by
/// stitching another SELECT onto the targeted query.
fn matches_union_select(lower: &str) -> bool {
    // " union select" with whitespace is the tell. Variations
    // include "union all select" and "union%20select" (URL-encoded
    // sometimes makes it through unescaped).
    lower.contains(" union select")
        || lower.contains(" union all select")
        || lower.contains("/*!union*/")
        || lower.contains("'union select")
}

/// Comment escape — closes a string + comments out the rest of the
/// query so the injected payload runs alone. `'--`, `/*` followed
/// by no matching `*/` near the end, and `#` (MySQL-style) all
/// count.
fn matches_comment_escape(lower: &str) -> bool {
    // `'--` or `' --` or `';--` near a quote; `'#` (MySQL).
    lower.contains("'--")
        || lower.contains("' --")
        || lower.contains("';--")
        || lower.contains("\"--")
        || lower.contains("\" --")
        || lower.contains("\";--")
        || lower.contains("'#")
        || lower.contains("'/*")
}

/// Stacked queries — `;` separating multiple statements. PostgreSQL
/// allows simple-query-protocol multi-statement, so this is high
/// signal in untrusted contexts.
///
/// Heuristic: scan for any `;` followed by a SQL verb. We don't try
/// to track string state because an injection's whole goal is to
/// escape a string — by the time the payload runs, the original
/// string context is already broken. False positives on string
/// literals containing `;<VERB>` are rare in practice.
fn matches_stacked_queries(lower: &str) -> bool {
    // Strip trailing whitespace + any trailing ';' characters (cosmetic).
    // Trimming commutes with lower-casing — no case mapping produces
    // or consumes whitespace or ';' — so trimming the lowered view
    // yields the same string as lowering the trimmed view did.
    let lower = lower.trim_end().trim_end_matches(';').trim();
    let verbs = [
        "select ",
        "insert ",
        "update ",
        "delete ",
        "drop ",
        "create ",
        "alter ",
        "truncate ",
        "grant ",
        "revoke ",
        "exec ",
        "execute ",
        "begin ",
        "commit ",
        "rollback ",
        "set ",
        "with ",
    ];
    let mut idx = 0;
    while let Some(off) = lower[idx..].find(';') {
        let pos = idx + off;
        let after = &lower[pos + 1..];
        let after_trim = after.trim_start();
        if verbs.iter().any(|v| after_trim.starts_with(v)) {
            return true;
        }
        idx = pos + 1;
        if idx >= lower.len() {
            break;
        }
    }
    false
}

/// Time-based blind injection — uses sleeps to extract data one bit
/// at a time. Common payload prefixes: `pg_sleep`, `WAITFOR DELAY`,
/// `SLEEP(`, `BENCHMARK(`.
fn matches_time_based(lower: &str) -> bool {
    lower.contains("pg_sleep(")
        || lower.contains("waitfor delay")
        || lower.contains("sleep(")
        || lower.contains("benchmark(")
}

/// Schema enumeration — `information_schema.tables`, `pg_catalog.pg_tables`,
/// commonly used after a UNION-based foothold to map the schema.
fn matches_information_schema_probe(lower: &str) -> bool {
    lower.contains("information_schema.tables")
        || lower.contains("information_schema.columns")
        || lower.contains("pg_catalog.pg_tables")
        || lower.contains("pg_namespace")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classic_or_one_eq_one_caught() {
        assert!(scan("SELECT * FROM users WHERE id = 1 OR 1=1")
            .contains(&"classic_or_payload".to_string()));
        assert!(scan("SELECT * FROM users WHERE id = 1 OR 1 = 1")
            .contains(&"classic_or_payload".to_string()));
        assert!(scan("SELECT * FROM users WHERE name = 'a' OR '1'='1'")
            .contains(&"classic_or_payload".to_string()));
        assert!(scan("SELECT * FROM users WHERE id = 1 OR TRUE")
            .contains(&"classic_or_payload".to_string()));
    }

    #[test]
    fn classic_or_legit_query_clean() {
        // Legitimate disjunction across actual columns shouldn't fire.
        assert!(!scan("SELECT * FROM users WHERE id = 1 OR id = 2")
            .contains(&"classic_or_payload".to_string()));
        assert!(
            !scan("SELECT * FROM logs WHERE level = 'error' OR level = 'warn'")
                .contains(&"classic_or_payload".to_string())
        );
    }

    #[test]
    fn union_select_caught() {
        assert!(scan("' UNION SELECT NULL,NULL,NULL --").contains(&"union_select".to_string()));
        assert!(scan("foo' UNION ALL SELECT username,password FROM users")
            .contains(&"union_select".to_string()));
    }

    #[test]
    fn union_legit_query_clean() {
        assert!(scan("SELECT id FROM users UNION SELECT id FROM admins")
            .contains(&"union_select".to_string()));
        // Note: above is intentional — UNION across legit tables IS
        // ambiguous from a pattern-matcher's view. We accept the
        // false positive on " union select" since that's what the
        // payload class is. Operators who union legitimately can
        // mute the rule.
    }

    #[test]
    fn comment_escape_caught() {
        assert!(scan("foo' --").contains(&"comment_escape".to_string()));
        assert!(scan("foo'-- and more SQL").contains(&"comment_escape".to_string()));
        assert!(scan("foo';-- ").contains(&"comment_escape".to_string()));
        assert!(scan("foo'#").contains(&"comment_escape".to_string()));
    }

    #[test]
    fn stacked_queries_caught() {
        assert!(
            scan("SELECT * FROM users; DROP TABLE logs;").contains(&"stacked_queries".to_string())
        );
        assert!(scan("'); DELETE FROM users WHERE 1=1;--").contains(&"stacked_queries".to_string()));
    }

    #[test]
    fn stacked_queries_ignores_trailing_semicolon() {
        let r = scan("SELECT 1;");
        assert!(!r.contains(&"stacked_queries".to_string()));
    }

    #[test]
    fn stacked_queries_ignores_semicolon_in_string_literal() {
        let r = scan("SELECT 'a;b' FROM dual");
        assert!(!r.contains(&"stacked_queries".to_string()));
    }

    #[test]
    fn time_based_blind_caught() {
        assert!(scan("'; SELECT pg_sleep(5)--").contains(&"time_based_blind".to_string()));
        assert!(
            scan("SELECT BENCHMARK(1000000, MD5('a'))").contains(&"time_based_blind".to_string())
        );
    }

    #[test]
    fn information_schema_probe_caught() {
        assert!(
            scan("' UNION SELECT table_name FROM information_schema.tables --")
                .contains(&"information_schema_probe".to_string())
        );
        assert!(scan("SELECT * FROM pg_catalog.pg_tables")
            .contains(&"information_schema_probe".to_string()));
    }

    #[test]
    fn multiple_patterns_all_reported() {
        // A single SQLi payload can match several patterns at once.
        // Use a comment_escape-bearing variant: `';--` immediately
        // after the closing quote.
        let r = scan("foo' OR 1=1 UNION SELECT 1,2,3 FROM information_schema.tables';--");
        assert!(
            r.contains(&"classic_or_payload".to_string()),
            "missing classic_or in {:?}",
            r
        );
        assert!(
            r.contains(&"union_select".to_string()),
            "missing union_select in {:?}",
            r
        );
        assert!(
            r.contains(&"comment_escape".to_string()),
            "missing comment_escape in {:?}",
            r
        );
        assert!(
            r.contains(&"information_schema_probe".to_string()),
            "missing schema probe in {:?}",
            r
        );
    }

    #[test]
    fn benign_query_clean() {
        let r = scan("SELECT id, name FROM users WHERE id = $1 LIMIT 10");
        assert!(r.is_empty(), "got false positives: {:?}", r);
    }

    /// Verbatim copy of the pre-optimisation `scan`: lower-cases the
    /// SQL once for most matchers and hands the *raw* SQL to
    /// `matches_stacked_queries`, which lower-cased a second time.
    /// The single-lowercase rewrite must be observationally identical
    /// to this.
    fn legacy_scan(sql: &str) -> Vec<String> {
        fn legacy_stacked(sql: &str) -> bool {
            let trimmed = sql.trim_end().trim_end_matches(';').trim();
            let lower = trimmed.to_lowercase();
            let verbs = [
                "select ",
                "insert ",
                "update ",
                "delete ",
                "drop ",
                "create ",
                "alter ",
                "truncate ",
                "grant ",
                "revoke ",
                "exec ",
                "execute ",
                "begin ",
                "commit ",
                "rollback ",
                "set ",
                "with ",
            ];
            let mut idx = 0;
            while let Some(off) = lower[idx..].find(';') {
                let pos = idx + off;
                let after_trim = lower[pos + 1..].trim_start();
                if verbs.iter().any(|v| after_trim.starts_with(v)) {
                    return true;
                }
                idx = pos + 1;
                if idx >= lower.len() {
                    break;
                }
            }
            false
        }

        let mut hits = Vec::new();
        let lower = sql.to_lowercase();
        if matches_classic_or(&lower) {
            hits.push("classic_or_payload".to_string());
        }
        if matches_union_select(&lower) {
            hits.push("union_select".to_string());
        }
        if matches_comment_escape(&lower) {
            hits.push("comment_escape".to_string());
        }
        if legacy_stacked(sql) {
            hits.push("stacked_queries".to_string());
        }
        if matches_time_based(&lower) {
            hits.push("time_based_blind".to_string());
        }
        if matches_information_schema_probe(&lower) {
            hits.push("information_schema_probe".to_string());
        }
        hits
    }

    /// Corpus exercised by every equivalence test below: benign SQL,
    /// each payload class, mixed/upper case, non-ASCII bodies and
    /// identifiers, and multi-byte characters straddling the
    /// excerpt/needle boundaries.
    const CORPUS: &[&str] = &[
        "",
        ";",
        ";;",
        "   ",
        "SELECT 1",
        "SELECT 1;",
        "select id, name from users where id = $1 limit 10",
        "SELECT * FROM users WHERE id = 1 OR 1=1",
        "SeLeCt * FrOm users WHERE id = 1 oR 1 = 1",
        "SELECT * FROM users WHERE name = 'a' OR '1'='1'",
        "SELECT * FROM users WHERE id = 1 OR TRUE--",
        "' UNION SELECT NULL,NULL,NULL --",
        "foo' UnIoN AlL sElEcT username,password FROM users",
        "/*!UNION*/ SELECT 1",
        "foo'--",
        "foo\" --",
        "foo'#",
        "SELECT * FROM users; DROP TABLE logs;",
        "SELECT * FROM users; drop TABLE logs",
        "'); DELETE FROM users WHERE 1=1;--",
        "SELECT 'a;b' FROM dual",
        "SELECT 1;   WiTh cte AS (SELECT 1) SELECT * FROM cte",
        "'; SELECT PG_SLEEP(5)--",
        "SELECT BENCHMARK(1000000, MD5('a'))",
        "SELECT 1 WAITFOR DELAY '0:0:5'",
        "' UNION SELECT table_name FROM INFORMATION_SCHEMA.TABLES --",
        "SELECT * FROM pg_catalog.pg_tables",
        "SELECT * FROM PG_NAMESPACE",
        // Non-ASCII: Unicode case mapping must be preserved verbatim.
        "SELECT * FROM ÜSERS WHERE naïve = 'café'",
        "SELECT * FROM «таблица» WHERE имя = 'ЗНАЧЕНИЕ'",
        "SELECT 'ΣΊΣΥΦΟΣ' FROM Σ",
        "SELECT * FROM ünïcode; DRÖP TABLE x",
        "SELECT * FROM t WHERE ı = 'İ' OR 1=1",
        // Dotted capital I and the Kelvin sign lower-case to ASCII
        // under Unicode rules but not under ASCII-only rules — the
        // rewrite must keep the Unicode behaviour.
        "SELECT BENCHMAR\u{212a}('a')",
        "SELECT 1 WA\u{130}TFOR DELAY",
        "SELECT 1; \u{130}NSERT INTO t VALUES (1)",
        "日本語のクエリ; SELECT 1",
        "SELECT '💥' OR 1=1",
        "ＳＥＬＥＣＴ 1 OR 1=1",
        // Trailing Greek capital sigma (Final_Sigma-sensitive: Σ
        // lower-cases to final-form ς at a word end, ordinary σ
        // otherwise) immediately followed by ';' and by whitespace —
        // pins that trim-then-lower and lower-then-trim agree here.
        "SELECT * FROM \u{3a3};",
        "SELECT * FROM \u{3a3} ",
    ];

    #[test]
    fn scan_matches_legacy_scan_on_corpus() {
        for sql in CORPUS {
            assert_eq!(
                scan(sql),
                legacy_scan(sql),
                "scan diverged from legacy_scan for {:?}",
                sql
            );
        }
    }

    #[test]
    fn scan_lowered_matches_scan_on_corpus() {
        for sql in CORPUS {
            let mut buf = String::new();
            lower_into(sql, &mut buf);
            assert_eq!(
                scan_lowered(&buf),
                scan(sql),
                "scan_lowered diverged from scan for {:?}",
                sql
            );
        }
    }

    #[test]
    fn lower_into_equals_to_lowercase_and_reuses_buffer() {
        let mut buf = String::new();
        for sql in CORPUS {
            lower_into(sql, &mut buf);
            assert_eq!(buf, sql.to_lowercase(), "lower_into wrong for {:?}", sql);
        }
        // Reused buffer must not retain any of the previous content.
        lower_into("SELECT LONG QUERY FROM SOMEWHERE", &mut buf);
        lower_into("A", &mut buf);
        assert_eq!(buf, "a");
        // …and its capacity is carried over rather than re-grown.
        assert!(buf.capacity() >= "select long query from somewhere".len());
    }
}
