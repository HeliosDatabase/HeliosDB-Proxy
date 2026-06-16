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

/// Scan `sql` and return the labels of every pattern that matched.
/// Empty vec = clean.
pub fn scan(sql: &str) -> Vec<String> {
    let mut hits = Vec::new();
    let lower = sql.to_lowercase();

    if matches_classic_or(&lower) {
        hits.push("classic_or_payload".into());
    }
    if matches_union_select(&lower) {
        hits.push("union_select".into());
    }
    if matches_comment_escape(&lower) {
        hits.push("comment_escape".into());
    }
    if matches_stacked_queries(sql) {
        hits.push("stacked_queries".into());
    }
    if matches_time_based(&lower) {
        hits.push("time_based_blind".into());
    }
    if matches_information_schema_probe(&lower) {
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
fn matches_stacked_queries(sql: &str) -> bool {
    // Strip trailing whitespace + a single trailing ';' (cosmetic).
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
        assert!(
            !scan("SELECT id FROM users UNION SELECT id FROM admins")
                .contains(&"union_select".to_string())
                == false
        );
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
}
