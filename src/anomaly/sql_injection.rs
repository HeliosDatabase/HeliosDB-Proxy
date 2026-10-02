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
    // All original payloads contain " or ". Search that shared fragment
    // once, then inspect the suffix at each occurrence. Advancing only to its
    // trailing space preserves overlapping occurrences (" or or 1=1").
    let mut start = 0;
    while let Some(offset) = lower[start..].find(" or ") {
        let pos = start + offset;
        let suffix = &lower[pos + 4..];
        if suffix.starts_with("1=1")
            || suffix.starts_with("1 = 1")
            || suffix.starts_with("'1'='1'")
            || suffix.starts_with("'1' = '1'")
            || suffix.starts_with("true")
        {
            // "true--" and "true#" are already covered by the original
            // "true" prefix. No boundary restriction is added here.
            return true;
        }
        // The original quote-prefixed needles intentionally do not require
        // the last closing quote. Keep that asymmetric fragment detection,
        // but only when the corresponding quote precedes the OR fragment.
        if pos > 0
            && ((lower.as_bytes()[pos - 1] == b'\'' && suffix.starts_with("'1'='1"))
                || (lower.as_bytes()[pos - 1] == b'"' && suffix.starts_with("\"1\"=\"1")))
        {
            return true;
        }
        start = pos + 3;
    }
    false
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
    // Inspect each possible comment introducer once instead of rescanning the
    // entire query for eight overlapping needles. These byte contexts reproduce
    // exactly: either quote followed by --, quote + one space/semicolon + --,
    // or a single quote followed by # or /*. No extra whitespace is accepted.
    let bytes = lower.as_bytes();
    for pos in memchr::memchr3_iter(b'-', b'/', b'#', bytes) {
        let previous = pos.checked_sub(1).map(|i| bytes[i]);
        match bytes[pos] {
            b'-' if bytes.get(pos + 1) == Some(&b'-')
                && (matches!(previous, Some(b'\'' | b'"'))
                    || (matches!(previous, Some(b' ' | b';'))
                        && pos >= 2
                        && matches!(bytes[pos - 2], b'\'' | b'"'))) =>
            {
                return true;
            }
            b'/' if bytes.get(pos + 1) == Some(&b'*') && previous == Some(b'\'') => {
                return true;
            }
            b'#' if previous == Some(b'\'') => return true,
            _ => {}
        }
    }
    false
}

/// Stacked queries — `;` separating multiple statements. PostgreSQL
/// allows simple-query-protocol multi-statement, so this is high
/// signal in untrusted contexts.
///
/// Heuristic: scan for any `;` followed by a SQL verb. We don't try
/// to track string state because an injection's whole goal is to
/// escape a string — by the time the payload runs, the original
/// string context is already broken. False positives on string
/// literals containing `;<VERB>` are rare in practice. A complete, lexically
/// unambiguous transaction containing ordinary DML is exempt from this one
/// pattern. Other injection patterns still inspect the entire input.
fn matches_stacked_queries(lower: &str) -> bool {
    // Strip trailing whitespace + any trailing ';' characters (cosmetic).
    // Trimming commutes with lower-casing — no case mapping produces
    // or consumes whitespace or ';' — so trimming the lowered view
    // yields the same string as lowering the trimmed view did.
    let lower = lower.trim_end().trim_end_matches(';').trim();
    let mut idx = 0;
    while let Some(off) = lower[idx..].find(';') {
        let pos = idx + off;
        let after = &lower[pos + 1..];
        let after_trim = after.trim_start();
        // Keep the original literal-space boundary, but dispatch on the verb
        // once rather than checking every verb's prefix on each semicolon.
        // The longest matched verb is eight bytes. Bound the search so many
        // semicolons without a following space cannot cause quadratic work.
        let verb = after_trim
            .as_bytes()
            .iter()
            .take(9)
            .position(|byte| *byte == b' ')
            .map(|end| &after_trim[..end]);
        if matches!(
            verb,
            Some(
                "select"
                    | "insert"
                    | "update"
                    | "delete"
                    | "drop"
                    | "create"
                    | "alter"
                    | "truncate"
                    | "grant"
                    | "revoke"
                    | "exec"
                    | "execute"
                    | "begin"
                    | "commit"
                    | "rollback"
                    | "set"
                    | "with"
            )
        ) {
            // Most stacked payloads cannot be a transaction batch. Preserve
            // comment/empty-statement prefixes for the full lexical check.
            if !(lower.starts_with("begin")
                || lower.starts_with("start")
                || lower.starts_with("/*")
                || lower.starts_with("--")
                || lower.starts_with(';'))
            {
                return true;
            }
            // A raw delimiter is authoritative here only when everything
            // before it is exactly BEGIN/START TRANSACTION. Never use this
            // shortcut for a semicolon inside a leading comment or literal.
            if matches!(
                verb,
                Some(
                    "drop"
                        | "create"
                        | "alter"
                        | "truncate"
                        | "grant"
                        | "revoke"
                        | "exec"
                        | "execute"
                )
            ) && matches!(
                lower[..pos].trim(),
                "begin" | "begin work" | "begin transaction" | "start transaction"
            ) {
                return true;
            }
            return !ordinary_transaction_batch(lower);
        }
        idx = pos + 1;
        if idx >= lower.len() {
            break;
        }
    }
    false
}

/// Recognize only a complete BEGIN/START TRANSACTION ... COMMIT/ROLLBACK
/// batch. This is a false-positive exemption, not a SQL safety decision:
/// stacked DDL, COPY, incomplete/ambiguous input and SQL after the transaction
/// retain the original heuristic. Quoted/comment text cannot supply delimiters.
fn ordinary_transaction_batch(sql: &str) -> bool {
    let mut rest = sql.as_bytes();
    let mut in_transaction = false;
    let mut finished = false;
    let mut head: &[u8] = b"";
    let mut second: &[u8] = b"";
    let mut tokens = 0;
    let mut body_statements = 0;
    let mut simple_body = None;
    loop {
        let token = match transaction_token(&mut rest) {
            Ok(token) => token,
            Err(()) => return false,
        };
        if token.is_none() || token == Some(b";".as_slice()) {
            if tokens != 0 {
                if finished {
                    return false;
                }
                if !in_transaction {
                    let begin = head == b"begin"
                        && (tokens == 1
                            || (tokens == 2 && matches!(second, b"work" | b"transaction")));
                    let start = head == b"start" && tokens == 2 && second == b"transaction";
                    if !begin && !start {
                        return false;
                    }
                    in_transaction = true;
                } else if matches!(head, b"commit" | b"rollback" | b"end" | b"abort") {
                    if tokens > 2 || (tokens == 2 && !matches!(second, b"work" | b"transaction")) {
                        return false;
                    }
                    finished = true;
                } else if matches!(
                    head,
                    b"select" | b"insert" | b"update" | b"delete" | b"with"
                ) {
                    body_statements += 1;
                } else {
                    return false;
                }
                tokens = 0;
            }
            if token.is_none() {
                return finished && body_statements > 0;
            }
        } else if let Some(token) = token {
            if tokens == 0 {
                if finished || (!in_transaction && !matches!(token, b"begin" | b"start")) {
                    return false;
                }
                head = token;
                if in_transaction
                    && matches!(
                        head,
                        b"select" | b"insert" | b"update" | b"delete" | b"with"
                    )
                {
                    // Statement heads/control tokens matter; ordinary DML
                    // punctuation and identifiers do not. Prove once that the
                    // remaining input has no complex lexical forms, then skip
                    // its bodies by quote/semicolon boundaries. Complex SQL
                    // retains the exact conservative token scanner below.
                    let simple = *simple_body.get_or_insert_with(|| {
                        memchr::memchr3(b'"', b'\\', 0, rest).is_none()
                            && memchr::memchr3_iter(b'/', b'-', b'$', rest).all(|pos| {
                                match rest[pos] {
                                    b'/' => rest.get(pos + 1) != Some(&b'*'),
                                    b'-' => rest.get(pos + 1) != Some(&b'-'),
                                    _ => false,
                                }
                            })
                    });
                    if simple && skip_simple_transaction_body(&mut rest).is_err() {
                        return false;
                    }
                }
            } else if tokens == 1 {
                second = token;
            }
            tokens += 1;
        }
    }
}

/// Skip a DML body after the caller has ruled out comments, dollar tokens,
/// quoted identifiers, backslashes and NUL bytes. Single-quoted literals still
/// need their doubled-quote handling; only an outside semicolon ends the body.
fn skip_simple_transaction_body(rest: &mut &[u8]) -> Result<(), ()> {
    let bytes = *rest;
    let mut i = 0;
    while let Some(offset) = memchr::memchr2(b';', b'\'', &bytes[i..]) {
        i += offset;
        if bytes[i] == b';' {
            *rest = &bytes[i..];
            return Ok(());
        }
        let mut tail = &bytes[i..];
        transaction_token(&mut tail)?;
        i = bytes.len() - tail.len();
    }
    *rest = &[];
    Ok(())
}

/// Allocation-free lexical tokens for the narrow exemption above. Backslashes
/// in strings are deliberately refused: their interpretation depends on the
/// session's standard_conforming_strings setting. Dollar quotes are also
/// refused: scan_lowered has erased their case-sensitive delimiter spelling.
/// Errors keep the alert.
fn transaction_token<'a>(rest: &mut &'a [u8]) -> Result<Option<&'a [u8]>, ()> {
    let b = *rest;
    let mut i = 0;
    while i < b.len() {
        if b[i].is_ascii_whitespace() {
            i += 1;
        } else if b[i..].starts_with(b"--") {
            i += 2;
            while i < b.len() && !matches!(b[i], b'\n' | b'\r') {
                i += 1;
            }
        } else if b[i..].starts_with(b"/*") {
            i += 2;
            let mut depth = 1;
            while depth > 0 {
                if b[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if b[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else if i < b.len() {
                    i += 1;
                } else {
                    return Err(());
                }
            }
        } else {
            break;
        }
    }
    let start = i;
    if i == b.len() {
        *rest = &b[i..];
        return Ok(None);
    }
    match b[i] {
        0 => return Err(()),
        b'\'' | b'"' => {
            let quote = b[i];
            i += 1;
            loop {
                match b.get(i) {
                    None | Some(0) => return Err(()),
                    Some(b'\\') if quote == b'\'' => return Err(()),
                    Some(c) if *c == quote => {
                        i += 1;
                        if b.get(i) == Some(&quote) {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    Some(_) => i += 1,
                }
            }
        }
        b'$' => {
            i += 1;
            if b.get(i)
                .is_some_and(|c| c.is_ascii_alphabetic() || *c == b'_' || *c >= 128)
            {
                i += 1;
                while b
                    .get(i)
                    .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_' || *c >= 128)
                {
                    i += 1;
                }
            }
            if b.get(i) == Some(&b'$') {
                return Err(());
            }
        }
        c if c.is_ascii_alphabetic() || c == b'_' || c >= 128 => {
            i += 1;
            while b.get(i).is_some_and(|c| {
                c.is_ascii_alphanumeric() || matches!(*c, b'_' | b'$') || *c >= 128
            }) {
                i += 1;
            }
        }
        _ => i += 1,
    }
    *rest = &b[i..];
    Ok(Some(&b[start..i]))
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
    fn classic_or_shared_prefix_preserves_overlap_and_partial_quotes() {
        for sql in [
            " or or 1=1",
            " or or or 1 = 1",
            "' or '1'='1",
            "\" or \"1\"=\"1",
            " or false or true#",
            " or true_suffix",
            "é or or 1=1",
        ] {
            assert!(matches_classic_or(sql), "{sql}");
        }
        for sql in [" or '1'='1", " or \"1\"=\"1", " or or id = 1"] {
            assert!(!matches_classic_or(sql), "{sql}");
        }
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
    fn comment_escape_preserves_exact_quote_contexts() {
        for sql in [
            "'--",
            "' --",
            "';--",
            "\"--",
            "\" --",
            "\";--",
            "'#",
            "'/*",
            "-- plain first; ' --",
            "x - 1 / 2; y'/*",
            "é'---",
        ] {
            assert!(matches_comment_escape(sql), "{sql}");
        }
        for sql in [
            "--",
            "/*",
            "#",
            "x - 1 / 2",
            "'  --",
            "'\t--",
            "' ;--",
            "\"#",
            "\"/*",
            "' #",
            "' /*",
            "' - -",
            "é--",
        ] {
            assert!(!matches_comment_escape(sql), "{sql}");
        }
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
    fn ordinary_transaction_batches_do_not_trigger_stacked_queries() {
        for sql in [
            "BEGIN; INSERT INTO test_replication (name) VALUES ('delta'); COMMIT;",
            "BEGIN; SELECT 1; UPDATE t SET v = 2; DELETE FROM t WHERE id = 3; ROLLBACK;",
            "BEGIN WORK; INSERT INTO t VALUES ('a; COMMIT; DROP TABLE x;'); END WORK;",
            "START TRANSACTION; WITH t AS (SELECT 1) SELECT * FROM t; COMMIT TRANSACTION;",
            "/* begin */ BEGIN; /* outer /* nested ; COMMIT */ comment */ SELECT 1; COMMIT; -- tail",
            "BEGIN; INSERT INTO t VALUES ($1, $2); COMMIT;",
            "BEGIN; SELECT 1 AS \"semi;commit\"; COMMIT;",
            "BEGIN; INSERT INTO t VALUES ('it''s; COMMIT;'); COMMIT",
            "/* ; DROP TABLE log; */ BEGIN; SELECT 1; COMMIT;",
            "-- ; DROP TABLE log;\rBEGIN; SELECT 1; COMMIT;",
            "START/**/TRANSACTION; SELECT 1; ABORT TRANSACTION;",
            "; ; BEGIN; ; SELECT 1; ; COMMIT; ; -- tail",
            "BEGIN; SELECT x$tag FROM t; COMMIT;",
            "BEGIN; UPDATE accounts SET balance = balance - 1 WHERE id = 42; COMMIT;",
            "BEGIN; SELECT 1 / 2, -1, 1 - - 2; COMMIT;",
        ] {
            assert!(!scan(sql).contains(&"stacked_queries".to_string()), "{sql}");
        }
    }

    #[test]
    fn transaction_exemption_keeps_suspicious_batches() {
        for sql in [
            "BEGIN; DROP TABLE users; COMMIT;",
            "BEGIN; SELECT 1; COPY t TO '/tmp/t'; COMMIT;",
            "BEGIN; DELETE FROM t; COMMIT; DROP TABLE t;",
            "BEGIN; SELECT 1; COMMIT AND CHAIN;",
            "BEGIN; SELECT 1; ROLLBACK TO savepoint_name;",
            "BEGIN; SELECT 1; /* COMMIT; */",
            "BEGIN; SELECT '; COMMIT;'",
            "BEGIN; SELECT 1; COMMIT; /* unclosed",
            "BEGIN; SELECT 'unterminated; COMMIT;",
            "BEGIN; SELECT 1 AS a$x$; DROP TABLE t; SELECT $x$; COMMIT;",
            "BEGIN; SELECT 1; COMMIT; SELECT 2;",
            // Dollar tags are case sensitive in SQL; the lowered scan cannot
            // safely establish their boundaries, even for benign batches.
            "BEGIN; INSERT INTO t VALUES ($tag$; COMMIT; DROP TABLE t;$tag$); ABORT;",
            "BEGIN; SELECT $$benign$$; COMMIT;",
            r"BEGIN; SELECT 'a\'; SELECT 2; COMMIT;",
        ] {
            assert!(scan(sql).contains(&"stacked_queries".to_string()), "{sql}");
        }
    }

    #[test]
    fn case_sensitive_dollar_tags_cannot_hide_stacked_ddl() {
        let sql = "BEGIN; SELECT $A$ x $a$; SELECT $A$; DROP TABLE t; SELECT $a$ x $A$; SELECT $a$; COMMIT;";
        assert!(scan(sql).iter().any(|hit| hit == "stacked_queries"));
        assert!(scan_lowered(&sql.to_lowercase())
            .iter()
            .any(|hit| hit == "stacked_queries"));
    }

    #[test]
    fn transaction_exemption_does_not_mute_other_injection_patterns() {
        for (sql, pattern) in [
            (
                "BEGIN; SELECT * FROM t WHERE id = 1 OR 1=1; COMMIT;",
                "classic_or_payload",
            ),
            ("BEGIN; SELECT 1 UNION SELECT 2; COMMIT;", "union_select"),
            ("BEGIN; SELECT pg_sleep(5); COMMIT;", "time_based_blind"),
            (
                "BEGIN; SELECT * FROM information_schema.tables; COMMIT;",
                "information_schema_probe",
            ),
            ("BEGIN; SELECT 'x' -- comment\n; COMMIT;", "comment_escape"),
        ] {
            assert!(scan(sql).iter().any(|hit| hit == pattern), "{sql}");
        }
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
