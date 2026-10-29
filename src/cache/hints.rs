//! Cache Hints Parser
//!
//! Parses SQL comments to extract cache control hints.
//!
//! # Supported Hints
//!
//! ```sql
//! /* helios:cache=skip */          -- Neither look up nor store this read
//! /* helios:cache_ttl=60 */        -- A shorter TTL (seconds, or 5m, 1h, ...)
//! /* helios:cache=semantic */      -- Use the semantic tier (L3) too
//! /*helios:cache=semantic,cache_ttl=30*/  -- several, comma-separated
//! ```
//!
//! A cache hint may share a comment with routing hints, but never put it next
//! to `route=primary` or `consistency=strong`: with `[routing_hints]` on,
//! those make the proxy treat the read as a write, so it runs on the primary
//! and bypasses every cache tier (neither looked up nor stored). A result a
//! standby serves (`route=standby`) is never stored either.
//!
//! Hints are read only from comments outside string literals and quoted
//! identifiers. A `-- helios:…` line comment is read too, but hints belong
//! in block comments: a line comment runs to the end of the line, so it also
//! comments out whatever the proxy appends to the statement (the
//! `[multi_tenancy]` row filter of a SELECT without `WHERE`).
//!
//! `QueryCache` acts on `cache=skip` (also `bypass` and `none`, which the
//! routing parser reads as a skip too and strips, and `no`, `off`, `false`,
//! `disable`), `cache_ttl`/`ttl` (seconds, or `500ms`, `5m`, `1h`,
//! `1d`), which can only shorten the TTL the configuration gives the read
//! and also bounds what the read is served (an entry older than it is a miss
//! on every tier, refilled; a zero TTL is `cache=skip`), and
//! `cache=semantic` (also `semantic`,
//! `cache=l3`, `cache=vector`). `cache_tables`, `cache_refresh` and
//! `cache=l1|l2|all` are parsed into [`CacheHint`] and have no effect.

use once_cell::sync::Lazy;
use regex::Regex;
use std::time::Duration;

/// Parsed cache hints from a SQL query
#[derive(Debug, Clone, Default)]
pub struct CacheHint {
    /// Skip caching entirely
    pub skip: bool,

    /// Override TTL (None = use default)
    pub ttl: Option<Duration>,

    /// Enable semantic/L3 caching
    pub semantic_cache: bool,

    /// Override table dependencies
    pub tables: Option<Vec<String>>,

    /// Force cache refresh (bypass read, update cache)
    pub refresh: bool,

    /// Specific cache level to use
    pub level: Option<CacheLevelHint>,
}

impl CacheHint {
    /// Fold `other` (hints from another text of the same statement, e.g. the
    /// client's own text when routing hints or a rewrite changed the
    /// forwarded one) into `self`: `skip`, `semantic_cache` and `refresh`
    /// hold if either holds; `ttl`, `tables` and `level` keep `self`'s value
    /// when it has one.
    pub fn merge(&mut self, other: CacheHint) {
        self.skip |= other.skip;
        self.semantic_cache |= other.semantic_cache;
        self.refresh |= other.refresh;
        if self.ttl.is_none() {
            self.ttl = other.ttl;
        }
        if self.tables.is_none() {
            self.tables = other.tables;
        }
        if self.level.is_none() {
            self.level = other.level;
        }
    }
}

/// Hint for specific cache level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheLevelHint {
    /// Only use L1 (connection-local)
    L1Only,
    /// Only use L2 (shared)
    L2Only,
    /// Only use L3 (semantic)
    L3Only,
    /// Use all levels
    All,
}

// Patterns of the library helper `strip_hints` only (not quote-aware);
// `parse_cache_hints` reads comments with `crate::sql_text`.
static HINT_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"/\*\s*helios:(\w+)(?:=([^*]+))?\s*\*/").unwrap());

static HINT_PATTERN_DOUBLE_DASH: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"--\s*helios:(\w+)(?:=(\S+))?").unwrap());

/// Bare words that are flags, never table names after `cache_tables=`.
const FLAG_WORDS: &[&str] = &[
    "semantic",
    "semantic_cache",
    "cache_refresh",
    "refresh",
    "nocache_read",
];

/// Parse cache hints from a SQL query.
///
/// Each `helios:` comment outside quotes is split on `,`: a `key=value`
/// piece is a hint, a bare word a flag. A bare word after a
/// `cache_tables=`/`tables=` piece continues its table list
/// (`cache_tables=users,orders`) unless it is a flag word.
pub fn parse_cache_hints(sql: &str) -> CacheHint {
    let mut hint = CacheHint::default();
    if memchr::memmem::find(sql.as_bytes(), b"helios:").is_none() {
        return hint;
    }
    for body in crate::sql_text::helios_hint_bodies(sql) {
        let mut in_tables = false;
        for piece in body.split(',') {
            let piece = piece.trim();
            if piece.is_empty() {
                continue;
            }
            match piece.split_once('=') {
                Some((key, value)) => {
                    let key = key.trim();
                    // A value ends at whitespace (`-- helios:cache=skip note`).
                    let value = value.split_whitespace().next().unwrap_or("");
                    in_tables = matches!(key.to_lowercase().as_str(), "cache_tables" | "tables");
                    apply_hint(&mut hint, key, Some(value));
                }
                None => {
                    let word = piece.split_whitespace().next().unwrap_or("");
                    let flag = FLAG_WORDS.contains(&word.to_lowercase().as_str());
                    match hint.tables.as_mut() {
                        Some(tables) if in_tables && !flag => tables.push(word.to_string()),
                        _ => {
                            in_tables = false;
                            apply_hint(&mut hint, word, None);
                        }
                    }
                }
            }
        }
    }
    hint
}

/// Apply a single hint to the CacheHint struct
fn apply_hint(hint: &mut CacheHint, key: &str, value: Option<&str>) {
    match key.to_lowercase().as_str() {
        "cache" => {
            if let Some(v) = value {
                match v.to_lowercase().as_str() {
                    // `bypass` and `none` are the routing parser's words for
                    // a skip too: with `[routing_hints]` it strips such a
                    // comment, and the cache reads it back from the client's
                    // text, so both parsers must agree on every value.
                    "skip" | "bypass" | "none" | "no" | "off" | "false" | "disable" => {
                        hint.skip = true;
                    }
                    "semantic" | "l3" | "vector" => {
                        hint.semantic_cache = true;
                    }
                    "l1" | "hot" | "local" => {
                        hint.level = Some(CacheLevelHint::L1Only);
                    }
                    "l2" | "warm" | "shared" => {
                        hint.level = Some(CacheLevelHint::L2Only);
                    }
                    "all" | "yes" | "on" | "true" | "enable" => {
                        hint.level = Some(CacheLevelHint::All);
                    }
                    _ => {}
                }
            }
        }
        "cache_ttl" | "ttl" => {
            if let Some(v) = value {
                if let Ok(secs) = v.parse::<u64>() {
                    hint.ttl = Some(Duration::from_secs(secs));
                } else if let Some(duration) = parse_duration(v) {
                    hint.ttl = Some(duration);
                }
                // A zero TTL ("always fresh") is a skip: no tier could serve
                // the read, and storing it would replace the entry unhinted
                // reads of the statement share with one never served.
                if hint.ttl == Some(Duration::ZERO) {
                    hint.skip = true;
                }
            }
        }
        "cache_tables" | "tables" => {
            if let Some(v) = value {
                let tables: Vec<String> = v
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !tables.is_empty() {
                    hint.tables = Some(tables);
                }
            }
        }
        "cache_refresh" | "refresh" | "nocache_read" => {
            hint.refresh = true;
        }
        "semantic" | "semantic_cache" => {
            hint.semantic_cache = true;
        }
        _ => {}
    }
}

/// Parse duration strings like "5m", "1h", "30s". `None` for anything else,
/// including a value too large for a `Duration`.
fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim().to_lowercase();

    if s.is_empty() {
        return None;
    }

    // Try to find the numeric part and unit
    let mut num_end = 0;
    for (i, c) in s.char_indices() {
        if c.is_ascii_digit() || c == '.' {
            num_end = i + c.len_utf8();
        } else {
            break;
        }
    }

    if num_end == 0 {
        return None;
    }

    let num: f64 = s[..num_end].parse().ok()?;
    let unit = &s[num_end..];

    let multiplier = match unit {
        "" | "s" | "sec" | "secs" | "second" | "seconds" => 1.0,
        "m" | "min" | "mins" | "minute" | "minutes" => 60.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => 3600.0,
        "d" | "day" | "days" => 86400.0,
        "ms" | "millis" | "milliseconds" => 0.001,
        _ => return None,
    };

    Duration::try_from_secs_f64(num * multiplier).ok()
}

/// Strip cache hints from SQL query.
///
/// Library helper, kept for API compatibility: it is not quote-aware (a
/// hint-looking text inside a literal is removed too) and the daemon does
/// not use it.
pub fn strip_hints(sql: &str) -> String {
    let result = HINT_PATTERN.replace_all(sql, "");
    let result = HINT_PATTERN_DOUBLE_DASH.replace_all(&result, "");
    result.trim().to_string()
}

/// Check if a query is cacheable (SELECT, VALUES, etc.)
pub fn is_cacheable_query(sql: &str) -> bool {
    let trimmed = sql.trim().to_uppercase();

    // Only cache read operations
    if trimmed.starts_with("SELECT")
        || trimmed.starts_with("VALUES")
        || trimmed.starts_with("TABLE")
        || trimmed.starts_with("WITH") && trimmed.contains("SELECT")
    {
        // Exclude queries with side effects
        !trimmed.contains("FOR UPDATE")
            && !trimmed.contains("FOR SHARE")
            && !trimmed.contains("FOR NO KEY UPDATE")
            && !trimmed.contains("FOR KEY SHARE")
            && !trimmed.contains("NOWAIT")
            && !trimmed.contains("SKIP LOCKED")
    } else {
        false
    }
}

/// Check if SQL is a write operation (for cache invalidation)
pub fn is_write_operation(sql: &str) -> bool {
    let trimmed = sql.trim().to_uppercase();

    trimmed.starts_with("INSERT")
        || trimmed.starts_with("UPDATE")
        || trimmed.starts_with("DELETE")
        || trimmed.starts_with("TRUNCATE")
        || trimmed.starts_with("DROP")
        || trimmed.starts_with("ALTER")
        || trimmed.starts_with("CREATE")
        || trimmed.starts_with("MERGE")
        || trimmed.starts_with("UPSERT")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_skip_hint() {
        let sql = "/* helios:cache=skip */ SELECT * FROM users";
        let hint = parse_cache_hints(sql);
        assert!(hint.skip);
        assert!(!hint.semantic_cache);
    }

    #[test]
    fn test_parse_ttl_hint() {
        let sql = "/* helios:cache_ttl=300 */ SELECT * FROM users";
        let hint = parse_cache_hints(sql);
        assert_eq!(hint.ttl, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_parse_ttl_with_unit() {
        let sql = "/* helios:ttl=5m */ SELECT * FROM users";
        let hint = parse_cache_hints(sql);
        assert_eq!(hint.ttl, Some(Duration::from_secs(300)));

        let sql2 = "/* helios:ttl=1h */ SELECT * FROM users";
        let hint2 = parse_cache_hints(sql2);
        assert_eq!(hint2.ttl, Some(Duration::from_secs(3600)));
    }

    #[test]
    fn test_parse_semantic_hint() {
        let sql = "/* helios:cache=semantic */ SELECT * FROM documents WHERE topic = 'AI'";
        let hint = parse_cache_hints(sql);
        assert!(hint.semantic_cache);
    }

    #[test]
    fn test_parse_tables_hint() {
        let sql = "/* helios:cache_tables=users,sessions */ SELECT u.* FROM users u JOIN sessions s ON u.id = s.user_id";
        let hint = parse_cache_hints(sql);
        assert_eq!(
            hint.tables,
            Some(vec!["users".to_string(), "sessions".to_string()])
        );
    }

    #[test]
    fn test_parse_refresh_hint() {
        let sql = "/* helios:cache_refresh */ SELECT * FROM users";
        let hint = parse_cache_hints(sql);
        assert!(hint.refresh);
    }

    #[test]
    fn test_parse_multiple_hints() {
        let sql = "/* helios:cache_ttl=60 */ /* helios:cache=semantic */ SELECT * FROM docs";
        let hint = parse_cache_hints(sql);
        assert_eq!(hint.ttl, Some(Duration::from_secs(60)));
        assert!(hint.semantic_cache);
    }

    #[test]
    fn test_parse_double_dash_hint() {
        let sql = "-- helios:cache=skip\nSELECT * FROM users";
        let hint = parse_cache_hints(sql);
        assert!(hint.skip);
    }

    #[test]
    fn test_strip_hints() {
        let sql = "/* helios:cache=skip */ SELECT * FROM users";
        let stripped = strip_hints(sql);
        assert_eq!(stripped, "SELECT * FROM users");

        let sql2 = "-- helios:ttl=60\nSELECT * FROM users";
        let stripped2 = strip_hints(sql2);
        assert_eq!(stripped2, "SELECT * FROM users");
    }

    #[test]
    fn test_is_cacheable_query() {
        assert!(is_cacheable_query("SELECT * FROM users"));
        assert!(is_cacheable_query("  select id from users  "));
        assert!(is_cacheable_query(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ));
        assert!(is_cacheable_query("VALUES (1, 2), (3, 4)"));
        assert!(is_cacheable_query("TABLE users"));

        // Not cacheable
        assert!(!is_cacheable_query("INSERT INTO users VALUES (1)"));
        assert!(!is_cacheable_query("UPDATE users SET name = 'test'"));
        assert!(!is_cacheable_query("DELETE FROM users"));
        assert!(!is_cacheable_query("SELECT * FROM users FOR UPDATE"));
        assert!(!is_cacheable_query("SELECT * FROM users FOR SHARE"));
    }

    #[test]
    fn test_is_write_operation() {
        assert!(is_write_operation("INSERT INTO users VALUES (1)"));
        assert!(is_write_operation("UPDATE users SET name = 'test'"));
        assert!(is_write_operation("DELETE FROM users"));
        assert!(is_write_operation("TRUNCATE users"));
        assert!(is_write_operation("DROP TABLE users"));
        assert!(is_write_operation("ALTER TABLE users ADD COLUMN age INT"));
        assert!(is_write_operation("CREATE TABLE test (id INT)"));

        // Not write operations
        assert!(!is_write_operation("SELECT * FROM users"));
        assert!(!is_write_operation("EXPLAIN SELECT * FROM users"));
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("60"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration("60s"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("1d"), Some(Duration::from_secs(86400)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("invalid"), None);
    }

    #[test]
    fn comma_separated_hints_in_one_comment() {
        let hint = parse_cache_hints(
            "SELECT * FROM t /*helios:route=standby,cache=semantic,cache_ttl=60*/",
        );
        assert!(hint.semantic_cache);
        assert_eq!(hint.ttl, Some(Duration::from_secs(60)));
        assert!(!hint.skip);
        let hint = parse_cache_hints("SELECT * FROM t -- helios:cache=skip, ttl=5m");
        assert!(hint.skip);
        assert_eq!(hint.ttl, Some(Duration::from_secs(300)));
        // A value ends at whitespace; the rest of a line comment is prose.
        let hint = parse_cache_hints("-- helios:cache=skip because reasons\nSELECT 1");
        assert!(hint.skip);
        // Lite's forms keep working.
        let hint =
            parse_cache_hints("/* helios:ttl=60 */ /* helios:cache=semantic */ SELECT * FROM docs");
        assert_eq!(hint.ttl, Some(Duration::from_secs(60)));
        assert!(hint.semantic_cache);
        assert_eq!(
            parse_cache_hints("/* helios:cache_tables=users,orders */ SELECT 1").tables,
            Some(vec!["users".to_string(), "orders".to_string()])
        );
    }

    #[test]
    fn flag_words_after_a_tables_list_stay_flags() {
        let hint = parse_cache_hints("SELECT 1 /*helios:cache_tables=a,b,semantic*/");
        assert_eq!(hint.tables, Some(vec!["a".to_string(), "b".to_string()]));
        assert!(hint.semantic_cache);
        let hint = parse_cache_hints("SELECT 1 /*helios:tables=a, refresh, c*/");
        assert_eq!(hint.tables, Some(vec!["a".to_string()]));
        assert!(hint.refresh);
    }

    /// `Duration::from_secs_f64` used to panic on these (and kill the
    /// connection's task): the `u64` parse fails, the float overflows.
    #[test]
    fn huge_ttl_values_never_panic() {
        for sql in [
            "SELECT 1 /*helios:cache_ttl=99999999999999999999*/",
            "SELECT 1 /*helios:ttl=999999999999999999d*/",
            "SELECT 1 -- helios:ttl=1e400",
        ] {
            assert_eq!(parse_cache_hints(sql).ttl, None, "{sql}");
        }
        assert_eq!(parse_duration("99999999999999999999"), None);
        assert_eq!(parse_duration("1.5m"), Some(Duration::from_secs(90)));
    }

    /// A zero TTL means "always fresh": a skip, so the read neither misses
    /// every tier nor replaces the shared entry with one never served.
    #[test]
    fn a_zero_ttl_is_a_skip() {
        for sql in [
            "SELECT 1 /*helios:cache_ttl=0*/",
            "SELECT 1 /*helios:ttl=0ms*/",
            "SELECT 1 /*helios:cache=semantic,cache_ttl=0s*/",
        ] {
            let hint = parse_cache_hints(sql);
            assert!(hint.skip, "{sql}");
            assert_eq!(hint.ttl, Some(Duration::ZERO), "{sql}");
        }
        assert!(!parse_cache_hints("SELECT 1 /*helios:cache_ttl=1*/").skip);
        assert!(!parse_cache_hints("SELECT 1 /*helios:ttl=500ms*/").skip);
    }

    #[test]
    fn hints_inside_string_literals_are_ignored() {
        let hint = parse_cache_hints(
            "SELECT * FROM t WHERE t = 'x /*helios:cache=semantic,cache_ttl=86400*/'",
        );
        assert!(!hint.semantic_cache && hint.ttl.is_none() && !hint.skip);
        let hint = parse_cache_hints("SELECT \"-- helios:cache=skip\" FROM t");
        assert!(!hint.skip);
        assert!(!parse_cache_hints("SELECT 'helios:cache=skip' FROM t").skip);
    }

    #[test]
    fn merge_unions_flags_and_keeps_first_values() {
        let mut forwarded = parse_cache_hints("SELECT 1 /*helios:cache_ttl=30*/");
        forwarded.merge(parse_cache_hints(
            "/*helios:cache=skip,cache_ttl=90,cache=l2*/ SELECT 1 /*helios:cache=semantic*/",
        ));
        assert!(forwarded.skip && forwarded.semantic_cache && !forwarded.refresh);
        assert_eq!(forwarded.ttl, Some(Duration::from_secs(30)));
        assert_eq!(forwarded.level, Some(CacheLevelHint::L2Only));
        let mut empty = CacheHint::default();
        empty.merge(parse_cache_hints(
            "SELECT 1 /*helios:ttl=5,cache_tables=a*/",
        ));
        assert_eq!(empty.ttl, Some(Duration::from_secs(5)));
        assert_eq!(empty.tables, Some(vec!["a".to_string()]));
    }

    /// The routing parser reads `cache=bypass` and `cache=none` as a skip
    /// and, with `[routing_hints]` on, strips the comment; the cache reads
    /// the client's text back and used to ignore both words, so a stale
    /// cached row answered a read that asked to bypass the cache.
    #[test]
    fn routing_skip_words_are_skips() {
        for v in [
            "skip", "bypass", "none", "BYPASS", "None", "no", "off", "false", "disable",
        ] {
            assert!(
                parse_cache_hints(&format!("SELECT 1 /*helios:cache={v}*/")).skip,
                "{v}"
            );
        }
        let mut forwarded = parse_cache_hints("SELECT v FROM t");
        forwarded.merge(parse_cache_hints("SELECT v FROM t /*helios:cache=bypass*/"));
        assert!(forwarded.skip);
    }

    /// Every `cache=` value the routing parser reads as a skip (and strips)
    /// is a skip here too.
    #[cfg(feature = "routing-hints")]
    #[test]
    fn both_parsers_agree_on_skips() {
        use crate::routing::hint_parser::CacheBehavior;
        use std::str::FromStr;
        for v in [
            "skip", "bypass", "none", "normal", "default", "refresh", "force", "update",
            "semantic", "l3", "vector", "l1", "hot", "l2", "warm", "Bypass", "NONE",
        ] {
            let routing = CacheBehavior::from_str(v).ok() == Some(CacheBehavior::Skip);
            let cache = parse_cache_hints(&format!("SELECT 1 /*helios:cache={v}*/")).skip;
            assert!(
                !routing || cache,
                "{v}: the routing parser skips, the cache does not"
            );
        }
    }

    #[test]
    fn test_cache_level_hints() {
        let sql = "/* helios:cache=l1 */ SELECT * FROM users";
        let hint = parse_cache_hints(sql);
        assert_eq!(hint.level, Some(CacheLevelHint::L1Only));

        let sql2 = "/* helios:cache=l2 */ SELECT * FROM users";
        let hint2 = parse_cache_hints(sql2);
        assert_eq!(hint2.level, Some(CacheLevelHint::L2Only));

        let sql3 = "/* helios:cache=l3 */ SELECT * FROM users";
        let hint3 = parse_cache_hints(sql3);
        assert!(hint3.semantic_cache);
    }
}
