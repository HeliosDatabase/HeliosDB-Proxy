//! Cache Hints Parser
//!
//! Parses SQL comments to extract cache control hints.
//!
//! # Supported Hints
//!
//! ```sql
//! /* helios:cache=skip */          -- Skip caching entirely
//! /* helios:cache_ttl=60 */        -- Override TTL (seconds)
//! /* helios:cache=semantic */      -- Enable semantic caching
//! /* helios:cache_tables=a,b */    -- Override table dependencies
//! /* helios:cache_refresh */       -- Force cache refresh
//! ```

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

// Regex patterns for hint parsing
static HINT_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"/\*\s*helios:(\w+)(?:=([^*]+))?\s*\*/").unwrap());

static HINT_PATTERN_DOUBLE_DASH: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"--\s*helios:(\w+)(?:=(\S+))?").unwrap());

/// Parse cache hints from a SQL query
pub fn parse_cache_hints(sql: &str) -> CacheHint {
    let mut hint = CacheHint::default();

    // Parse /* helios:key=value */ style hints
    for cap in HINT_PATTERN.captures_iter(sql) {
        let key = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let value = cap.get(2).map(|m| m.as_str().trim());

        apply_hint(&mut hint, key, value);
    }

    // Parse -- helios:key=value style hints
    for cap in HINT_PATTERN_DOUBLE_DASH.captures_iter(sql) {
        let key = cap.get(1).map(|m| m.as_str()).unwrap_or("");
        let value = cap.get(2).map(|m| m.as_str().trim());

        apply_hint(&mut hint, key, value);
    }

    hint
}

/// Apply a single hint to the CacheHint struct
fn apply_hint(hint: &mut CacheHint, key: &str, value: Option<&str>) {
    match key.to_lowercase().as_str() {
        "cache" => {
            if let Some(v) = value {
                match v.to_lowercase().as_str() {
                    "skip" | "no" | "off" | "false" | "disable" => {
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

/// Parse duration strings like "5m", "1h", "30s"
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

    Some(Duration::from_secs_f64(num * multiplier))
}

/// Strip cache hints from SQL query
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
