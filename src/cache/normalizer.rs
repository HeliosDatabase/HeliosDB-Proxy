//! Query Normalizer
//!
//! Normalizes SQL queries for cache key generation by:
//! - Replacing literal values with placeholders
//! - Normalizing whitespace
//! - Extracting table names
//! - Computing stable hashes

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use regex::Regex;
use once_cell::sync::Lazy;

/// Normalized query representation
#[derive(Debug, Clone)]
pub struct NormalizedQuery {
    /// Normalized query fingerprint (literals replaced with ?)
    pub fingerprint: String,

    /// Hash of the fingerprint for fast comparison
    pub hash: u64,

    /// Tables referenced in the query
    pub tables: Vec<String>,

    /// Extracted parameter values
    pub parameters: Vec<String>,
}

impl NormalizedQuery {
    /// Get the fingerprint for display
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// Get the hash value
    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Get referenced tables
    pub fn tables(&self) -> &[String] {
        &self.tables
    }
}

/// Query normalizer for cache key generation
#[derive(Debug, Clone)]
pub struct QueryNormalizer {
    /// Whether to preserve parameter order
    preserve_order: bool,
}

// Regex patterns for normalization
static STRING_LITERAL: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"'(?:[^'\\]|\\.)*'"#).unwrap()
});

#[allow(dead_code)]
static DOUBLE_QUOTED: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#""(?:[^"\\]|\\.)*""#).unwrap()
});

static NUMBER_LITERAL: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b\d+(?:\.\d+)?(?:e[+-]?\d+)?\b").unwrap()
});

static WHITESPACE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\s+").unwrap()
});

static TABLE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)").unwrap()
});

static HINT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"/\*[^*]*\*/").unwrap()
});

static COMMENT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"--[^\n]*").unwrap()
});

impl QueryNormalizer {
    /// Create a new query normalizer
    pub fn new() -> Self {
        Self {
            preserve_order: true,
        }
    }

    /// Create a normalizer that doesn't preserve parameter order
    pub fn unordered() -> Self {
        Self {
            preserve_order: false,
        }
    }

    /// Normalize a SQL query
    pub fn normalize(&self, sql: &str) -> NormalizedQuery {
        let mut parameters = Vec::new();

        // Strip comments and hints first
        let sql = HINT_PATTERN.replace_all(sql, "");
        let sql = COMMENT_PATTERN.replace_all(&sql, "");

        // Extract tables before normalization
        let tables = self.extract_tables(&sql);

        // Replace string literals with placeholders
        let sql = STRING_LITERAL.replace_all(&sql, |caps: &regex::Captures| {
            let value = caps.get(0).unwrap().as_str();
            // Remove quotes and store the value
            let inner = &value[1..value.len()-1];
            parameters.push(inner.to_string());
            "?"
        });

        // Replace number literals with placeholders
        let sql = NUMBER_LITERAL.replace_all(&sql, |caps: &regex::Captures| {
            let value = caps.get(0).unwrap().as_str();
            parameters.push(value.to_string());
            "?"
        });

        // Normalize whitespace
        let sql = WHITESPACE.replace_all(&sql, " ");

        // Trim and convert to uppercase for consistency
        let fingerprint = sql.trim().to_uppercase();

        // Compute hash
        let mut hasher = DefaultHasher::new();
        fingerprint.hash(&mut hasher);
        if self.preserve_order {
            // Include parameters in hash if order matters
            for param in &parameters {
                param.hash(&mut hasher);
            }
        }
        let hash = hasher.finish();

        NormalizedQuery {
            fingerprint,
            hash,
            tables,
            parameters,
        }
    }

    /// Extract table names from a SQL query
    fn extract_tables(&self, sql: &str) -> Vec<String> {
        let mut tables = Vec::new();

        for cap in TABLE_PATTERN.captures_iter(sql) {
            if let Some(table_match) = cap.get(1) {
                let table = table_match.as_str().to_lowercase();
                // Remove schema prefix if present
                let table_name = table.split('.').next_back().unwrap_or(&table);
                if !tables.contains(&table_name.to_string()) {
                    tables.push(table_name.to_string());
                }
            }
        }

        tables
    }

    /// Normalize for comparison only (no parameter extraction)
    pub fn fingerprint(&self, sql: &str) -> String {
        // Strip comments
        let sql = HINT_PATTERN.replace_all(sql, "");
        let sql = COMMENT_PATTERN.replace_all(&sql, "");

        // Replace literals
        let sql = STRING_LITERAL.replace_all(&sql, "?");
        let sql = NUMBER_LITERAL.replace_all(&sql, "?");

        // Normalize whitespace
        let sql = WHITESPACE.replace_all(&sql, " ");

        sql.trim().to_uppercase()
    }

    /// Check if two queries are equivalent (same fingerprint)
    pub fn are_equivalent(&self, sql1: &str, sql2: &str) -> bool {
        self.fingerprint(sql1) == self.fingerprint(sql2)
    }
}

impl Default for QueryNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Quick fingerprint generation without full normalization
pub fn quick_fingerprint(sql: &str) -> u64 {
    let normalized = QueryNormalizer::new().fingerprint(sql);
    let mut hasher = DefaultHasher::new();
    normalized.hash(&mut hasher);
    hasher.finish()
}

/// Extract tables from SQL without full normalization
pub fn extract_tables(sql: &str) -> Vec<String> {
    let normalizer = QueryNormalizer::new();
    normalizer.extract_tables(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_simple_query() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM users WHERE id = 123";
        let normalized = normalizer.normalize(query);

        assert_eq!(normalized.fingerprint, "SELECT * FROM USERS WHERE ID = ?");
        assert_eq!(normalized.parameters, vec!["123"]);
        assert_eq!(normalized.tables, vec!["users"]);
    }

    #[test]
    fn test_normalize_string_literals() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM users WHERE name = 'John Doe'";
        let normalized = normalizer.normalize(query);

        assert_eq!(normalized.fingerprint, "SELECT * FROM USERS WHERE NAME = ?");
        assert_eq!(normalized.parameters, vec!["John Doe"]);
    }

    #[test]
    fn test_normalize_multiple_parameters() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM users WHERE age > 18 AND status = 'active' AND score < 100";
        let normalized = normalizer.normalize(query);

        assert_eq!(normalized.fingerprint, "SELECT * FROM USERS WHERE AGE > ? AND STATUS = ? AND SCORE < ?");
        assert_eq!(normalized.parameters.len(), 3);
    }

    #[test]
    fn test_extract_tables_join() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id";
        let normalized = normalizer.normalize(query);

        assert!(normalized.tables.contains(&"users".to_string()));
        assert!(normalized.tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_normalize_removes_comments() {
        let normalizer = QueryNormalizer::new();
        let query = "/* helios:cache_ttl=60 */ SELECT * FROM users -- inline comment\nWHERE id = 1";
        let normalized = normalizer.normalize(query);

        assert_eq!(normalized.fingerprint, "SELECT * FROM USERS WHERE ID = ?");
    }

    #[test]
    fn test_normalize_whitespace() {
        let normalizer = QueryNormalizer::new();
        let query1 = "SELECT  *  FROM   users   WHERE   id=1";
        let query2 = "SELECT * FROM users WHERE id=1";

        assert_eq!(
            normalizer.fingerprint(query1),
            normalizer.fingerprint(query2)
        );
    }

    #[test]
    fn test_equivalent_queries() {
        let normalizer = QueryNormalizer::new();

        // Same query with different literal values
        let query1 = "SELECT * FROM users WHERE id = 123";
        let query2 = "SELECT * FROM users WHERE id = 456";

        assert!(normalizer.are_equivalent(query1, query2));

        // Different query structure
        let query3 = "SELECT * FROM users WHERE name = 'test'";
        assert!(!normalizer.are_equivalent(query1, query3));
    }

    #[test]
    fn test_hash_consistency() {
        let normalizer = QueryNormalizer::new();

        let query1 = "SELECT * FROM users WHERE id = 1";
        let query2 = "SELECT * FROM users WHERE id = 1";

        let norm1 = normalizer.normalize(query1);
        let norm2 = normalizer.normalize(query2);

        assert_eq!(norm1.hash, norm2.hash);
    }

    #[test]
    fn test_hash_different_params() {
        let normalizer = QueryNormalizer::new();

        // With preserve_order=true, different params should have different hashes
        let query1 = "SELECT * FROM users WHERE id = 1";
        let query2 = "SELECT * FROM users WHERE id = 2";

        let norm1 = normalizer.normalize(query1);
        let norm2 = normalizer.normalize(query2);

        assert_ne!(norm1.hash, norm2.hash);
    }

    #[test]
    fn test_unordered_normalizer() {
        let normalizer = QueryNormalizer::unordered();

        // With preserve_order=false, different params should have same hash
        let query1 = "SELECT * FROM users WHERE id = 1";
        let query2 = "SELECT * FROM users WHERE id = 2";

        let norm1 = normalizer.normalize(query1);
        let norm2 = normalizer.normalize(query2);

        // Fingerprints are the same
        assert_eq!(norm1.fingerprint, norm2.fingerprint);
    }

    #[test]
    fn test_extract_tables_various() {
        let normalizer = QueryNormalizer::new();

        let queries = vec![
            ("INSERT INTO users VALUES (1)", vec!["users"]),
            ("UPDATE products SET price = 10", vec!["products"]),
            ("DELETE FROM orders WHERE id = 1", vec!["orders"]),
            ("SELECT * FROM schema.table", vec!["table"]),
            ("TABLE users", vec!["users"]),
        ];

        for (sql, expected_tables) in queries {
            let normalized = normalizer.normalize(sql);
            for table in expected_tables {
                assert!(
                    normalized.tables.contains(&table.to_string()),
                    "Query '{}' should contain table '{}'",
                    sql,
                    table
                );
            }
        }
    }

    #[test]
    fn test_decimal_numbers() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM products WHERE price < 99.99 AND rating > 4.5";
        let normalized = normalizer.normalize(query);

        assert!(normalized.parameters.contains(&"99.99".to_string()));
        assert!(normalized.parameters.contains(&"4.5".to_string()));
    }

    #[test]
    fn test_scientific_notation() {
        let normalizer = QueryNormalizer::new();
        let query = "SELECT * FROM data WHERE value = 1e10";
        let normalized = normalizer.normalize(query);

        assert!(normalized.fingerprint.contains("VALUE = ?"));
    }

    #[test]
    fn test_quick_fingerprint() {
        let hash1 = quick_fingerprint("SELECT * FROM users WHERE id = 1");
        let hash2 = quick_fingerprint("SELECT * FROM users WHERE id = 2");

        // Quick fingerprint ignores parameter values
        assert_eq!(hash1, hash2);
    }
}
