//! Query Fingerprinting
//!
//! Normalize queries and generate fingerprints for grouping similar queries.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use regex::Regex;

/// Query fingerprinter
#[derive(Debug)]
pub struct QueryFingerprinter {
    /// Regex for string literals
    string_literal_re: Regex,
    /// Regex for numeric literals
    numeric_literal_re: Regex,
    /// Regex for IN lists
    in_list_re: Regex,
    /// Regex for whitespace
    whitespace_re: Regex,
    /// Regex for UUID
    uuid_re: Regex,
    /// Regex for hex values
    hex_re: Regex,
}

impl QueryFingerprinter {
    /// Create a new fingerprinter
    pub fn new() -> Self {
        Self {
            string_literal_re: Regex::new(r"'[^']*'").expect("Invalid regex"),
            numeric_literal_re: Regex::new(r"\b\d+(\.\d+)?\b").expect("Invalid regex"),
            in_list_re: Regex::new(r"(?i)IN\s*\([^)]+\)").expect("Invalid regex"),
            whitespace_re: Regex::new(r"\s+").expect("Invalid regex"),
            uuid_re: Regex::new(
                r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
            )
            .expect("Invalid regex"),
            hex_re: Regex::new(r"0x[0-9a-fA-F]+").expect("Invalid regex"),
        }
    }

    /// Generate fingerprint from query
    pub fn fingerprint(&self, query: &str) -> QueryFingerprint {
        let normalized = self.normalize(query);
        let hash = self.compute_hash(&normalized);

        QueryFingerprint {
            hash,
            normalized: normalized.clone(),
            tables: self.extract_tables(query),
            operation: self.detect_operation(query),
            original_length: query.len(),
        }
    }

    /// Normalize query (remove literals, standardize whitespace)
    pub fn normalize(&self, query: &str) -> String {
        let mut normalized = query.to_string();

        // Replace UUIDs first
        normalized = self.uuid_re.replace_all(&normalized, "?").to_string();

        // Replace hex values
        normalized = self.hex_re.replace_all(&normalized, "?").to_string();

        // Replace string literals with ?
        normalized = self
            .string_literal_re
            .replace_all(&normalized, "?")
            .to_string();

        // Replace numeric literals with ?
        normalized = self
            .numeric_literal_re
            .replace_all(&normalized, "?")
            .to_string();

        // Replace IN lists with (?)
        normalized = self
            .in_list_re
            .replace_all(&normalized, "IN (?)")
            .to_string();

        // Normalize whitespace
        normalized = self.whitespace_re.replace_all(&normalized, " ").to_string();

        normalized.trim().to_lowercase()
    }

    /// Compute hash of normalized query
    fn compute_hash(&self, normalized: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        hasher.finish()
    }

    /// Extract table names from query
    fn extract_tables(&self, query: &str) -> Vec<String> {
        let query_upper = query.to_uppercase();
        let mut tables = HashSet::new();

        // FROM clause
        if let Some(from_pos) = query_upper.find("FROM") {
            let after_from = &query[from_pos + 4..];
            if let Some(table) = self.extract_first_identifier(after_from) {
                tables.insert(table);
            }
        }

        // JOIN clauses
        for keyword in [
            "JOIN",
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "OUTER JOIN",
        ] {
            let mut search_pos = 0;
            while let Some(pos) = query_upper[search_pos..].find(keyword) {
                let absolute_pos = search_pos + pos + keyword.len();
                if absolute_pos < query.len() {
                    let after_join = &query[absolute_pos..];
                    if let Some(table) = self.extract_first_identifier(after_join) {
                        tables.insert(table);
                    }
                }
                search_pos = absolute_pos;
            }
        }

        // INSERT INTO
        if let Some(pos) = query_upper.find("INSERT INTO") {
            let after_insert = &query[pos + 11..];
            if let Some(table) = self.extract_first_identifier(after_insert) {
                tables.insert(table);
            }
        }

        // UPDATE
        if let Some(pos) = query_upper.find("UPDATE") {
            let after_update = &query[pos + 6..];
            if let Some(table) = self.extract_first_identifier(after_update) {
                tables.insert(table);
            }
        }

        // DELETE FROM
        if let Some(pos) = query_upper.find("DELETE FROM") {
            let after_delete = &query[pos + 11..];
            if let Some(table) = self.extract_first_identifier(after_delete) {
                tables.insert(table);
            }
        }

        tables.into_iter().collect()
    }

    /// Extract first identifier from string
    fn extract_first_identifier(&self, s: &str) -> Option<String> {
        let trimmed = s.trim();
        let mut chars = trimmed.chars().peekable();

        // Skip leading whitespace
        while chars.peek().map(|c| c.is_whitespace()).unwrap_or(false) {
            chars.next();
        }

        // Collect identifier characters
        let mut ident = String::new();
        while let Some(&c) = chars.peek() {
            if c.is_alphanumeric() || c == '_' || c == '.' || c == '"' {
                ident.push(c);
                chars.next();
            } else {
                break;
            }
        }

        if ident.is_empty() {
            None
        } else {
            // Remove quotes and return lowercase
            let cleaned = ident.replace('"', "").to_lowercase();
            Some(cleaned)
        }
    }

    /// Detect operation type
    #[allow(clippy::if_same_then_else)]
    fn detect_operation(&self, query: &str) -> OperationType {
        let trimmed = query.trim().to_uppercase();

        if trimmed.starts_with("SELECT") {
            OperationType::Select
        } else if trimmed.starts_with("INSERT") {
            OperationType::Insert
        } else if trimmed.starts_with("UPDATE") {
            OperationType::Update
        } else if trimmed.starts_with("DELETE") {
            OperationType::Delete
        } else if trimmed.starts_with("CREATE") {
            OperationType::Ddl
        } else if trimmed.starts_with("ALTER") {
            OperationType::Ddl
        } else if trimmed.starts_with("DROP") {
            OperationType::Ddl
        } else if trimmed.starts_with("BEGIN") || trimmed.starts_with("START TRANSACTION") {
            OperationType::Transaction
        } else if trimmed.starts_with("COMMIT") || trimmed.starts_with("ROLLBACK") {
            OperationType::Transaction
        } else if trimmed.starts_with("SET") {
            OperationType::Utility
        } else if trimmed.starts_with("EXPLAIN") {
            OperationType::Utility
        } else if trimmed.starts_with("ANALYZE") {
            OperationType::Utility
        } else {
            OperationType::Other
        }
    }
}

impl Default for QueryFingerprinter {
    fn default() -> Self {
        Self::new()
    }
}

/// Query fingerprint
#[derive(Debug, Clone)]
pub struct QueryFingerprint {
    /// 64-bit hash of normalized query
    pub hash: u64,

    /// Normalized query text
    pub normalized: String,

    /// Tables involved
    pub tables: Vec<String>,

    /// Operation type
    pub operation: OperationType,

    /// Original query length
    pub original_length: usize,
}

impl QueryFingerprint {
    /// Get a short identifier for this fingerprint
    pub fn short_id(&self) -> String {
        format!("{:016x}", self.hash)
    }
}

/// Operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    Select,
    Insert,
    Update,
    Delete,
    Ddl,
    Transaction,
    Utility,
    Other,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Select => write!(f, "SELECT"),
            OperationType::Insert => write!(f, "INSERT"),
            OperationType::Update => write!(f, "UPDATE"),
            OperationType::Delete => write!(f, "DELETE"),
            OperationType::Ddl => write!(f, "DDL"),
            OperationType::Transaction => write!(f, "TRANSACTION"),
            OperationType::Utility => write!(f, "UTILITY"),
            OperationType::Other => write!(f, "OTHER"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprinter_new() {
        let fp = QueryFingerprinter::new();
        assert!(fp.string_literal_re.is_match("'hello'"));
    }

    #[test]
    fn test_normalize_string_literals() {
        let fp = QueryFingerprinter::new();

        let normalized = fp.normalize("SELECT * FROM users WHERE name = 'Alice'");
        assert_eq!(normalized, "select * from users where name = ?");
    }

    #[test]
    fn test_normalize_numeric_literals() {
        let fp = QueryFingerprinter::new();

        let normalized = fp.normalize("SELECT * FROM users WHERE id = 123 AND age > 25");
        assert_eq!(normalized, "select * from users where id = ? and age > ?");
    }

    #[test]
    fn test_normalize_in_list() {
        let fp = QueryFingerprinter::new();

        let normalized = fp.normalize("SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)");
        assert_eq!(normalized, "select * from users where id in (?)");
    }

    #[test]
    fn test_normalize_uuid() {
        let fp = QueryFingerprinter::new();

        let normalized =
            fp.normalize("SELECT * FROM users WHERE id = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'");
        assert!(normalized.contains("?"));
    }

    #[test]
    fn test_same_fingerprint_different_values() {
        let fp = QueryFingerprinter::new();

        let fp1 = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        let fp2 = fp.fingerprint("SELECT * FROM users WHERE id = 2");

        assert_eq!(fp1.hash, fp2.hash);
        assert_eq!(fp1.normalized, fp2.normalized);
    }

    #[test]
    fn test_different_fingerprint_different_queries() {
        let fp = QueryFingerprinter::new();

        let fp1 = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        let fp2 = fp.fingerprint("SELECT * FROM orders WHERE id = 1");

        assert_ne!(fp1.hash, fp2.hash);
    }

    #[test]
    fn test_extract_tables() {
        let fp = QueryFingerprinter::new();

        let result = fp.fingerprint("SELECT * FROM users WHERE id = 1");
        assert!(result.tables.contains(&"users".to_string()));

        let result = fp.fingerprint("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
        assert!(result.tables.contains(&"users".to_string()));
        assert!(result.tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_detect_operation() {
        let fp = QueryFingerprinter::new();

        assert_eq!(
            fp.detect_operation("SELECT * FROM users"),
            OperationType::Select
        );
        assert_eq!(
            fp.detect_operation("INSERT INTO users VALUES (1)"),
            OperationType::Insert
        );
        assert_eq!(
            fp.detect_operation("UPDATE users SET name = 'Bob'"),
            OperationType::Update
        );
        assert_eq!(
            fp.detect_operation("DELETE FROM users WHERE id = 1"),
            OperationType::Delete
        );
        assert_eq!(
            fp.detect_operation("CREATE TABLE foo (id INT)"),
            OperationType::Ddl
        );
        assert_eq!(fp.detect_operation("BEGIN"), OperationType::Transaction);
    }

    #[test]
    fn test_operation_display() {
        assert_eq!(OperationType::Select.to_string(), "SELECT");
        assert_eq!(OperationType::Insert.to_string(), "INSERT");
    }
}
