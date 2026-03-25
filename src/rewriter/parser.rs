//! SQL Parser
//!
//! SQL parsing utilities for query rewriting.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// SQL parser
pub struct SqlParser {
    /// Dialect-specific settings
    dialect: SqlDialect,
}

/// SQL dialect
#[derive(Debug, Clone, Copy, Default)]
pub enum SqlDialect {
    #[default]
    PostgreSQL,
    MySQL,
    SQLite,
}

impl SqlParser {
    /// Create a new parser
    pub fn new() -> Self {
        Self {
            dialect: SqlDialect::PostgreSQL,
        }
    }

    /// Create a parser with specific dialect
    pub fn with_dialect(dialect: SqlDialect) -> Self {
        Self { dialect }
    }

    /// Parse a SQL query
    pub fn parse(&self, sql: &str) -> Result<ParsedQuery, ParseError> {
        let trimmed = sql.trim();

        if trimmed.is_empty() {
            return Err(ParseError::EmptyQuery);
        }

        let upper = trimmed.to_uppercase();
        let first_word = upper.split_whitespace().next().unwrap_or("");

        let is_select = first_word == "SELECT";
        let is_insert = first_word == "INSERT";
        let is_update = first_word == "UPDATE";
        let is_delete = first_word == "DELETE";
        let is_ddl = matches!(first_word, "CREATE" | "ALTER" | "DROP" | "TRUNCATE");

        let tables = self.extract_tables(trimmed);
        let has_select_star = is_select && self.has_select_star(trimmed);
        let has_limit = upper.contains(" LIMIT ");
        let has_where = upper.contains(" WHERE ");

        let normalized = self.normalize(trimmed);

        Ok(ParsedQuery {
            original: trimmed.to_string(),
            normalized,
            tables,
            has_select_star,
            has_limit,
            has_where,
            is_select,
            is_insert,
            is_update,
            is_delete,
            is_ddl,
        })
    }

    /// Normalize a query (replace literals with placeholders)
    pub fn normalize(&self, sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut chars = sql.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                // String literals
                '\'' => {
                    result.push('?');
                    let mut escaped = false;
                    for inner in chars.by_ref() {
                        if inner == '\'' && !escaped {
                            break;
                        }
                        escaped = inner == '\\' && !escaped;
                    }
                }
                // Double-quoted identifiers (keep them)
                '"' => {
                    result.push(c);
                    for inner in chars.by_ref() {
                        result.push(inner);
                        if inner == '"' {
                            break;
                        }
                    }
                }
                // Numbers
                '0'..='9' => {
                    result.push('?');
                    while chars.peek().map(|c| c.is_ascii_digit() || *c == '.').unwrap_or(false) {
                        chars.next();
                    }
                }
                // Parameter placeholders
                '$' => {
                    result.push('?');
                    while chars.peek().map(|c| c.is_ascii_digit()).unwrap_or(false) {
                        chars.next();
                    }
                }
                // Everything else
                _ => result.push(c),
            }
        }

        // Collapse whitespace
        let mut prev_space = false;
        result.chars().filter(|&c| {
            if c.is_whitespace() {
                if prev_space {
                    return false;
                }
                prev_space = true;
            } else {
                prev_space = false;
            }
            true
        }).collect::<String>().trim().to_string()
    }

    /// Extract table names from query
    fn extract_tables(&self, sql: &str) -> Vec<String> {
        let mut tables = Vec::new();
        let upper = sql.to_uppercase();
        let words: Vec<&str> = sql.split_whitespace().collect();
        let upper_words: Vec<&str> = upper.split_whitespace().collect();

        // Look for FROM, JOIN, INTO, UPDATE table names
        let table_keywords = ["FROM", "JOIN", "INTO", "UPDATE"];

        for (i, word) in upper_words.iter().enumerate() {
            if table_keywords.contains(&word.trim_end_matches(',')) {
                if let Some(table) = words.get(i + 1) {
                    let table = table.trim_matches(|c| c == ',' || c == '(' || c == ')' || c == ';');
                    if !table.is_empty() && !is_keyword(table) {
                        // Handle schema.table format
                        let table_name = table.split('.').last().unwrap_or(table);
                        tables.push(table_name.to_string());
                    }
                }
            }
        }

        // Deduplicate
        tables.sort();
        tables.dedup();
        tables
    }

    /// Check if query has SELECT *
    fn has_select_star(&self, sql: &str) -> bool {
        let upper = sql.to_uppercase();

        // Check for SELECT * (with potential whitespace variations)
        if let Some(select_pos) = upper.find("SELECT") {
            let after_select = &upper[select_pos + 6..];
            let trimmed = after_select.trim_start();

            // Check for SELECT * or SELECT DISTINCT *
            if trimmed.starts_with("*") {
                return true;
            }
            if trimmed.starts_with("DISTINCT") {
                let after_distinct = trimmed[8..].trim_start();
                if after_distinct.starts_with("*") {
                    return true;
                }
            }
            if trimmed.starts_with("ALL") {
                let after_all = trimmed[3..].trim_start();
                if after_all.starts_with("*") {
                    return true;
                }
            }
        }

        false
    }

    /// Convert AST back to SQL
    pub fn to_sql(&self, parsed: &ParsedQuery) -> String {
        // For now, return the normalized version
        // In production, use sqlparser-rs for full AST manipulation
        parsed.original.clone()
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parsed query representation
#[derive(Debug, Clone)]
pub struct ParsedQuery {
    /// Original query
    pub original: String,

    /// Normalized query (literals replaced)
    pub normalized: String,

    /// Tables referenced
    pub tables: Vec<String>,

    /// Has SELECT *
    pub has_select_star: bool,

    /// Has LIMIT clause
    pub has_limit: bool,

    /// Has WHERE clause
    pub has_where: bool,

    /// Is SELECT statement
    pub is_select: bool,

    /// Is INSERT statement
    pub is_insert: bool,

    /// Is UPDATE statement
    pub is_update: bool,

    /// Is DELETE statement
    pub is_delete: bool,

    /// Is DDL statement
    pub is_ddl: bool,
}

impl ParsedQuery {
    /// Calculate query fingerprint
    pub fn fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.normalized.to_uppercase().hash(&mut hasher);
        hasher.finish()
    }

    /// Check if query modifies data
    pub fn is_write(&self) -> bool {
        self.is_insert || self.is_update || self.is_delete || self.is_ddl
    }

    /// Check if query is read-only
    pub fn is_read(&self) -> bool {
        self.is_select && !self.is_ddl
    }
}

/// SQL statement type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlStatement {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Alter,
    Drop,
    Truncate,
    Other,
}

impl SqlStatement {
    /// Parse from SQL string
    pub fn from_sql(sql: &str) -> Self {
        let first_word = sql.trim().split_whitespace().next().unwrap_or("");
        match first_word.to_uppercase().as_str() {
            "SELECT" => Self::Select,
            "INSERT" => Self::Insert,
            "UPDATE" => Self::Update,
            "DELETE" => Self::Delete,
            "CREATE" => Self::Create,
            "ALTER" => Self::Alter,
            "DROP" => Self::Drop,
            "TRUNCATE" => Self::Truncate,
            _ => Self::Other,
        }
    }

    /// Check if statement modifies data
    pub fn is_write(&self) -> bool {
        matches!(self, Self::Insert | Self::Update | Self::Delete | Self::Create | Self::Alter | Self::Drop | Self::Truncate)
    }
}

/// Parse error
#[derive(Debug, Clone)]
pub enum ParseError {
    /// Empty query
    EmptyQuery,

    /// Invalid syntax
    InvalidSyntax(String),

    /// Unsupported statement
    UnsupportedStatement(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyQuery => write!(f, "Empty query"),
            Self::InvalidSyntax(msg) => write!(f, "Invalid syntax: {}", msg),
            Self::UnsupportedStatement(stmt) => write!(f, "Unsupported statement: {}", stmt),
        }
    }
}

impl std::error::Error for ParseError {}

impl From<ParseError> for super::RewriteError {
    fn from(e: ParseError) -> Self {
        super::RewriteError::ParseError(e.to_string())
    }
}

/// Check if a word is a SQL keyword
fn is_keyword(word: &str) -> bool {
    let upper = word.to_uppercase();
    matches!(upper.as_str(),
        "SELECT" | "FROM" | "WHERE" | "AND" | "OR" | "NOT" |
        "INSERT" | "INTO" | "VALUES" | "UPDATE" | "SET" | "DELETE" |
        "CREATE" | "ALTER" | "DROP" | "TABLE" | "INDEX" | "VIEW" |
        "JOIN" | "LEFT" | "RIGHT" | "INNER" | "OUTER" | "CROSS" | "ON" |
        "GROUP" | "BY" | "ORDER" | "HAVING" | "LIMIT" | "OFFSET" |
        "UNION" | "INTERSECT" | "EXCEPT" | "AS" | "DISTINCT" | "ALL" |
        "NULL" | "TRUE" | "FALSE" | "CASE" | "WHEN" | "THEN" | "ELSE" | "END" |
        "EXISTS" | "IN" | "BETWEEN" | "LIKE" | "IS" | "ASC" | "DESC"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let parser = SqlParser::new();
        let parsed = parser.parse("SELECT * FROM users WHERE id = 1").unwrap();

        assert!(parsed.is_select);
        assert!(parsed.has_select_star);
        assert!(parsed.has_where);
        assert!(!parsed.has_limit);
        assert!(parsed.tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_parse_insert() {
        let parser = SqlParser::new();
        let parsed = parser.parse("INSERT INTO users (name) VALUES ('test')").unwrap();

        assert!(parsed.is_insert);
        assert!(parsed.tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_normalize() {
        let parser = SqlParser::new();

        let normalized = parser.normalize("SELECT * FROM users WHERE id = 123 AND name = 'test'");
        assert!(normalized.contains("id = ?"));
        assert!(normalized.contains("name = ?"));
    }

    #[test]
    fn test_fingerprint() {
        let parser = SqlParser::new();

        let q1 = parser.parse("SELECT * FROM users WHERE id = 1").unwrap();
        let q2 = parser.parse("SELECT * FROM users WHERE id = 2").unwrap();
        let q3 = parser.parse("SELECT * FROM orders WHERE id = 1").unwrap();

        // Same query structure should have same fingerprint
        assert_eq!(q1.fingerprint(), q2.fingerprint());
        // Different query structure should have different fingerprint
        assert_ne!(q1.fingerprint(), q3.fingerprint());
    }

    #[test]
    fn test_extract_tables() {
        let parser = SqlParser::new();

        let parsed = parser.parse(
            "SELECT u.*, o.total FROM users u JOIN orders o ON u.id = o.user_id"
        ).unwrap();

        assert!(parsed.tables.contains(&"u".to_string()) || parsed.tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_has_select_star() {
        let parser = SqlParser::new();

        assert!(parser.has_select_star("SELECT * FROM users"));
        assert!(parser.has_select_star("SELECT DISTINCT * FROM users"));
        assert!(!parser.has_select_star("SELECT id, name FROM users"));
    }

    #[test]
    fn test_empty_query() {
        let parser = SqlParser::new();
        assert!(matches!(parser.parse(""), Err(ParseError::EmptyQuery)));
        assert!(matches!(parser.parse("   "), Err(ParseError::EmptyQuery)));
    }

    #[test]
    fn test_sql_statement_type() {
        assert_eq!(SqlStatement::from_sql("SELECT * FROM users"), SqlStatement::Select);
        assert_eq!(SqlStatement::from_sql("INSERT INTO users"), SqlStatement::Insert);
        assert_eq!(SqlStatement::from_sql("UPDATE users SET"), SqlStatement::Update);
        assert_eq!(SqlStatement::from_sql("DELETE FROM users"), SqlStatement::Delete);
        assert_eq!(SqlStatement::from_sql("CREATE TABLE users"), SqlStatement::Create);
    }
}
