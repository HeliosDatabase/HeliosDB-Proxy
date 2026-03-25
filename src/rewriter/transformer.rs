//! Transformation Engine
//!
//! Applies transformations to SQL queries.

use super::rules::Transformation;
use regex::Regex;

/// Transformation engine
pub struct TransformationEngine {
    /// Custom transformation functions
    custom_functions: std::collections::HashMap<String, Box<dyn CustomTransform>>,
}

impl TransformationEngine {
    /// Create a new transformation engine
    pub fn new() -> Self {
        Self {
            custom_functions: std::collections::HashMap::new(),
        }
    }

    /// Register a custom transformation function
    pub fn register_custom(&mut self, name: String, transform: Box<dyn CustomTransform>) {
        self.custom_functions.insert(name, transform);
    }

    /// Apply a transformation to a query
    pub fn apply(&self, query: &str, transformation: &Transformation) -> Result<String, TransformError> {
        match transformation {
            Transformation::NoOp => Ok(query.to_string()),

            Transformation::Replace(replacement) => {
                Ok(replacement.clone())
            }

            Transformation::AddIndexHint { table, index } => {
                self.add_index_hint(query, table, index)
            }

            Transformation::ExpandSelectStar { columns } => {
                self.expand_select_star(query, columns)
            }

            Transformation::AddLimit(limit) => {
                self.add_limit(query, *limit)
            }

            Transformation::AddWhereClause(condition) => {
                self.add_where_clause(query, condition)
            }

            Transformation::AppendWhereAnd(condition) => {
                self.append_where_and(query, condition)
            }

            Transformation::ReplaceTable { from, to } => {
                self.replace_table(query, from, to)
            }

            Transformation::AddOrderBy { column, descending } => {
                self.add_order_by(query, column, *descending)
            }

            Transformation::AddHint(hint) => {
                Ok(format!("/*{}*/ {}", hint, query))
            }

            Transformation::AddBranchHint(branch) => {
                Ok(format!("/*helios:branch={}*/ {}", branch, query))
            }

            Transformation::AddTimeout(duration) => {
                let ms = duration.as_millis();
                Ok(format!("/*helios:timeout={}ms*/ {}", ms, query))
            }

            Transformation::Custom(name) => {
                if let Some(transform) = self.custom_functions.get(name) {
                    transform.transform(query)
                } else {
                    Err(TransformError::UnknownCustomFunction(name.clone()))
                }
            }

            Transformation::Chain(transformations) => {
                let mut result = query.to_string();
                for t in transformations {
                    result = self.apply(&result, t)?;
                }
                Ok(result)
            }
        }
    }

    /// Add index hint to query
    fn add_index_hint(&self, query: &str, table: &str, index: &str) -> Result<String, TransformError> {
        // PostgreSQL style: /*+ IndexScan(table index) */
        // Insert after SELECT keyword
        let upper = query.to_uppercase();

        if let Some(pos) = upper.find("SELECT") {
            let insert_pos = pos + 6;
            let hint = format!(" /*+ IndexScan({} {}) */", table, index);

            let mut result = query.to_string();
            result.insert_str(insert_pos, &hint);
            Ok(result)
        } else {
            // For non-SELECT queries, prepend the hint
            Ok(format!("/*+ IndexScan({} {}) */ {}", table, index, query))
        }
    }

    /// Expand SELECT * to column list
    fn expand_select_star(&self, query: &str, columns: &[String]) -> Result<String, TransformError> {
        // Find SELECT * pattern and replace with column list
        let re = Regex::new(r"(?i)SELECT\s+(\*|DISTINCT\s+\*|ALL\s+\*)")
            .map_err(|e| TransformError::RegexError(e.to_string()))?;

        if let Some(caps) = re.find(query) {
            let matched = caps.as_str();
            let is_distinct = matched.to_uppercase().contains("DISTINCT");
            let is_all = matched.to_uppercase().contains("ALL");

            let column_list = columns.join(", ");
            let replacement = if is_distinct {
                format!("SELECT DISTINCT {}", column_list)
            } else if is_all {
                format!("SELECT ALL {}", column_list)
            } else {
                format!("SELECT {}", column_list)
            };

            Ok(re.replace(query, replacement.as_str()).to_string())
        } else {
            // No SELECT * found, return unchanged
            Ok(query.to_string())
        }
    }

    /// Add LIMIT clause
    fn add_limit(&self, query: &str, limit: u32) -> Result<String, TransformError> {
        let upper = query.to_uppercase();

        // Don't add if LIMIT already exists
        if upper.contains(" LIMIT ") {
            return Ok(query.to_string());
        }

        // Remove trailing semicolon if present
        let trimmed = query.trim_end_matches(';').trim();

        // Add LIMIT before potential FOR UPDATE/SHARE clause
        if upper.contains(" FOR ") {
            let for_pos = upper.rfind(" FOR ").unwrap();
            let (before_for, after_for) = trimmed.split_at(for_pos);
            Ok(format!("{} LIMIT {}{};", before_for, limit, after_for))
        } else {
            Ok(format!("{} LIMIT {};", trimmed, limit))
        }
    }

    /// Add WHERE clause
    fn add_where_clause(&self, query: &str, condition: &str) -> Result<String, TransformError> {
        let upper = query.to_uppercase();

        // Remove trailing semicolon
        let trimmed = query.trim_end_matches(';').trim();

        if upper.contains(" WHERE ") {
            // Add to existing WHERE with AND
            self.append_where_and(trimmed, condition)
        } else {
            // Find position to insert WHERE (before GROUP BY, ORDER BY, LIMIT, etc.)
            let insert_keywords = [" GROUP BY", " ORDER BY", " LIMIT ", " OFFSET ", " FOR "];
            let mut insert_pos = trimmed.len();

            for keyword in &insert_keywords {
                if let Some(pos) = upper.find(keyword) {
                    if pos < insert_pos {
                        insert_pos = pos;
                    }
                }
            }

            let (before, after) = trimmed.split_at(insert_pos);
            Ok(format!("{} WHERE {}{};", before, condition, after))
        }
    }

    /// Append to existing WHERE clause with AND
    fn append_where_and(&self, query: &str, condition: &str) -> Result<String, TransformError> {
        let upper = query.to_uppercase();
        let trimmed = query.trim_end_matches(';').trim();

        if let Some(where_pos) = upper.find(" WHERE ") {
            // Find end of WHERE clause
            let after_where = &upper[where_pos + 7..];
            let end_keywords = [" GROUP BY", " ORDER BY", " LIMIT ", " OFFSET ", " FOR "];

            let mut end_pos = trimmed.len();
            for keyword in &end_keywords {
                if let Some(pos) = after_where.find(keyword) {
                    let abs_pos = where_pos + 7 + pos;
                    if abs_pos < end_pos {
                        end_pos = abs_pos;
                    }
                }
            }

            let (before, after) = trimmed.split_at(end_pos);
            Ok(format!("{} AND ({}){}; ", before, condition, after))
        } else {
            // No WHERE, add new WHERE clause
            self.add_where_clause(trimmed, condition)
        }
    }

    /// Replace table name
    fn replace_table(&self, query: &str, from: &str, to: &str) -> Result<String, TransformError> {
        // Use word-boundary aware replacement
        let pattern = format!(r"\b{}\b", regex::escape(from));
        let re = Regex::new(&pattern)
            .map_err(|e| TransformError::RegexError(e.to_string()))?;

        Ok(re.replace_all(query, to).to_string())
    }

    /// Add ORDER BY clause
    fn add_order_by(&self, query: &str, column: &str, descending: bool) -> Result<String, TransformError> {
        let upper = query.to_uppercase();
        let trimmed = query.trim_end_matches(';').trim();

        // Don't add if ORDER BY already exists
        if upper.contains(" ORDER BY ") {
            return Ok(query.to_string());
        }

        let direction = if descending { "DESC" } else { "ASC" };

        // Find position to insert (before LIMIT, OFFSET, FOR)
        let insert_keywords = [" LIMIT ", " OFFSET ", " FOR "];
        let mut insert_pos = trimmed.len();

        for keyword in &insert_keywords {
            if let Some(pos) = upper.find(keyword) {
                if pos < insert_pos {
                    insert_pos = pos;
                }
            }
        }

        let (before, after) = trimmed.split_at(insert_pos);
        Ok(format!("{} ORDER BY {} {}{};", before, column, direction, after))
    }
}

impl Default for TransformationEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Custom transformation trait
pub trait CustomTransform: Send + Sync {
    /// Transform the query
    fn transform(&self, query: &str) -> Result<String, TransformError>;
}

/// Transform error
#[derive(Debug, Clone)]
pub enum TransformError {
    /// Regex error
    RegexError(String),

    /// Parse error
    ParseError(String),

    /// Unknown custom function
    UnknownCustomFunction(String),

    /// Transformation not applicable
    NotApplicable(String),
}

impl std::fmt::Display for TransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RegexError(msg) => write!(f, "Regex error: {}", msg),
            Self::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Self::UnknownCustomFunction(name) => write!(f, "Unknown custom function: {}", name),
            Self::NotApplicable(msg) => write!(f, "Not applicable: {}", msg),
        }
    }
}

impl std::error::Error for TransformError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_limit() {
        let engine = TransformationEngine::new();

        let result = engine.add_limit("SELECT * FROM users", 100).unwrap();
        assert!(result.contains("LIMIT 100"));

        // Should not add duplicate LIMIT
        let result2 = engine.add_limit("SELECT * FROM users LIMIT 50", 100).unwrap();
        assert!(result2.contains("LIMIT 50"));
        assert!(!result2.contains("LIMIT 100"));
    }

    #[test]
    fn test_add_where() {
        let engine = TransformationEngine::new();

        let result = engine.add_where_clause("SELECT * FROM users", "active = true").unwrap();
        assert!(result.contains("WHERE active = true"));

        // Should add AND to existing WHERE
        let result2 = engine.add_where_clause("SELECT * FROM users WHERE id = 1", "active = true").unwrap();
        assert!(result2.contains("AND (active = true)"));
    }

    #[test]
    fn test_replace_table() {
        let engine = TransformationEngine::new();

        let result = engine.replace_table("SELECT * FROM old_users", "old_users", "users_v2").unwrap();
        assert!(result.contains("users_v2"));
        assert!(!result.contains("old_users"));
    }

    #[test]
    fn test_expand_select_star() {
        let engine = TransformationEngine::new();

        let result = engine.expand_select_star(
            "SELECT * FROM users",
            &["id".to_string(), "name".to_string(), "email".to_string()]
        ).unwrap();

        assert!(result.contains("id, name, email"));
        assert!(!result.contains("*"));
    }

    #[test]
    fn test_expand_select_distinct_star() {
        let engine = TransformationEngine::new();

        let result = engine.expand_select_star(
            "SELECT DISTINCT * FROM users",
            &["id".to_string(), "name".to_string()]
        ).unwrap();

        assert!(result.contains("SELECT DISTINCT id, name"));
    }

    #[test]
    fn test_add_index_hint() {
        let engine = TransformationEngine::new();

        let result = engine.add_index_hint("SELECT * FROM users WHERE id = 1", "users", "idx_users_id").unwrap();
        assert!(result.contains("IndexScan(users idx_users_id)"));
    }

    #[test]
    fn test_add_order_by() {
        let engine = TransformationEngine::new();

        let result = engine.add_order_by("SELECT * FROM users", "created_at", true).unwrap();
        assert!(result.contains("ORDER BY created_at DESC"));
    }

    #[test]
    fn test_add_hint() {
        let engine = TransformationEngine::new();

        let result = engine.apply("SELECT * FROM users", &Transformation::AddHint("parallel=4".to_string())).unwrap();
        assert!(result.contains("/*parallel=4*/"));
    }

    #[test]
    fn test_add_branch_hint() {
        let engine = TransformationEngine::new();

        let result = engine.apply("SELECT * FROM analytics", &Transformation::AddBranchHint("analytics".to_string())).unwrap();
        assert!(result.contains("/*helios:branch=analytics*/"));
    }

    #[test]
    fn test_chain_transformations() {
        let engine = TransformationEngine::new();

        let result = engine.apply(
            "SELECT * FROM users",
            &Transformation::Chain(vec![
                Transformation::AddLimit(100),
                Transformation::AddOrderBy {
                    column: "id".to_string(),
                    descending: false,
                },
            ])
        ).unwrap();

        assert!(result.contains("LIMIT 100"));
        assert!(result.contains("ORDER BY id ASC"));
    }
}
