//! Prepared Statement Tracker
//!
//! Tracks prepared statements for recreation when switching connections
//! in transaction or statement pooling modes.

use std::collections::HashMap;

/// Prepared statement information
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Statement name
    pub name: String,
    /// SQL query
    pub query: String,
    /// Parameter types (OIDs)
    pub param_types: Vec<u32>,
    /// When the statement was prepared
    pub prepared_at: chrono::DateTime<chrono::Utc>,
    /// Number of times executed
    pub execution_count: u64,
}

/// Tracker for prepared statements
///
/// Maintains a registry of prepared statements so they can be
/// recreated on new backend connections.
#[derive(Debug, Default)]
pub struct PreparedStatementTracker {
    /// Statements by name
    statements: HashMap<String, PreparedStatement>,
    /// Maximum statements to track (to prevent memory bloat)
    max_statements: usize,
    /// Total statements prepared
    total_prepared: u64,
    /// Total statements deallocated
    total_deallocated: u64,
}

impl PreparedStatementTracker {
    /// Create a new tracker with default capacity
    pub fn new() -> Self {
        Self::with_capacity(1000)
    }

    /// Create a new tracker with specified capacity
    pub fn with_capacity(max_statements: usize) -> Self {
        Self {
            statements: HashMap::with_capacity(max_statements.min(100)),
            max_statements,
            total_prepared: 0,
            total_deallocated: 0,
        }
    }

    /// Register a prepared statement
    ///
    /// # Arguments
    /// * `name` - Statement name (empty for unnamed)
    /// * `query` - The SQL query
    /// * `param_types` - Parameter type OIDs
    pub fn register(&mut self, name: String, query: String, param_types: Vec<u32>) {
        // Don't track unnamed statements
        if name.is_empty() {
            return;
        }

        // Check capacity
        if self.statements.len() >= self.max_statements {
            // Remove least recently used (oldest)
            if let Some(oldest) = self
                .statements
                .iter()
                .min_by_key(|(_, s)| s.prepared_at)
                .map(|(k, _)| k.clone())
            {
                self.statements.remove(&oldest);
                self.total_deallocated += 1;
            }
        }

        self.statements.insert(
            name.clone(),
            PreparedStatement {
                name,
                query,
                param_types,
                prepared_at: chrono::Utc::now(),
                execution_count: 0,
            },
        );

        self.total_prepared += 1;
    }

    /// Remove a prepared statement
    pub fn unregister(&mut self, name: &str) -> Option<PreparedStatement> {
        let stmt = self.statements.remove(name);
        if stmt.is_some() {
            self.total_deallocated += 1;
        }
        stmt
    }

    /// Clear all statements (DEALLOCATE ALL)
    pub fn clear(&mut self) {
        self.total_deallocated += self.statements.len() as u64;
        self.statements.clear();
    }

    /// Get a prepared statement by name
    pub fn get(&self, name: &str) -> Option<&PreparedStatement> {
        self.statements.get(name)
    }

    /// Record an execution of a statement
    pub fn record_execution(&mut self, name: &str) {
        if let Some(stmt) = self.statements.get_mut(name) {
            stmt.execution_count += 1;
        }
    }

    /// Check if a statement exists
    pub fn contains(&self, name: &str) -> bool {
        self.statements.contains_key(name)
    }

    /// Get all statements (for recreation on new connection)
    pub fn all_statements(&self) -> impl Iterator<Item = &PreparedStatement> {
        self.statements.values()
    }

    /// Get statement count
    pub fn len(&self) -> usize {
        self.statements.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    /// Generate PREPARE statements for all tracked statements
    ///
    /// Returns SQL to recreate all statements on a new connection.
    pub fn generate_prepare_sql(&self) -> Vec<String> {
        self.statements
            .values()
            .map(|stmt| {
                if stmt.param_types.is_empty() {
                    format!("PREPARE {} AS {}", stmt.name, stmt.query)
                } else {
                    let types: Vec<String> = stmt
                        .param_types
                        .iter()
                        .map(|t| oid_to_type_name(*t))
                        .collect();
                    format!(
                        "PREPARE {} ({}) AS {}",
                        stmt.name,
                        types.join(", "),
                        stmt.query
                    )
                }
            })
            .collect()
    }

    /// Get statistics
    pub fn stats(&self) -> TrackerStats {
        TrackerStats {
            active_statements: self.statements.len(),
            total_prepared: self.total_prepared,
            total_deallocated: self.total_deallocated,
            max_capacity: self.max_statements,
        }
    }
}

/// Tracker statistics
#[derive(Debug, Clone)]
pub struct TrackerStats {
    /// Currently tracked statements
    pub active_statements: usize,
    /// Total statements ever prepared
    pub total_prepared: u64,
    /// Total statements deallocated
    pub total_deallocated: u64,
    /// Maximum capacity
    pub max_capacity: usize,
}

/// Convert PostgreSQL OID to type name
///
/// This is a simplified mapping for common types.
fn oid_to_type_name(oid: u32) -> String {
    match oid {
        16 => "boolean".to_string(),
        17 => "bytea".to_string(),
        18 => "char".to_string(),
        19 => "name".to_string(),
        20 => "bigint".to_string(),
        21 => "smallint".to_string(),
        23 => "integer".to_string(),
        25 => "text".to_string(),
        26 => "oid".to_string(),
        700 => "real".to_string(),
        701 => "double precision".to_string(),
        790 => "money".to_string(),
        1042 => "char".to_string(),
        1043 => "varchar".to_string(),
        1082 => "date".to_string(),
        1083 => "time".to_string(),
        1114 => "timestamp".to_string(),
        1184 => "timestamptz".to_string(),
        1186 => "interval".to_string(),
        1700 => "numeric".to_string(),
        2950 => "uuid".to_string(),
        3802 => "jsonb".to_string(),
        _ => format!("unknown({})", oid),
    }
}

/// Parse PREPARE statement to extract components
///
/// Returns (name, param_types, query) if successful.
pub fn parse_prepare_statement(sql: &str) -> Option<(String, Vec<String>, String)> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();

    if !upper.starts_with("PREPARE ") {
        return None;
    }

    // PREPARE name [(type, ...)] AS query
    let rest = &sql[8..].trim_start(); // After "PREPARE "

    // Find name (until space or open paren)
    let name_end = rest
        .find(|c: char| c.is_whitespace() || c == '(')
        .unwrap_or(rest.len());
    let name = rest[..name_end].to_string();
    let rest = rest[name_end..].trim_start();

    // Check for parameter types
    let (param_types, rest) = if rest.starts_with('(') {
        // Find matching close paren
        if let Some(close) = rest.find(')') {
            let types_str = &rest[1..close];
            let types: Vec<String> = types_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            (types, rest[close + 1..].trim_start())
        } else {
            (Vec::new(), rest)
        }
    } else {
        (Vec::new(), rest)
    };

    // Check for AS
    let upper_rest = rest.to_uppercase();
    if !upper_rest.starts_with("AS ") {
        return None;
    }

    let query = rest[3..].trim_start().to_string();

    Some((name, param_types, query))
}

/// Parse DEALLOCATE statement
///
/// Returns the statement name or None for DEALLOCATE ALL.
pub fn parse_deallocate_statement(sql: &str) -> Option<Option<String>> {
    let sql = sql.trim();
    let upper = sql.to_uppercase();

    if !upper.starts_with("DEALLOCATE ") {
        return None;
    }

    let rest = sql[11..].trim();
    let upper_rest = rest.to_uppercase();

    if upper_rest == "ALL" || upper_rest.starts_with("ALL ") || upper_rest.starts_with("ALL;") {
        Some(None) // DEALLOCATE ALL
    } else {
        // Remove optional PREPARE keyword
        let name = if upper_rest.starts_with("PREPARE ") {
            rest[8..].trim()
        } else {
            rest
        };
        // Remove trailing semicolon if present
        let name = name.trim_end_matches(';').trim();
        Some(Some(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get() {
        let mut tracker = PreparedStatementTracker::new();

        tracker.register(
            "stmt1".to_string(),
            "SELECT * FROM users WHERE id = $1".to_string(),
            vec![23],
        );

        assert!(tracker.contains("stmt1"));
        let stmt = tracker.get("stmt1").unwrap();
        assert_eq!(stmt.query, "SELECT * FROM users WHERE id = $1");
        assert_eq!(stmt.param_types, vec![23]);
    }

    #[test]
    fn test_unregister() {
        let mut tracker = PreparedStatementTracker::new();

        tracker.register("stmt1".to_string(), "SELECT 1".to_string(), vec![]);

        assert!(tracker.contains("stmt1"));
        tracker.unregister("stmt1");
        assert!(!tracker.contains("stmt1"));
    }

    #[test]
    fn test_clear() {
        let mut tracker = PreparedStatementTracker::new();

        tracker.register("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        tracker.register("stmt2".to_string(), "SELECT 2".to_string(), vec![]);

        assert_eq!(tracker.len(), 2);
        tracker.clear();
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_capacity_limit() {
        let mut tracker = PreparedStatementTracker::with_capacity(3);

        tracker.register("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        tracker.register("stmt2".to_string(), "SELECT 2".to_string(), vec![]);
        tracker.register("stmt3".to_string(), "SELECT 3".to_string(), vec![]);

        // Adding a 4th should evict the oldest
        tracker.register("stmt4".to_string(), "SELECT 4".to_string(), vec![]);

        assert_eq!(tracker.len(), 3);
        assert!(tracker.contains("stmt4"));
    }

    #[test]
    fn test_generate_prepare_sql() {
        let mut tracker = PreparedStatementTracker::new();

        tracker.register(
            "get_user".to_string(),
            "SELECT * FROM users WHERE id = $1".to_string(),
            vec![23],
        );

        let sqls = tracker.generate_prepare_sql();
        assert_eq!(sqls.len(), 1);
        assert!(sqls[0].contains("PREPARE get_user"));
        assert!(sqls[0].contains("integer"));
    }

    #[test]
    fn test_parse_prepare_statement() {
        let result = parse_prepare_statement("PREPARE stmt1 AS SELECT 1");
        assert!(result.is_some());
        let (name, params, query) = result.unwrap();
        assert_eq!(name, "stmt1");
        assert!(params.is_empty());
        assert_eq!(query, "SELECT 1");

        let result = parse_prepare_statement(
            "PREPARE stmt2 (integer, text) AS SELECT * FROM t WHERE id = $1 AND name = $2",
        );
        assert!(result.is_some());
        let (name, params, query) = result.unwrap();
        assert_eq!(name, "stmt2");
        assert_eq!(params, vec!["integer", "text"]);
        assert!(query.starts_with("SELECT"));
    }

    #[test]
    fn test_parse_deallocate_statement() {
        assert_eq!(parse_deallocate_statement("DEALLOCATE ALL"), Some(None));
        assert_eq!(
            parse_deallocate_statement("DEALLOCATE stmt1"),
            Some(Some("stmt1".to_string()))
        );
        assert_eq!(
            parse_deallocate_statement("DEALLOCATE PREPARE stmt2"),
            Some(Some("stmt2".to_string()))
        );
        assert_eq!(parse_deallocate_statement("SELECT 1"), None);
    }

    #[test]
    fn test_execution_tracking() {
        let mut tracker = PreparedStatementTracker::new();

        tracker.register("stmt1".to_string(), "SELECT 1".to_string(), vec![]);

        tracker.record_execution("stmt1");
        tracker.record_execution("stmt1");

        let stmt = tracker.get("stmt1").unwrap();
        assert_eq!(stmt.execution_count, 2);
    }

    #[test]
    fn test_unnamed_statements_ignored() {
        let mut tracker = PreparedStatementTracker::new();

        tracker.register("".to_string(), "SELECT 1".to_string(), vec![]);

        assert!(tracker.is_empty());
    }
}
