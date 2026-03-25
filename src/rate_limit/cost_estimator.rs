//! Query Cost Estimator
//!
//! Estimates the cost/weight of queries for rate limiting purposes.
//! Different query types consume different amounts of resources.

use std::collections::HashMap;

/// Type of database operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    /// SELECT query
    Select,
    /// INSERT query
    Insert,
    /// UPDATE query
    Update,
    /// DELETE query
    Delete,
    /// DDL operations (CREATE, ALTER, DROP)
    Ddl,
    /// Full table scan detected
    FullTableScan,
    /// Vector/embedding search
    VectorSearch,
    /// Transaction control (BEGIN, COMMIT, ROLLBACK)
    TransactionControl,
    /// Administrative queries (ANALYZE, VACUUM, etc.)
    Administrative,
    /// Unknown/other
    Unknown,
}

impl Default for OperationType {
    fn default() -> Self {
        Self::Unknown
    }
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Select => write!(f, "SELECT"),
            OperationType::Insert => write!(f, "INSERT"),
            OperationType::Update => write!(f, "UPDATE"),
            OperationType::Delete => write!(f, "DELETE"),
            OperationType::Ddl => write!(f, "DDL"),
            OperationType::FullTableScan => write!(f, "FULL_SCAN"),
            OperationType::VectorSearch => write!(f, "VECTOR"),
            OperationType::TransactionControl => write!(f, "TXN_CTRL"),
            OperationType::Administrative => write!(f, "ADMIN"),
            OperationType::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// Query cost estimator
///
/// Estimates the resource cost of queries based on their type and content.
#[derive(Debug, Clone)]
pub struct QueryCostEstimator {
    /// Base cost per query
    base_cost: u32,

    /// Cost multipliers by operation type
    operation_costs: HashMap<OperationType, f32>,

    /// Additional cost for queries matching patterns
    pattern_costs: Vec<(String, u32)>,

    /// Whether to detect full table scans
    detect_full_scans: bool,

    /// Keywords that indicate expensive operations
    expensive_keywords: Vec<String>,
}

impl QueryCostEstimator {
    /// Create a new cost estimator with default settings
    pub fn new() -> Self {
        let mut operation_costs = HashMap::new();
        operation_costs.insert(OperationType::Select, 1.0);
        operation_costs.insert(OperationType::Insert, 2.0);
        operation_costs.insert(OperationType::Update, 3.0);
        operation_costs.insert(OperationType::Delete, 3.0);
        operation_costs.insert(OperationType::Ddl, 10.0);
        operation_costs.insert(OperationType::FullTableScan, 5.0);
        operation_costs.insert(OperationType::VectorSearch, 3.0);
        operation_costs.insert(OperationType::TransactionControl, 0.5);
        operation_costs.insert(OperationType::Administrative, 5.0);
        operation_costs.insert(OperationType::Unknown, 1.0);

        Self {
            base_cost: 1,
            operation_costs,
            pattern_costs: Vec::new(),
            detect_full_scans: true,
            expensive_keywords: vec![
                "CROSS JOIN".to_string(),
                "CARTESIAN".to_string(),
                "FULL OUTER JOIN".to_string(),
                "ORDER BY".to_string(),
                "GROUP BY".to_string(),
                "DISTINCT".to_string(),
                "UNION".to_string(),
            ],
        }
    }

    /// Create an estimator with custom base cost
    pub fn with_base_cost(mut self, cost: u32) -> Self {
        self.base_cost = cost;
        self
    }

    /// Set cost multiplier for an operation type
    pub fn with_operation_cost(mut self, op: OperationType, cost: f32) -> Self {
        self.operation_costs.insert(op, cost);
        self
    }

    /// Add a pattern-based cost
    pub fn with_pattern_cost(mut self, pattern: impl Into<String>, cost: u32) -> Self {
        self.pattern_costs.push((pattern.into().to_uppercase(), cost));
        self
    }

    /// Enable/disable full scan detection
    pub fn with_full_scan_detection(mut self, enabled: bool) -> Self {
        self.detect_full_scans = enabled;
        self
    }

    /// Estimate the cost of a query
    pub fn estimate_cost(&self, query: &str) -> u32 {
        let upper = query.to_uppercase();
        let op_type = self.detect_operation(&upper);

        // Get base multiplier for operation type
        let multiplier = self.operation_costs.get(&op_type).copied().unwrap_or(1.0);

        // Start with base cost * multiplier
        let mut cost = (self.base_cost as f32 * multiplier) as u32;

        // Add cost for expensive keywords
        for keyword in &self.expensive_keywords {
            if upper.contains(keyword) {
                cost += 1;
            }
        }

        // Add cost for matching patterns
        for (pattern, pattern_cost) in &self.pattern_costs {
            if upper.contains(pattern) {
                cost += pattern_cost;
            }
        }

        // Detect potential full table scans
        if self.detect_full_scans && self.is_likely_full_scan(&upper) {
            let scan_multiplier = self
                .operation_costs
                .get(&OperationType::FullTableScan)
                .copied()
                .unwrap_or(5.0);
            cost = (cost as f32 * scan_multiplier) as u32;
        }

        // Ensure minimum cost of 1
        cost.max(1)
    }

    /// Estimate write cost based on sync mode
    #[cfg(feature = "lag-routing")]
    pub fn estimate_write_cost_sync_mode(&self, sync_mode: crate::lag::SyncMode) -> u32 {
        use crate::lag::SyncMode;
        match sync_mode {
            SyncMode::Sync => 5,      // Waits for standby ACK
            SyncMode::SemiSync => 3,  // Bounded wait
            SyncMode::Async => 1,     // Fire and forget
            SyncMode::Unknown => 2,
        }
    }

    /// Detect the type of operation
    pub fn detect_operation(&self, query: &str) -> OperationType {
        let upper = query.trim().to_uppercase();

        // Check for transaction control first
        if upper.starts_with("BEGIN")
            || upper.starts_with("COMMIT")
            || upper.starts_with("ROLLBACK")
            || upper.starts_with("SAVEPOINT")
            || upper.starts_with("START TRANSACTION")
            || upper.starts_with("END")
        {
            return OperationType::TransactionControl;
        }

        // Check for DDL
        if upper.starts_with("CREATE")
            || upper.starts_with("ALTER")
            || upper.starts_with("DROP")
            || upper.starts_with("TRUNCATE")
        {
            return OperationType::Ddl;
        }

        // Check for administrative
        if upper.starts_with("ANALYZE")
            || upper.starts_with("VACUUM")
            || upper.starts_with("REINDEX")
            || upper.starts_with("CLUSTER")
        {
            return OperationType::Administrative;
        }

        // Check for DML
        if upper.starts_with("SELECT") || upper.starts_with("WITH") {
            // Check for vector search
            if upper.contains("VECTOR_SEARCH")
                || upper.contains("<->")
                || upper.contains("COSINE")
                || upper.contains("L2_DISTANCE")
                || upper.contains("EMBEDDING")
            {
                return OperationType::VectorSearch;
            }
            return OperationType::Select;
        }

        if upper.starts_with("INSERT") {
            return OperationType::Insert;
        }

        if upper.starts_with("UPDATE") {
            return OperationType::Update;
        }

        if upper.starts_with("DELETE") {
            return OperationType::Delete;
        }

        OperationType::Unknown
    }

    /// Check if query is likely a full table scan
    fn is_likely_full_scan(&self, upper: &str) -> bool {
        // SELECT without WHERE is likely a full scan
        if upper.starts_with("SELECT") || upper.contains(" SELECT ") {
            // Has no WHERE clause
            if !upper.contains("WHERE") {
                // But has FROM (not just SELECT 1)
                if upper.contains("FROM") {
                    // Not a COUNT(*) or similar aggregate without conditions
                    return true;
                }
            }
        }

        // DELETE or UPDATE without WHERE
        if (upper.starts_with("DELETE") || upper.starts_with("UPDATE")) && !upper.contains("WHERE")
        {
            return true;
        }

        false
    }

    /// Extract any cost hint from the query
    pub fn extract_cost_hint(&self, query: &str) -> Option<u32> {
        // Look for /*helios:cost=X*/ pattern
        if let Some(start) = query.find("/*helios:cost=") {
            let after_prefix = &query[start + 14..];
            if let Some(end) = after_prefix.find("*/") {
                let cost_str = &after_prefix[..end];
                return cost_str.trim().parse().ok();
            }
        }

        // Also check for /*cost:X*/ pattern
        if let Some(start) = query.find("/*cost:") {
            let after_prefix = &query[start + 7..];
            if let Some(end) = after_prefix.find("*/") {
                let cost_str = &after_prefix[..end];
                return cost_str.trim().parse().ok();
            }
        }

        None
    }

    /// Estimate cost with hint override
    pub fn estimate_cost_with_hint(&self, query: &str) -> u32 {
        // Check for explicit cost hint
        if let Some(hint_cost) = self.extract_cost_hint(query) {
            return hint_cost;
        }

        // Fall back to estimation
        self.estimate_cost(query)
    }

    /// Get the operation cost multiplier
    pub fn get_operation_multiplier(&self, op: OperationType) -> f32 {
        self.operation_costs.get(&op).copied().unwrap_or(1.0)
    }

    /// Set base cost
    pub fn set_base_cost(&mut self, cost: u32) {
        self.base_cost = cost;
    }

    /// Get base cost
    pub fn base_cost(&self) -> u32 {
        self.base_cost
    }
}

impl Default for QueryCostEstimator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_select() {
        let estimator = QueryCostEstimator::new();

        assert_eq!(
            estimator.detect_operation("SELECT * FROM users"),
            OperationType::Select
        );
        assert_eq!(
            estimator.detect_operation("select id from users where id = 1"),
            OperationType::Select
        );
    }

    #[test]
    fn test_detect_insert() {
        let estimator = QueryCostEstimator::new();

        assert_eq!(
            estimator.detect_operation("INSERT INTO users (name) VALUES ('test')"),
            OperationType::Insert
        );
    }

    #[test]
    fn test_detect_update() {
        let estimator = QueryCostEstimator::new();

        assert_eq!(
            estimator.detect_operation("UPDATE users SET name = 'test' WHERE id = 1"),
            OperationType::Update
        );
    }

    #[test]
    fn test_detect_delete() {
        let estimator = QueryCostEstimator::new();

        assert_eq!(
            estimator.detect_operation("DELETE FROM users WHERE id = 1"),
            OperationType::Delete
        );
    }

    #[test]
    fn test_detect_ddl() {
        let estimator = QueryCostEstimator::new();

        assert_eq!(
            estimator.detect_operation("CREATE TABLE test (id INT)"),
            OperationType::Ddl
        );
        assert_eq!(
            estimator.detect_operation("ALTER TABLE users ADD COLUMN age INT"),
            OperationType::Ddl
        );
        assert_eq!(
            estimator.detect_operation("DROP TABLE test"),
            OperationType::Ddl
        );
    }

    #[test]
    fn test_detect_transaction_control() {
        let estimator = QueryCostEstimator::new();

        assert_eq!(
            estimator.detect_operation("BEGIN"),
            OperationType::TransactionControl
        );
        assert_eq!(
            estimator.detect_operation("COMMIT"),
            OperationType::TransactionControl
        );
        assert_eq!(
            estimator.detect_operation("ROLLBACK"),
            OperationType::TransactionControl
        );
        assert_eq!(
            estimator.detect_operation("START TRANSACTION"),
            OperationType::TransactionControl
        );
    }

    #[test]
    fn test_detect_vector_search() {
        let estimator = QueryCostEstimator::new();

        assert_eq!(
            estimator.detect_operation("SELECT * FROM docs ORDER BY embedding <-> '[1,2,3]'"),
            OperationType::VectorSearch
        );
        assert_eq!(
            estimator.detect_operation("SELECT vector_search(embedding, query)"),
            OperationType::VectorSearch
        );
    }

    #[test]
    fn test_detect_administrative() {
        let estimator = QueryCostEstimator::new();

        assert_eq!(
            estimator.detect_operation("ANALYZE users"),
            OperationType::Administrative
        );
        assert_eq!(
            estimator.detect_operation("VACUUM FULL users"),
            OperationType::Administrative
        );
    }

    #[test]
    fn test_estimate_cost_by_type() {
        let estimator = QueryCostEstimator::new();

        // SELECT = 1x base
        let select_cost = estimator.estimate_cost("SELECT id FROM users WHERE id = 1");

        // INSERT = 2x base
        let insert_cost = estimator.estimate_cost("INSERT INTO users (name) VALUES ('test')");

        // UPDATE = 3x base
        let update_cost = estimator.estimate_cost("UPDATE users SET name = 'test' WHERE id = 1");

        assert!(insert_cost > select_cost);
        assert!(update_cost > insert_cost);
    }

    #[test]
    fn test_full_scan_detection() {
        let estimator = QueryCostEstimator::new();

        // Full scan (SELECT * FROM without WHERE)
        let scan_cost = estimator.estimate_cost("SELECT * FROM users");

        // Not a full scan (has WHERE)
        let indexed_cost = estimator.estimate_cost("SELECT * FROM users WHERE id = 1");

        assert!(scan_cost > indexed_cost);
    }

    #[test]
    fn test_expensive_keywords() {
        let estimator = QueryCostEstimator::new();

        // Simple select
        let simple_cost = estimator.estimate_cost("SELECT id FROM users WHERE id = 1");

        // Select with ORDER BY, GROUP BY
        let complex_cost =
            estimator.estimate_cost("SELECT COUNT(*) FROM users GROUP BY status ORDER BY status");

        assert!(complex_cost > simple_cost);
    }

    #[test]
    fn test_extract_cost_hint() {
        let estimator = QueryCostEstimator::new();

        assert_eq!(
            estimator.extract_cost_hint("/*helios:cost=10*/ SELECT * FROM users"),
            Some(10)
        );
        assert_eq!(
            estimator.extract_cost_hint("/*cost:5*/ SELECT * FROM users"),
            Some(5)
        );
        assert_eq!(
            estimator.extract_cost_hint("SELECT * FROM users"),
            None
        );
    }

    #[test]
    fn test_estimate_cost_with_hint() {
        let estimator = QueryCostEstimator::new();

        // With hint - use hint value
        let hint_cost = estimator.estimate_cost_with_hint("/*helios:cost=100*/ SELECT * FROM users");
        assert_eq!(hint_cost, 100);

        // Without hint - estimate
        let estimated_cost = estimator.estimate_cost_with_hint("SELECT * FROM users WHERE id = 1");
        assert!(estimated_cost < 100);
    }

    #[test]
    fn test_custom_operation_cost() {
        let estimator = QueryCostEstimator::new()
            .with_operation_cost(OperationType::Select, 5.0);

        // SELECT should now cost 5x base
        let cost = estimator.estimate_cost("SELECT id FROM users WHERE id = 1");
        assert_eq!(cost, 5);
    }

    #[test]
    fn test_custom_pattern_cost() {
        let estimator = QueryCostEstimator::new()
            .with_pattern_cost("EXPENSIVE_TABLE", 20);

        // Query matching pattern should have extra cost
        let cost = estimator.estimate_cost("SELECT * FROM EXPENSIVE_TABLE WHERE id = 1");
        assert!(cost > 20);
    }

    #[test]
    fn test_minimum_cost() {
        let estimator = QueryCostEstimator::new()
            .with_operation_cost(OperationType::TransactionControl, 0.0);

        // Even with 0 multiplier, should have minimum cost of 1
        let cost = estimator.estimate_cost("BEGIN");
        assert!(cost >= 1);
    }

    #[test]
    fn test_with_query() {
        let estimator = QueryCostEstimator::new();

        // CTE query should be detected as SELECT
        let op = estimator.detect_operation(
            "WITH cte AS (SELECT * FROM users) SELECT * FROM cte WHERE id = 1"
        );
        assert_eq!(op, OperationType::Select);
    }

    #[test]
    fn test_delete_without_where() {
        let estimator = QueryCostEstimator::new();

        // DELETE without WHERE is a full scan
        let cost_without_where = estimator.estimate_cost("DELETE FROM temp_table");
        let cost_with_where = estimator.estimate_cost("DELETE FROM temp_table WHERE id = 1");

        assert!(cost_without_where > cost_with_where);
    }
}
