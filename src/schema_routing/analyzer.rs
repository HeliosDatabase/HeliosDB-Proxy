//! Query Analyzer
//!
//! Analyzes SQL queries to determine routing requirements.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::registry::{
    SchemaRegistry, TableSchema, AccessPattern, WorkloadType,
};

/// Query analyzer for schema-aware routing
#[derive(Debug)]
pub struct QueryAnalyzer {
    /// Schema registry reference
    schema: Arc<SchemaRegistry>,
}

impl QueryAnalyzer {
    /// Create a new query analyzer
    pub fn new(schema: Arc<SchemaRegistry>) -> Self {
        Self { schema }
    }

    /// Analyze a query and determine routing requirements
    pub fn analyze(&self, query: &str) -> QueryAnalysis {
        let normalized = self.normalize_query(query);
        let tables = self.extract_tables(&normalized);
        let access_patterns = self.detect_access_patterns(&normalized, &tables);
        let shard_keys = self.extract_shard_keys(&normalized, &tables);
        let workload_type = self.classify_workload(&normalized, &tables);

        QueryAnalysis {
            original_query: query.to_string(),
            tables,
            access_patterns,
            shard_keys,
            workload_type,
            complexity: self.estimate_complexity(&normalized),
            selectivity: self.estimate_selectivity(&normalized),
            is_read_only: self.is_read_only(&normalized),
            has_aggregations: self.has_aggregations(&normalized),
            has_joins: self.has_joins(&normalized),
            has_subqueries: self.has_subqueries(&normalized),
        }
    }

    /// Normalize query for analysis
    fn normalize_query(&self, query: &str) -> String {
        query.to_uppercase()
            .replace(['\n', '\t'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Extract tables from query
    pub fn extract_tables(&self, query: &str) -> Vec<TableRef> {
        let mut tables = Vec::new();
        let words: Vec<&str> = query.split_whitespace().collect();

        // Find tables after FROM, JOIN, INTO, UPDATE
        let table_keywords = ["FROM", "JOIN", "INTO", "UPDATE"];

        for (i, word) in words.iter().enumerate() {
            if table_keywords.contains(word) {
                if let Some(table_name) = words.get(i + 1) {
                    let name = table_name.trim_matches(|c| c == ',' || c == '(' || c == ')');
                    if !name.is_empty() && !is_keyword(name) {
                        let alias = self.find_alias(&words, i + 1);
                        tables.push(TableRef {
                            name: name.to_lowercase(),
                            alias,
                            schema: self.schema.get_table(&name.to_lowercase()),
                        });
                    }
                }
            }
        }

        tables
    }

    /// Find alias for a table
    fn find_alias(&self, words: &[&str], table_idx: usize) -> Option<String> {
        if let Some(next) = words.get(table_idx + 1) {
            if next.eq_ignore_ascii_case("AS") {
                return words.get(table_idx + 2).map(|s| s.to_lowercase());
            } else if !is_keyword(next) && !next.starts_with('(') {
                return Some(next.to_lowercase());
            }
        }
        None
    }

    /// Detect access patterns for each table
    fn detect_access_patterns(&self, query: &str, tables: &[TableRef]) -> Vec<AccessPattern> {
        let mut patterns = Vec::new();

        for table in tables {
            let pattern = self.detect_table_access_pattern(query, table);
            patterns.push(pattern);
        }

        patterns
    }

    /// Detect access pattern for a specific table
    fn detect_table_access_pattern(&self, query: &str, table: &TableRef) -> AccessPattern {
        // Check for vector operations
        if self.has_vector_operator(query) {
            return AccessPattern::VectorSearch;
        }

        // Check for point lookup (equality on PK)
        if let Some(schema) = &table.schema {
            if self.has_equality_on_pk(query, schema) {
                return AccessPattern::PointLookup;
            }
        }

        // Check for range predicates
        if self.has_range_predicate(query) {
            return AccessPattern::RangeScan;
        }

        // Check for time-series patterns
        if self.is_time_series_append(query) {
            return AccessPattern::TimeSeriesAppend;
        }

        // Default to full scan if no WHERE clause
        if !query.contains("WHERE") {
            return AccessPattern::FullScan;
        }

        AccessPattern::Mixed
    }

    /// Check for equality on primary key
    fn has_equality_on_pk(&self, query: &str, schema: &TableSchema) -> bool {
        if schema.primary_key.is_empty() {
            return false;
        }

        for pk_col in &schema.primary_key {
            let pattern = format!("{} =", pk_col.to_uppercase());
            if query.contains(&pattern) {
                return true;
            }
        }

        false
    }

    /// Check for range predicates
    fn has_range_predicate(&self, query: &str) -> bool {
        query.contains(" > ") || query.contains(" < ")
            || query.contains(" >= ") || query.contains(" <= ")
            || query.contains(" BETWEEN ")
    }

    /// Check for vector operators
    fn has_vector_operator(&self, query: &str) -> bool {
        query.contains("<->") || query.contains("<#>") || query.contains("<=>")
            || query.contains("VECTOR") || query.contains("EMBEDDING")
            || query.contains("COSINE_DISTANCE") || query.contains("L2_DISTANCE")
    }

    /// Check for time-series append pattern
    fn is_time_series_append(&self, query: &str) -> bool {
        query.starts_with("INSERT") && (
            query.contains("TIMESTAMP") || query.contains("CREATED_AT")
                || query.contains("EVENT_TIME")
        )
    }

    /// Extract shard keys from query
    fn extract_shard_keys(&self, query: &str, tables: &[TableRef]) -> HashMap<String, ShardKeyValue> {
        let mut shard_keys = HashMap::new();

        for table in tables {
            if let Some(schema) = &table.schema {
                if let Some(shard_key) = &schema.shard_key {
                    if let Some(value) = self.extract_shard_key_value(query, shard_key) {
                        shard_keys.insert(shard_key.clone(), value);
                    }
                }
            }
        }

        shard_keys
    }

    /// Extract shard key value from query
    fn extract_shard_key_value(&self, query: &str, shard_key: &str) -> Option<ShardKeyValue> {
        // Look for patterns like "shard_key = 'value'" or "shard_key = value"
        let pattern = format!("{} =", shard_key.to_uppercase());
        if let Some(idx) = query.find(&pattern) {
            let rest = &query[idx + pattern.len()..];
            let value = rest.split_whitespace().next()?;
            let clean_value = value.trim_matches(|c| c == '\'' || c == '"' || c == ',');
            return Some(ShardKeyValue::Single(clean_value.to_string()));
        }

        // Look for IN clause
        let in_pattern = format!("{} IN", shard_key.to_uppercase());
        if let Some(idx) = query.find(&in_pattern) {
            let rest = &query[idx + in_pattern.len()..];
            if let Some(start) = rest.find('(') {
                if let Some(end) = rest.find(')') {
                    let values_str = &rest[start + 1..end];
                    let values: Vec<String> = values_str
                        .split(',')
                        .map(|v| v.trim().trim_matches(|c| c == '\'' || c == '"').to_string())
                        .collect();
                    return Some(ShardKeyValue::Multiple(values));
                }
            }
        }

        None
    }

    /// Classify workload type
    fn classify_workload(&self, query: &str, tables: &[TableRef]) -> WorkloadType {
        // Vector queries
        if self.has_vector_operator(query) {
            return WorkloadType::Vector;
        }

        // OLAP indicators
        if self.has_aggregations(query) || self.has_group_by(query) || self.has_window_functions(query) {
            return WorkloadType::OLAP;
        }

        // Simple CRUD is OLTP
        if self.is_simple_crud(query) {
            return WorkloadType::OLTP;
        }

        // Check table hints
        for table in tables {
            if let Some(schema) = &table.schema {
                if schema.workload != WorkloadType::Mixed {
                    return schema.workload;
                }
            }
        }

        WorkloadType::Mixed
    }

    /// Check if query has aggregations
    pub fn has_aggregations(&self, query: &str) -> bool {
        query.contains("COUNT(") || query.contains("SUM(")
            || query.contains("AVG(") || query.contains("MIN(")
            || query.contains("MAX(")
    }

    /// Check if query has GROUP BY
    fn has_group_by(&self, query: &str) -> bool {
        query.contains("GROUP BY")
    }

    /// Check if query has window functions
    fn has_window_functions(&self, query: &str) -> bool {
        query.contains("OVER(") || query.contains("OVER (")
            || query.contains("ROW_NUMBER") || query.contains("RANK()")
            || query.contains("DENSE_RANK") || query.contains("LAG(")
            || query.contains("LEAD(")
    }

    /// Check if query is simple CRUD
    fn is_simple_crud(&self, query: &str) -> bool {
        let is_simple_select = query.starts_with("SELECT")
            && !self.has_joins(query)
            && !self.has_subqueries(query)
            && !self.has_aggregations(query);

        let is_simple_insert = query.starts_with("INSERT")
            && !query.contains("SELECT");

        let is_simple_update = query.starts_with("UPDATE")
            && query.contains("WHERE");

        let is_simple_delete = query.starts_with("DELETE")
            && query.contains("WHERE");

        is_simple_select || is_simple_insert || is_simple_update || is_simple_delete
    }

    /// Check if query is read-only
    pub fn is_read_only(&self, query: &str) -> bool {
        query.starts_with("SELECT") || query.starts_with("WITH")
            || query.starts_with("EXPLAIN") || query.starts_with("SHOW")
    }

    /// Check if query has joins
    pub fn has_joins(&self, query: &str) -> bool {
        query.contains(" JOIN ")
    }

    /// Check if query has subqueries
    pub fn has_subqueries(&self, query: &str) -> bool {
        // Count SELECT keywords (more than one suggests subqueries)
        query.matches("SELECT").count() > 1
    }

    /// Estimate query complexity (0-100)
    fn estimate_complexity(&self, query: &str) -> u32 {
        let mut complexity: u32 = 10; // Base complexity

        // Add for joins
        complexity += (query.matches(" JOIN ").count() as u32) * 15;

        // Add for subqueries
        let select_count = query.matches("SELECT").count() as u32;
        if select_count > 1 {
            complexity += (select_count - 1) * 20;
        }

        // Add for aggregations
        if self.has_aggregations(query) {
            complexity += 10;
        }

        // Add for GROUP BY
        if self.has_group_by(query) {
            complexity += 10;
        }

        // Add for window functions
        if self.has_window_functions(query) {
            complexity += 15;
        }

        // Add for ORDER BY
        if query.contains("ORDER BY") {
            complexity += 5;
        }

        // Add for DISTINCT
        if query.contains("DISTINCT") {
            complexity += 5;
        }

        complexity.min(100)
    }

    /// Estimate selectivity (0.0 - 1.0)
    fn estimate_selectivity(&self, query: &str) -> f64 {
        if !query.contains("WHERE") {
            return 1.0; // Full table scan
        }

        let mut selectivity = 0.5; // Default with WHERE

        // Equality predicates are highly selective
        let eq_count = query.matches(" = ").count();
        selectivity *= 0.9_f64.powi(eq_count as i32);

        // LIMIT reduces result set
        if query.contains("LIMIT") {
            selectivity *= 0.5;
        }

        selectivity.max(0.001) // Never assume 0 selectivity
    }

    /// Extract columns from query
    pub fn extract_columns(&self, query: &str) -> Vec<String> {
        let mut columns = HashSet::new();
        let words: Vec<&str> = query.split_whitespace().collect();

        // Find column names between SELECT and FROM
        if let Some(select_idx) = words.iter().position(|w| *w == "SELECT") {
            if let Some(from_idx) = words.iter().position(|w| *w == "FROM") {
                for word in &words[select_idx + 1..from_idx] {
                    let col = word.trim_matches(|c| c == ',' || c == '(' || c == ')');
                    if !col.is_empty() && !is_keyword(col) && col != "*" {
                        // Handle table.column format
                        if let Some(dot_idx) = col.find('.') {
                            columns.insert(col[dot_idx + 1..].to_lowercase());
                        } else {
                            columns.insert(col.to_lowercase());
                        }
                    }
                }
            }
        }

        columns.into_iter().collect()
    }
}

/// Check if a word is a SQL keyword
fn is_keyword(word: &str) -> bool {
    let keywords = [
        "SELECT", "FROM", "WHERE", "JOIN", "ON", "AND", "OR", "NOT",
        "IN", "IS", "NULL", "AS", "ORDER", "BY", "GROUP", "HAVING",
        "LIMIT", "OFFSET", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
        "DELETE", "CREATE", "DROP", "ALTER", "INDEX", "TABLE", "LEFT",
        "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "NATURAL", "USING",
        "DISTINCT", "ALL", "UNION", "INTERSECT", "EXCEPT", "CASE",
        "WHEN", "THEN", "ELSE", "END", "BETWEEN", "LIKE", "ILIKE",
        "EXISTS", "WITH", "RECURSIVE", "ASC", "DESC", "NULLS", "FIRST", "LAST",
    ];
    keywords.contains(&word.to_uppercase().as_str())
}

/// Table reference from query
#[derive(Debug, Clone)]
pub struct TableRef {
    /// Table name
    pub name: String,
    /// Table alias
    pub alias: Option<String>,
    /// Table schema (if found)
    pub schema: Option<TableSchema>,
}

/// Shard key value
#[derive(Debug, Clone)]
pub enum ShardKeyValue {
    /// Single value
    Single(String),
    /// Multiple values (IN clause)
    Multiple(Vec<String>),
}

/// Query analysis result
#[derive(Debug, Clone)]
pub struct QueryAnalysis {
    /// Original query
    pub original_query: String,
    /// Tables referenced
    pub tables: Vec<TableRef>,
    /// Access patterns per table
    pub access_patterns: Vec<AccessPattern>,
    /// Extracted shard keys
    pub shard_keys: HashMap<String, ShardKeyValue>,
    /// Classified workload type
    pub workload_type: WorkloadType,
    /// Estimated complexity (0-100)
    pub complexity: u32,
    /// Estimated selectivity (0.0 - 1.0)
    pub selectivity: f64,
    /// Is read-only query
    pub is_read_only: bool,
    /// Has aggregation functions
    pub has_aggregations: bool,
    /// Has JOIN clauses
    pub has_joins: bool,
    /// Has subqueries
    pub has_subqueries: bool,
}

impl QueryAnalysis {
    /// Check if query involves vector operations
    pub fn is_vector_query(&self) -> bool {
        self.access_patterns.contains(&AccessPattern::VectorSearch)
    }

    /// Check if query is analytics (OLAP)
    pub fn is_analytics(&self) -> bool {
        self.workload_type == WorkloadType::OLAP
    }

    /// Get primary table (first table in query)
    pub fn primary_table(&self) -> Option<&TableRef> {
        self.tables.first()
    }

    /// Check if query targets a specific shard
    pub fn has_shard_key(&self) -> bool {
        !self.shard_keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_registry() -> Arc<SchemaRegistry> {
        let registry = SchemaRegistry::new();

        let users = TableSchema::new("users")
            .with_workload(WorkloadType::OLTP)
            .with_access_pattern(AccessPattern::PointLookup)
            .with_primary_key(vec!["id".to_string()])
            .with_shard_key("id");

        let events = TableSchema::new("events")
            .with_workload(WorkloadType::OLAP)
            .with_access_pattern(AccessPattern::FullScan);

        let embeddings = TableSchema::new("embeddings")
            .with_workload(WorkloadType::Vector)
            .with_access_pattern(AccessPattern::VectorSearch);

        registry.register_table(users);
        registry.register_table(events);
        registry.register_table(embeddings);

        Arc::new(registry)
    }

    #[test]
    fn test_extract_tables() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let query = "SELECT * FROM users WHERE id = 1";
        let tables = analyzer.extract_tables(&analyzer.normalize_query(query));

        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
    }

    #[test]
    fn test_extract_tables_with_join() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let query = "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id";
        let tables = analyzer.extract_tables(&analyzer.normalize_query(query));

        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0].name, "users");
        assert_eq!(tables[0].alias, Some("u".to_string()));
    }

    #[test]
    fn test_classify_oltp() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let query = "SELECT * FROM users WHERE id = 1";
        let analysis = analyzer.analyze(query);

        assert_eq!(analysis.workload_type, WorkloadType::OLTP);
        assert!(analysis.is_read_only);
    }

    #[test]
    fn test_classify_olap() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let query = "SELECT COUNT(*), SUM(amount) FROM events GROUP BY date";
        let analysis = analyzer.analyze(query);

        assert_eq!(analysis.workload_type, WorkloadType::OLAP);
        assert!(analysis.has_aggregations);
    }

    #[test]
    fn test_classify_vector() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let query = "SELECT * FROM embeddings ORDER BY embedding <-> '[1,2,3]' LIMIT 10";
        let analysis = analyzer.analyze(query);

        assert_eq!(analysis.workload_type, WorkloadType::Vector);
        assert!(analysis.is_vector_query());
    }

    #[test]
    fn test_extract_shard_key() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let query = "SELECT * FROM users WHERE id = 'user_123'";
        let analysis = analyzer.analyze(query);

        assert!(analysis.has_shard_key());
        assert!(analysis.shard_keys.contains_key("id"));
    }

    #[test]
    fn test_complexity_estimation() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let simple = "SELECT * FROM users WHERE id = 1";
        let complex = "SELECT u.*, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY COUNT(o.id) DESC";

        let simple_analysis = analyzer.analyze(simple);
        let complex_analysis = analyzer.analyze(complex);

        assert!(simple_analysis.complexity < complex_analysis.complexity);
    }

    #[test]
    fn test_detect_point_lookup() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let query = "SELECT * FROM users WHERE id = 1";
        let analysis = analyzer.analyze(query);

        assert!(analysis.access_patterns.contains(&AccessPattern::PointLookup));
    }

    #[test]
    fn test_detect_full_scan() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let query = "SELECT * FROM events";
        let analysis = analyzer.analyze(query);

        assert!(analysis.access_patterns.contains(&AccessPattern::FullScan));
    }

    #[test]
    fn test_has_joins() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let with_join = "SELECT * FROM users u JOIN orders o ON u.id = o.user_id";
        let without_join = "SELECT * FROM users";

        assert!(analyzer.analyze(with_join).has_joins);
        assert!(!analyzer.analyze(without_join).has_joins);
    }

    #[test]
    fn test_extract_columns() {
        let registry = create_test_registry();
        let analyzer = QueryAnalyzer::new(registry);

        let query = "SELECT id, name, email FROM users WHERE id = 1";
        let normalized = analyzer.normalize_query(query);
        let columns = analyzer.extract_columns(&normalized);

        assert!(columns.contains(&"id".to_string()));
        assert!(columns.contains(&"name".to_string()));
        assert!(columns.contains(&"email".to_string()));
    }
}
