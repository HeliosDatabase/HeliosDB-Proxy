//! Tenant Query Transformer
//!
//! This module transforms SQL queries to apply tenant isolation, primarily
//! for row-level security where queries need WHERE clause injection.

use std::collections::{HashMap, HashSet};

use super::config::{IsolationStrategy, TenantConfig, TenantId};

/// Result of query transformation
#[derive(Debug, Clone)]
pub struct TransformResult {
    /// Transformed query
    pub query: String,

    /// Whether transformation was applied
    pub transformed: bool,

    /// Tables that were filtered
    pub filtered_tables: Vec<String>,

    /// Warnings generated during transformation
    pub warnings: Vec<String>,
}

impl TransformResult {
    /// Create a passthrough result (no transformation)
    pub fn passthrough(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            transformed: false,
            filtered_tables: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Create a transformed result
    pub fn transformed(query: impl Into<String>, tables: Vec<String>) -> Self {
        Self {
            query: query.into(),
            transformed: true,
            filtered_tables: tables,
            warnings: Vec::new(),
        }
    }

    /// Add a warning
    pub fn with_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }
}

/// Query transformer for tenant isolation
pub struct TenantQueryTransformer {
    /// Tables that require tenant filtering
    tenant_tables: HashMap<String, String>,

    /// Tables to exclude from filtering
    excluded_tables: HashSet<String>,

    /// Whether to use parameterized queries
    use_parameters: bool,

    /// Custom filter template (default: "{column} = '{value}'")
    filter_template: Option<String>,
}

impl Default for TenantQueryTransformer {
    fn default() -> Self {
        Self::new()
    }
}

impl TenantQueryTransformer {
    /// Create a new query transformer
    pub fn new() -> Self {
        Self {
            tenant_tables: HashMap::new(),
            excluded_tables: HashSet::new(),
            use_parameters: false,
            filter_template: None,
        }
    }

    /// Register a table with its tenant column
    pub fn register_table(mut self, table: impl Into<String>, column: impl Into<String>) -> Self {
        self.tenant_tables
            .insert(table.into().to_lowercase(), column.into());
        self
    }

    /// Register multiple tables with the same column
    pub fn register_tables(mut self, tables: &[&str], column: impl Into<String>) -> Self {
        let col = column.into();
        for table in tables {
            self.tenant_tables
                .insert(table.to_lowercase(), col.clone());
        }
        self
    }

    /// Exclude a table from filtering
    pub fn exclude_table(mut self, table: impl Into<String>) -> Self {
        self.excluded_tables.insert(table.into().to_lowercase());
        self
    }

    /// Use parameterized queries
    pub fn with_parameters(mut self) -> Self {
        self.use_parameters = true;
        self
    }

    /// Set custom filter template
    pub fn with_filter_template(mut self, template: impl Into<String>) -> Self {
        self.filter_template = Some(template.into());
        self
    }

    /// Get the tenant column for a table
    pub fn get_tenant_column(&self, table: &str) -> Option<&str> {
        self.tenant_tables
            .get(&table.to_lowercase())
            .map(|s| s.as_str())
    }

    /// Check if a table requires filtering
    pub fn requires_filtering(&self, table: &str) -> bool {
        let lower = table.to_lowercase();
        self.tenant_tables.contains_key(&lower) && !self.excluded_tables.contains(&lower)
    }

    /// Transform a query for a tenant
    pub fn transform(
        &self,
        query: &str,
        tenant: &TenantId,
        config: &TenantConfig,
    ) -> TransformResult {
        // Only transform for row-level isolation
        let tenant_column = match &config.isolation {
            IsolationStrategy::Row { tenant_column, .. } => tenant_column,
            _ => return TransformResult::passthrough(query),
        };

        // Parse and transform query
        let upper = query.trim().to_uppercase();

        if upper.starts_with("SELECT") {
            self.transform_select(query, tenant, tenant_column)
        } else if upper.starts_with("UPDATE") {
            self.transform_update(query, tenant, tenant_column)
        } else if upper.starts_with("DELETE") {
            self.transform_delete(query, tenant, tenant_column)
        } else if upper.starts_with("INSERT") {
            self.transform_insert(query, tenant, tenant_column)
        } else {
            TransformResult::passthrough(query)
        }
    }

    /// Transform a SELECT query
    fn transform_select(
        &self,
        query: &str,
        tenant: &TenantId,
        tenant_column: &str,
    ) -> TransformResult {
        let tables = self.extract_tables(query);
        let filtered_tables: Vec<String> = tables
            .iter()
            .filter(|t| self.requires_filtering(t))
            .cloned()
            .collect();

        if filtered_tables.is_empty() {
            return TransformResult::passthrough(query);
        }

        let filter = self.build_filter(tenant, tenant_column, &filtered_tables);
        let transformed = self.inject_where_clause(query, &filter);

        TransformResult::transformed(transformed, filtered_tables)
    }

    /// Transform an UPDATE query
    fn transform_update(
        &self,
        query: &str,
        tenant: &TenantId,
        tenant_column: &str,
    ) -> TransformResult {
        let table = self.extract_update_table(query);

        if let Some(table) = table {
            if self.requires_filtering(&table) {
                let filter = self.build_single_filter(tenant, tenant_column);
                let transformed = self.inject_where_clause(query, &filter);
                return TransformResult::transformed(transformed, vec![table]);
            }
        }

        TransformResult::passthrough(query)
    }

    /// Transform a DELETE query
    fn transform_delete(
        &self,
        query: &str,
        tenant: &TenantId,
        tenant_column: &str,
    ) -> TransformResult {
        let table = self.extract_delete_table(query);

        if let Some(table) = table {
            if self.requires_filtering(&table) {
                let filter = self.build_single_filter(tenant, tenant_column);
                let transformed = self.inject_where_clause(query, &filter);
                return TransformResult::transformed(transformed, vec![table]);
            }
        }

        TransformResult::passthrough(query)
    }

    /// Transform an INSERT query (add tenant_id column)
    fn transform_insert(
        &self,
        query: &str,
        tenant: &TenantId,
        tenant_column: &str,
    ) -> TransformResult {
        let table = self.extract_insert_table(query);

        if let Some(table) = table {
            if self.requires_filtering(&table) {
                // For INSERT, we need to add tenant_id to the values
                let transformed = self.inject_tenant_value(query, tenant, tenant_column);
                return TransformResult::transformed(transformed, vec![table])
                    .with_warning("Tenant column injection may require schema awareness");
            }
        }

        TransformResult::passthrough(query)
    }

    /// Build a filter clause for multiple tables
    fn build_filter(
        &self,
        tenant: &TenantId,
        default_column: &str,
        tables: &[String],
    ) -> String {
        let filters: Vec<String> = tables
            .iter()
            .map(|table| {
                let column = self
                    .get_tenant_column(table)
                    .unwrap_or(default_column);
                if self.use_parameters {
                    format!("{}.{} = $1", table, column)
                } else {
                    format!("{}.{} = '{}'", table, column, tenant.0)
                }
            })
            .collect();

        filters.join(" AND ")
    }

    /// Build a single filter clause
    fn build_single_filter(&self, tenant: &TenantId, column: &str) -> String {
        if self.use_parameters {
            format!("{} = $1", column)
        } else {
            match &self.filter_template {
                Some(template) => template
                    .replace("{column}", column)
                    .replace("{value}", &tenant.0),
                None => format!("{} = '{}'", column, tenant.0),
            }
        }
    }

    /// Inject WHERE clause into query
    fn inject_where_clause(&self, query: &str, filter: &str) -> String {
        let upper = query.to_uppercase();

        // Find existing WHERE clause
        if let Some(where_pos) = upper.find(" WHERE ") {
            // Add to existing WHERE
            let (before, after) = query.split_at(where_pos + 7);
            format!("{}{} AND {}", before, filter, after)
        } else {
            // Find position to insert WHERE
            // Look for ORDER BY, GROUP BY, LIMIT, etc.
            let insert_before = [" ORDER ", " GROUP ", " LIMIT ", " HAVING ", " UNION "]
                .iter()
                .filter_map(|kw| upper.find(kw))
                .min();

            match insert_before {
                Some(pos) => {
                    let (before, after) = query.split_at(pos);
                    format!("{} WHERE {}{}", before, filter, after)
                }
                None => {
                    // Append at end
                    format!("{} WHERE {}", query.trim_end_matches(';'), filter)
                }
            }
        }
    }

    /// Inject tenant value into INSERT statement
    fn inject_tenant_value(&self, query: &str, tenant: &TenantId, column: &str) -> String {
        // Simple implementation - a real one would parse SQL properly
        let upper = query.to_uppercase();

        if let Some(values_pos) = upper.find(" VALUES ") {
            if let Some(paren_pos) = query[values_pos..].find('(') {
                let insert_pos = values_pos + paren_pos + 1;

                // Check if there's a column list
                if let Some(cols_start) = upper.find('(') {
                    if cols_start < values_pos {
                        // There's a column list - add column to it
                        let cols_end = upper[cols_start..].find(')').unwrap_or(0) + cols_start;
                        let before_cols_end = &query[..cols_end];
                        let after_cols_end = &query[cols_end..];

                        // Insert column name
                        let with_column =
                            format!("{}, {}{}", before_cols_end, column, after_cols_end);

                        // Now insert the value
                        let upper_new = with_column.to_uppercase();
                        if let Some(new_values_pos) = upper_new.find(" VALUES ") {
                            if let Some(new_paren_pos) = with_column[new_values_pos..].find('(') {
                                let new_insert_pos = new_values_pos + new_paren_pos + 1;
                                let before = &with_column[..new_insert_pos];
                                let after = &with_column[new_insert_pos..];
                                return format!("{}'{}'", before, tenant.0)
                                    + if !after.starts_with(')') { ", " } else { "" }
                                    + after;
                            }
                        }
                    }
                }

                // No column list or couldn't parse - just add to values
                let before = &query[..insert_pos];
                let after = &query[insert_pos..];
                return format!("{}'{}'", before, tenant.0)
                    + if !after.starts_with(')') { ", " } else { "" }
                    + after;
            }
        }

        query.to_string()
    }

    /// Extract table names from SELECT query
    fn extract_tables(&self, query: &str) -> Vec<String> {
        let upper = query.to_uppercase();
        let mut tables = Vec::new();

        // Find FROM clause
        if let Some(from_pos) = upper.find(" FROM ") {
            let after_from = &query[from_pos + 6..];

            // Find end of table list
            let end_markers = [" WHERE ", " JOIN ", " LEFT ", " RIGHT ", " INNER ", " OUTER ",
                " GROUP ", " ORDER ", " LIMIT ", " HAVING "];
            let end_pos = end_markers
                .iter()
                .filter_map(|m| after_from.to_uppercase().find(m))
                .min()
                .unwrap_or(after_from.len());

            let table_section = &after_from[..end_pos];

            // Parse table names (handle aliases)
            for part in table_section.split(',') {
                let trimmed = part.trim();
                if let Some(table) = trimmed.split_whitespace().next() {
                    let clean = table
                        .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']');
                    if !clean.is_empty() {
                        tables.push(clean.to_string());
                    }
                }
            }
        }

        // Also look for JOINs
        let words: Vec<&str> = query.split_whitespace().collect();
        for (i, word) in words.iter().enumerate() {
            if word.to_uppercase() == "JOIN" && i + 1 < words.len() {
                let table = words[i + 1]
                    .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']');
                if !table.is_empty() && !tables.contains(&table.to_string()) {
                    tables.push(table.to_string());
                }
            }
        }

        tables
    }

    /// Extract table name from UPDATE query
    fn extract_update_table(&self, query: &str) -> Option<String> {
        let upper = query.to_uppercase();
        if let Some(update_pos) = upper.find("UPDATE ") {
            let after_update = &query[update_pos + 7..];
            if let Some(set_pos) = after_update.to_uppercase().find(" SET ") {
                let table_section = &after_update[..set_pos];
                let table = table_section
                    .trim()
                    .split_whitespace()
                    .next()?
                    .trim_matches(|c| c == '"' || c == '`');
                return Some(table.to_string());
            }
        }
        None
    }

    /// Extract table name from DELETE query
    fn extract_delete_table(&self, query: &str) -> Option<String> {
        let upper = query.to_uppercase();
        if let Some(from_pos) = upper.find(" FROM ") {
            let after_from = &query[from_pos + 6..];
            let end_pos = after_from.to_uppercase().find(" WHERE ")
                .unwrap_or(after_from.len());
            let table_section = &after_from[..end_pos];
            let table = table_section
                .trim()
                .split_whitespace()
                .next()?
                .trim_matches(|c| c == '"' || c == '`');
            return Some(table.to_string());
        }
        None
    }

    /// Extract table name from INSERT query
    fn extract_insert_table(&self, query: &str) -> Option<String> {
        let upper = query.to_uppercase();
        if let Some(into_pos) = upper.find(" INTO ") {
            let after_into = &query[into_pos + 6..];
            let end_pos = after_into.find(|c: char| c == '(' || c.is_whitespace())
                .unwrap_or(after_into.len());
            let table = after_into[..end_pos]
                .trim()
                .trim_matches(|c| c == '"' || c == '`');
            return Some(table.to_string());
        }
        None
    }

    /// Generate SET search_path command for schema isolation
    pub fn set_schema_search_path(
        &self,
        _tenant: &TenantId,
        config: &TenantConfig,
    ) -> Option<String> {
        if let IsolationStrategy::Schema { schema_name, .. } = &config.isolation {
            Some(format!("SET search_path TO {}", schema_name))
        } else {
            None
        }
    }

    /// Generate USE database command for database isolation
    pub fn use_database(&self, _tenant: &TenantId, config: &TenantConfig) -> Option<String> {
        if let IsolationStrategy::Database { database_name } = &config.isolation {
            Some(format!("USE {}", database_name))
        } else {
            None
        }
    }
}

/// Validate that a query doesn't try to bypass tenant isolation
pub fn validate_query(query: &str, tenant: &TenantId, config: &TenantConfig) -> QueryValidation {
    let mut validation = QueryValidation {
        valid: true,
        violations: Vec::new(),
    };

    let upper = query.to_uppercase();

    // Check for dangerous operations
    if let IsolationStrategy::Row { tenant_column, .. } = &config.isolation {
        // Check if query tries to modify tenant column
        if upper.contains(&format!("{} =", tenant_column.to_uppercase())) {
            let set_pattern = format!("SET {} =", tenant_column.to_uppercase());
            if upper.contains(&set_pattern) {
                validation.valid = false;
                validation
                    .violations
                    .push(format!("Cannot modify tenant column: {}", tenant_column));
            }
        }

        // Check for TRUNCATE (bypasses row-level security)
        if upper.starts_with("TRUNCATE ") {
            validation.valid = false;
            validation
                .violations
                .push("TRUNCATE not allowed with row-level isolation".to_string());
        }

        // Check for DROP TABLE
        if upper.contains("DROP TABLE") {
            validation.valid = false;
            validation
                .violations
                .push("DROP TABLE not allowed with row-level isolation".to_string());
        }
    }

    // Check for cross-schema access in schema isolation
    if let IsolationStrategy::Schema { schema_name, .. } = &config.isolation {
        // Look for schema.table patterns that don't match tenant's schema
        let parts: Vec<&str> = upper.split_whitespace().collect();
        for part in parts {
            if part.contains('.') && !part.starts_with(&schema_name.to_uppercase()) {
                let schema = part.split('.').next().unwrap_or("");
                if !schema.eq_ignore_ascii_case("pg_catalog")
                    && !schema.eq_ignore_ascii_case("information_schema")
                {
                    validation.valid = false;
                    validation.violations.push(format!(
                        "Cross-schema access not allowed: {}",
                        part
                    ));
                }
            }
        }
    }

    validation
}

/// Result of query validation
#[derive(Debug, Clone)]
pub struct QueryValidation {
    /// Whether query is valid
    pub valid: bool,

    /// List of violations
    pub violations: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_row_config(tenant_id: &str) -> TenantConfig {
        TenantConfig::builder()
            .id(tenant_id)
            .name("Test")
            .row_isolation("shared_db", "tenant_id")
            .build()
    }

    #[test]
    fn test_transform_select() {
        let transformer = TenantQueryTransformer::new()
            .register_table("users", "tenant_id")
            .register_table("orders", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        let result = transformer.transform(
            "SELECT * FROM users WHERE active = true",
            &tenant,
            &config,
        );

        assert!(result.transformed);
        assert!(result.query.contains("tenant_id = 'acme'"));
        assert!(result.query.contains("AND active = true"));
    }

    #[test]
    fn test_transform_select_no_where() {
        let transformer = TenantQueryTransformer::new()
            .register_table("users", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        let result = transformer.transform(
            "SELECT * FROM users ORDER BY id",
            &tenant,
            &config,
        );

        assert!(result.transformed);
        assert!(result.query.contains("WHERE users.tenant_id = 'acme'"));
        assert!(result.query.contains("ORDER BY id"));
    }

    #[test]
    fn test_transform_update() {
        let transformer = TenantQueryTransformer::new()
            .register_table("users", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        let result = transformer.transform(
            "UPDATE users SET name = 'John' WHERE id = 1",
            &tenant,
            &config,
        );

        assert!(result.transformed);
        assert!(result.query.contains("tenant_id = 'acme'"));
    }

    #[test]
    fn test_transform_delete() {
        let transformer = TenantQueryTransformer::new()
            .register_table("users", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        let result = transformer.transform(
            "DELETE FROM users WHERE id = 1",
            &tenant,
            &config,
        );

        assert!(result.transformed);
        assert!(result.query.contains("tenant_id = 'acme'"));
    }

    #[test]
    fn test_no_transform_for_unregistered_table() {
        let transformer = TenantQueryTransformer::new()
            .register_table("users", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        let result = transformer.transform(
            "SELECT * FROM logs WHERE level = 'error'",
            &tenant,
            &config,
        );

        assert!(!result.transformed);
    }

    #[test]
    fn test_no_transform_for_schema_isolation() {
        let transformer = TenantQueryTransformer::new()
            .register_table("users", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .schema_isolation("shared", "acme")
            .build();

        let result = transformer.transform(
            "SELECT * FROM users",
            &tenant,
            &config,
        );

        assert!(!result.transformed);
    }

    #[test]
    fn test_excluded_tables() {
        let transformer = TenantQueryTransformer::new()
            .register_table("users", "tenant_id")
            .register_table("audit_log", "tenant_id")
            .exclude_table("audit_log");

        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        let result = transformer.transform(
            "SELECT * FROM audit_log",
            &tenant,
            &config,
        );

        assert!(!result.transformed);
    }

    #[test]
    fn test_extract_tables() {
        let transformer = TenantQueryTransformer::new();

        let tables = transformer.extract_tables(
            "SELECT * FROM users u, orders o WHERE u.id = o.user_id"
        );
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));

        let tables = transformer.extract_tables(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        );
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_set_schema_search_path() {
        let transformer = TenantQueryTransformer::new();
        let tenant = TenantId::new("acme");

        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .schema_isolation("shared", "acme_schema")
            .build();

        let path = transformer.set_schema_search_path(&tenant, &config);
        assert_eq!(path, Some("SET search_path TO acme_schema".to_string()));
    }

    #[test]
    fn test_query_validation() {
        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        // Valid query
        let validation = validate_query("SELECT * FROM users", &tenant, &config);
        assert!(validation.valid);

        // Invalid - TRUNCATE
        let validation = validate_query("TRUNCATE users", &tenant, &config);
        assert!(!validation.valid);

        // Invalid - DROP TABLE
        let validation = validate_query("DROP TABLE users", &tenant, &config);
        assert!(!validation.valid);
    }

    #[test]
    fn test_schema_cross_access_validation() {
        let tenant = TenantId::new("acme");
        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .schema_isolation("shared", "acme")
            .build();

        // Valid - own schema
        let validation = validate_query("SELECT * FROM acme.users", &tenant, &config);
        assert!(validation.valid);

        // Invalid - other tenant's schema
        let validation = validate_query("SELECT * FROM other_tenant.users", &tenant, &config);
        assert!(!validation.valid);

        // Valid - system catalog
        let validation = validate_query("SELECT * FROM pg_catalog.pg_tables", &tenant, &config);
        assert!(validation.valid);
    }
}
