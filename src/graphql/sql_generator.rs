//! SQL Generator
//!
//! Generates optimized SQL from GraphQL queries.

use std::collections::HashMap;
use std::sync::Arc;

use super::{GraphQLSchema, RelationType, to_snake_case};

/// SQL query with parameters
#[derive(Debug, Clone)]
pub struct SqlQuery {
    /// SQL statement
    pub sql: String,
    /// Query parameters
    pub params: Vec<serde_json::Value>,
    /// Source table
    pub table: Option<String>,
    /// Is a count query
    pub is_count: bool,
}

impl SqlQuery {
    /// Create a new SQL query
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
            table: None,
            is_count: false,
        }
    }

    /// Add a parameter
    pub fn param(mut self, value: serde_json::Value) -> Self {
        self.params.push(value);
        self
    }

    /// Set the source table
    pub fn from_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Mark as count query
    pub fn count(mut self) -> Self {
        self.is_count = true;
        self
    }

    /// Get the parameter placeholder for the given index
    pub fn placeholder(index: usize) -> String {
        format!("${}", index + 1)
    }
}

/// Query plan for SQL generation
#[derive(Debug, Clone)]
pub enum QueryPlan {
    /// Single table query
    Single {
        /// Selection
        selection: Selection,
        /// Filters
        filters: Vec<Filter>,
        /// Limit
        limit: Option<u32>,
        /// Offset
        offset: Option<u32>,
    },
    /// Query with relationship (JOIN or LATERAL)
    Relationship {
        /// Parent selection
        parent: Selection,
        /// Child selection
        child: Selection,
        /// Relationship type
        relation_type: RelationType,
        /// Join condition
        join_column: String,
        /// Parent join column
        parent_column: String,
    },
    /// Batch multiple queries
    Batch {
        /// Individual queries to batch
        queries: Vec<QueryPlan>,
    },
    /// Multiple independent plans
    Multiple {
        /// Plans
        plans: Vec<QueryPlan>,
    },
}

/// Field selection
#[derive(Debug, Clone)]
pub struct Selection {
    /// Table name
    pub table_name: String,
    /// Selected fields
    pub fields: Vec<String>,
    /// Nested relationships
    pub relationships: Vec<(String, QueryPlan)>,
}

impl Selection {
    /// Create a new selection
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            fields: Vec::new(),
            relationships: Vec::new(),
        }
    }

    /// Add a field
    pub fn field(mut self, name: impl Into<String>) -> Self {
        self.fields.push(name.into());
        self
    }

    /// Add multiple fields
    pub fn fields(mut self, names: Vec<String>) -> Self {
        self.fields.extend(names);
        self
    }

    /// Add a relationship
    pub fn relationship(mut self, name: impl Into<String>, plan: QueryPlan) -> Self {
        self.relationships.push((name.into(), plan));
        self
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Get the primary key column (assuming "id" for now)
    pub fn primary_key(&self) -> &str {
        "id"
    }
}

/// Filter condition
#[derive(Debug, Clone)]
pub struct Filter {
    /// Field name
    pub field: String,
    /// Operator
    pub operator: FilterOperator,
    /// Value
    pub value: serde_json::Value,
}

impl Filter {
    /// Create a new filter
    pub fn new(field: impl Into<String>, operator: FilterOperator, value: serde_json::Value) -> Self {
        Self {
            field: field.into(),
            operator,
            value,
        }
    }

    /// Create an equality filter
    pub fn eq(field: impl Into<String>, value: serde_json::Value) -> Self {
        Self::new(field, FilterOperator::Eq, value)
    }

    /// Create a not-equal filter
    pub fn ne(field: impl Into<String>, value: serde_json::Value) -> Self {
        Self::new(field, FilterOperator::Ne, value)
    }

    /// Create a greater-than filter
    pub fn gt(field: impl Into<String>, value: serde_json::Value) -> Self {
        Self::new(field, FilterOperator::Gt, value)
    }

    /// Create an IN filter
    pub fn in_values(field: impl Into<String>, values: Vec<serde_json::Value>) -> Self {
        Self::new(field, FilterOperator::In, serde_json::Value::Array(values))
    }
}

/// Filter operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOperator {
    /// Equals
    Eq,
    /// Not equals
    Ne,
    /// Greater than
    Gt,
    /// Greater than or equal
    Gte,
    /// Less than
    Lt,
    /// Less than or equal
    Lte,
    /// Contains (LIKE %value%)
    Contains,
    /// Starts with (LIKE value%)
    StartsWith,
    /// Ends with (LIKE %value)
    EndsWith,
    /// In list
    In,
    /// Not in list
    NotIn,
    /// Is null
    IsNull,
    /// Is not null
    IsNotNull,
}

impl FilterOperator {
    /// Get the SQL operator
    pub fn to_sql(&self) -> &'static str {
        match self {
            FilterOperator::Eq => "=",
            FilterOperator::Ne => "<>",
            FilterOperator::Gt => ">",
            FilterOperator::Gte => ">=",
            FilterOperator::Lt => "<",
            FilterOperator::Lte => "<=",
            FilterOperator::Contains => "LIKE",
            FilterOperator::StartsWith => "LIKE",
            FilterOperator::EndsWith => "LIKE",
            FilterOperator::In => "IN",
            FilterOperator::NotIn => "NOT IN",
            FilterOperator::IsNull => "IS NULL",
            FilterOperator::IsNotNull => "IS NOT NULL",
        }
    }

    /// Check if operator needs a value
    pub fn needs_value(&self) -> bool {
        !matches!(self, FilterOperator::IsNull | FilterOperator::IsNotNull)
    }

    /// Check if operator uses LIKE patterns
    pub fn is_like(&self) -> bool {
        matches!(
            self,
            FilterOperator::Contains | FilterOperator::StartsWith | FilterOperator::EndsWith
        )
    }
}

/// SQL Generator
#[derive(Debug)]
pub struct SqlGenerator {
    /// Schema reference
    schema: Arc<GraphQLSchema>,
    /// Quote identifier character
    quote_char: char,
    /// Parameter style
    param_style: ParamStyle,
}

/// Parameter placeholder style
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamStyle {
    /// Positional ($1, $2, ...)
    Positional,
    /// Named (:name)
    Named,
    /// Question mark (?)
    QuestionMark,
}

impl SqlGenerator {
    /// Create a new SQL generator
    pub fn new(schema: Arc<GraphQLSchema>) -> Self {
        Self {
            schema,
            quote_char: '"',
            param_style: ParamStyle::Positional,
        }
    }

    /// Set the quote character for identifiers
    pub fn with_quote_char(mut self, char: char) -> Self {
        self.quote_char = char;
        self
    }

    /// Set the parameter style
    pub fn with_param_style(mut self, style: ParamStyle) -> Self {
        self.param_style = style;
        self
    }

    /// Generate SQL from a query plan
    pub fn generate(&self, plan: &QueryPlan) -> Result<Vec<SqlQuery>, SqlGeneratorError> {
        match plan {
            QueryPlan::Single { selection, filters, limit, offset } => {
                Ok(vec![self.generate_single(selection, filters, *limit, *offset)?])
            }
            QueryPlan::Relationship { parent, child, relation_type, join_column, parent_column } => {
                self.generate_relationship(parent, child, *relation_type, join_column, parent_column)
            }
            QueryPlan::Batch { queries } => {
                self.generate_batch(queries)
            }
            QueryPlan::Multiple { plans } => {
                let mut results = Vec::new();
                for p in plans {
                    results.extend(self.generate(p)?);
                }
                Ok(results)
            }
        }
    }

    /// Generate SQL for a single table query
    fn generate_single(
        &self,
        selection: &Selection,
        filters: &[Filter],
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<SqlQuery, SqlGeneratorError> {
        let mut params = Vec::new();
        let mut param_index = 0;

        // Build SELECT clause
        let columns = if selection.fields.is_empty() {
            "*".to_string()
        } else {
            selection.fields.iter()
                .map(|f| self.quote_identifier(&to_snake_case(f)))
                .collect::<Vec<_>>()
                .join(", ")
        };

        // Build FROM clause
        let table = self.quote_identifier(&selection.table_name);

        // Build WHERE clause
        let where_clause = if filters.is_empty() {
            String::new()
        } else {
            let conditions: Vec<String> = filters.iter()
                .map(|f| {
                    let col = self.quote_identifier(&to_snake_case(&f.field));
                    if f.operator.needs_value() {
                        param_index += 1;
                        params.push(self.prepare_value(&f.operator, &f.value));
                        format!("{} {} {}", col, f.operator.to_sql(), self.placeholder(param_index - 1))
                    } else {
                        format!("{} {}", col, f.operator.to_sql())
                    }
                })
                .collect();
            format!(" WHERE {}", conditions.join(" AND "))
        };

        // Build LIMIT/OFFSET
        let mut limit_offset = String::new();
        if let Some(l) = limit {
            limit_offset.push_str(&format!(" LIMIT {}", l));
        }
        if let Some(o) = offset {
            limit_offset.push_str(&format!(" OFFSET {}", o));
        }

        let sql = format!(
            "SELECT {} FROM {}{}{}",
            columns,
            table,
            where_clause,
            limit_offset
        );

        Ok(SqlQuery {
            sql,
            params,
            table: Some(selection.table_name.clone()),
            is_count: false,
        })
    }

    /// Generate SQL for a relationship query
    fn generate_relationship(
        &self,
        parent: &Selection,
        child: &Selection,
        relation_type: RelationType,
        join_column: &str,
        parent_column: &str,
    ) -> Result<Vec<SqlQuery>, SqlGeneratorError> {
        match relation_type {
            RelationType::OneToOne | RelationType::ManyToOne => {
                // Use JOIN for *-to-one relationships
                Ok(vec![self.generate_with_join(parent, child, join_column, parent_column)?])
            }
            RelationType::OneToMany | RelationType::ManyToMany => {
                // Use LATERAL for *-to-many relationships
                Ok(vec![self.generate_with_lateral(parent, child, join_column, parent_column)?])
            }
        }
    }

    /// Generate SQL with JOIN
    fn generate_with_join(
        &self,
        parent: &Selection,
        child: &Selection,
        join_column: &str,
        parent_column: &str,
    ) -> Result<SqlQuery, SqlGeneratorError> {
        let parent_alias = "p";
        let child_alias = "c";

        let parent_cols: Vec<String> = if parent.fields.is_empty() {
            vec![format!("{}.*", parent_alias)]
        } else {
            parent.fields.iter()
                .map(|f| format!("{}.{}", parent_alias, self.quote_identifier(&to_snake_case(f))))
                .collect()
        };

        let child_cols: Vec<String> = if child.fields.is_empty() {
            vec![format!("{}.*", child_alias)]
        } else {
            child.fields.iter()
                .map(|f| format!("{}.{}", child_alias, self.quote_identifier(&to_snake_case(f))))
                .collect()
        };

        let all_cols = [parent_cols, child_cols].concat();

        let sql = format!(
            "SELECT {} FROM {} {} LEFT JOIN {} {} ON {}.{} = {}.{}",
            all_cols.join(", "),
            self.quote_identifier(&parent.table_name),
            parent_alias,
            self.quote_identifier(&child.table_name),
            child_alias,
            child_alias,
            self.quote_identifier(&to_snake_case(join_column)),
            parent_alias,
            self.quote_identifier(&to_snake_case(parent_column))
        );

        Ok(SqlQuery::new(sql).from_table(&parent.table_name))
    }

    /// Generate SQL with LATERAL subquery
    fn generate_with_lateral(
        &self,
        parent: &Selection,
        child: &Selection,
        join_column: &str,
        parent_column: &str,
    ) -> Result<SqlQuery, SqlGeneratorError> {
        let parent_alias = "p";
        let child_alias = "c";

        let parent_cols: Vec<String> = if parent.fields.is_empty() {
            vec![format!("{}.*", parent_alias)]
        } else {
            parent.fields.iter()
                .map(|f| format!("{}.{}", parent_alias, self.quote_identifier(&to_snake_case(f))))
                .collect()
        };

        let child_cols: Vec<String> = if child.fields.is_empty() {
            vec!["*".to_string()]
        } else {
            child.fields.iter()
                .map(|f| self.quote_identifier(&to_snake_case(f)))
                .collect()
        };

        let sql = format!(
            "SELECT {}, LATERAL (
                SELECT json_agg(sub.*) FROM (
                    SELECT {} FROM {} {} WHERE {}.{} = {}.{}
                ) sub
            ) AS {}
            FROM {} {}",
            parent_cols.join(", "),
            child_cols.join(", "),
            self.quote_identifier(&child.table_name),
            child_alias,
            child_alias,
            self.quote_identifier(&to_snake_case(join_column)),
            parent_alias,
            self.quote_identifier(&to_snake_case(parent_column)),
            self.quote_identifier(&to_snake_case(&child.table_name)),
            self.quote_identifier(&parent.table_name),
            parent_alias
        );

        Ok(SqlQuery::new(sql).from_table(&parent.table_name))
    }

    /// Generate batched SQL queries
    fn generate_batch(&self, queries: &[QueryPlan]) -> Result<Vec<SqlQuery>, SqlGeneratorError> {
        // For now, just generate individual queries
        // In production, could use UNION ALL or multi-statement
        let mut results = Vec::new();
        for query in queries {
            results.extend(self.generate(query)?);
        }
        Ok(results)
    }

    /// Quote an identifier
    fn quote_identifier(&self, name: &str) -> String {
        format!("{}{}{}", self.quote_char, name, self.quote_char)
    }

    /// Get parameter placeholder
    fn placeholder(&self, index: usize) -> String {
        match self.param_style {
            ParamStyle::Positional => format!("${}", index + 1),
            ParamStyle::Named => format!(":p{}", index),
            ParamStyle::QuestionMark => "?".to_string(),
        }
    }

    /// Prepare a value for a filter
    fn prepare_value(&self, operator: &FilterOperator, value: &serde_json::Value) -> serde_json::Value {
        match operator {
            FilterOperator::Contains => {
                if let serde_json::Value::String(s) = value {
                    serde_json::Value::String(format!("%{}%", s))
                } else {
                    value.clone()
                }
            }
            FilterOperator::StartsWith => {
                if let serde_json::Value::String(s) = value {
                    serde_json::Value::String(format!("{}%", s))
                } else {
                    value.clone()
                }
            }
            FilterOperator::EndsWith => {
                if let serde_json::Value::String(s) = value {
                    serde_json::Value::String(format!("%{}", s))
                } else {
                    value.clone()
                }
            }
            _ => value.clone(),
        }
    }

    /// Generate a count query
    pub fn generate_count(&self, table: &str, filters: &[Filter]) -> Result<SqlQuery, SqlGeneratorError> {
        let mut params = Vec::new();
        let mut param_index = 0;

        let where_clause = if filters.is_empty() {
            String::new()
        } else {
            let conditions: Vec<String> = filters.iter()
                .map(|f| {
                    let col = self.quote_identifier(&to_snake_case(&f.field));
                    if f.operator.needs_value() {
                        param_index += 1;
                        params.push(self.prepare_value(&f.operator, &f.value));
                        format!("{} {} {}", col, f.operator.to_sql(), self.placeholder(param_index - 1))
                    } else {
                        format!("{} {}", col, f.operator.to_sql())
                    }
                })
                .collect();
            format!(" WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "SELECT COUNT(*) FROM {}{}",
            self.quote_identifier(table),
            where_clause
        );

        Ok(SqlQuery {
            sql,
            params,
            table: Some(table.to_string()),
            is_count: true,
        })
    }

    /// Generate an INSERT query
    pub fn generate_insert(
        &self,
        table: &str,
        values: &HashMap<String, serde_json::Value>,
    ) -> Result<SqlQuery, SqlGeneratorError> {
        if values.is_empty() {
            return Err(SqlGeneratorError::EmptyValues);
        }

        let columns: Vec<String> = values.keys()
            .map(|k| self.quote_identifier(&to_snake_case(k)))
            .collect();

        let placeholders: Vec<String> = (0..values.len())
            .map(|i| self.placeholder(i))
            .collect();

        let params: Vec<serde_json::Value> = values.values().cloned().collect();

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
            self.quote_identifier(table),
            columns.join(", "),
            placeholders.join(", ")
        );

        Ok(SqlQuery {
            sql,
            params,
            table: Some(table.to_string()),
            is_count: false,
        })
    }

    /// Generate an UPDATE query
    pub fn generate_update(
        &self,
        table: &str,
        id: &serde_json::Value,
        values: &HashMap<String, serde_json::Value>,
    ) -> Result<SqlQuery, SqlGeneratorError> {
        if values.is_empty() {
            return Err(SqlGeneratorError::EmptyValues);
        }

        let set_clauses: Vec<String> = values.keys()
            .enumerate()
            .map(|(i, k)| format!("{} = {}", self.quote_identifier(&to_snake_case(k)), self.placeholder(i)))
            .collect();

        let mut params: Vec<serde_json::Value> = values.values().cloned().collect();
        params.push(id.clone());

        let id_placeholder = self.placeholder(params.len() - 1);

        let sql = format!(
            "UPDATE {} SET {} WHERE {} = {} RETURNING *",
            self.quote_identifier(table),
            set_clauses.join(", "),
            self.quote_identifier("id"),
            id_placeholder
        );

        Ok(SqlQuery {
            sql,
            params,
            table: Some(table.to_string()),
            is_count: false,
        })
    }

    /// Generate a DELETE query
    pub fn generate_delete(&self, table: &str, id: &serde_json::Value) -> Result<SqlQuery, SqlGeneratorError> {
        let sql = format!(
            "DELETE FROM {} WHERE {} = {} RETURNING {}",
            self.quote_identifier(table),
            self.quote_identifier("id"),
            self.placeholder(0),
            self.quote_identifier("id")
        );

        Ok(SqlQuery {
            sql,
            params: vec![id.clone()],
            table: Some(table.to_string()),
            is_count: false,
        })
    }
}

/// SQL generator error
#[derive(Debug, Clone)]
pub enum SqlGeneratorError {
    /// Empty values for insert/update
    EmptyValues,
    /// Invalid filter
    InvalidFilter(String),
    /// Unknown table
    UnknownTable(String),
    /// Invalid relationship
    InvalidRelationship(String),
}

impl std::fmt::Display for SqlGeneratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlGeneratorError::EmptyValues => write!(f, "No values provided"),
            SqlGeneratorError::InvalidFilter(msg) => write!(f, "Invalid filter: {}", msg),
            SqlGeneratorError::UnknownTable(table) => write!(f, "Unknown table: {}", table),
            SqlGeneratorError::InvalidRelationship(msg) => write!(f, "Invalid relationship: {}", msg),
        }
    }
}

impl std::error::Error for SqlGeneratorError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::introspector::GraphQLSchema;

    fn create_generator() -> SqlGenerator {
        let schema = Arc::new(GraphQLSchema::new());
        SqlGenerator::new(schema)
    }

    #[test]
    fn test_generate_simple_select() {
        let generator = create_generator();
        let selection = Selection::new("users")
            .field("id")
            .field("name");

        let plan = QueryPlan::Single {
            selection,
            filters: vec![],
            limit: None,
            offset: None,
        };

        let queries = generator.generate(&plan).unwrap();
        assert_eq!(queries.len(), 1);
        assert!(queries[0].sql.contains("SELECT"));
        assert!(queries[0].sql.contains("\"users\""));
    }

    #[test]
    fn test_generate_with_filters() {
        let generator = create_generator();
        let selection = Selection::new("users");

        let plan = QueryPlan::Single {
            selection,
            filters: vec![
                Filter::eq("id", serde_json::json!("123")),
            ],
            limit: None,
            offset: None,
        };

        let queries = generator.generate(&plan).unwrap();
        assert_eq!(queries.len(), 1);
        assert!(queries[0].sql.contains("WHERE"));
        assert!(queries[0].sql.contains("$1"));
        assert_eq!(queries[0].params.len(), 1);
    }

    #[test]
    fn test_generate_with_limit_offset() {
        let generator = create_generator();
        let selection = Selection::new("users");

        let plan = QueryPlan::Single {
            selection,
            filters: vec![],
            limit: Some(10),
            offset: Some(20),
        };

        let queries = generator.generate(&plan).unwrap();
        assert!(queries[0].sql.contains("LIMIT 10"));
        assert!(queries[0].sql.contains("OFFSET 20"));
    }

    #[test]
    fn test_generate_join() {
        let generator = create_generator();
        let parent = Selection::new("users").field("id").field("name");
        let child = Selection::new("profiles").field("bio");

        let plan = QueryPlan::Relationship {
            parent,
            child,
            relation_type: RelationType::OneToOne,
            join_column: "user_id".to_string(),
            parent_column: "id".to_string(),
        };

        let queries = generator.generate(&plan).unwrap();
        assert_eq!(queries.len(), 1);
        assert!(queries[0].sql.contains("LEFT JOIN"));
    }

    #[test]
    fn test_generate_lateral() {
        let generator = create_generator();
        let parent = Selection::new("users").field("id");
        let child = Selection::new("posts").field("title");

        let plan = QueryPlan::Relationship {
            parent,
            child,
            relation_type: RelationType::OneToMany,
            join_column: "user_id".to_string(),
            parent_column: "id".to_string(),
        };

        let queries = generator.generate(&plan).unwrap();
        assert_eq!(queries.len(), 1);
        assert!(queries[0].sql.contains("LATERAL"));
        assert!(queries[0].sql.contains("json_agg"));
    }

    #[test]
    fn test_generate_count() {
        let generator = create_generator();
        let query = generator.generate_count("users", &[]).unwrap();

        assert!(query.sql.contains("COUNT(*)"));
        assert!(query.is_count);
    }

    #[test]
    fn test_generate_insert() {
        let generator = create_generator();
        let mut values = HashMap::new();
        values.insert("name".to_string(), serde_json::json!("John"));
        values.insert("email".to_string(), serde_json::json!("john@example.com"));

        let query = generator.generate_insert("users", &values).unwrap();

        assert!(query.sql.contains("INSERT INTO"));
        assert!(query.sql.contains("RETURNING"));
        assert_eq!(query.params.len(), 2);
    }

    #[test]
    fn test_generate_update() {
        let generator = create_generator();
        let mut values = HashMap::new();
        values.insert("name".to_string(), serde_json::json!("Jane"));

        let query = generator.generate_update("users", &serde_json::json!("123"), &values).unwrap();

        assert!(query.sql.contains("UPDATE"));
        assert!(query.sql.contains("SET"));
        assert!(query.sql.contains("WHERE"));
        assert!(query.sql.contains("RETURNING"));
    }

    #[test]
    fn test_generate_delete() {
        let generator = create_generator();
        let query = generator.generate_delete("users", &serde_json::json!("123")).unwrap();

        assert!(query.sql.contains("DELETE FROM"));
        assert!(query.sql.contains("WHERE"));
        assert!(query.sql.contains("RETURNING"));
    }

    #[test]
    fn test_filter_operators() {
        assert_eq!(FilterOperator::Eq.to_sql(), "=");
        assert_eq!(FilterOperator::Ne.to_sql(), "<>");
        assert_eq!(FilterOperator::Gt.to_sql(), ">");
        assert_eq!(FilterOperator::Contains.to_sql(), "LIKE");
        assert_eq!(FilterOperator::In.to_sql(), "IN");
        assert_eq!(FilterOperator::IsNull.to_sql(), "IS NULL");
    }

    #[test]
    fn test_like_pattern_preparation() {
        let generator = create_generator();

        let contains = generator.prepare_value(
            &FilterOperator::Contains,
            &serde_json::json!("test")
        );
        assert_eq!(contains, serde_json::json!("%test%"));

        let starts = generator.prepare_value(
            &FilterOperator::StartsWith,
            &serde_json::json!("test")
        );
        assert_eq!(starts, serde_json::json!("test%"));

        let ends = generator.prepare_value(
            &FilterOperator::EndsWith,
            &serde_json::json!("test")
        );
        assert_eq!(ends, serde_json::json!("%test"));
    }

    #[test]
    fn test_empty_insert_error() {
        let generator = create_generator();
        let result = generator.generate_insert("users", &HashMap::new());
        assert!(matches!(result, Err(SqlGeneratorError::EmptyValues)));
    }

    #[test]
    fn test_param_styles() {
        let schema = Arc::new(GraphQLSchema::new());

        let positional = SqlGenerator::new(schema.clone());
        assert_eq!(positional.placeholder(0), "$1");
        assert_eq!(positional.placeholder(2), "$3");

        let question = SqlGenerator::new(schema.clone()).with_param_style(ParamStyle::QuestionMark);
        assert_eq!(question.placeholder(0), "?");

        let named = SqlGenerator::new(schema).with_param_style(ParamStyle::Named);
        assert_eq!(named.placeholder(0), ":p0");
    }
}
