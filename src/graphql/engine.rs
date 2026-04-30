//! GraphQL Engine
//!
//! Main entry point for GraphQL query execution.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use super::{
    GraphQLConfig, GraphQLSchema, SqlGenerator, QueryValidator,
    GraphQLMetrics, ExecutionContext, ErrorCode, OperationType,
    QueryPlan, Selection, Filter,
};
use super::sql_generator::FilterOperator;

/// GraphQL request
#[derive(Debug, Clone)]
pub struct GraphQLRequest {
    /// GraphQL query string
    pub query: String,
    /// Operation name (for multi-operation documents)
    pub operation_name: Option<String>,
    /// Query variables
    pub variables: Option<HashMap<String, serde_json::Value>>,
    /// Extensions
    pub extensions: Option<HashMap<String, serde_json::Value>>,
}

impl GraphQLRequest {
    /// Create a new GraphQL request
    pub fn new(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            operation_name: None,
            variables: None,
            extensions: None,
        }
    }

    /// Set operation name
    pub fn with_operation(mut self, name: impl Into<String>) -> Self {
        self.operation_name = Some(name.into());
        self
    }

    /// Set variables
    pub fn with_variables(mut self, vars: HashMap<String, serde_json::Value>) -> Self {
        self.variables = Some(vars);
        self
    }

    /// Add a variable
    pub fn var(mut self, name: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        let vars = self.variables.get_or_insert_with(HashMap::new);
        vars.insert(name.into(), value.into());
        self
    }
}

/// GraphQL response
#[derive(Debug, Clone)]
pub struct GraphQLResponse {
    /// Response data
    pub data: Option<serde_json::Value>,
    /// Errors
    pub errors: Option<Vec<GraphQLError>>,
    /// Extensions (timing, tracing, etc.)
    pub extensions: Option<HashMap<String, serde_json::Value>>,
}

impl GraphQLResponse {
    /// Create a successful response
    pub fn success(data: serde_json::Value) -> Self {
        Self {
            data: Some(data),
            errors: None,
            extensions: None,
        }
    }

    /// Create an error response
    pub fn error(error: GraphQLError) -> Self {
        Self {
            data: None,
            errors: Some(vec![error]),
            extensions: None,
        }
    }

    /// Create a response with multiple errors
    pub fn errors(errors: Vec<GraphQLError>) -> Self {
        Self {
            data: None,
            errors: Some(errors),
            extensions: None,
        }
    }

    /// Add extension data
    pub fn with_extension(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        let extensions = self.extensions.get_or_insert_with(HashMap::new);
        extensions.insert(key.into(), value);
        self
    }

    /// Check if the response has errors
    pub fn has_errors(&self) -> bool {
        self.errors.as_ref().map(|e| !e.is_empty()).unwrap_or(false)
    }

    /// Convert to JSON
    pub fn to_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();

        if let Some(ref data) = self.data {
            result.insert("data".to_string(), data.clone());
        }

        if let Some(ref errors) = self.errors {
            let error_array: Vec<_> = errors.iter().map(|e| e.to_json()).collect();
            result.insert("errors".to_string(), serde_json::Value::Array(error_array));
        }

        if let Some(ref extensions) = self.extensions {
            result.insert(
                "extensions".to_string(),
                serde_json::Value::Object(
                    extensions.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                ),
            );
        }

        serde_json::Value::Object(result)
    }
}

/// GraphQL error
#[derive(Debug, Clone)]
pub struct GraphQLError {
    /// Error message
    pub message: String,
    /// Error locations in the query
    pub locations: Option<Vec<ErrorLocation>>,
    /// Path to the field that caused the error
    pub path: Option<Vec<PathSegment>>,
    /// Error extensions
    pub extensions: Option<HashMap<String, serde_json::Value>>,
    /// Error code
    pub code: ErrorCode,
}

impl GraphQLError {
    /// Create a new error
    pub fn new(message: impl Into<String>, code: ErrorCode) -> Self {
        Self {
            message: message.into(),
            locations: None,
            path: None,
            extensions: None,
            code,
        }
    }

    /// Create a parse error
    pub fn parse_error(message: impl Into<String>) -> Self {
        Self::new(message, ErrorCode::ParseError)
    }

    /// Create a validation error
    pub fn validation_error(message: impl Into<String>) -> Self {
        Self::new(message, ErrorCode::ValidationError)
    }

    /// Create an authorization error
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(message, ErrorCode::Unauthorized)
    }

    /// Create a not found error
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(message, ErrorCode::NotFound)
    }

    /// Create an internal error
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(message, ErrorCode::InternalError)
    }

    /// Set location
    pub fn with_location(mut self, line: u32, column: u32) -> Self {
        self.locations = Some(vec![ErrorLocation { line, column }]);
        self
    }

    /// Set path
    pub fn with_path(mut self, path: Vec<PathSegment>) -> Self {
        self.path = Some(path);
        self
    }

    /// Add extension
    pub fn with_extension(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        let extensions = self.extensions.get_or_insert_with(HashMap::new);
        extensions.insert(key.into(), value);
        self
    }

    /// Convert to JSON
    pub fn to_json(&self) -> serde_json::Value {
        let mut result = serde_json::Map::new();
        result.insert("message".to_string(), serde_json::Value::String(self.message.clone()));

        if let Some(ref locations) = self.locations {
            let loc_array: Vec<_> = locations.iter().map(|l| {
                let mut loc = serde_json::Map::new();
                loc.insert("line".to_string(), serde_json::Value::Number(l.line.into()));
                loc.insert("column".to_string(), serde_json::Value::Number(l.column.into()));
                serde_json::Value::Object(loc)
            }).collect();
            result.insert("locations".to_string(), serde_json::Value::Array(loc_array));
        }

        if let Some(ref path) = self.path {
            let path_array: Vec<_> = path.iter().map(|s| s.to_json()).collect();
            result.insert("path".to_string(), serde_json::Value::Array(path_array));
        }

        let mut extensions = self.extensions.clone().unwrap_or_default();
        extensions.insert(
            "code".to_string(),
            serde_json::Value::String(format!("{:?}", self.code)),
        );
        result.insert(
            "extensions".to_string(),
            serde_json::Value::Object(extensions.into_iter().collect()),
        );

        serde_json::Value::Object(result)
    }
}

impl std::fmt::Display for GraphQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for GraphQLError {}

/// Error location in the GraphQL document
#[derive(Debug, Clone, Copy)]
pub struct ErrorLocation {
    /// Line number (1-based)
    pub line: u32,
    /// Column number (1-based)
    pub column: u32,
}

/// Path segment (field name or array index)
#[derive(Debug, Clone)]
pub enum PathSegment {
    /// Field name
    Field(String),
    /// Array index
    Index(usize),
}

impl PathSegment {
    /// Convert to JSON value
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            PathSegment::Field(name) => serde_json::Value::String(name.clone()),
            PathSegment::Index(idx) => serde_json::Value::Number((*idx).into()),
        }
    }
}

/// Parsed GraphQL document
#[derive(Debug, Clone)]
pub struct ParsedDocument {
    /// Operation type
    pub operation_type: OperationType,
    /// Operation name
    pub operation_name: Option<String>,
    /// Selection set
    pub selections: Vec<ParsedSelection>,
    /// Variable definitions
    pub variable_definitions: Vec<VariableDefinition>,
    /// Fragment definitions
    pub fragments: HashMap<String, FragmentDefinition>,
}

/// Parsed field selection
#[derive(Debug, Clone)]
pub struct ParsedSelection {
    /// Field name
    pub name: String,
    /// Alias
    pub alias: Option<String>,
    /// Arguments
    pub arguments: HashMap<String, serde_json::Value>,
    /// Nested selections
    pub selections: Vec<ParsedSelection>,
    /// Directives
    pub directives: Vec<Directive>,
}

impl ParsedSelection {
    /// Get the response key (alias or name)
    pub fn response_key(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Variable definition
#[derive(Debug, Clone)]
pub struct VariableDefinition {
    /// Variable name (without $)
    pub name: String,
    /// Type string
    pub var_type: String,
    /// Default value
    pub default_value: Option<serde_json::Value>,
}

/// Fragment definition
#[derive(Debug, Clone)]
pub struct FragmentDefinition {
    /// Fragment name
    pub name: String,
    /// Type condition
    pub type_condition: String,
    /// Selections
    pub selections: Vec<ParsedSelection>,
}

/// GraphQL directive
#[derive(Debug, Clone)]
pub struct Directive {
    /// Directive name
    pub name: String,
    /// Arguments
    pub arguments: HashMap<String, serde_json::Value>,
}

/// GraphQL Engine
///
/// Main entry point for GraphQL query execution.
#[derive(Debug)]
pub struct GraphQLEngine {
    /// Configuration
    config: Arc<GraphQLConfig>,
    /// Schema
    schema: Arc<GraphQLSchema>,
    /// SQL generator
    sql_generator: Arc<SqlGenerator>,
    /// Query validator
    validator: QueryValidator,
    /// Metrics
    metrics: Arc<GraphQLMetrics>,
}

impl GraphQLEngine {
    /// Create a new GraphQL engine
    pub fn new(config: GraphQLConfig, schema: GraphQLSchema) -> Self {
        let config = Arc::new(config);
        let schema = Arc::new(schema);

        Self {
            sql_generator: Arc::new(SqlGenerator::new(schema.clone())),
            validator: QueryValidator::new(config.clone()),
            metrics: Arc::new(GraphQLMetrics::new()),
            config,
            schema,
        }
    }

    /// Execute a GraphQL request
    pub async fn execute(&self, request: GraphQLRequest) -> GraphQLResponse {
        self.execute_with_context(request, ExecutionContext::default()).await
    }

    /// Execute a GraphQL request with context
    pub async fn execute_with_context(
        &self,
        request: GraphQLRequest,
        context: ExecutionContext,
    ) -> GraphQLResponse {
        let start = Instant::now();

        // 1. Parse the query
        let document = match self.parse(&request.query) {
            Ok(doc) => doc,
            Err(e) => {
                self.metrics.record_error(&e);
                return GraphQLResponse::error(e);
            }
        };

        // 2. Validate the query
        if let Err(e) = self.validate(&document) {
            self.metrics.record_error(&e);
            return GraphQLResponse::error(e);
        }

        // 3. Check authorization
        if let Err(e) = self.authorize(&document, &context) {
            self.metrics.record_error(&e);
            return GraphQLResponse::error(e);
        }

        // 4. Plan query execution
        let plan = match self.plan(&document, &request.variables) {
            Ok(p) => p,
            Err(e) => {
                self.metrics.record_error(&e);
                return GraphQLResponse::error(e);
            }
        };

        // 5. Generate SQL
        let sql_queries = match self.sql_generator.generate(&plan) {
            Ok(queries) => queries,
            Err(e) => {
                let error = GraphQLError::internal(format!("SQL generation failed: {}", e));
                self.metrics.record_error(&error);
                return GraphQLResponse::error(error);
            }
        };

        // 6. Execute SQL (mock for now - would use actual database connection)
        let results = match self.execute_queries(&sql_queries, &context).await {
            Ok(r) => r,
            Err(e) => {
                self.metrics.record_error(&e);
                return GraphQLResponse::error(e);
            }
        };

        // 7. Shape response
        let data = match self.shape_response(&document, &results) {
            Ok(d) => d,
            Err(e) => {
                self.metrics.record_error(&e);
                return GraphQLResponse::error(e);
            }
        };

        let elapsed = start.elapsed();
        self.metrics.record_query(elapsed, document.operation_type);

        GraphQLResponse::success(data)
            .with_extension("timing", serde_json::json!({
                "durationMs": elapsed.as_millis()
            }))
    }

    /// Parse a GraphQL query string
    fn parse(&self, query: &str) -> Result<ParsedDocument, GraphQLError> {
        // Simple parser for basic queries
        // In production, would use a proper GraphQL parser
        let query = query.trim();

        // Detect operation type
        let (operation_type, remaining) = if query.starts_with("mutation") {
            (OperationType::Mutation, query.strip_prefix("mutation").unwrap_or(query))
        } else if query.starts_with("subscription") {
            (OperationType::Subscription, query.strip_prefix("subscription").unwrap_or(query))
        } else if query.starts_with("query") {
            (OperationType::Query, query.strip_prefix("query").unwrap_or(query))
        } else if query.starts_with("{") {
            (OperationType::Query, query)
        } else {
            return Err(GraphQLError::parse_error("Invalid query format"));
        };

        // Extract operation name if present
        let remaining = remaining.trim();
        let (operation_name, remaining) = if remaining.starts_with('{') {
            (None, remaining)
        } else if let Some(brace_pos) = remaining.find('{') {
            let name_part = remaining[..brace_pos].trim();
            // Handle variables in name (e.g., "GetUser($id: ID!)")
            let name = name_part.split('(').next().unwrap_or(name_part).trim();
            if name.is_empty() {
                (None, &remaining[brace_pos..])
            } else {
                (Some(name.to_string()), &remaining[brace_pos..])
            }
        } else {
            return Err(GraphQLError::parse_error("Missing selection set"));
        };

        // Parse selection set
        let selections = self.parse_selection_set(remaining)?;

        Ok(ParsedDocument {
            operation_type,
            operation_name,
            selections,
            variable_definitions: Vec::new(),
            fragments: HashMap::new(),
        })
    }

    /// Parse a selection set
    fn parse_selection_set(&self, input: &str) -> Result<Vec<ParsedSelection>, GraphQLError> {
        let input = input.trim();

        if !input.starts_with('{') {
            return Err(GraphQLError::parse_error("Expected '{'"));
        }

        // Find matching closing brace
        let mut depth = 0;
        let mut end_pos = 0;
        for (i, c) in input.chars().enumerate() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end_pos = i;
                        break;
                    }
                }
                _ => {}
            }
        }

        if depth != 0 {
            return Err(GraphQLError::parse_error("Unmatched braces"));
        }

        let inner = &input[1..end_pos].trim();
        self.parse_fields(inner)
    }

    /// Parse fields in a selection set
    fn parse_fields(&self, input: &str) -> Result<Vec<ParsedSelection>, GraphQLError> {
        let mut selections = Vec::new();
        let mut current_pos = 0;
        let chars: Vec<char> = input.chars().collect();

        while current_pos < chars.len() {
            // Skip whitespace
            while current_pos < chars.len() && chars[current_pos].is_whitespace() {
                current_pos += 1;
            }

            if current_pos >= chars.len() {
                break;
            }

            // Parse field name (possibly with alias)
            let field_start = current_pos;
            while current_pos < chars.len()
                && (chars[current_pos].is_alphanumeric() || chars[current_pos] == '_')
            {
                current_pos += 1;
            }

            if current_pos == field_start {
                current_pos += 1;
                continue;
            }

            let mut field_name: String = chars[field_start..current_pos].iter().collect();
            let mut alias = None;

            // Check for alias
            while current_pos < chars.len() && chars[current_pos].is_whitespace() {
                current_pos += 1;
            }

            if current_pos < chars.len() && chars[current_pos] == ':' {
                alias = Some(field_name);
                current_pos += 1;

                // Skip whitespace
                while current_pos < chars.len() && chars[current_pos].is_whitespace() {
                    current_pos += 1;
                }

                // Parse actual field name
                let name_start = current_pos;
                while current_pos < chars.len()
                    && (chars[current_pos].is_alphanumeric() || chars[current_pos] == '_')
                {
                    current_pos += 1;
                }
                field_name = chars[name_start..current_pos].iter().collect();
            }

            // Skip whitespace
            while current_pos < chars.len() && chars[current_pos].is_whitespace() {
                current_pos += 1;
            }

            // Parse arguments if present
            let mut arguments = HashMap::new();
            if current_pos < chars.len() && chars[current_pos] == '(' {
                let args_start = current_pos;
                let mut depth = 1;
                current_pos += 1;

                while current_pos < chars.len() && depth > 0 {
                    match chars[current_pos] {
                        '(' => depth += 1,
                        ')' => depth -= 1,
                        _ => {}
                    }
                    current_pos += 1;
                }

                let args_str: String = chars[args_start + 1..current_pos - 1].iter().collect();
                arguments = self.parse_arguments(&args_str)?;
            }

            // Skip whitespace
            while current_pos < chars.len() && chars[current_pos].is_whitespace() {
                current_pos += 1;
            }

            // Parse nested selection set if present
            let nested_selections = if current_pos < chars.len() && chars[current_pos] == '{' {
                let nested_start = current_pos;
                let mut depth = 1;
                current_pos += 1;

                while current_pos < chars.len() && depth > 0 {
                    match chars[current_pos] {
                        '{' => depth += 1,
                        '}' => depth -= 1,
                        _ => {}
                    }
                    current_pos += 1;
                }

                let nested_str: String = chars[nested_start..current_pos].iter().collect();
                self.parse_selection_set(&nested_str)?
            } else {
                Vec::new()
            };

            selections.push(ParsedSelection {
                name: field_name,
                alias,
                arguments,
                selections: nested_selections,
                directives: Vec::new(),
            });
        }

        Ok(selections)
    }

    /// Parse arguments
    fn parse_arguments(&self, input: &str) -> Result<HashMap<String, serde_json::Value>, GraphQLError> {
        let mut arguments = HashMap::new();

        for part in input.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if let Some(colon_pos) = part.find(':') {
                let key = part[..colon_pos].trim().to_string();
                let value_str = part[colon_pos + 1..].trim();

                let value = self.parse_value(value_str)?;
                arguments.insert(key, value);
            }
        }

        Ok(arguments)
    }

    /// Parse a GraphQL value
    fn parse_value(&self, input: &str) -> Result<serde_json::Value, GraphQLError> {
        let input = input.trim();

        if input == "null" {
            Ok(serde_json::Value::Null)
        } else if input == "true" {
            Ok(serde_json::Value::Bool(true))
        } else if input == "false" {
            Ok(serde_json::Value::Bool(false))
        } else if input.starts_with('"') && input.ends_with('"') {
            Ok(serde_json::Value::String(input[1..input.len() - 1].to_string()))
        } else if let Ok(n) = input.parse::<i64>() {
            Ok(serde_json::Value::Number(n.into()))
        } else if let Ok(n) = input.parse::<f64>() {
            Ok(serde_json::json!(n))
        } else {
            // Treat as enum value or variable reference
            Ok(serde_json::Value::String(input.to_string()))
        }
    }

    /// Validate a parsed document
    fn validate(&self, document: &ParsedDocument) -> Result<(), GraphQLError> {
        self.validator.validate(document, &self.schema)
    }

    /// Check authorization
    fn authorize(&self, _document: &ParsedDocument, _context: &ExecutionContext) -> Result<(), GraphQLError> {
        // Authorization checks would go here
        // For now, allow all queries
        Ok(())
    }

    /// Plan query execution
    fn plan(
        &self,
        document: &ParsedDocument,
        _variables: &Option<HashMap<String, serde_json::Value>>,
    ) -> Result<QueryPlan, GraphQLError> {
        // Convert parsed document to query plan
        let selections: Vec<_> = document.selections.iter()
            .map(|s| self.selection_to_plan(s))
            .collect();

        Ok(QueryPlan::Multiple { plans: selections })
    }

    /// Convert a selection to a plan
    fn selection_to_plan(&self, selection: &ParsedSelection) -> QueryPlan {
        // Get filters from arguments
        let filters = self.extract_filters(&selection.arguments);

        // Get limit/offset from arguments
        let limit = selection.arguments.get("limit")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        let offset = selection.arguments.get("offset")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);

        // Build selection
        let sel = Selection {
            table_name: super::to_snake_case(&selection.name),
            fields: selection.selections.iter()
                .filter(|s| s.selections.is_empty())
                .map(|s| s.name.clone())
                .collect(),
            relationships: selection.selections.iter()
                .filter(|s| !s.selections.is_empty())
                .map(|s| (s.name.clone(), self.selection_to_plan(s)))
                .collect(),
        };

        QueryPlan::Single {
            selection: sel,
            filters,
            limit,
            offset,
        }
    }

    /// Extract filters from arguments
    fn extract_filters(&self, arguments: &HashMap<String, serde_json::Value>) -> Vec<Filter> {
        let mut filters = Vec::new();

        // Handle 'id' argument
        if let Some(id) = arguments.get("id") {
            filters.push(Filter {
                field: "id".to_string(),
                operator: FilterOperator::Eq,
                value: id.clone(),
            });
        }

        // Handle 'where' argument
        if let Some(where_obj) = arguments.get("where") {
            if let Some(obj) = where_obj.as_object() {
                for (field, condition) in obj {
                    if let Some(cond_obj) = condition.as_object() {
                        for (op, value) in cond_obj {
                            let operator = match op.as_str() {
                                "eq" => FilterOperator::Eq,
                                "ne" => FilterOperator::Ne,
                                "gt" => FilterOperator::Gt,
                                "gte" => FilterOperator::Gte,
                                "lt" => FilterOperator::Lt,
                                "lte" => FilterOperator::Lte,
                                "contains" => FilterOperator::Contains,
                                "startsWith" => FilterOperator::StartsWith,
                                "endsWith" => FilterOperator::EndsWith,
                                "in" => FilterOperator::In,
                                _ => continue,
                            };

                            filters.push(Filter {
                                field: field.clone(),
                                operator,
                                value: value.clone(),
                            });
                        }
                    }
                }
            }
        }

        filters
    }

    /// Execute SQL queries
    async fn execute_queries(
        &self,
        queries: &[super::SqlQuery],
        _context: &ExecutionContext,
    ) -> Result<Vec<Vec<serde_json::Value>>, GraphQLError> {
        // Mock execution - in production would use database connection
        // For now, return empty results
        Ok(queries.iter().map(|_| Vec::new()).collect())
    }

    /// Shape the response according to the GraphQL query
    fn shape_response(
        &self,
        document: &ParsedDocument,
        _results: &[Vec<serde_json::Value>],
    ) -> Result<serde_json::Value, GraphQLError> {
        // Build response structure from selections
        let mut data = serde_json::Map::new();

        for selection in &document.selections {
            let key = selection.response_key().to_string();
            // In production, would match results to selections
            // For now, return null for each field
            data.insert(key, serde_json::Value::Null);
        }

        Ok(serde_json::Value::Object(data))
    }

    /// Get the schema
    pub fn schema(&self) -> &GraphQLSchema {
        &self.schema
    }

    /// Get the configuration
    pub fn config(&self) -> &GraphQLConfig {
        &self.config
    }

    /// Get metrics
    pub fn metrics(&self) -> &GraphQLMetrics {
        &self.metrics
    }

    /// Generate SDL (Schema Definition Language)
    pub fn generate_sdl(&self) -> String {
        self.schema.to_sdl()
    }
}

impl Clone for GraphQLEngine {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            schema: self.schema.clone(),
            sql_generator: self.sql_generator.clone(),
            validator: QueryValidator::new(self.config.clone()),
            metrics: self.metrics.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graphql::introspector::GraphQLSchema;

    fn create_test_engine() -> GraphQLEngine {
        let config = GraphQLConfig::default();
        let schema = GraphQLSchema::new();
        GraphQLEngine::new(config, schema)
    }

    #[test]
    fn test_parse_simple_query() {
        let engine = create_test_engine();
        let query = "query { users { id name } }";

        let doc = engine.parse(query).unwrap();
        assert_eq!(doc.operation_type, OperationType::Query);
        assert_eq!(doc.selections.len(), 1);
        assert_eq!(doc.selections[0].name, "users");
        assert_eq!(doc.selections[0].selections.len(), 2);
    }

    #[test]
    fn test_parse_named_query() {
        let engine = create_test_engine();
        let query = "query GetUsers { users { id } }";

        let doc = engine.parse(query).unwrap();
        assert_eq!(doc.operation_name, Some("GetUsers".to_string()));
    }

    #[test]
    fn test_parse_mutation() {
        let engine = create_test_engine();
        let query = "mutation { createUser(name: \"test\") { id } }";

        let doc = engine.parse(query).unwrap();
        assert_eq!(doc.operation_type, OperationType::Mutation);
    }

    #[test]
    fn test_parse_with_arguments() {
        let engine = create_test_engine();
        let query = "{ user(id: \"123\") { name } }";

        let doc = engine.parse(query).unwrap();
        let user_selection = &doc.selections[0];
        assert_eq!(user_selection.name, "user");
        assert!(user_selection.arguments.contains_key("id"));
    }

    #[test]
    fn test_parse_with_alias() {
        let engine = create_test_engine();
        let query = "{ myUser: user(id: \"123\") { name } }";

        let doc = engine.parse(query).unwrap();
        let selection = &doc.selections[0];
        assert_eq!(selection.alias, Some("myUser".to_string()));
        assert_eq!(selection.name, "user");
        assert_eq!(selection.response_key(), "myUser");
    }

    #[test]
    fn test_graphql_request_builder() {
        let request = GraphQLRequest::new("{ users { id } }")
            .with_operation("GetUsers")
            .var("limit", 10);

        assert_eq!(request.query, "{ users { id } }");
        assert_eq!(request.operation_name, Some("GetUsers".to_string()));
        assert!(request.variables.unwrap().contains_key("limit"));
    }

    #[test]
    fn test_graphql_response_success() {
        let response = GraphQLResponse::success(serde_json::json!({"users": []}));

        assert!(response.data.is_some());
        assert!(!response.has_errors());
    }

    #[test]
    fn test_graphql_response_error() {
        let error = GraphQLError::parse_error("Syntax error");
        let response = GraphQLResponse::error(error);

        assert!(response.data.is_none());
        assert!(response.has_errors());
    }

    #[test]
    fn test_graphql_error_to_json() {
        let error = GraphQLError::validation_error("Field not found")
            .with_location(1, 10)
            .with_path(vec![PathSegment::Field("users".to_string())]);

        let json = error.to_json();
        assert_eq!(json["message"], "Field not found");
        assert!(json["locations"].is_array());
        assert!(json["path"].is_array());
    }

    #[tokio::test]
    async fn test_execute_simple_query() {
        let engine = create_test_engine();
        let request = GraphQLRequest::new("{ users { id name } }");

        let response = engine.execute(request).await;

        // Should succeed even with mock results
        assert!(!response.has_errors());
        assert!(response.data.is_some());
    }

    #[test]
    fn test_parse_nested_selections() {
        let engine = create_test_engine();
        let query = "{ users { id posts { title comments { content } } } }";

        let doc = engine.parse(query).unwrap();
        let users = &doc.selections[0];
        assert_eq!(users.selections.len(), 2); // id, posts

        let posts = &users.selections[1];
        assert_eq!(posts.name, "posts");
        assert_eq!(posts.selections.len(), 2); // title, comments
    }

    #[test]
    fn test_parse_value() {
        let engine = create_test_engine();

        assert_eq!(engine.parse_value("null").unwrap(), serde_json::Value::Null);
        assert_eq!(engine.parse_value("true").unwrap(), serde_json::Value::Bool(true));
        assert_eq!(engine.parse_value("false").unwrap(), serde_json::Value::Bool(false));
        assert_eq!(engine.parse_value("\"hello\"").unwrap(), serde_json::Value::String("hello".to_string()));
        assert_eq!(engine.parse_value("42").unwrap(), serde_json::json!(42));
    }
}
