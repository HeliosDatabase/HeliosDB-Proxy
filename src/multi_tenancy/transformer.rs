//! Tenant Query Transformer
//!
//! This module transforms SQL queries to apply tenant isolation, primarily
//! for row-level security where queries need WHERE clause injection.

use std::collections::{HashMap, HashSet};

use super::config::{IsolationStrategy, TenantConfig, TenantId};
use crate::config::DEFAULT_REFUSED_FUNCTIONS;
use crate::sql_text::{lex_units, LexKind, LexUnit};

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

    /// Set when the statement touches a tenant table in a shape the
    /// transformer cannot rewrite with confidence: it must not be forwarded
    /// (the relay answers SQLSTATE 42501). The message says why.
    pub refusal: Option<String>,
}

impl TransformResult {
    /// Create a passthrough result (no transformation)
    pub fn passthrough(query: impl Into<String>) -> Self {
        Self {
            query: query.into(),
            transformed: false,
            filtered_tables: Vec::new(),
            warnings: Vec::new(),
            refusal: None,
        }
    }

    /// Create a transformed result
    pub fn transformed(query: impl Into<String>, tables: Vec<String>) -> Self {
        Self {
            query: query.into(),
            transformed: true,
            filtered_tables: tables,
            warnings: Vec::new(),
            refusal: None,
        }
    }

    /// A refused result: the statement must not be forwarded.
    pub fn refused(reason: impl Into<String>) -> Self {
        let mut res = Self::passthrough(String::new());
        res.refusal = Some(format!(
            "statement refused under row-level tenant isolation: {}",
            reason.into()
        ));
        res
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

    /// Functions a statement may not call under row isolation (lower-case)
    refused_functions: HashSet<String>,
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
            refused_functions: DEFAULT_REFUSED_FUNCTIONS
                .iter()
                .map(|f| f.to_string())
                .collect(),
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
            self.tenant_tables.insert(table.to_lowercase(), col.clone());
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

    /// Replace the functions refused under row isolation (the default is
    /// `DEFAULT_REFUSED_FUNCTIONS`); matched case-insensitively.
    pub fn with_refused_functions<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.refused_functions = names
            .into_iter()
            .map(|name| name.as_ref().to_lowercase())
            .collect();
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

        self.rewrite_row(query, tenant, tenant_column)
    }

    /// Rewrite a statement under row isolation, or refuse it.
    ///
    /// The statement is lexed once by the result-cache lexer ([`lex_units`]),
    /// so keywords count only outside literals, quoted identifiers, comments
    /// and parentheses. Every shape that names a tenant table is either
    /// rewritten here or refused (SQLSTATE 42501, see
    /// [`TransformResult::refusal`]); none is passed through unfiltered.
    fn rewrite_row(&self, query: &str, tenant: &TenantId, column: &str) -> TransformResult {
        if tenant.0.chars().any(|c| c == '\0' || c == '\\') {
            return refuse("the tenant id contains a NUL byte or a backslash");
        }
        let Some(all) = lex_units(query) else {
            // The lexer gave up: where the backends end a token is unknown,
            // so fail closed if any tenant table might be named.
            let lower = query.to_lowercase();
            let named = self
                .tenant_tables
                .keys()
                .any(|t| self.requires_filtering(t) && lower.contains(t.as_str()));
            return if named || lower.contains("u&") {
                refuse("the text is ambiguous to the parser and may name a tenant table")
            } else {
                TransformResult::passthrough(query)
            };
        };
        let units: Vec<LexUnit> = all
            .into_iter()
            .filter(|u| u.kind != LexKind::Comment)
            .collect();
        if (0..units.len()).any(|i| is_unicode_escape(query, &units, i)) {
            return refuse(
                "a Unicode-escaped identifier (U&\"...\") or a UESCAPE clause cannot be read with confidence",
            );
        }
        if let Some(reason) = self.dynamic_sql_reason(query, &units) {
            return refuse(&reason);
        }
        let statements = split_statements(query, &units);
        if statements.len() > 1 {
            return if self.tenant_refs(query, &units).is_empty() {
                TransformResult::passthrough(query)
            } else {
                refuse("more than one statement names a tenant table")
            };
        }
        let Some(stmt) = statements.first().copied() else {
            return TransformResult::passthrough(query);
        };
        let refs = self.tenant_refs(query, stmt);
        if refs.is_empty() {
            return TransformResult::passthrough(query);
        }
        let depth = depths(query, stmt);
        if refs.iter().any(|&i| depth[i] > 0) {
            return refuse(
                "a tenant table is read inside a subquery, CTE or parenthesised expression",
            );
        }
        let first = stmt[0];
        if is_word(query, &first, "select") {
            self.rewrite_select(query, stmt, &depth, &refs, tenant, column)
        } else if is_word(query, &first, "update") {
            match target_at(query, stmt, 1, true) {
                Some(target) => self.rewrite_write(query, stmt, &depth, target, tenant, column),
                None => refuse("the UPDATE target is not a table name"),
            }
        } else if is_word(query, &first, "delete")
            && stmt.get(1).is_some_and(|u| is_word(query, u, "from"))
        {
            match target_at(query, stmt, 2, true) {
                Some(target) => self.rewrite_write(query, stmt, &depth, target, tenant, column),
                None => refuse("the DELETE target is not a table name"),
            }
        } else if is_word(query, &first, "insert")
            && stmt.get(1).is_some_and(|u| is_word(query, u, "into"))
        {
            self.rewrite_insert(query, stmt, &depth, &refs, tenant, column)
        } else {
            refuse("the statement shape is not rewritten (WITH, TABLE, VALUES, a parenthesised query, ...)")
        }
    }

    /// Indices of the units of `stmt` that name a tenant table, except a
    /// qualifier (a unit followed by `.`), which names a FROM item rather
    /// than reading a table.
    fn tenant_refs(&self, sql: &str, stmt: &[LexUnit]) -> Vec<usize> {
        (0..stmt.len())
            .filter(|&i| {
                self.tenant_table_of(sql, &stmt[i]).is_some()
                    && !stmt.get(i + 1).is_some_and(|next| is_punct(sql, next, "."))
            })
            .collect()
    }

    /// The registered tenant table a unit names (lower-case), when it is one
    /// that gets filtered: quoted or not.
    fn tenant_table_of(&self, sql: &str, unit: &LexUnit) -> Option<String> {
        let name = unit_name(sql, unit)?;
        self.requires_filtering(&name).then_some(name)
    }

    /// Rewrite a SELECT. Each branch of a set operation is filtered on its
    /// own FROM and JOIN tenant tables.
    fn rewrite_select(
        &self,
        sql: &str,
        stmt: &[LexUnit],
        depth: &[u32],
        refs: &[usize],
        tenant: &TenantId,
        column: &str,
    ) -> TransformResult {
        let captured = from_items(sql, stmt, depth);
        if captured
            .iter()
            .any(|&i| self.tenant_table_of(sql, &stmt[i]).is_some() && !item_ends(sql, stmt, i))
        {
            return refuse("a tenant table is aliased, which the row filter cannot qualify");
        }
        if refs.iter().any(|i| !captured.contains(i)) {
            return refuse(
                "a tenant table is named outside the FROM and JOIN items the rewrite filters",
            );
        }
        let mut edits = Vec::new();
        let mut filtered: Vec<String> = Vec::new();
        for (start, end) in set_branches(sql, stmt, depth) {
            let mut tables: Vec<String> = Vec::new();
            for i in captured
                .iter()
                .copied()
                .filter(|i| (start..end).contains(i))
            {
                if let Some(name) = self.tenant_table_of(sql, &stmt[i]) {
                    if !tables.contains(&name) {
                        tables.push(name);
                    }
                }
            }
            if tables.is_empty() {
                continue;
            }
            let head = (start..end)
                .find(|&i| !is_word(sql, &stmt[i], "all") && !is_word(sql, &stmt[i], "distinct"));
            if !head.is_some_and(|h| is_word(sql, &stmt[h], "select")) {
                return refuse("a set operation branch that names a tenant table is not a SELECT");
            }
            let filter = self.build_filter(tenant, column, &tables);
            match filter_edits(sql, stmt, depth, start, end, &filter) {
                Some(found) => edits.extend(found),
                None => return refuse("no place to put the tenant filter"),
            }
            filtered.extend(tables);
        }
        TransformResult::transformed(apply_edits(sql, edits), filtered)
    }

    /// Why `units` run dynamic SQL or call a refused function, if they do:
    /// a `DO` or `CALL` statement, or a function from `refused_functions`
    /// (a word followed by `(`, schema-qualified or not). Literals never count.
    fn dynamic_sql_reason(&self, sql: &str, units: &[LexUnit]) -> Option<String> {
        for statement in split_statements(sql, units) {
            if let Some(first) = statement.first() {
                if is_one_of(sql, first, &["do", "call"]) {
                    return Some("DO and CALL can run dynamic SQL".to_string());
                }
            }
        }
        for (i, unit) in units.iter().enumerate() {
            if !matches!(unit.kind, LexKind::Word | LexKind::Ident) {
                continue;
            }
            let called = units
                .get(i + 1)
                .is_some_and(|next| is_punct(sql, next, "("));
            if !called {
                continue;
            }
            if let Some(name) = unit_name(sql, unit) {
                if self.refused_functions.contains(&name) {
                    return Some(format!(
                        "the function {name} is refused under row isolation"
                    ));
                }
            }
        }
        None
    }

    /// The tenant column of the table `unit` names, or `default` for a table
    /// without its own registration.
    fn column_of(&self, sql: &str, unit: &LexUnit, default: &str) -> String {
        self.tenant_table_of(sql, unit)
            .and_then(|name| self.get_tenant_column(&name).map(str::to_string))
            .unwrap_or_else(|| default.to_string())
    }

    /// Rewrite an INSERT into a tenant table. Only `INSERT INTO <table>
    /// (<columns>) VALUES (<row>) [, (<row>) ...] [ON CONFLICT [(<cols>)]
    /// DO NOTHING] [RETURNING ...]` is accepted, and the tenant column and
    /// its value are added to the column list and to every row. Anything
    /// else (no column list, the tenant column in the list, `SELECT`,
    /// `DEFAULT VALUES`, `ON CONFLICT ... DO UPDATE`) is refused: a conflicting
    /// row can belong to another tenant, so it must not be updated.
    fn rewrite_insert(
        &self,
        sql: &str,
        stmt: &[LexUnit],
        depth: &[u32],
        refs: &[usize],
        tenant: &TenantId,
        column: &str,
    ) -> TransformResult {
        if refs != [2] {
            return refuse("an INSERT reads a tenant table besides its target");
        }
        let name = self.tenant_table_of(sql, &stmt[2]).unwrap_or_default();
        let column = self.column_of(sql, &stmt[2], column);
        let value = format!("'{}'", escape_literal(&tenant.0));
        match insert_edits(sql, stmt, depth, &column, &value) {
            Some(edits) => TransformResult::transformed(apply_edits(sql, edits), vec![name]),
            None => refuse(
                "an INSERT into a tenant table is accepted only as INSERT INTO <table> \
                 (<columns without the tenant column>) VALUES (<row>)[, (<row>) ...] \
                 [ON CONFLICT [(<columns>)] DO NOTHING] [RETURNING ...]",
            ),
        }
    }

    /// Rewrite an UPDATE or DELETE whose target is the only tenant table it
    /// names.
    fn rewrite_write(
        &self,
        sql: &str,
        stmt: &[LexUnit],
        depth: &[u32],
        target: usize,
        tenant: &TenantId,
        column: &str,
    ) -> TransformResult {
        let column = self.column_of(sql, &stmt[target], column);
        if is_word(sql, &stmt[0], "update") && set_names_column(sql, stmt, depth, &column) {
            return refuse(
                "an UPDATE assigns the tenant column, which would move the row to another tenant",
            );
        }
        if self.tenant_refs(sql, stmt) != [target] {
            return refuse("a tenant table is named besides the target of the write");
        }
        let name = self.tenant_table_of(sql, &stmt[target]).unwrap_or_default();
        let filter = self.build_single_filter(tenant, &column);
        match filter_edits(sql, stmt, depth, 0, stmt.len(), &filter) {
            Some(edits) => TransformResult::transformed(apply_edits(sql, edits), vec![name]),
            None => refuse("no place to put the tenant filter"),
        }
    }

    /// Build a filter clause for multiple tables
    fn build_filter(&self, tenant: &TenantId, default_column: &str, tables: &[String]) -> String {
        let filters: Vec<String> = tables
            .iter()
            .map(|table| {
                let column = self.get_tenant_column(table).unwrap_or(default_column);
                if self.use_parameters {
                    format!("{}.{} = $1", table, column)
                } else {
                    format!("{}.{} = '{}'", table, column, escape_literal(&tenant.0))
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
                    .replace("{value}", &escape_literal(&tenant.0)),
                None => format!("{} = '{}'", column, escape_literal(&tenant.0)),
            }
        }
    }

    /// The FROM and JOIN table names of `query` (a test helper: the rewrite
    /// reads the same items as indices of the lexed statement).
    #[cfg(test)]
    fn extract_tables(&self, query: &str) -> Vec<String> {
        let Some(all) = lex_units(query) else {
            return Vec::new();
        };
        let units: Vec<LexUnit> = all
            .into_iter()
            .filter(|u| u.kind != LexKind::Comment)
            .collect();
        let depth = depths(query, &units);
        from_items(query, &units, &depth)
            .into_iter()
            .filter_map(|i| unit_name(query, &units[i]))
            .collect()
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

/// SQLSTATE of a statement refused under row isolation (`insufficient_privilege`).
pub const REFUSAL_SQLSTATE: &str = "42501";

/// Keywords that end a WHERE condition, and mark where a missing WHERE goes.
const CLAUSE_END: &[&str] = &[
    "order",
    "group",
    "having",
    "window",
    "limit",
    "offset",
    "fetch",
    "for",
    "union",
    "intersect",
    "except",
    "returning",
];

/// Set operators: each branch of a set operation is rewritten on its own.
const SET_OPERATORS: &[&str] = &["union", "intersect", "except"];

/// Keywords after which a FROM or JOIN list is over.
const FROM_END: &[&str] = &[
    "where",
    "group",
    "having",
    "window",
    "order",
    "limit",
    "offset",
    "fetch",
    "for",
    "union",
    "intersect",
    "except",
    "returning",
    "on",
    "using",
    "set",
    "values",
    "select",
];

/// A refusal of `reason`, for [`TransformResult::refusal`].
fn refuse(reason: &str) -> TransformResult {
    TransformResult::refused(reason)
}

/// The text of `unit` in `sql`.
fn text<'a>(sql: &'a str, unit: &LexUnit) -> &'a str {
    &sql[unit.start..unit.end]
}

/// Whether `unit` is the unquoted word `word` (ASCII case-insensitive).
fn is_word(sql: &str, unit: &LexUnit, word: &str) -> bool {
    unit.kind == LexKind::Word && text(sql, unit).eq_ignore_ascii_case(word)
}

/// Whether `unit` is the punctuation `p`.
fn is_punct(sql: &str, unit: &LexUnit, p: &str) -> bool {
    unit.kind == LexKind::Punct && text(sql, unit) == p
}

/// The name a word or quoted identifier spells (lower-case, unquoted).
fn unit_name(sql: &str, unit: &LexUnit) -> Option<String> {
    let name = match unit.kind {
        LexKind::Word => text(sql, unit).to_ascii_lowercase(),
        LexKind::Ident => text(sql, unit).trim_matches('"').to_lowercase(),
        _ => return None,
    };
    // PostgreSQL keeps the first 63 bytes of an identifier.
    Some(crate::sql_text::truncated_identifier(&name).to_string())
}

/// Whether the word `unit` is one of the lower-case `words`.
fn is_one_of(sql: &str, unit: &LexUnit, words: &[&str]) -> bool {
    unit.kind == LexKind::Word && words.iter().any(|w| is_word(sql, unit, w))
}

/// Parenthesis depth of each unit: a `(` or `)` sits outside what it encloses.
fn depths(sql: &str, stmt: &[LexUnit]) -> Vec<u32> {
    let mut depth = 0u32;
    stmt.iter()
        .map(|unit| {
            if is_punct(sql, unit, ")") {
                depth = depth.saturating_sub(1);
                depth
            } else if is_punct(sql, unit, "(") {
                depth += 1;
                depth - 1
            } else {
                depth
            }
        })
        .collect()
}

/// The statements of `units` (comments already dropped), split at each `;`.
/// A statement holding nothing is left out.
fn split_statements<'a>(sql: &str, units: &'a [LexUnit]) -> Vec<&'a [LexUnit]> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, unit) in units.iter().enumerate() {
        if is_punct(sql, unit, ";") {
            if start < i {
                out.push(&units[start..i]);
            }
            start = i + 1;
        }
    }
    if start < units.len() {
        out.push(&units[start..]);
    }
    out
}

/// The index of the target table of a DML statement at `at`, past an
/// optional `ONLY` when `only` is set; `None` unless a word or quoted
/// identifier stands there.
fn target_at(sql: &str, stmt: &[LexUnit], at: usize, only: bool) -> Option<usize> {
    let mut i = at;
    if only && stmt.get(i).is_some_and(|u| is_word(sql, u, "only")) {
        i += 1;
    }
    stmt.get(i)
        .filter(|u| matches!(u.kind, LexKind::Word | LexKind::Ident))
        .map(|_| i)
}

/// The byte ranges `(start, end)` of the branches of a statement: the parts
/// between top-level set operators.
fn set_branches(sql: &str, stmt: &[LexUnit], depth: &[u32]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, unit) in stmt.iter().enumerate() {
        if depth[i] == 0 && is_one_of(sql, unit, SET_OPERATORS) {
            out.push((start, i));
            start = i + 1;
        }
    }
    out.push((start, stmt.len()));
    out
}

/// Indices of the FROM and JOIN items of a statement, at the top level: the
/// last unit of each table name (the table of a qualified `schema.table`).
/// A name after `FROM` or `JOIN`, or after a comma in a FROM list, is an item.
fn from_items(sql: &str, stmt: &[LexUnit], depth: &[u32]) -> Vec<usize> {
    let mut out = Vec::new();
    let (mut in_from, mut expect) = (false, false);
    let mut i = 0;
    while i < stmt.len() {
        let unit = stmt[i];
        if depth[i] != 0 {
            i += 1;
            continue;
        }
        if is_one_of(sql, &unit, &["from", "join"]) {
            in_from = true;
            expect = true;
        } else if is_one_of(sql, &unit, FROM_END) {
            in_from = false;
            expect = false;
        } else if is_punct(sql, &unit, ",") {
            expect = in_from;
        } else if is_punct(sql, &unit, "(") {
            expect = false;
        } else if expect && matches!(unit.kind, LexKind::Word | LexKind::Ident) {
            let mut last = i;
            while last + 2 < stmt.len()
                && is_punct(sql, &stmt[last + 1], ".")
                && matches!(stmt[last + 2].kind, LexKind::Word | LexKind::Ident)
                && depth[last + 2] == 0
            {
                last += 2;
            }
            out.push(last);
            expect = false;
            i = last + 1;
            continue;
        }
        i += 1;
    }
    out
}

/// The edits that put `filter` into the WHERE of the branch `stmt[start..end]`:
/// `WHERE <filter> AND <condition>` (parenthesised only when the condition
/// has a top-level OR), or ` WHERE <filter>` right after the last unit before
/// the first clause that ends the branch. Keywords count only at the top
/// level, so a `WHERE` in a literal, a comment or a subquery is not one.
fn filter_edits(
    sql: &str,
    stmt: &[LexUnit],
    depth: &[u32],
    start: usize,
    end: usize,
    filter: &str,
) -> Option<Vec<(usize, String)>> {
    let top = |i: usize| depth[i] == 0;
    let mut clause = end;
    let mut where_at = None;
    for (i, unit) in stmt.iter().enumerate().take(end).skip(start) {
        if !top(i) || unit.kind != LexKind::Word {
            continue;
        }
        if is_word(sql, unit, "where") {
            where_at = Some(i);
            break;
        }
        if is_one_of(sql, unit, CLAUSE_END) {
            clause = i;
            break;
        }
    }
    match where_at {
        Some(w) => {
            let first = w + 1;
            let cond_end = (first..end)
                .find(|&i| top(i) && is_one_of(sql, &stmt[i], CLAUSE_END))
                .unwrap_or(end);
            if cond_end == first {
                return None;
            }
            let last = cond_end - 1;
            let has_or = (first..cond_end).any(|i| top(i) && is_word(sql, &stmt[i], "or"));
            Some(if has_or {
                vec![
                    (stmt[first].start, format!("{filter} AND (")),
                    (stmt[last].end, ")".to_string()),
                ]
            } else {
                vec![(stmt[first].start, format!("{filter} AND "))]
            })
        }
        None => {
            if clause == start {
                return None;
            }
            Some(vec![(stmt[clause - 1].end, format!(" WHERE {filter}"))])
        }
    }
}

/// `sql` with each `(position, text)` inserted at its position.
fn apply_edits(sql: &str, mut edits: Vec<(usize, String)>) -> String {
    edits.sort_by_key(|(at, _)| *at);
    let added: usize = edits.iter().map(|(_, text)| text.len()).sum();
    let mut out = String::with_capacity(sql.len() + added);
    let mut last = 0;
    for (at, text) in edits {
        out.push_str(&sql[last..at]);
        out.push_str(&text);
        last = at;
    }
    out.push_str(&sql[last..]);
    out
}

/// `value` with each `'` doubled, for a quoted SQL string literal.
fn escape_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// Whether unit `i` is a Unicode escape that can spell a name the lexer does
/// not read: a quoted identifier written directly after `&` (`U&"…"`), or the
/// `UESCAPE` word. Refused under row isolation whatever it spells.
fn is_unicode_escape(sql: &str, units: &[LexUnit], i: usize) -> bool {
    let unit = &units[i];
    if is_word(sql, unit, "uescape") {
        return true;
    }
    if unit.kind != LexKind::Ident || i == 0 {
        return false;
    }
    let prev = &units[i - 1];
    prev.kind == LexKind::Op && prev.end == unit.start && text(sql, prev).ends_with('&')
}

/// Whether the SET list of the UPDATE `stmt` names `column`, quoted or not,
/// qualified or not.
fn set_names_column(sql: &str, stmt: &[LexUnit], depth: &[u32], column: &str) -> bool {
    let Some(set) = (0..stmt.len()).find(|&i| depth[i] == 0 && is_word(sql, &stmt[i], "set"))
    else {
        return false;
    };
    let column = column.to_lowercase();
    for (i, unit) in stmt.iter().enumerate().skip(set + 1) {
        if depth[i] == 0 && is_one_of(sql, unit, &["where", "from", "returning"]) {
            break;
        }
        if unit_name(sql, unit).is_some_and(|name| name == column) {
            return true;
        }
    }
    false
}

/// The index of the `)` closing the `(` at `open`, at the same depth.
fn matching_close(sql: &str, stmt: &[LexUnit], depth: &[u32], open: usize) -> Option<usize> {
    let level = depth[open];
    (open + 1..stmt.len()).find(|&p| depth[p] == level && is_punct(sql, &stmt[p], ")"))
}

/// The edits of an accepted INSERT into a tenant table, or `None` for any
/// other shape: `INSERT INTO <table> (<names>) VALUES (<row>) [, (<row>)]...`
/// followed by nothing, `RETURNING ...`, or `ON CONFLICT [(<names>)] DO NOTHING`
/// (optionally followed by `RETURNING ...`). The names must not include
/// `column`. `value` is the quoted tenant literal.
fn insert_edits(
    sql: &str,
    stmt: &[LexUnit],
    depth: &[u32],
    column: &str,
    value: &str,
) -> Option<Vec<(usize, String)>> {
    let n = stmt.len();
    if n < 4 || !is_punct(sql, &stmt[3], "(") {
        return None;
    }
    let close = matching_close(sql, stmt, depth, 3)?;
    if close == 4 || !matches!(stmt[close - 1].kind, LexKind::Word | LexKind::Ident) {
        return None;
    }
    let tenant_column = column.to_lowercase();
    let mut k = 4;
    while k < close {
        let unit = &stmt[k];
        let named = matches!(unit.kind, LexKind::Word | LexKind::Ident);
        if !named || unit_name(sql, unit).is_some_and(|name| name == tenant_column) {
            return None;
        }
        k += 1;
        if k < close {
            if !is_punct(sql, &stmt[k], ",") {
                return None;
            }
            k += 1;
        }
    }
    let mut edits = vec![(stmt[close - 1].end, format!(", {column}"))];
    let mut k = close + 1;
    if !(k < n && is_word(sql, &stmt[k], "values")) {
        return None;
    }
    k += 1;
    loop {
        if !(k < n && is_punct(sql, &stmt[k], "(") && depth[k] == 0) {
            return None;
        }
        let row_close = matching_close(sql, stmt, depth, k)?;
        if row_close == k + 1 {
            return None;
        }
        edits.push((stmt[row_close - 1].end, format!(", {value}")));
        k = row_close + 1;
        if k < n && is_punct(sql, &stmt[k], ",") {
            k += 1;
        } else {
            break;
        }
    }
    if k == n || is_word(sql, &stmt[k], "returning") {
        return Some(edits);
    }
    if is_word(sql, &stmt[k], "on") && k + 1 < n && is_word(sql, &stmt[k + 1], "conflict") {
        let mut j = k + 2;
        if j < n && is_punct(sql, &stmt[j], "(") {
            j = matching_close(sql, stmt, depth, j)? + 1;
        }
        if !(j + 1 < n && is_word(sql, &stmt[j], "do") && is_word(sql, &stmt[j + 1], "nothing")) {
            return None;
        }
        j += 2;
        if j == n || is_word(sql, &stmt[j], "returning") {
            return Some(edits);
        }
    }
    None
}

/// The statement items that may follow a FROM or JOIN item without an alias:
/// the clause keywords and the join keywords.
const JOIN_WORDS: &[&str] = &[
    "join",
    "natural",
    "cross",
    "left",
    "right",
    "full",
    "inner",
    "outer",
    "lateral",
    "tablesample",
];

/// Whether the FROM or JOIN item whose name is at `i` ends there (no alias
/// follows): the end of the statement, `,`, `)`, a clause keyword or a join
/// keyword. Anything else is an alias, which the row filter cannot qualify.
fn item_ends(sql: &str, stmt: &[LexUnit], i: usize) -> bool {
    match stmt.get(i + 1) {
        None => true,
        Some(next) => {
            is_punct(sql, next, ",")
                || is_punct(sql, next, ")")
                || (next.kind == LexKind::Word
                    && (is_one_of(sql, next, FROM_END) || is_one_of(sql, next, JOIN_WORDS)))
        }
    }
}

/// Validate that a query doesn't try to bypass tenant isolation
pub fn validate_query(query: &str, _tenant: &TenantId, config: &TenantConfig) -> QueryValidation {
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
                    validation
                        .violations
                        .push(format!("Cross-schema access not allowed: {}", part));
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

        let result =
            transformer.transform("SELECT * FROM users WHERE active = true", &tenant, &config);

        assert!(result.transformed);
        assert!(result.query.contains("tenant_id = 'acme'"));
        assert!(result.query.contains("AND active = true"));
    }

    #[test]
    fn test_transform_select_no_where() {
        let transformer = TenantQueryTransformer::new().register_table("users", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        let result = transformer.transform("SELECT * FROM users ORDER BY id", &tenant, &config);

        assert!(result.transformed);
        assert!(result.query.contains("WHERE users.tenant_id = 'acme'"));
        assert!(result.query.contains("ORDER BY id"));
    }

    #[test]
    fn test_transform_update() {
        let transformer = TenantQueryTransformer::new().register_table("users", "tenant_id");

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
        let transformer = TenantQueryTransformer::new().register_table("users", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        let result = transformer.transform("DELETE FROM users WHERE id = 1", &tenant, &config);

        assert!(result.transformed);
        assert!(result.query.contains("tenant_id = 'acme'"));
    }

    #[test]
    fn test_no_transform_for_unregistered_table() {
        let transformer = TenantQueryTransformer::new().register_table("users", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = create_row_config("acme");

        let result =
            transformer.transform("SELECT * FROM logs WHERE level = 'error'", &tenant, &config);

        assert!(!result.transformed);
    }

    #[test]
    fn test_no_transform_for_schema_isolation() {
        let transformer = TenantQueryTransformer::new().register_table("users", "tenant_id");

        let tenant = TenantId::new("acme");
        let config = TenantConfig::builder()
            .id("acme")
            .name("Acme")
            .schema_isolation("shared", "acme")
            .build();

        let result = transformer.transform("SELECT * FROM users", &tenant, &config);

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

        let result = transformer.transform("SELECT * FROM audit_log", &tenant, &config);

        assert!(!result.transformed);
    }

    #[test]
    fn test_extract_tables() {
        let transformer = TenantQueryTransformer::new();

        let tables =
            transformer.extract_tables("SELECT * FROM users u, orders o WHERE u.id = o.user_id");
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));

        let tables = transformer
            .extract_tables("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
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

    /// Row-isolated transform of `sql` for tenant `acme`, with `orders`
    /// tenant-scoped on `tenant_id`.
    fn orders(sql: &str) -> TransformResult {
        TenantQueryTransformer::new()
            .register_table("orders", "tenant_id")
            .transform(sql, &TenantId::new("acme"), &create_row_config("acme"))
    }

    /// The rewritten text of `sql`, which must be transformed and not refused.
    fn rewritten(sql: &str) -> String {
        let res = orders(sql);
        assert!(res.refusal.is_none(), "refused: {:?}", res.refusal);
        assert!(res.transformed, "not transformed: {sql}");
        res.query
    }

    /// Asserts that `sql` is refused under row isolation (SQLSTATE 42501).
    fn assert_refused(sql: &str) {
        let res = orders(sql);
        assert!(res.refusal.is_some(), "not refused: {sql}");
        assert!(!res.transformed, "a refused statement is not rewritten");
    }

    #[test]
    fn or_in_select_stays_inside_the_tenant_filter() {
        assert_eq!(
            rewritten("SELECT * FROM orders WHERE status = 'open' OR status = 'late'"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' AND (status = 'open' OR status = 'late')"
        );
        assert_eq!(
            rewritten(
                "SELECT * FROM orders WHERE status = 'open' OR status = 'late' ORDER BY id LIMIT 10"
            ),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' AND (status = 'open' OR status = 'late') ORDER BY id LIMIT 10"
        );
    }

    #[test]
    fn or_in_update_stays_inside_the_tenant_filter() {
        assert_eq!(
            rewritten("UPDATE orders SET s = 'x' WHERE id = 1 OR id = 2"),
            "UPDATE orders SET s = 'x' WHERE tenant_id = 'acme' AND (id = 1 OR id = 2)"
        );
        assert_eq!(
            rewritten("UPDATE orders SET s = 'x' WHERE id = 1 OR id = 2 RETURNING id"),
            "UPDATE orders SET s = 'x' WHERE tenant_id = 'acme' AND (id = 1 OR id = 2) RETURNING id"
        );
    }

    #[test]
    fn or_in_delete_stays_inside_the_tenant_filter() {
        assert_eq!(
            rewritten("DELETE FROM orders WHERE id = 1 OR id = 2"),
            "DELETE FROM orders WHERE tenant_id = 'acme' AND (id = 1 OR id = 2)"
        );
        assert_eq!(
            rewritten("DELETE FROM orders WHERE id = 1 OR id = 2 RETURNING id"),
            "DELETE FROM orders WHERE tenant_id = 'acme' AND (id = 1 OR id = 2) RETURNING id"
        );
    }

    #[test]
    fn nested_parentheses_need_no_extra_wrapping() {
        assert_eq!(
            rewritten("SELECT * FROM orders WHERE (a = 1 OR b = 2) AND c = 3"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' AND (a = 1 OR b = 2) AND c = 3"
        );
    }

    #[test]
    fn trailing_line_comment_is_never_commented_out() {
        assert_eq!(
            rewritten("SELECT * FROM orders -- note"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' -- note"
        );
        assert_eq!(
            rewritten("SELECT * FROM orders WHERE a = 1 OR b = 2 -- note"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' AND (a = 1 OR b = 2) -- note"
        );
    }

    #[test]
    fn trailing_block_comment_stays_after_the_filter() {
        assert_eq!(
            rewritten("SELECT * FROM orders /* note */"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' /* note */"
        );
    }

    #[test]
    fn semicolon_then_comment_keeps_the_filter_before_the_semicolon() {
        assert_eq!(
            rewritten("SELECT * FROM orders;"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme';"
        );
        assert_eq!(
            rewritten("SELECT * FROM orders; -- note"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme'; -- note"
        );
    }

    #[test]
    fn keywords_inside_literals_and_comments_do_not_place_the_filter() {
        assert_eq!(
            rewritten("SELECT ' WHERE ' AS s FROM orders"),
            "SELECT ' WHERE ' AS s FROM orders WHERE orders.tenant_id = 'acme'"
        );
        assert_eq!(
            rewritten("SELECT ' ORDER BY ' AS s FROM orders"),
            "SELECT ' ORDER BY ' AS s FROM orders WHERE orders.tenant_id = 'acme'"
        );
        assert_eq!(
            rewritten("SELECT * FROM orders WHERE note = ' WHERE x ' OR id = 1"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' AND (note = ' WHERE x ' OR id = 1)"
        );
        assert_eq!(
            rewritten("SELECT * FROM orders /* ORDER BY id */"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' /* ORDER BY id */"
        );
        assert_eq!(
            rewritten("SELECT * FROM orders -- WHERE x\nORDER BY id"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' -- WHERE x\nORDER BY id"
        );
    }

    #[test]
    fn leading_comments_do_not_hide_the_statement() {
        assert_eq!(
            rewritten("/* x */ SELECT * FROM orders"),
            "/* x */ SELECT * FROM orders WHERE orders.tenant_id = 'acme'"
        );
        assert_eq!(
            rewritten("-- x\nSELECT * FROM orders"),
            "-- x\nSELECT * FROM orders WHERE orders.tenant_id = 'acme'"
        );
    }

    #[test]
    fn set_operations_and_locking_clauses_are_rewritten_per_branch() {
        assert_eq!(
            rewritten("SELECT * FROM orders UNION SELECT * FROM orders"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' UNION SELECT * FROM orders WHERE orders.tenant_id = 'acme'"
        );
        assert_eq!(
            rewritten("SELECT * FROM orders FOR UPDATE"),
            "SELECT * FROM orders WHERE orders.tenant_id = 'acme' FOR UPDATE"
        );
    }

    #[test]
    fn delete_using_keeps_its_filter_valid() {
        assert_eq!(
            rewritten("DELETE FROM orders USING logs"),
            "DELETE FROM orders USING logs WHERE tenant_id = 'acme'"
        );
    }

    #[test]
    fn statement_shapes_that_cannot_be_rewritten_are_refused() {
        assert_refused("WITH o AS (SELECT * FROM orders) SELECT * FROM o");
        assert_refused("(SELECT * FROM orders)");
        assert_refused("TABLE orders");
        assert_refused("SELECT * FROM orders; SELECT 1");
        assert_refused("SELECT * FROM logs WHERE id IN (SELECT id FROM orders)");
        assert_refused("UPDATE logs SET x = 1 WHERE id IN (SELECT id FROM orders)");
        assert_refused("INSERT INTO logs SELECT * FROM orders");
        assert_refused("SELECT * FROM logs JOIN logs2 ON true, orders");
    }

    #[test]
    fn refusal_is_not_taken_for_statements_without_tenant_tables() {
        for sql in [
            "WITH l AS (SELECT * FROM logs) SELECT * FROM l",
            "(SELECT 1)",
            "TABLE logs",
            "/* x */ SELECT * FROM logs",
            "SELECT * FROM logs; SELECT 1",
        ] {
            let res = orders(sql);
            assert!(res.refusal.is_none(), "wrongly refused: {sql}");
            assert!(!res.transformed, "untouched: {sql}");
        }
    }

    /// Row-isolated transform of `sql` for `tenant`, with `orders` tenant-scoped
    /// on `tenant_id`.
    fn orders_for(tenant: &str, sql: &str) -> TransformResult {
        TenantQueryTransformer::new()
            .register_table("orders", "tenant_id")
            .transform(sql, &TenantId::new(tenant), &create_row_config(tenant))
    }

    #[test]
    fn unicode_escaped_identifiers_are_refused_whatever_they_spell() {
        assert_refused(r#"SELECT * FROM U&"\006Frders""#);
        assert_refused(r#"SELECT * FROM u&"o\0072ders" UESCAPE '!'"#);
        assert_refused(r#"SELECT * FROM U&"!006Frders" UESCAPE '!'"#);
        assert_refused(r#"SELECT * FROM logs U&"x""#);
    }

    #[test]
    fn insert_adds_the_tenant_column_and_value_to_every_row() {
        assert_eq!(
            rewritten("INSERT INTO orders (id, status) VALUES (1, 'open')"),
            "INSERT INTO orders (id, status, tenant_id) VALUES (1, 'open', 'acme')"
        );
        assert_eq!(
            rewritten("INSERT INTO orders (id) VALUES (1), (2)"),
            "INSERT INTO orders (id, tenant_id) VALUES (1, 'acme'), (2, 'acme')"
        );
        assert_eq!(
            rewritten("INSERT INTO orders (id) VALUES (1) ON CONFLICT (id) DO NOTHING"),
            "INSERT INTO orders (id, tenant_id) VALUES (1, 'acme') ON CONFLICT (id) DO NOTHING"
        );
        assert_eq!(
            rewritten("INSERT INTO orders (id) VALUES (1) RETURNING id"),
            "INSERT INTO orders (id, tenant_id) VALUES (1, 'acme') RETURNING id"
        );
    }

    #[test]
    fn insert_shapes_that_could_write_another_tenant_are_refused() {
        assert_refused("INSERT INTO orders VALUES ('other')");
        assert_refused("INSERT INTO orders SELECT * FROM logs");
        assert_refused("INSERT INTO orders (id) SELECT id FROM logs");
        assert_refused("INSERT INTO orders DEFAULT VALUES");
        assert_refused("INSERT INTO orders (id, tenant_id) VALUES (1, 'other')");
        assert_refused(
            "INSERT INTO orders (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET status = 'x'",
        );
        assert_refused(
            "INSERT INTO orders (id) VALUES (1) ON CONFLICT (id) WHERE id > 0 DO NOTHING",
        );
    }

    #[test]
    fn insert_into_other_tables_is_untouched() {
        let res = orders("INSERT INTO logs (a) VALUES (1)");
        assert!(res.refusal.is_none());
        assert!(!res.transformed);
    }

    #[test]
    fn update_that_assigns_the_tenant_column_is_refused() {
        assert_refused("UPDATE orders SET tenant_id = 'other' WHERE id = 1");
        assert_refused("UPDATE orders SET orders.tenant_id = 'other' WHERE id = 1");
        assert_refused(r#"UPDATE orders SET "tenant_id" = 'other' WHERE id = 1"#);
        assert_refused("UPDATE orders SET status = 'x', tenant_id = 'other'");
    }

    #[test]
    fn tenant_ids_are_escaped_in_every_literal() {
        assert_eq!(
            orders_for("a'b", "SELECT * FROM orders").query,
            "SELECT * FROM orders WHERE orders.tenant_id = 'a''b'"
        );
        assert_eq!(
            orders_for("a'b", "UPDATE orders SET s = 1 WHERE id = 1").query,
            "UPDATE orders SET s = 1 WHERE tenant_id = 'a''b' AND id = 1"
        );
        assert_eq!(
            orders_for("a'b", "INSERT INTO orders (id) VALUES (1)").query,
            "INSERT INTO orders (id, tenant_id) VALUES (1, 'a''b')"
        );
    }

    #[test]
    fn a_tenant_id_with_a_nul_byte_is_refused() {
        assert!(orders_for("a\0b", "SELECT * FROM orders").refusal.is_some());
    }

    /// Row-isolated transform of `sql` for tenant `acme`, with `orders`
    /// tenant-scoped on `org_id` (not the config's `tenant_id`).
    fn orders_on_org(sql: &str) -> TransformResult {
        TenantQueryTransformer::new()
            .register_table("orders", "org_id")
            .transform(sql, &TenantId::new("acme"), &create_row_config("acme"))
    }

    #[test]
    fn a_derived_table_cannot_shadow_a_tenant_table() {
        assert_refused("SELECT o.* FROM orders o, (SELECT 'acme' AS tenant_id) orders");
        assert_refused("SELECT * FROM logs JOIN (VALUES ('acme')) orders(tenant_id) ON true");
    }

    #[test]
    fn an_aliased_tenant_table_is_refused() {
        assert_refused("SELECT * FROM orders o");
        assert_refused("SELECT * FROM orders AS o");
        assert_refused("SELECT * FROM logs JOIN orders o ON true");
        assert_refused("SELECT * FROM orders AS OF TIMESTAMP '2020-01-01'");
    }

    #[test]
    fn a_tenant_table_followed_by_a_clause_is_not_aliased() {
        rewritten("SELECT * FROM orders WHERE id = 1");
        rewritten("SELECT * FROM orders LEFT JOIN logs ON true");
        rewritten("SELECT * FROM orders, logs");
        rewritten("SELECT * FROM orders ORDER BY id");
    }

    #[test]
    fn dynamic_sql_and_refused_functions_are_refused_under_row_isolation() {
        assert_refused("DO $$ BEGIN END $$");
        assert_refused("/* x */ CALL refresh()");
        assert_refused("SELECT * FROM logs; DO $$ BEGIN END $$");
        assert_refused("SELECT table_to_xml('orders', true, false, '')");
        assert_refused("SELECT pg_catalog.PG_READ_FILE('/etc/passwd')");
        assert_refused("SELECT dblink('host=x', 'SELECT 1')");
    }

    #[test]
    fn ordinary_literals_that_name_tables_or_functions_are_not_refused() {
        for sql in [
            "SELECT * FROM logs WHERE note = 'orders shipped'",
            "SELECT * FROM logs WHERE note = 'table_to_xml(orders)'",
        ] {
            assert!(orders(sql).refusal.is_none(), "wrongly refused: {sql}");
        }
    }

    #[test]
    fn a_long_spelling_of_a_tenant_table_is_the_same_table() {
        // PostgreSQL keeps the first 63 bytes of an identifier.
        let name = "a".repeat(63);
        let long = format!("{name}bbbbbbb");
        let transformer = TenantQueryTransformer::new().register_table(name.as_str(), "tenant_id");
        let res = transformer.transform(
            &format!("SELECT * FROM {long}"),
            &TenantId::new("acme"),
            &create_row_config("acme"),
        );
        assert!(res.refusal.is_none(), "refused: {:?}", res.refusal);
        assert!(res.transformed, "not transformed: {}", res.query);
        assert!(res.query.contains(&format!("{name}.tenant_id = 'acme'")));
    }

    #[test]
    fn a_tenant_id_with_a_backslash_is_refused() {
        assert!(orders_for("a\\b", "SELECT * FROM orders").refusal.is_some());
    }

    #[test]
    fn writes_use_the_tables_own_tenant_column() {
        assert_eq!(
            orders_on_org("UPDATE orders SET s = 1 WHERE id = 1").query,
            "UPDATE orders SET s = 1 WHERE org_id = 'acme' AND id = 1"
        );
        assert_eq!(
            orders_on_org("DELETE FROM orders WHERE id = 1").query,
            "DELETE FROM orders WHERE org_id = 'acme' AND id = 1"
        );
        assert_eq!(
            orders_on_org("INSERT INTO orders (id) VALUES (1)").query,
            "INSERT INTO orders (id, org_id) VALUES (1, 'acme')"
        );
        assert!(orders_on_org("UPDATE orders SET org_id = 'x' WHERE id = 1")
            .refusal
            .is_some());
    }

    #[test]
    fn an_unconfigured_tenant_is_row_isolated_by_the_default_or_refused() {
        use crate::multi_tenancy::{MultiTenancyConfig, TenantManagerBuilder};
        let manager = |default: IsolationStrategy| {
            TenantManagerBuilder::new()
                .config(MultiTenancyConfig {
                    default_config: TenantConfig::new(TenantId::new("default"), default),
                    allow_unknown_tenants: true,
                    ..Default::default()
                })
                .query_transformer(
                    TenantQueryTransformer::new().register_table("orders", "tenant_id"),
                )
                .build()
        };
        let row = manager(IsolationStrategy::row("public", "tenant_id"));
        assert_eq!(
            row.transform_query("SELECT * FROM orders", &TenantId::new("zed"))
                .query,
            "SELECT * FROM orders WHERE orders.tenant_id = 'zed'"
        );
        let schema = manager(IsolationStrategy::schema("public", "public"));
        assert!(schema
            .transform_query("SELECT * FROM orders", &TenantId::new("zed"))
            .refusal
            .is_some());
    }
}
