//! Per-agent SQL contracts + a contract validator with machine-readable
//! repair hints.
//!
//! An agent contract is a scoped grant: which SQL verbs and tables an agent
//! may touch, which predicates a query must carry (e.g. a tenant filter), and
//! whether reads must be bounded by a LIMIT. Queries are validated against
//! the contract BEFORE execution; a violation is returned as a structured
//! [`Violation`] — a violation class, the offending fragment, and a suggested
//! rewrite — so an LLM agent can read it and self-correct in one round trip
//! instead of flailing against an opaque error.
//!
//! Validation is intentionally a lightweight static inspection (verb +
//! table + predicate + LIMIT detection), the same altitude as a pg_hba /
//! pgcat-style guard; it is a policy gate, not a full SQL parser.

use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::protocol::{contains_ci, starts_with_ci};

/// A predicate an agent's queries must carry when they touch `table`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateRule {
    pub table: String,
    /// Column that must appear in the query's WHERE clause.
    pub column: String,
}

/// A scoped grant for one agent identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentContract {
    /// Identifier matched against the connecting agent.
    pub id: String,
    /// Reject write/DDL statements.
    #[serde(default = "default_true")]
    pub read_only: bool,
    /// If set, only these SQL verbs are allowed (upper-case, e.g. "SELECT").
    #[serde(default)]
    pub allowed_verbs: Option<Vec<String>>,
    /// If set, only these tables may be referenced.
    #[serde(default)]
    pub allowed_tables: Option<Vec<String>>,
    /// Tables that may never be referenced (takes precedence over allow).
    #[serde(default)]
    pub denied_tables: Vec<String>,
    /// Predicates that must be present when the named table is touched.
    #[serde(default)]
    pub require_predicate_on: Vec<PredicateRule>,
    /// Require a LIMIT on SELECTs.
    #[serde(default)]
    pub require_limit: bool,
    /// Suggested/enforced row cap (used in repair hints and to back
    /// `require_limit`).
    #[serde(default)]
    pub max_rows: Option<u64>,
}

fn default_true() -> bool {
    true
}

/// A contract violation, serialized to the agent as a machine-readable hint.
#[derive(Debug, Clone, Serialize)]
pub struct Violation {
    /// Stable class, e.g. "write_forbidden", "table_forbidden",
    /// "missing_predicate", "missing_limit", "verb_forbidden".
    pub violation: String,
    /// Human/agent-readable explanation.
    pub detail: String,
    /// The offending input (the SQL).
    pub offending: String,
    /// A concrete corrected statement the agent can retry, when one can be
    /// synthesised.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_rewrite: Option<String>,
}

impl Violation {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| self.detail.clone())
    }
}

static TABLE_RE: Lazy<Regex> = Lazy::new(|| {
    // FROM/JOIN/INTO/UPDATE <table> — captures schema-qualified identifiers.
    Regex::new(r"(?i)\b(?:FROM|JOIN|INTO|UPDATE)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)")
        .expect("valid table regex")
});

/// Extract the leading SQL verb (upper-case), e.g. "SELECT".
fn verb_of(sql: &str) -> String {
    sql.trim_start()
        .split(|c: char| c.is_whitespace() || c == '(')
        .next()
        .unwrap_or("")
        .to_ascii_uppercase()
}

/// Extract referenced table names (lower-cased, bare name without schema).
fn tables_of(sql: &str) -> Vec<String> {
    TABLE_RE
        .captures_iter(sql)
        .filter_map(|c| c.get(1))
        .map(|m| {
            let full = m.as_str().to_ascii_lowercase();
            // bare table name (strip schema prefix) for matching
            full.rsplit('.').next().unwrap_or(&full).to_string()
        })
        .collect()
}

fn is_write_verb(verb: &str) -> bool {
    matches!(
        verb,
        "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "DROP" | "ALTER" | "TRUNCATE" | "GRANT"
            | "REVOKE" | "MERGE" | "CALL" | "DO" | "COPY" | "VACUUM" | "REINDEX" | "CLUSTER"
            | "LOCK" | "COMMENT"
    )
}

/// Validate `sql` against `contract`. `Ok(())` admits the query; `Err`
/// carries a structured repair hint.
pub fn validate(sql: &str, contract: &AgentContract) -> Result<(), Violation> {
    let trimmed = sql.trim();
    let verb = verb_of(trimmed);

    // 1. read-only
    if contract.read_only && is_write_verb(&verb) {
        return Err(Violation {
            violation: "write_forbidden".into(),
            detail: format!("agent '{}' is read-only; '{}' statements are not permitted", contract.id, verb),
            offending: sql.to_string(),
            suggested_rewrite: None,
        });
    }

    // 2. allowed verbs
    if let Some(ref verbs) = contract.allowed_verbs {
        if !verbs.iter().any(|v| v.eq_ignore_ascii_case(&verb)) {
            return Err(Violation {
                violation: "verb_forbidden".into(),
                detail: format!("verb '{}' not in this agent's allowed set {:?}", verb, verbs),
                offending: sql.to_string(),
                suggested_rewrite: None,
            });
        }
    }

    let tables = tables_of(trimmed);

    // 3. denied tables (highest precedence)
    for t in &tables {
        if contract.denied_tables.iter().any(|d| d.eq_ignore_ascii_case(t)) {
            return Err(Violation {
                violation: "table_forbidden".into(),
                detail: format!("table '{}' is denied to agent '{}'", t, contract.id),
                offending: sql.to_string(),
                suggested_rewrite: None,
            });
        }
    }

    // 4. allowed-tables allowlist
    if let Some(ref allowed) = contract.allowed_tables {
        for t in &tables {
            if !allowed.iter().any(|a| a.eq_ignore_ascii_case(t)) {
                return Err(Violation {
                    violation: "table_not_allowed".into(),
                    detail: format!("table '{}' not in this agent's allowed set {:?}", t, allowed),
                    offending: sql.to_string(),
                    suggested_rewrite: None,
                });
            }
        }
    }

    // 5. required predicates per touched table
    for rule in &contract.require_predicate_on {
        if tables.iter().any(|t| t.eq_ignore_ascii_case(&rule.table)) && !mentions_predicate(trimmed, &rule.column) {
            let rewrite = inject_predicate(trimmed, &rule.column);
            return Err(Violation {
                violation: "missing_predicate".into(),
                detail: format!(
                    "queries on '{}' must filter by '{}'",
                    rule.table, rule.column
                ),
                offending: sql.to_string(),
                suggested_rewrite: Some(rewrite),
            });
        }
    }

    // 6. require LIMIT on SELECT
    if contract.require_limit && verb == "SELECT" && !contains_ci(trimmed, " LIMIT ") && !ends_with_limit(trimmed) {
        let cap = contract.max_rows.unwrap_or(1000);
        return Err(Violation {
            violation: "missing_limit".into(),
            detail: format!("SELECTs must be bounded; add LIMIT {} or fewer", cap),
            offending: sql.to_string(),
            suggested_rewrite: Some(format!("{} LIMIT {}", trimmed.trim_end_matches(';').trim_end(), cap)),
        });
    }

    Ok(())
}

/// Does the statement reference `column` in a WHERE-ish position? Heuristic:
/// there is a WHERE clause and the column name appears after it.
fn mentions_predicate(sql: &str, column: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    if let Some(where_pos) = upper.find(" WHERE ") {
        let after = &sql[where_pos..];
        contains_ci(after, column)
    } else {
        false
    }
}

fn ends_with_limit(sql: &str) -> bool {
    // Catch "... LIMIT <n>" at the tail (the leading-space `contains_ci`
    // check in `validate` misses a LIMIT that is the final clause).
    let up = sql.trim_end_matches(';').trim_end().to_ascii_uppercase();
    let words: Vec<&str> = up.split_whitespace().collect();
    let n = words.len();
    n >= 2 && words[n - 2] == "LIMIT"
}

/// Best-effort: add `WHERE <col> = $1` (or ` AND <col> = $1` when a WHERE
/// already exists) so the agent has a concrete statement to retry.
fn inject_predicate(sql: &str, column: &str) -> String {
    let trimmed = sql.trim().trim_end_matches(';').trim_end();
    if starts_with_ci(trimmed, "SELECT") || starts_with_ci(trimmed, "UPDATE") || starts_with_ci(trimmed, "DELETE") {
        if contains_ci(trimmed, " WHERE ") {
            format!("{} AND {} = $1", trimmed, column)
        } else {
            // Insert before ORDER BY / GROUP BY / LIMIT if present, else append.
            let up = trimmed.to_ascii_uppercase();
            let cut = ["ORDER BY", "GROUP BY", "LIMIT", "HAVING"]
                .iter()
                .filter_map(|kw| up.find(kw))
                .min();
            match cut {
                Some(pos) => format!("{} WHERE {} = $1 {}", trimmed[..pos].trim_end(), column, &trimmed[pos..]),
                None => format!("{} WHERE {} = $1", trimmed, column),
            }
        }
    } else {
        format!("{} /* add filter: {} = $1 */", trimmed, column)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract() -> AgentContract {
        AgentContract {
            id: "analyst".into(),
            read_only: true,
            allowed_verbs: None,
            allowed_tables: Some(vec!["users".into(), "orders".into()]),
            denied_tables: vec!["secrets".into()],
            require_predicate_on: vec![PredicateRule { table: "orders".into(), column: "tenant_id".into() }],
            require_limit: true,
            max_rows: Some(1000),
        }
    }

    #[test]
    fn allows_compliant_query() {
        let c = contract();
        assert!(validate("SELECT id FROM users WHERE id = 1 LIMIT 10", &c).is_ok());
    }

    #[test]
    fn blocks_write_when_read_only() {
        let v = validate("DELETE FROM users", &contract()).unwrap_err();
        assert_eq!(v.violation, "write_forbidden");
    }

    #[test]
    fn blocks_denied_table() {
        let v = validate("SELECT * FROM secrets LIMIT 1", &contract()).unwrap_err();
        assert_eq!(v.violation, "table_forbidden");
    }

    #[test]
    fn blocks_table_not_in_allowlist() {
        let v = validate("SELECT * FROM invoices LIMIT 1", &contract()).unwrap_err();
        assert_eq!(v.violation, "table_not_allowed");
    }

    #[test]
    fn requires_predicate_with_rewrite() {
        let v = validate("SELECT * FROM orders LIMIT 5", &contract()).unwrap_err();
        assert_eq!(v.violation, "missing_predicate");
        let rw = v.suggested_rewrite.unwrap();
        assert!(rw.to_lowercase().contains("tenant_id"));
    }

    #[test]
    fn requires_limit_with_rewrite() {
        let v = validate("SELECT id FROM users WHERE id = 1", &contract()).unwrap_err();
        assert_eq!(v.violation, "missing_limit");
        assert!(v.suggested_rewrite.unwrap().to_uppercase().contains("LIMIT 1000"));
    }

    #[test]
    fn table_extraction_handles_schema_and_joins() {
        let t = tables_of("SELECT * FROM public.users u JOIN orders o ON o.uid = u.id");
        assert!(t.contains(&"users".to_string()));
        assert!(t.contains(&"orders".to_string()));
    }
}
