//! Shadow-execution module (T3.4 R&D).
//!
//! Runs the same SQL on a primary node AND a shadow node, then
//! compares the two results. Used for:
//!
//! - **Major-version upgrade validation** (T2.1 stage 3): run
//!   production-shape queries on the source-version primary and the
//!   target-version standby; alert if results diverge.
//! - **Schema-migration validation**: shadow a candidate schema
//!   change against the live primary; flag plan-shape regressions
//!   before promotion.
//! - **Read-replica drift detection**: catch silent corruption or
//!   replication-lag-induced staleness in CI.
//!
//! Results are compared by row-count + per-row hash (matches the
//! T0-IT3 checksum design), so non-deterministic orderings are
//! tolerated. The shadow side runs in a tokio task and is fire-and-
//! forget for the application — the application sees only the
//! primary's result. Drift is reported via `ShadowExecuteReport`
//! returned from the task handle, or pushed to a channel if the
//! caller wires one in.
//!
//! ## Status
//!
//! Module scaffolding + comparison logic + tests. Wiring this into
//! the live request path (so every Nth query is shadowed) is a
//! follow-up gated on an explicit feature flag — production
//! deployments don't want surprise duplicate query load.

use crate::backend::{BackendClient, BackendConfig, ParamValue, QueryResult, TextValue};
use crate::{ProxyError, Result};
use std::time::Instant;

/// One shadow-execution result.
#[derive(Debug, Clone)]
pub struct ShadowExecuteReport {
    /// SQL that was shadowed.
    pub sql: String,
    /// Whether both sides returned at all.
    pub both_succeeded: bool,
    /// Whether the row counts match.
    pub row_count_match: bool,
    /// Whether the row hashes match (only meaningful when
    /// `row_count_match`).
    pub row_hash_match: bool,
    /// Wall-clock time on the primary side.
    pub primary_elapsed_us: u64,
    /// Wall-clock time on the shadow side.
    pub shadow_elapsed_us: u64,
    /// Error from primary, if any.
    pub primary_error: Option<String>,
    /// Error from shadow, if any.
    pub shadow_error: Option<String>,
    /// True when either side exceeded the comparison budget (row or byte
    /// ceiling). The comparison is still an exact order-independent digest,
    /// but an oversized run is deliberately **not** certified clean — the
    /// operator asked for a bounded comparison (O-03).
    pub budget_exceeded: bool,
}

impl ShadowExecuteReport {
    pub fn is_clean(&self) -> bool {
        self.both_succeeded && self.row_count_match && self.row_hash_match && !self.budget_exceeded
    }
}

/// Row/byte ceiling for one shadow comparison (O-03). Defaults are documented
/// in `docs/admin-api.md` and can be overridden per request.
#[derive(Debug, Clone, Copy)]
pub struct ShadowBudget {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for ShadowBudget {
    fn default() -> Self {
        Self {
            max_rows: 10_000,
            max_bytes: 16 * 1024 * 1024,
        }
    }
}

impl ShadowBudget {
    /// Does this result exceed the budget? O(rows) with an early exit once the
    /// byte ceiling is crossed, so it never walks a multi-GiB result to the end.
    pub fn exceeded(&self, qr: &QueryResult) -> bool {
        if qr.rows.len() > self.max_rows {
            return true;
        }
        let mut bytes = 0usize;
        for r in &qr.rows {
            for v in r {
                bytes = bytes.saturating_add(match v {
                    TextValue::Null => 1,
                    TextValue::Text(s) => s.len(),
                });
                if bytes > self.max_bytes {
                    return true;
                }
            }
        }
        false
    }
}

/// Run `sql` on `primary` and `shadow` **concurrently**. Returns the primary's
/// result for the application to consume, plus a shadow report containing the
/// comparison.
///
/// The shadow side runs in its own tokio task (own connection) started before
/// the primary is awaited, so its latency overlaps the primary's instead of
/// being added to it. Comparison is order-independent digest + row count; no
/// copy of either result set is retained beyond what the backend client itself
/// materialised, and a result over `budget` marks the report
/// `budget_exceeded` (not certified clean).
///
/// `params` are interpolated into the SQL using the same text-format
/// substitution the failover-replay engine uses (no extended protocol).
pub async fn shadow_execute(
    primary: &mut BackendClient,
    shadow_cfg: &BackendConfig,
    sql: &str,
    params: &[ParamValue],
    budget: ShadowBudget,
) -> Result<(QueryResult, ShadowExecuteReport)> {
    // Start the shadow side first so it overlaps the primary. It owns its
    // config/SQL/params so the task is independent of the borrow above.
    let shadow_task = {
        let cfg = shadow_cfg.clone();
        let sql = sql.to_string();
        let params = params.to_vec();
        tokio::spawn(async move { run_shadow(&cfg, &sql, &params).await })
    };

    let primary_start = Instant::now();
    let primary_outcome = if params.is_empty() {
        primary.simple_query(sql).await
    } else {
        primary.query_with_params(sql, params).await
    };
    let primary_elapsed_us = primary_start.elapsed().as_micros() as u64;

    let shadow_outcome = match shadow_task.await {
        Ok(v) => v,
        Err(e) => (
            Err(ProxyError::Internal(format!("shadow task join: {}", e))),
            0,
        ),
    };

    let (row_count_match, row_hash_match, budget_exceeded) =
        match (primary_outcome.as_ref(), shadow_outcome.0.as_ref()) {
            (Ok(p), Ok(s)) => {
                let count_match = p.rows.len() == s.rows.len();
                let over = budget.exceeded(p) || budget.exceeded(s);
                let hash_match = count_match && row_set_hash(&p.rows) == row_set_hash(&s.rows);
                (count_match, hash_match, over)
            }
            _ => (false, false, false),
        };

    let report = ShadowExecuteReport {
        sql: sql.to_string(),
        both_succeeded: primary_outcome.is_ok() && shadow_outcome.0.is_ok(),
        row_count_match,
        row_hash_match,
        primary_elapsed_us,
        shadow_elapsed_us: shadow_outcome.1,
        primary_error: primary_outcome.as_ref().err().map(|e| e.to_string()),
        shadow_error: shadow_outcome.0.as_ref().err().map(|e| e.to_string()),
        budget_exceeded,
    };

    let qr =
        primary_outcome.map_err(|e| ProxyError::Internal(format!("primary execute: {}", e)))?;
    Ok((qr, report))
}

async fn run_shadow(
    cfg: &BackendConfig,
    sql: &str,
    params: &[ParamValue],
) -> (Result<QueryResult>, u64) {
    let start = Instant::now();
    let result = match BackendClient::connect(cfg).await {
        Ok(mut client) => {
            let outcome = if params.is_empty() {
                client.simple_query(sql).await
            } else {
                client.query_with_params(sql, params).await
            };
            client.close().await;
            outcome.map_err(|e| ProxyError::Internal(format!("shadow execute: {}", e)))
        }
        Err(e) => Err(ProxyError::Internal(format!("shadow connect: {}", e))),
    };
    let us = start.elapsed().as_micros() as u64;
    (result, us)
}

/// Order-independent hash of a row set. Each row is hashed
/// individually (xor-fold of FNV-1a over the row's text-form),
/// then the row hashes are combined with addition (commutative,
/// associative — so row order in the result set doesn't change
/// the final hash).
///
/// This intentionally does NOT use a cryptographic hash. The signal
/// the comparison wants is "did we get the same set of rows," not
/// "is this committable evidence." Cryptographic commit is the job
/// of the audit-chain plugin (T2.4-P2).
pub fn row_set_hash(rows: &[Vec<TextValue>]) -> u128 {
    let mut acc: u128 = 0;
    for row in rows {
        acc = acc.wrapping_add(row_hash(row) as u128);
    }
    acc
}

fn row_hash(row: &[TextValue]) -> u64 {
    // FNV-1a 64-bit
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for v in row {
        let bytes = match v {
            TextValue::Null => &[0u8][..],
            TextValue::Text(s) => s.as_bytes(),
        };
        // sentinel between fields so "ab" + "" doesn't collide with "" + "ab"
        h = h.wrapping_mul(0x100_0000_01b3) ^ 0xff;
        for b in bytes {
            h = h.wrapping_mul(0x100_0000_01b3) ^ (*b as u64);
        }
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(values: &[Option<&str>]) -> Vec<TextValue> {
        values
            .iter()
            .map(|v| match v {
                Some(s) => TextValue::Text((*s).to_string()),
                None => TextValue::Null,
            })
            .collect()
    }

    #[test]
    fn identical_row_sets_hash_equal() {
        let a = vec![
            row(&[Some("1"), Some("alice")]),
            row(&[Some("2"), Some("bob")]),
        ];
        let b = vec![
            row(&[Some("1"), Some("alice")]),
            row(&[Some("2"), Some("bob")]),
        ];
        assert_eq!(row_set_hash(&a), row_set_hash(&b));
    }

    #[test]
    fn order_does_not_affect_hash() {
        let a = vec![row(&[Some("1"), Some("a")]), row(&[Some("2"), Some("b")])];
        let b = vec![row(&[Some("2"), Some("b")]), row(&[Some("1"), Some("a")])];
        assert_eq!(row_set_hash(&a), row_set_hash(&b));
    }

    #[test]
    fn changed_value_changes_hash() {
        let a = vec![row(&[Some("1"), Some("alice")])];
        let b = vec![row(&[Some("1"), Some("ALICE")])];
        assert_ne!(row_set_hash(&a), row_set_hash(&b));
    }

    #[test]
    fn null_distinguishes_from_empty_string() {
        let a = vec![row(&[None])];
        let b = vec![row(&[Some("")])];
        assert_ne!(row_set_hash(&a), row_set_hash(&b));
    }

    #[test]
    fn missing_row_changes_hash() {
        let a = vec![row(&[Some("1")])];
        let b = vec![row(&[Some("1")]), row(&[Some("2")])];
        assert_ne!(row_set_hash(&a), row_set_hash(&b));
    }

    #[test]
    fn report_is_clean_only_when_all_match() {
        let r = ShadowExecuteReport {
            sql: "SELECT 1".into(),
            both_succeeded: true,
            row_count_match: true,
            row_hash_match: true,
            primary_elapsed_us: 1,
            shadow_elapsed_us: 1,
            primary_error: None,
            shadow_error: None,
            budget_exceeded: false,
        };
        assert!(r.is_clean());

        let mut r2 = r.clone();
        r2.row_hash_match = false;
        assert!(!r2.is_clean());

        let mut r3 = r.clone();
        r3.both_succeeded = false;
        assert!(!r3.is_clean());

        // An oversized comparison is matched but deliberately not certified.
        let mut r4 = r.clone();
        r4.budget_exceeded = true;
        assert!(!r4.is_clean());
    }

    #[test]
    fn budget_flags_oversized_results() {
        let qr = |rows: usize| QueryResult {
            columns: Vec::new(),
            rows: (0..rows)
                .map(|i| vec![TextValue::Text(format!("row-{i}"))])
                .collect(),
            command_tag: String::new(),
        };
        let budget = ShadowBudget {
            max_rows: 3,
            max_bytes: 1024,
        };
        assert!(!budget.exceeded(&qr(3)));
        assert!(budget.exceeded(&qr(4)), "row ceiling");

        let byte_budget = ShadowBudget {
            max_rows: 100,
            max_bytes: 4,
        };
        assert!(
            byte_budget.exceeded(&qr(3)),
            "byte ceiling: 3 rows exceed 4 bytes"
        );
    }

    #[test]
    fn field_separator_prevents_concat_collision() {
        // Without a separator between fields, ["ab",""] and ["","ab"]
        // would hash the same. Verify our sentinel disambiguates.
        let a = vec![row(&[Some("ab"), Some("")])];
        let b = vec![row(&[Some(""), Some("ab")])];
        assert_ne!(row_set_hash(&a), row_set_hash(&b));
    }
}
