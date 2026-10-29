//! Lock-free fixed-bucket latency histograms and bounded-label counters for
//! `/metrics` (P-03).
//!
//! The `prometheus` crate was removed on purpose; metrics are hand-formatted in
//! `src/admin.rs`. A [`LatencyHistogram`] is a fixed array of atomic bucket
//! counters plus a sum: [`LatencyHistogram::record`] is a binary search over at
//! most [`MAX_BUCKETS`] bounds and two relaxed atomic adds, with no allocation
//! and no lock, so it can sit on the write path. Bucket bounds come from
//! `[limits] latency_buckets_ms`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Most buckets a histogram may have (excluding the implicit `+Inf`).
pub const MAX_BUCKETS: usize = 32;

/// Default bucket upper bounds in milliseconds.
pub fn default_latency_buckets_ms() -> Vec<f64> {
    vec![
        1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
    ]
}

/// Check bucket bounds: 1..=[`MAX_BUCKETS`] finite values, each at least one
/// microsecond, strictly increasing at microsecond resolution.
pub fn validate_buckets_ms(bounds_ms: &[f64]) -> Result<(), String> {
    if bounds_ms.is_empty() || bounds_ms.len() > MAX_BUCKETS {
        return Err(format!(
            "needs 1 to {MAX_BUCKETS} bucket bounds, got {}",
            bounds_ms.len()
        ));
    }
    let mut prev = 0;
    for &ms in bounds_ms {
        if !ms.is_finite() {
            return Err(format!("bound {ms} is not a finite number"));
        }
        let us = to_micros(ms);
        if us <= prev {
            return Err(format!(
                "bounds must be at least 0.001 ms and strictly increasing; {ms} is not"
            ));
        }
        prev = us;
    }
    Ok(())
}

fn to_micros(ms: f64) -> u64 {
    (ms * 1000.0).round().max(0.0) as u64
}

/// A cumulative-bucket latency histogram (Prometheus `histogram` semantics).
#[derive(Debug)]
pub struct LatencyHistogram {
    /// Upper bounds in microseconds, strictly increasing.
    bounds_us: Box<[u64]>,
    /// Observations per bucket (not cumulative); the last slot is `+Inf`.
    buckets: Box<[AtomicU64]>,
    sum_us: AtomicU64,
}

impl LatencyHistogram {
    /// `bounds_ms` must satisfy [`validate_buckets_ms`]; configuration
    /// validation guarantees that for the daemon.
    pub fn new(bounds_ms: &[f64]) -> Self {
        let bounds_us: Box<[u64]> = bounds_ms.iter().map(|&ms| to_micros(ms)).collect();
        let buckets = (0..=bounds_us.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            bounds_us,
            buckets,
            sum_us: AtomicU64::new(0),
        }
    }

    /// Record one observation.
    pub fn record(&self, elapsed: Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        // First bound >= us; past the end means +Inf.
        let i = self.bounds_us.partition_point(|&b| b < us);
        self.buckets[i].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
    }

    /// Cumulative counts per bound plus `+Inf` (which is the total count).
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut running = 0;
        let cumulative = self
            .buckets
            .iter()
            .map(|b| {
                running += b.load(Ordering::Relaxed);
                running
            })
            .collect();
        HistogramSnapshot {
            bounds_us: self.bounds_us.to_vec(),
            cumulative,
            sum_us: self.sum_us.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of a [`LatencyHistogram`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct HistogramSnapshot {
    /// Bucket upper bounds in microseconds.
    pub bounds_us: Vec<u64>,
    /// Cumulative observation counts, one per bound and a final `+Inf` slot.
    pub cumulative: Vec<u64>,
    /// Sum of all observations in microseconds.
    pub sum_us: u64,
}

impl HistogramSnapshot {
    /// Total observations.
    pub fn count(&self) -> u64 {
        self.cumulative.last().copied().unwrap_or(0)
    }

    /// Append the Prometheus text exposition of this histogram, in seconds.
    pub fn write_prometheus(&self, out: &mut String, name: &str, help: &str) {
        use std::fmt::Write as _;
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        for (bound, n) in self.bounds_us.iter().zip(&self.cumulative) {
            let _ = writeln!(
                out,
                "{name}_bucket{{le=\"{}\"}} {n}",
                *bound as f64 / 1_000_000.0
            );
        }
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {}", self.count());
        let _ = writeln!(out, "{name}_sum {}", self.sum_us as f64 / 1_000_000.0);
        let _ = writeln!(out, "{name}_count {}", self.count());
    }
}

/// Why in-session Transaction Replay did not transparently continue a session
/// after a backend fault. A closed set, so the Prometheus label stays bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayRefusal {
    /// The statement may or may not have run (SQLSTATE 08007 to the client).
    OutcomeUnknown,
    /// The work cannot be re-executed safely (mode, COPY, a non-replayable
    /// transaction, or a fault that does not prove execution stopped).
    NotReplayable,
    /// The transaction outgrew `[limits] tr_max_replay_statements` / `_bytes`.
    CapExceeded,
    /// Replay ran but the new backend answered differently (40001).
    Diverged,
    /// The recovery deadline (`write_timeout_secs`) ran out.
    Deadline,
    /// No usable replacement backend could be reached.
    BackendUnavailable,
}

impl ReplayRefusal {
    pub const ALL: [ReplayRefusal; 6] = [
        ReplayRefusal::OutcomeUnknown,
        ReplayRefusal::NotReplayable,
        ReplayRefusal::CapExceeded,
        ReplayRefusal::Diverged,
        ReplayRefusal::Deadline,
        ReplayRefusal::BackendUnavailable,
    ];

    pub fn label(self) -> &'static str {
        match self {
            ReplayRefusal::OutcomeUnknown => "outcome_unknown",
            ReplayRefusal::NotReplayable => "not_replayable",
            ReplayRefusal::CapExceeded => "cap_exceeded",
            ReplayRefusal::Diverged => "diverged",
            ReplayRefusal::Deadline => "deadline",
            ReplayRefusal::BackendUnavailable => "backend_unavailable",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// The daemon's latency histograms and replay-refusal counters, shared by the
/// data path (writers) and the admin API (reader).
#[derive(Debug)]
pub struct ProxyLatency {
    /// Backend fault seen by a live session until a replacement backend is
    /// connected and authenticated.
    pub failover_reconnect: LatencyHistogram,
    /// Replaying an open transaction's statements on the replacement backend.
    pub tr_replay: LatencyHistogram,
    /// Waiting for a client slot at `[limits] max_client_connections`.
    pub admission_wait: LatencyHistogram,
    /// Applying a response's captured operations to the recovery journal.
    pub journal_apply: LatencyHistogram,
    /// Operator-driven `POST /api/replay` runs.
    pub operator_replay: LatencyHistogram,
    /// Waits for a slot on a node at its `max_connections` cap, or for a
    /// `max_concurrent_reconnects_per_node` permit (H-05).
    pub backend_slot_wait: LatencyHistogram,
    /// Request waits for one of the tenant's active-query permits at its
    /// `[multi_tenancy]` cap (ac858d61400b); only requests that waited, every
    /// wait however it ended (admitted, refused, cancelled, abandoned).
    pub tenant_query_wait: LatencyHistogram,
    refusals: [AtomicU64; ReplayRefusal::ALL.len()],
}

impl ProxyLatency {
    pub fn new(bounds_ms: &[f64]) -> Self {
        Self {
            failover_reconnect: LatencyHistogram::new(bounds_ms),
            tr_replay: LatencyHistogram::new(bounds_ms),
            admission_wait: LatencyHistogram::new(bounds_ms),
            journal_apply: LatencyHistogram::new(bounds_ms),
            operator_replay: LatencyHistogram::new(bounds_ms),
            backend_slot_wait: LatencyHistogram::new(bounds_ms),
            tenant_query_wait: LatencyHistogram::new(bounds_ms),
            refusals: Default::default(),
        }
    }

    pub fn record_refusal(&self, reason: ReplayRefusal) {
        self.refusals[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn refusals(&self, reason: ReplayRefusal) -> u64 {
        self.refusals[reason.index()].load(Ordering::Relaxed)
    }

    /// The histograms (`bounds_us`, cumulative counts, `sum_us`, `count`) and
    /// refusal counters as JSON, for `GET /metrics`.
    pub fn to_json(&self) -> serde_json::Value {
        let hist = |h: &LatencyHistogram| {
            let s = h.snapshot();
            serde_json::json!({
                "bounds_us": s.bounds_us,
                "cumulative": s.cumulative,
                "sum_us": s.sum_us,
                "count": s.count(),
            })
        };
        let refusals: serde_json::Map<String, serde_json::Value> = ReplayRefusal::ALL
            .iter()
            .map(|r| (r.label().to_string(), self.refusals(*r).into()))
            .collect();
        serde_json::json!({
            "failover_reconnect": hist(&self.failover_reconnect),
            "tr_replay": hist(&self.tr_replay),
            "admission_wait": hist(&self.admission_wait),
            "journal_apply": hist(&self.journal_apply),
            "operator_replay": hist(&self.operator_replay),
            "backend_slot_wait": hist(&self.backend_slot_wait),
            "tenant_query_wait": hist(&self.tenant_query_wait),
            "tr_replay_refusals": refusals,
        })
    }

    /// Append every histogram and the refusal counter in Prometheus text.
    pub fn write_prometheus(&self, out: &mut String) {
        use std::fmt::Write as _;
        for (hist, name, help) in [
            (
                &self.failover_reconnect,
                "heliosdb_proxy_failover_reconnect_seconds",
                "Backend fault on a live session until a replacement backend is connected",
            ),
            (
                &self.tr_replay,
                "heliosdb_proxy_tr_replay_seconds",
                "Replaying an open transaction on the replacement backend",
            ),
            (
                &self.admission_wait,
                "heliosdb_proxy_admission_wait_seconds",
                "Client wait for a slot at the max_client_connections cap",
            ),
            (
                &self.journal_apply,
                "heliosdb_proxy_journal_apply_seconds",
                "Applying captured operations to the recovery journal",
            ),
            (
                &self.operator_replay,
                "heliosdb_proxy_operator_replay_seconds",
                "Operator POST /api/replay runs",
            ),
            (
                &self.backend_slot_wait,
                "heliosdb_proxy_backend_slot_wait_seconds",
                "Waits for a slot on a node at its max_connections cap or for a reconnect permit",
            ),
            (
                &self.tenant_query_wait,
                "heliosdb_proxy_tenant_query_wait_seconds",
                "Request wait for a tenant active-query permit, all tenants together, admitted or not (only requests that waited)",
            ),
        ] {
            hist.snapshot().write_prometheus(out, name, help);
        }
        let _ = writeln!(
            out,
            "# HELP heliosdb_proxy_tr_replay_refusals_total Backend faults Transaction Replay did not transparently continue, by reason"
        );
        let _ = writeln!(
            out,
            "# TYPE heliosdb_proxy_tr_replay_refusals_total counter"
        );
        for reason in ReplayRefusal::ALL {
            let _ = writeln!(
                out,
                "heliosdb_proxy_tr_replay_refusals_total{{reason=\"{}\"}} {}",
                reason.label(),
                self.refusals(reason)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_validated() {
        assert!(validate_buckets_ms(&default_latency_buckets_ms()).is_ok());
        assert!(validate_buckets_ms(&[]).is_err());
        assert!(validate_buckets_ms(&[1.0; MAX_BUCKETS + 1]).is_err());
        assert!(validate_buckets_ms(&[5.0, 5.0]).is_err());
        assert!(validate_buckets_ms(&[5.0, 2.0]).is_err());
        assert!(validate_buckets_ms(&[0.0]).is_err());
        assert!(validate_buckets_ms(&[-1.0]).is_err());
        assert!(validate_buckets_ms(&[f64::NAN]).is_err());
        assert!(
            validate_buckets_ms(&[0.0001, 0.0002]).is_err(),
            "sub-microsecond"
        );
        assert!(validate_buckets_ms(&[0.5, 1.5]).is_ok());
    }

    #[test]
    fn observations_land_in_the_first_bucket_that_holds_them() {
        let h = LatencyHistogram::new(&[1.0, 10.0, 100.0]);
        h.record(Duration::from_micros(1000)); // == 1 ms -> le 0.001
        h.record(Duration::from_micros(1001)); // -> le 0.01
        h.record(Duration::from_millis(50)); // -> le 0.1
        h.record(Duration::from_secs(3)); // -> +Inf
        h.record(Duration::ZERO); // -> le 0.001
        let s = h.snapshot();
        assert_eq!(s.cumulative, vec![2, 3, 4, 5]);
        assert_eq!(s.count(), 5);
        assert_eq!(s.sum_us, 1000 + 1001 + 50_000 + 3_000_000);
    }

    #[test]
    fn prometheus_text_is_cumulative_in_seconds() {
        let h = LatencyHistogram::new(&[1.0, 2.5]);
        h.record(Duration::from_micros(500));
        h.record(Duration::from_micros(2000));
        h.record(Duration::from_millis(9));
        let mut out = String::new();
        h.snapshot()
            .write_prometheus(&mut out, "x_seconds", "help text");
        assert_eq!(
            out,
            "# HELP x_seconds help text\n\
             # TYPE x_seconds histogram\n\
             x_seconds_bucket{le=\"0.001\"} 1\n\
             x_seconds_bucket{le=\"0.0025\"} 2\n\
             x_seconds_bucket{le=\"+Inf\"} 3\n\
             x_seconds_sum 0.0115\n\
             x_seconds_count 3\n"
        );
    }

    #[test]
    fn refusal_reasons_are_a_bounded_label_set() {
        let p = ProxyLatency::new(&[1.0]);
        p.record_refusal(ReplayRefusal::Deadline);
        p.record_refusal(ReplayRefusal::Deadline);
        p.record_refusal(ReplayRefusal::OutcomeUnknown);
        let mut out = String::new();
        p.write_prometheus(&mut out);
        assert!(out.contains("heliosdb_proxy_tr_replay_refusals_total{reason=\"deadline\"} 2\n"));
        assert!(
            out.contains("heliosdb_proxy_tr_replay_refusals_total{reason=\"outcome_unknown\"} 1\n")
        );
        let labels: Vec<_> = ReplayRefusal::ALL.iter().map(|r| r.label()).collect();
        assert_eq!(labels.len(), 6);
        assert_eq!(
            out.matches("heliosdb_proxy_tr_replay_refusals_total{")
                .count(),
            6
        );
        for name in [
            "failover_reconnect",
            "tr_replay",
            "admission_wait",
            "journal_apply",
            "operator_replay",
            "backend_slot_wait",
            "tenant_query_wait",
        ] {
            assert!(out.contains(&format!("# TYPE heliosdb_proxy_{name}_seconds histogram\n")));
        }
    }
}
