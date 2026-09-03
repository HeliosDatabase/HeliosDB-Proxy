//! Anomaly detection (T3.1).
//!
//! Statistical + heuristic detector for production-shape security
//! and operational anomalies. In-process sliding windows; no
//! external data store. Four detector families:
//!
//! 1. **Rate spike** — z-score on per-tenant queries-per-second
//!    against a rolling EWMA baseline.
//! 2. **Credential stuffing** — failed-auth burst per (user, ip)
//!    inside a sliding 60s window.
//! 3. **SQL injection** — heuristic pattern match against well-known
//!    payload shapes (UNION-based, comment escapes, stacked queries,
//!    boolean blind, time-based blind).
//! 4. **Novel query** — query fingerprint never seen before, useful
//!    on high-churn application workloads only as an informational
//!    signal (low confidence by default; admins can tighten via
//!    config).
//!
//! Why not a trained classifier today?
//!
//! Production anomaly classifiers want labels — feedback loops from
//! analyst-marked false positives. Without that loop in place, a
//! trained model overfits to whatever traffic was present at
//! training time. Statistical detectors are honest about their
//! priors (the EWMA + z-score) and degrade gracefully. The
//! [`AnomalyEvent`] trail makes it possible to bolt a learned
//! classifier on later: events become labeled training data.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::VecDeque;

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub mod ewma;
pub mod sql_injection;

pub use ewma::{Ewma, RateWindow};

/// Anomaly severity — surfaces in admin output and lets operators
/// filter detections at scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

/// One anomaly detection event.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AnomalyEvent {
    /// Per-tenant request-rate spike against the rolling baseline.
    RateSpike {
        tenant: String,
        rate_per_sec: f64,
        baseline: f64,
        z_score: f64,
        severity: Severity,
        detected_at: String,
    },
    /// Failed-auth burst from a single (user, ip) pair.
    AuthBurst {
        user: String,
        client_ip: String,
        failures: u32,
        window_secs: u32,
        severity: Severity,
        detected_at: String,
    },
    /// SQL-injection-shaped statement matched one or more
    /// well-known payload patterns.
    SqlInjection {
        sql_excerpt: String,
        patterns_matched: Vec<String>,
        severity: Severity,
        detected_at: String,
    },
    /// First-seen query fingerprint. Informational by default.
    NovelQuery {
        fingerprint: String,
        sql_excerpt: String,
        detected_at: String,
    },
}

impl AnomalyEvent {
    pub fn severity(&self) -> Severity {
        match self {
            AnomalyEvent::RateSpike { severity, .. } => *severity,
            AnomalyEvent::AuthBurst { severity, .. } => *severity,
            AnomalyEvent::SqlInjection { severity, .. } => *severity,
            AnomalyEvent::NovelQuery { .. } => Severity::Info,
        }
    }
}

/// Tunables. Defaults match production-friendly behaviour: spike
/// threshold above 3σ, credential burst above 10 failures / 60s.
#[derive(Debug, Clone)]
pub struct AnomalyConfig {
    /// Rolling window for the per-tenant EWMA, in seconds.
    pub rate_window_secs: u64,
    /// Minimum z-score before a rate spike fires.
    pub spike_z_threshold: f64,
    /// Window for failed-auth bursts, in seconds.
    pub auth_window_secs: u64,
    /// Failures inside the auth window that trigger Critical.
    pub auth_critical_count: u32,
    /// Failures inside the auth window that trigger Warning.
    pub auth_warning_count: u32,
    /// Maximum events kept in the in-memory ring buffer.
    pub event_buffer_size: usize,
    /// Treat novel queries as informational events. Set false to
    /// suppress on high-churn workloads (e.g. ad-hoc analytics).
    pub emit_novel_queries: bool,
    /// Upper bound on the novel-query fingerprint set before it is
    /// cleared. Bounds memory on high-cardinality SQL (the detector is
    /// informational, so clearing on overflow is acceptable). Defaults
    /// to [`MAX_SEEN_FINGERPRINTS`].
    pub max_seen_fingerprints: usize,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            rate_window_secs: 60,
            spike_z_threshold: 3.0,
            auth_window_secs: 60,
            auth_critical_count: 10,
            auth_warning_count: 5,
            event_buffer_size: 1024,
            emit_novel_queries: true,
            max_seen_fingerprints: MAX_SEEN_FINGERPRINTS,
        }
    }
}

/// Top-level detector. Cheap to clone via Arc — the inner state is
/// guarded by parking_lot RwLocks scoped per-detector to avoid
/// cross-detector contention.
#[derive(Clone)]
pub struct AnomalyDetector {
    config: Arc<AnomalyConfig>,
    // Per-key sliding windows live in DashMaps so concurrent sessions for
    // different tenants/users hit different shards instead of serializing
    // on one global writer per query.
    rate_windows: Arc<DashMap<String, RateWindow>>,
    auth_windows: Arc<DashMap<(String, String), AuthBurstWindow>>,
    seen_fingerprints: Arc<DashMap<String, ()>>,
    events: Arc<RwLock<VecDeque<AnomalyEvent>>>,
}

/// Default upper bound on the novel-query fingerprint set. Without a cap the
/// set grows unbounded on high-cardinality SQL (a slow memory leak); the
/// detector is informational, so clearing on overflow is acceptable. This is
/// only the DEFAULT — the live cap is [`AnomalyConfig::max_seen_fingerprints`]
/// (exposed via the `[anomaly]` config section) and read on the hot path.
const MAX_SEEN_FINGERPRINTS: usize = 100_000;

impl AnomalyDetector {
    pub fn new(config: AnomalyConfig) -> Self {
        Self {
            config: Arc::new(config),
            rate_windows: Arc::new(DashMap::new()),
            auth_windows: Arc::new(DashMap::new()),
            seen_fingerprints: Arc::new(DashMap::new()),
            events: Arc::new(RwLock::new(VecDeque::with_capacity(1024))),
        }
    }

    /// Record a query event. Detectors may emit zero or more events.
    /// Returns the events emitted by THIS call (the caller can also
    /// poll `recent_events` for the full ring buffer).
    pub fn record_query(&self, ctx: &QueryObservation<'_>) -> Vec<AnomalyEvent> {
        let mut emitted = Vec::new();

        // Rate-spike detector. Per-tenant shard lock only.
        {
            let mut window = self
                .rate_windows
                .entry(ctx.tenant.clone())
                .or_insert_with(|| RateWindow::new(self.config.rate_window_secs));
            if let Some(spike) = window.observe_and_score(ctx.timestamp) {
                if spike.z_score >= self.config.spike_z_threshold {
                    let severity = if spike.z_score >= self.config.spike_z_threshold * 2.0 {
                        Severity::Critical
                    } else {
                        Severity::Warning
                    };
                    let ev = AnomalyEvent::RateSpike {
                        tenant: ctx.tenant.clone(),
                        rate_per_sec: spike.rate,
                        baseline: spike.baseline,
                        z_score: spike.z_score,
                        severity,
                        detected_at: chrono::Utc::now().to_rfc3339(),
                    };
                    drop(window); // release shard before pushing to the event ring
                    emitted.push(ev.clone());
                    self.push_event(ev);
                }
            }
        }

        // Novel-query detector. The common already-seen path takes only a
        // shard read; a fresh fingerprint upgrades to a shard write.
        if self.config.emit_novel_queries
            && !self
                .seen_fingerprints
                .contains_key(ctx.fingerprint.as_ref())
        {
            // Bound the set so unique-SQL traffic can't leak memory.
            if self.seen_fingerprints.len() >= self.config.max_seen_fingerprints {
                self.seen_fingerprints.clear();
            }
            // insert returns the prior value; None means we won the race to
            // first-see this fingerprint, so emit exactly once.
            if self
                .seen_fingerprints
                .insert(ctx.fingerprint.as_ref().to_owned(), ())
                .is_none()
            {
                let ev = AnomalyEvent::NovelQuery {
                    fingerprint: ctx.fingerprint.as_ref().to_owned(),
                    sql_excerpt: excerpt(&ctx.sql, 120),
                    detected_at: chrono::Utc::now().to_rfc3339(),
                };
                emitted.push(ev.clone());
                self.push_event(ev);
            }
        }

        // SQL-injection detector. Pure heuristic — runs even if the
        // upstream pre-query already passed; multiple layers is the
        // point.
        //
        // The statement is lower-cased exactly once, into a
        // thread-local scratch buffer whose capacity is reused across
        // queries; every matcher reads that single lowered view.
        let matches = LOWER_SCRATCH.with(|cell| {
            let mut buf = cell.borrow_mut();
            sql_injection::lower_into(&ctx.sql, &mut buf);
            sql_injection::scan_lowered(&buf)
        });
        if !matches.is_empty() {
            let severity = if matches.len() >= 2 {
                Severity::Critical
            } else {
                Severity::Warning
            };
            let ev = AnomalyEvent::SqlInjection {
                sql_excerpt: excerpt(&ctx.sql, 200),
                patterns_matched: matches,
                severity,
                detected_at: chrono::Utc::now().to_rfc3339(),
            };
            emitted.push(ev.clone());
            self.push_event(ev);
        }

        emitted
    }

    /// Record an authentication outcome. Failed auths feed the
    /// credential-stuffing detector.
    pub fn record_auth(
        &self,
        user: &str,
        client_ip: &str,
        succeeded: bool,
        timestamp: Instant,
        iso_timestamp: &str,
    ) -> Option<AnomalyEvent> {
        if succeeded {
            // Successful auth resets the burst counter — common
            // case after the operator unlocks an account.
            self.auth_windows
                .remove(&(user.to_string(), client_ip.to_string()));
            return None;
        }
        let count = {
            let mut window = self
                .auth_windows
                .entry((user.to_string(), client_ip.to_string()))
                .or_insert_with(|| AuthBurstWindow::new(self.config.auth_window_secs));
            window.record_failure(timestamp)
        };
        let severity = if count >= self.config.auth_critical_count {
            Severity::Critical
        } else if count >= self.config.auth_warning_count {
            Severity::Warning
        } else {
            return None;
        };
        let ev = AnomalyEvent::AuthBurst {
            user: user.to_string(),
            client_ip: client_ip.to_string(),
            failures: count,
            window_secs: self.config.auth_window_secs as u32,
            severity,
            detected_at: iso_timestamp.to_string(),
        };
        self.push_event(ev.clone());
        Some(ev)
    }

    /// Snapshot of the most recent events. Newest first.
    pub fn recent_events(&self, limit: usize) -> Vec<AnomalyEvent> {
        let evs = self.events.read();
        let n = limit.min(evs.len());
        let mut out = Vec::with_capacity(n);
        for ev in evs.iter().rev().take(n) {
            out.push(ev.clone());
        }
        out
    }

    /// Total events ever recorded (since process start). Useful for
    /// metrics export.
    pub fn event_count(&self) -> usize {
        self.events.read().len()
    }

    fn push_event(&self, ev: AnomalyEvent) {
        let mut evs = self.events.write();
        if evs.len() >= self.config.event_buffer_size {
            evs.pop_front();
        }
        evs.push_back(ev);
    }
}

/// Per-query observation passed to the detector. Built by the proxy
/// at hook time; populated as much as the proxy knows about the
/// query.
///
/// `fingerprint` and `sql` are [`Cow`]s so the hot path can lend the
/// detector views into buffers it already owns — the proxy's wire
/// frame for the SQL, a reusable scratch String for the fingerprint —
/// instead of copying every statement into the observation. The
/// detector only takes ownership on the rare paths that actually
/// retain a value (a first-seen fingerprint, an emitted event's
/// bounded excerpt). `.into()` on a `&str` or `String` still builds
/// one, so owning callers are unaffected.
#[derive(Debug, Clone)]
pub struct QueryObservation<'a> {
    /// Tenant identifier (or "default" / "" when no multi-tenancy).
    pub tenant: String,
    /// Canonical query fingerprint (literals normalised). Same shape
    /// the analytics module produces.
    pub fingerprint: Cow<'a, str>,
    /// Raw SQL — used for SQL-injection scanning + UI excerpt.
    pub sql: Cow<'a, str>,
    /// Wall-clock timestamp the query arrived. Detectors compute
    /// rates against this.
    pub timestamp: Instant,
}

/// Sliding 60s window of failed auths. Auto-evicts entries older
/// than `window_secs`.
struct AuthBurstWindow {
    window: Duration,
    failures: VecDeque<Instant>,
}

impl AuthBurstWindow {
    fn new(window_secs: u64) -> Self {
        Self {
            window: Duration::from_secs(window_secs),
            failures: VecDeque::new(),
        }
    }

    fn record_failure(&mut self, now: Instant) -> u32 {
        // Evict entries older than the window.
        while let Some(&front) = self.failures.front() {
            if now.duration_since(front) > self.window {
                self.failures.pop_front();
            } else {
                break;
            }
        }
        self.failures.push_back(now);
        self.failures.len() as u32
    }
}

thread_local! {
    /// Reusable lower-case scratch buffer for the SQL-injection scan.
    /// Thread-local rather than a field on the detector: the detector
    /// is shared across every connection task, and this keeps the
    /// buffer lock-free while still amortising the allocation.
    static LOWER_SCRATCH: RefCell<String> = const { RefCell::new(String::new()) };
}

/// Bounded, copy-at-most-`max`-bytes excerpt of `s` for display in an
/// event. Truncation snaps *down* to the nearest UTF-8 character
/// boundary, so a multi-byte character straddling `max` is dropped
/// rather than split (slicing mid-character would panic).
fn excerpt(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs<'a>(tenant: &str, fp: &'a str, sql: &'a str) -> QueryObservation<'a> {
        QueryObservation {
            tenant: tenant.into(),
            fingerprint: fp.into(),
            sql: sql.into(),
            timestamp: Instant::now(),
        }
    }

    #[test]
    fn novel_query_fires_once_per_fingerprint() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let evs = d.record_query(&obs("acme", "fp1", "SELECT 1"));
        assert!(evs
            .iter()
            .any(|e| matches!(e, AnomalyEvent::NovelQuery { .. })));
        let evs2 = d.record_query(&obs("acme", "fp1", "SELECT 1"));
        assert!(!evs2
            .iter()
            .any(|e| matches!(e, AnomalyEvent::NovelQuery { .. })));
    }

    #[test]
    fn novel_query_can_be_suppressed_via_config() {
        let cfg = AnomalyConfig {
            emit_novel_queries: false,
            ..Default::default()
        };
        let d = AnomalyDetector::new(cfg);
        let evs = d.record_query(&obs("acme", "fp1", "SELECT 1"));
        assert!(!evs
            .iter()
            .any(|e| matches!(e, AnomalyEvent::NovelQuery { .. })));
    }

    #[test]
    fn default_max_seen_fingerprints_matches_const() {
        assert_eq!(
            AnomalyConfig::default().max_seen_fingerprints,
            MAX_SEEN_FINGERPRINTS
        );
    }

    #[test]
    fn seen_fingerprint_set_is_bounded_by_configured_cap() {
        // A tiny cap forces the set to clear on overflow, so a previously-seen
        // fingerprint re-fires as novel after the reset — proving the cap comes
        // from config (not the const).
        let cfg = AnomalyConfig {
            max_seen_fingerprints: 2,
            ..Default::default()
        };
        let d = AnomalyDetector::new(cfg);
        let is_novel = |evs: &[AnomalyEvent]| {
            evs.iter()
                .any(|e| matches!(e, AnomalyEvent::NovelQuery { .. }))
        };
        assert!(is_novel(&d.record_query(&obs("a", "fp0", "SELECT 1"))));
        assert!(is_novel(&d.record_query(&obs("a", "fp1", "SELECT 2"))));
        // len == 2 >= cap 2: this insert clears the set first, then records fp2.
        assert!(is_novel(&d.record_query(&obs("a", "fp2", "SELECT 3"))));
        // fp0 was evicted by the clear, so it is novel again.
        assert!(is_novel(&d.record_query(&obs("a", "fp0", "SELECT 1"))));
    }

    #[test]
    fn sql_injection_detector_flags_classic_or_payload() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let evs = d.record_query(&obs(
            "acme",
            "fp-inj",
            "SELECT * FROM users WHERE id = 1 OR 1=1 --",
        ));
        let sqli = evs
            .iter()
            .find(|e| matches!(e, AnomalyEvent::SqlInjection { .. }));
        assert!(sqli.is_some(), "expected SqlInjection event in {:?}", evs);
    }

    #[test]
    fn auth_burst_warning_below_critical_threshold() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let now = Instant::now();
        let mut last = None;
        for _ in 0..6 {
            last = d.record_auth("alice", "10.0.0.1", false, now, "ts");
        }
        match last {
            Some(AnomalyEvent::AuthBurst {
                failures, severity, ..
            }) => {
                assert_eq!(failures, 6);
                assert_eq!(severity, Severity::Warning);
            }
            other => panic!("expected AuthBurst Warning, got {:?}", other),
        }
    }

    #[test]
    fn auth_burst_critical_at_high_threshold() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let now = Instant::now();
        let mut last = None;
        for _ in 0..12 {
            last = d.record_auth("alice", "10.0.0.1", false, now, "ts");
        }
        match last {
            Some(AnomalyEvent::AuthBurst {
                failures, severity, ..
            }) => {
                assert_eq!(failures, 12);
                assert_eq!(severity, Severity::Critical);
            }
            other => panic!("expected AuthBurst Critical, got {:?}", other),
        }
    }

    #[test]
    fn auth_success_resets_burst_window() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let now = Instant::now();
        for _ in 0..6 {
            let _ = d.record_auth("alice", "10.0.0.1", false, now, "ts");
        }
        // Successful auth clears the window — next failure starts at 1.
        let _ = d.record_auth("alice", "10.0.0.1", true, now, "ts");
        let r = d.record_auth("alice", "10.0.0.1", false, now, "ts");
        // 1 failure is below the warning threshold (5) — None.
        assert!(r.is_none());
    }

    #[test]
    fn recent_events_returns_newest_first() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let _ = d.record_query(&obs("a", "fp1", "SELECT 1"));
        let _ = d.record_query(&obs("a", "fp2", "SELECT 2"));
        let _ = d.record_query(&obs("a", "fp3", "SELECT 3"));
        let recent = d.recent_events(10);
        // First event in `recent` is the newest novel-query (fp3).
        match &recent[0] {
            AnomalyEvent::NovelQuery { fingerprint, .. } => {
                assert_eq!(fingerprint, "fp3")
            }
            other => panic!("expected NovelQuery fp3, got {:?}", other),
        }
    }

    #[test]
    fn recent_events_respects_limit() {
        let d = AnomalyDetector::new(AnomalyConfig::default());
        for i in 0..50 {
            let fp = format!("fp{}", i);
            let _ = d.record_query(&obs("a", &fp, "SELECT 1"));
        }
        assert_eq!(d.recent_events(10).len(), 10);
        assert_eq!(d.recent_events(100).len(), 50);
    }

    #[test]
    fn event_buffer_evicts_oldest_when_full() {
        let cfg = AnomalyConfig {
            event_buffer_size: 5,
            ..Default::default()
        };
        let d = AnomalyDetector::new(cfg);
        for i in 0..20 {
            let _ = d.record_query(&obs("a", &format!("fp{}", i), "SELECT 1"));
        }
        // Buffer holds at most 5; total event_count reflects current
        // buffer size, not lifetime count (simpler than tracking
        // separately).
        assert_eq!(d.event_count(), 5);
    }

    #[test]
    fn excerpt_truncates_on_char_boundary() {
        // A multi-byte character straddling `max` must be dropped,
        // not split — slicing mid-character would panic.
        let s = "é".repeat(20); // 40 bytes, boundaries at even offsets
        let e = excerpt(&s, 5);
        assert_eq!(e, "éé…");
        // Whole excerpt stays inside the byte budget.
        assert!(e.len() - "…".len() <= 5);
        // A max landing exactly on a boundary keeps that many bytes.
        assert_eq!(excerpt(&s, 4), "éé…");
        // Short strings are returned verbatim.
        assert_eq!(excerpt("ünïcode", 64), "ünïcode");
        // Degenerate max smaller than the first character.
        assert_eq!(excerpt(&s, 1), "…");
    }

    #[test]
    fn sql_injection_excerpt_survives_multibyte_payload() {
        // Regression: a >200-byte payload whose 200th byte falls
        // inside a multi-byte character used to panic while building
        // the event excerpt.
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let sql = format!("SELECT * FROM t WHERE n = '{}' OR 1=1", "é".repeat(200));
        let evs = d.record_query(&obs("acme", "fp-mb", &sql));
        assert!(
            evs.iter()
                .any(|e| matches!(e, AnomalyEvent::SqlInjection { .. })),
            "expected SqlInjection event in {:?}",
            evs
        );
    }

    #[test]
    fn injection_detection_is_case_and_unicode_stable_across_observations() {
        // The scan runs off a reused thread-local lower-case buffer;
        // a long non-ASCII statement followed by a short ASCII one
        // must not leak residue between observations.
        let d = AnomalyDetector::new(AnomalyConfig::default());
        let long_unicode = format!("SELECT * FROM «Ünïcode» WHERE n = '{}'", "Ä".repeat(300));
        let _ = d.record_query(&obs("acme", "fp-a", &long_unicode));
        let evs = d.record_query(&obs("acme", "fp-b", "SELECT 1"));
        assert!(
            !evs.iter()
                .any(|e| matches!(e, AnomalyEvent::SqlInjection { .. })),
            "clean query flagged after a long one: {:?}",
            evs
        );
        // …and an upper-case payload still fires.
        let evs = d.record_query(&obs(
            "acme",
            "fp-c",
            "FOO' UNION SELECT PASSWORD FROM USERS",
        ));
        assert!(
            evs.iter()
                .any(|e| matches!(e, AnomalyEvent::SqlInjection { .. })),
            "upper-case payload missed: {:?}",
            evs
        );
    }

    #[test]
    fn borrowed_and_owned_observations_detect_identically() {
        let sql = "SELECT * FROM users WHERE id = 1 OR 1=1 --";
        let d1 = AnomalyDetector::new(AnomalyConfig::default());
        let borrowed = d1.record_query(&QueryObservation {
            tenant: "acme".into(),
            fingerprint: Cow::Borrowed("fp"),
            sql: Cow::Borrowed(sql),
            timestamp: Instant::now(),
        });
        let d2 = AnomalyDetector::new(AnomalyConfig::default());
        let owned = d2.record_query(&QueryObservation {
            tenant: "acme".into(),
            fingerprint: Cow::Owned("fp".to_string()),
            sql: Cow::Owned(sql.to_string()),
            timestamp: Instant::now(),
        });
        // Same event kinds, in the same order, with the same payload
        // fields (only the wall-clock stamp may differ).
        assert_eq!(borrowed.len(), owned.len());
        for (a, b) in borrowed.iter().zip(owned.iter()) {
            assert_eq!(
                std::mem::discriminant(a),
                std::mem::discriminant(b),
                "event kinds diverged: {:?} vs {:?}",
                a,
                b
            );
            if let (
                AnomalyEvent::SqlInjection {
                    sql_excerpt: ea,
                    patterns_matched: pa,
                    severity: sa,
                    ..
                },
                AnomalyEvent::SqlInjection {
                    sql_excerpt: eb,
                    patterns_matched: pb,
                    severity: sb,
                    ..
                },
            ) = (a, b)
            {
                assert_eq!(ea, eb);
                assert_eq!(pa, pb);
                assert_eq!(sa, sb);
            }
            if let (
                AnomalyEvent::NovelQuery {
                    fingerprint: fa,
                    sql_excerpt: ea,
                    ..
                },
                AnomalyEvent::NovelQuery {
                    fingerprint: fb,
                    sql_excerpt: eb,
                    ..
                },
            ) = (a, b)
            {
                assert_eq!(fa, fb);
                assert_eq!(ea, eb);
            }
        }
        assert!(
            borrowed
                .iter()
                .any(|e| matches!(e, AnomalyEvent::SqlInjection { .. })),
            "expected the payload to be flagged: {:?}",
            borrowed
        );
        // A borrowed fingerprint is still retained by the seen-set.
        assert!(d1
            .record_query(&obs("acme", "fp", sql))
            .iter()
            .all(|e| !matches!(e, AnomalyEvent::NovelQuery { .. })));
    }
}
