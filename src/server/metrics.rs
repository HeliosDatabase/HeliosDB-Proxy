use super::*;

/// Server metrics
#[derive(Default)]
pub(super) struct ServerMetrics {
    /// Total connections accepted
    pub(super) connections_accepted: AtomicU64,
    /// Total connections refused because the `[limits] max_client_connections`
    /// cap was already saturated. The refusal happens after the socket is
    /// accepted and its first startup message classified (so a `CancelRequest`
    /// is never refused), hence a rejected connection IS also counted in
    /// `connections_accepted` — and in `connections_closed` when it ends, so
    /// their difference stays a correct active-session gauge. This counter is
    /// the separate "how often did the cap bite" signal.
    pub(super) connections_rejected: AtomicU64,
    /// Total connections closed
    pub(super) connections_closed: AtomicU64,
    /// Total queries processed
    pub(super) queries_processed: AtomicU64,
    /// Total bytes received from clients
    pub(super) bytes_received: AtomicU64,
    /// Total bytes sent to clients
    pub(super) bytes_sent: AtomicU64,
    /// Failover count
    pub(super) failovers: AtomicU64,
    /// Responses whose capture-for-caching was abandoned because the response
    /// exceeded `[cache] max_cacheable_response_bytes`. Non-zero means reads
    /// are bypassing the response caches on size grounds (the client still
    /// receives every byte) — either the workload returns huge result sets or
    /// the ceiling is set too low.
    pub(super) cache_capture_oversize: AtomicU64,
    /// P-03: client admissions that had to wait for a permit at the
    /// `max_client_connections` cap (the bounded H-05 queue bit).
    pub(super) admission_waited: AtomicU64,
    /// P-03: admissions whose bounded wait expired and were refused with 53300.
    /// A subset of `connections_rejected`; the difference is the immediate
    /// refusals.
    pub(super) admission_timeouts: AtomicU64,
    /// P-03: jittered waits performed by the primary-select recovery loops. A
    /// large burst after a failover is the reconnect wave H-05 bounds.
    pub(super) reconnect_attempts: AtomicU64,
    /// Backend connections the backend refused at `max_connections` (53300)
    /// that were retried after freeing an idle pooled connection.
    pub(super) backend_capacity_waits: AtomicU64,
    /// Clients (or transaction-mode redials) refused with 53300 because the
    /// backend stayed at `max_connections` for the whole `acquire_timeout`.
    pub(super) backend_capacity_refusals: AtomicU64,
    /// TR-07: transactions the recovery journal recorded as committed.
    pub(super) journal_committed: AtomicU64,
    /// TR-07: captured transactions the backend rolled back (or that were
    /// lost with their session) and were dropped from the active journal.
    pub(super) journal_rolled_back: AtomicU64,
    /// TR-07: statements appended to the journal (committed or not).
    pub(super) journal_statements: AtomicU64,
    /// In-session Transaction Replay (`tr_mode`) counters.
    pub(super) tr: TrMetrics,
}

/// In-session Transaction Replay counters (see `TrMode` / `tr_decide`).
#[derive(Default)]
pub(super) struct TrMetrics {
    /// Sessions re-homed onto a replacement backend after a fault.
    pub(super) failovers: AtomicU64,
    /// In-flight statements transparently re-executed on the new backend.
    pub(super) statements_reexecuted: AtomicU64,
    /// Explicit transactions successfully replayed on the new backend.
    pub(super) transactions_replayed: AtomicU64,
    /// Transaction replays that failed (client received SQLSTATE 40001).
    pub(super) replay_failures: AtomicU64,
    /// SQLSTATE 08007 `transaction_resolution_unknown` errors returned.
    pub(super) unknown_outcome_errors: AtomicU64,
    /// Transactions marked non-replayable because they exceeded
    /// `[limits] tr_max_replay_statements` / `tr_max_replay_bytes`.
    pub(super) replay_cap_exceeded: AtomicU64,
    /// Sessions whose `SET` tracking stopped at
    /// `[limits] tr_max_session_set_statements`.
    pub(super) session_set_cap_exceeded: AtomicU64,
}

impl TrMetrics {
    pub(super) fn snapshot(&self) -> TrMetricsSnapshot {
        TrMetricsSnapshot {
            failovers: self.failovers.load(Ordering::Relaxed),
            statements_reexecuted: self.statements_reexecuted.load(Ordering::Relaxed),
            transactions_replayed: self.transactions_replayed.load(Ordering::Relaxed),
            replay_failures: self.replay_failures.load(Ordering::Relaxed),
            unknown_outcome_errors: self.unknown_outcome_errors.load(Ordering::Relaxed),
            replay_cap_exceeded: self.replay_cap_exceeded.load(Ordering::Relaxed),
            session_set_cap_exceeded: self.session_set_cap_exceeded.load(Ordering::Relaxed),
        }
    }
}

/// Metrics snapshot for external consumption
#[derive(Debug, Clone)]
pub struct ServerMetricsSnapshot {
    pub connections_accepted: u64,
    /// Connections refused at accept time by the `[limits]
    /// max_client_connections` cap. Disjoint from `connections_accepted`.
    pub connections_rejected: u64,
    pub connections_closed: u64,
    pub queries_processed: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub failovers: u64,
    /// Cacheable reads whose response outgrew
    /// `[cache] max_cacheable_response_bytes` and were therefore not cached.
    pub cache_capture_oversize: u64,
    /// P-03: admissions that waited on the bounded cap queue (H-05).
    pub admission_waited: u64,
    /// P-03: admissions whose bounded wait expired (subset of rejections).
    pub admission_timeouts: u64,
    /// P-03: jittered waits in the primary-select recovery loops.
    pub reconnect_attempts: u64,
    /// Startup / redial retries after the backend refused at `max_connections`.
    pub backend_capacity_waits: u64,
    /// Refusals passed to the client as 53300 after the capacity wait expired.
    pub backend_capacity_refusals: u64,
    /// TR-07: transactions the recovery journal recorded as committed.
    pub journal_committed: u64,
    /// TR-07: captured transactions the backend rolled back.
    pub journal_rolled_back: u64,
    /// TR-07: statements appended to the journal.
    pub journal_statements: u64,
    /// In-session Transaction Replay (`tr_mode`) counters.
    pub tr: TrMetricsSnapshot,
}

/// In-session Transaction Replay counters (see `TrMode`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrMetricsSnapshot {
    /// Sessions re-homed onto a replacement backend after a backend fault.
    pub failovers: u64,
    /// In-flight statements transparently re-executed on the new backend.
    pub statements_reexecuted: u64,
    /// Explicit transactions successfully replayed on the new backend.
    pub transactions_replayed: u64,
    /// Transaction replays that failed (client received SQLSTATE 40001).
    pub replay_failures: u64,
    /// SQLSTATE 08007 `transaction_resolution_unknown` errors returned.
    pub unknown_outcome_errors: u64,
    /// Transactions marked non-replayable by `[limits] tr_max_replay_*`.
    pub replay_cap_exceeded: u64,
    /// Sessions whose `SET` tracking hit `[limits] tr_max_session_set_statements`.
    pub session_set_cap_exceeded: u64,
}

/// Pool mode statistics snapshot (when pool-modes feature is enabled)
#[cfg(feature = "pool-modes")]
#[derive(Debug, Clone)]
pub struct PoolModeStatsSnapshot {
    /// Current pooling mode
    pub mode: String,
    /// Total connections across all pools
    pub total_connections: usize,
    /// Active (leased) connections
    pub active_leases: usize,
    /// Idle connections
    pub idle_connections: usize,
    /// Number of nodes in the pool
    pub node_count: usize,
    /// Total connection acquires
    pub acquires: u64,
    /// Total connection releases
    pub releases: u64,
    /// Failed acquire attempts
    pub acquire_failures: u64,
    /// Acquire timeouts
    pub acquire_timeouts: u64,
    /// Completed transactions (Transaction mode)
    pub transactions_completed: u64,
    /// Total statements executed
    pub statements_executed: u64,
    /// Average lease duration in milliseconds
    pub avg_lease_duration_ms: u64,
}
