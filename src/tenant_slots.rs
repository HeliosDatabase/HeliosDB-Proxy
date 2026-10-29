//! Per-tenant client admission (H-05 slice 2) and request budgets
//! (ac858d61400b).
//!
//! `[multi_tenancy] max_clients_per_tenant` / `tenant_max_clients` bound the
//! client sessions one known tenant holds on this proxy, with a FIFO
//! semaphore per capped tenant. A session holds its tenant's slot (an owned
//! permit) for its whole life. The tenant is read from the startup packet the
//! same way the tenant manager identifies it (`identify_by`: a startup
//! parameter, or `database`). Tenants without a cap, and sessions naming no
//! known tenant, have no entry and are admitted by the process-wide
//! `[limits] max_client_connections` alone.
//!
//! Admission takes the tenant slot BEFORE the process-wide one, so sessions
//! of a tenant queued at its own cap never sit on process-wide capacity that
//! other tenants could use.
//!
//! The request budgets ([`TenantBudget`]) are identified the same way, from
//! the startup packet:
//!
//! * `max_active_queries_per_tenant` / `tenant_max_active_queries`: a FIFO
//!   semaphore of active-query permits per capped tenant. A request that
//!   starts while its session is idle takes a permit (borrowed, no `Arc`
//!   refcount traffic) and holds it until the session is idle again, so an
//!   explicit transaction holds one permit from BEGIN to its end. At the cap a
//!   request waits up to `active_query_wait_ms`, then is refused (53300).
//! * `idle_permit_timeout_ms`: a session that sits idle while holding a
//!   permit (in a transaction, holding session-level advisory locks, or
//!   behind a plugin-blocked Query that left an extended cycle open) is
//!   terminated after that long.
//! * `max_replay_bytes_per_tenant` / `tenant_max_replay_bytes`: one counter
//!   of transaction-replay bytes per tenant, the sum of its sessions'
//!   [`TenantCharge`]s. A statement whose recording would take the tenant over
//!   marks its transaction non-replayable.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{AcquireError, Notify, OwnedSemaphorePermit, Semaphore, SemaphorePermit};

use crate::config::{ProxyConfig, TrMode};
use crate::metrics_hist::LatencyHistogram;

struct TenantSlot {
    limit: usize,
    sem: Arc<Semaphore>,
    waiting: AtomicUsize,
    waits: AtomicU64,
    refusals: AtomicU64,
}

/// Counts one waiter while alive: a wait future dropped mid-wait (a cancel,
/// a client gone) never leaves the `waiting` gauge raised. With `record`,
/// the wait's length goes into that histogram once it ends, however it ends
/// (admitted, timed out, or dropped).
struct Waiting<'s> {
    gauge: &'s AtomicUsize,
    started: Instant,
    record: Option<&'s LatencyHistogram>,
}

impl<'s> Waiting<'s> {
    fn new(gauge: &'s AtomicUsize, record: Option<&'s LatencyHistogram>) -> Self {
        gauge.fetch_add(1, Ordering::Relaxed);
        Self {
            gauge,
            started: Instant::now(),
            record,
        }
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.gauge.fetch_sub(1, Ordering::Relaxed);
        if let Some(h) = self.record {
            h.record(self.started.elapsed());
        }
    }
}

impl TenantSlot {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            sem: Arc::new(Semaphore::new(limit)),
            waiting: AtomicUsize::new(0),
            waits: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
        }
    }

    /// Permits taken right now.
    fn in_use(&self) -> usize {
        self.limit - self.sem.available_permits().min(self.limit)
    }

    /// Count a refusal; returns the running total.
    fn refuse(&self) -> u64 {
        self.refusals.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The timed FIFO wait shared by the client caps (owned permits) and the
    /// active-query caps (borrowed permits): `acquire` builds the semaphore
    /// future. `None` refuses at once. Returns the permit and the time waited,
    /// or the running refusal count. With `record`, every wait (one per count
    /// in `waits`) goes into that histogram when it ends.
    async fn wait_for<P, F>(
        &self,
        wait: Option<Duration>,
        record: Option<&LatencyHistogram>,
        acquire: impl FnOnce() -> F,
    ) -> std::result::Result<(P, Duration), u64>
    where
        F: Future<Output = std::result::Result<P, AcquireError>>,
    {
        let Some(wait) = wait else {
            return Err(self.refuse());
        };
        self.waits.fetch_add(1, Ordering::Relaxed);
        let waiting = Waiting::new(&self.waiting, record);
        let started = waiting.started;
        let acquired = tokio::time::timeout(wait, acquire()).await;
        drop(waiting);
        match acquired {
            Ok(Ok(p)) => Ok((p, started.elapsed())),
            _ => Err(self.refuse()),
        }
    }
}

/// What `/metrics` reports for one capped tenant.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TenantSlotStats {
    pub tenant: String,
    pub limit: usize,
    pub in_use: usize,
    /// Sessions waiting for a slot right now.
    pub waiting: usize,
    pub waits: u64,
    pub refusals: u64,
}

/// The tenant is at its cap and no slot came free within the wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRefusal {
    pub tenant: String,
    pub limit: usize,
}

impl std::fmt::Display for TenantRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "too many connections for tenant \"{}\" (limit {})",
            self.tenant, self.limit
        )
    }
}

/// One tenant's request budgets (ac858d61400b): its active-query cap and
/// its replay-byte budget, either of which may be absent. Built once at
/// startup.
pub struct TenantBudget {
    tenant: String,
    /// `None`: no active-query cap.
    queries: Option<TenantSlot>,
    /// `active_query_wait_ms`; `None` refuses at once.
    query_wait: Option<Duration>,
    /// `idle_permit_timeout_ms`; `None` never terminates an idle holder.
    idle_permit_timeout: Option<Duration>,
    queries_admitted: AtomicU64,
    /// Permit holders idle right now (in a transaction, holding locks, behind
    /// a blocked query that left a cycle open).
    queries_idle: AtomicUsize,
    /// Waits ended by a CancelRequest or the client going away.
    query_wait_cancels: AtomicU64,
    idle_permit_timeouts: AtomicU64,
    /// 0 = no replay budget.
    replay_limit: usize,
    replay_in_use: AtomicUsize,
    replay_refusals: AtomicU64,
}

/// A request refused at its tenant's active-query cap. `nth` is the tenant's
/// running refusal count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantQueryRefusal {
    pub tenant: String,
    pub limit: usize,
    pub nth: u64,
}

impl std::fmt::Display for TenantQueryRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "too many active queries for tenant \"{}\" (limit {})",
            self.tenant, self.limit
        )
    }
}

/// What `/metrics` reports for one tenant with a request budget.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TenantBudgetStats {
    pub tenant: String,
    /// The active-query cap; 0 = none.
    pub query_limit: usize,
    /// Permits held right now.
    pub queries_active: usize,
    /// Permit holders idle right now (in a transaction, holding locks, behind
    /// a blocked query that left a cycle open).
    pub queries_idle: usize,
    /// Requests waiting for a permit right now.
    pub queries_waiting: usize,
    pub queries_admitted: u64,
    pub query_waits: u64,
    pub query_refusals: u64,
    pub query_wait_cancels: u64,
    pub idle_permit_timeouts: u64,
    /// The replay-byte budget; 0 = none.
    pub replay_byte_limit: usize,
    /// Replay bytes the tenant's sessions hold right now.
    pub replay_bytes: usize,
    pub replay_refusals: u64,
}

impl TenantBudget {
    fn new(
        tenant: String,
        query_limit: usize,
        query_wait: Option<Duration>,
        idle_permit_timeout: Option<Duration>,
        replay_limit: usize,
    ) -> Self {
        Self {
            tenant,
            queries: (query_limit > 0).then(|| TenantSlot::new(query_limit)),
            query_wait,
            idle_permit_timeout,
            queries_admitted: AtomicU64::new(0),
            queries_idle: AtomicUsize::new(0),
            query_wait_cancels: AtomicU64::new(0),
            idle_permit_timeouts: AtomicU64::new(0),
            replay_limit,
            replay_in_use: AtomicUsize::new(0),
            replay_refusals: AtomicU64::new(0),
        }
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn has_query_cap(&self) -> bool {
        self.queries.is_some()
    }

    pub fn has_replay_budget(&self) -> bool {
        self.replay_limit > 0
    }

    pub fn idle_permit_timeout(&self) -> Option<Duration> {
        self.idle_permit_timeout
    }

    /// Fast path: one CAS. The permit is borrowed (no `Arc` refcount
    /// traffic). `None` at the cap, or without an active-query cap.
    pub fn try_acquire_query(&self) -> Option<SemaphorePermit<'_>> {
        let permit = self.queries.as_ref()?.sem.try_acquire().ok()?;
        self.queries_admitted.fetch_add(1, Ordering::Relaxed);
        Some(permit)
    }

    /// Slow path: a FIFO wait up to `active_query_wait_ms`. Cancel-safe:
    /// dropping it leaves no waiter counted. `Ok` carries the time waited
    /// when there was a wait (a permit that came free since the fast path
    /// failed is taken without one). Every wait goes into `waits` when it
    /// ends, however it ends: admitted, refused at its timeout, or dropped
    /// (a CancelRequest, the client gone). The admission is not counted here:
    /// a CancelRequest can still claim the request, so the caller counts it
    /// with [`Self::note_query_admitted`] once it goes ahead. Callers gate on
    /// [`Self::has_query_cap`]; without a cap every call is refused with
    /// limit 0.
    pub async fn wait_query(
        &self,
        waits: &LatencyHistogram,
    ) -> std::result::Result<(SemaphorePermit<'_>, Option<Duration>), TenantQueryRefusal> {
        let Some(slot) = self.queries.as_ref() else {
            return Err(self.refusal(0, 0));
        };
        if let Ok(p) = slot.sem.try_acquire() {
            return Ok((p, None));
        }
        match slot
            .wait_for(self.query_wait, Some(waits), || slot.sem.acquire())
            .await
        {
            Ok((p, waited)) => Ok((p, Some(waited))),
            Err(nth) => Err(self.refusal(slot.limit, nth)),
        }
    }

    /// A request admitted through [`Self::wait_query`] goes ahead.
    pub fn note_query_admitted(&self) {
        self.queries_admitted.fetch_add(1, Ordering::Relaxed);
    }

    fn refusal(&self, limit: usize, nth: u64) -> TenantQueryRefusal {
        TenantQueryRefusal {
            tenant: self.tenant.clone(),
            limit,
            nth,
        }
    }

    /// A wait ended by a CancelRequest or by the client going away.
    pub fn note_wait_cancelled(&self) {
        self.query_wait_cancels.fetch_add(1, Ordering::Relaxed);
    }

    /// A permit holder was terminated by `idle_permit_timeout_ms`.
    pub fn note_idle_permit_timeout(&self) {
        self.idle_permit_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    fn stats(&self) -> TenantBudgetStats {
        let q = self.queries.as_ref();
        TenantBudgetStats {
            tenant: self.tenant.clone(),
            query_limit: q.map_or(0, |s| s.limit),
            queries_active: q.map_or(0, TenantSlot::in_use),
            queries_idle: self.queries_idle.load(Ordering::Relaxed),
            queries_waiting: q.map_or(0, |s| s.waiting.load(Ordering::Relaxed)),
            queries_admitted: self.queries_admitted.load(Ordering::Relaxed),
            query_waits: q.map_or(0, |s| s.waits.load(Ordering::Relaxed)),
            query_refusals: q.map_or(0, |s| s.refusals.load(Ordering::Relaxed)),
            query_wait_cancels: self.query_wait_cancels.load(Ordering::Relaxed),
            idle_permit_timeouts: self.idle_permit_timeouts.load(Ordering::Relaxed),
            replay_byte_limit: self.replay_limit,
            replay_bytes: self.replay_in_use.load(Ordering::Relaxed),
            replay_refusals: self.replay_refusals.load(Ordering::Relaxed),
        }
    }
}

/// A session's hold on its tenant's active-query cap, local to the client
/// loop. Keeps `queries_idle` exact: only a held permit is ever marked idle,
/// and dropping the hold clears the mark before the permit returns.
pub struct QueryHold<'a> {
    budget: &'a TenantBudget,
    permit: Option<SemaphorePermit<'a>>,
    idle: bool,
}

impl<'a> QueryHold<'a> {
    pub fn new(budget: &'a TenantBudget) -> Self {
        Self {
            budget,
            permit: None,
            idle: false,
        }
    }

    pub fn budget(&self) -> &'a TenantBudget {
        self.budget
    }

    pub fn is_held(&self) -> bool {
        self.permit.is_some()
    }

    pub fn hold(&mut self, permit: SemaphorePermit<'a>) {
        self.permit = Some(permit);
    }

    /// Return the permit (and clear the idle mark).
    pub fn release(&mut self) {
        self.set_idle(false);
        self.permit = None;
    }

    /// Mark the holder idle (waiting for its client) or busy. Only a held
    /// permit is ever marked idle; the gauge moves on transitions only.
    pub fn set_idle(&mut self, idle: bool) {
        let idle = idle && self.permit.is_some();
        if idle != self.idle {
            self.idle = idle;
            if idle {
                self.budget.queries_idle.fetch_add(1, Ordering::Relaxed);
            } else {
                self.budget.queries_idle.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

impl Drop for QueryHold<'_> {
    fn drop(&mut self) {
        self.set_idle(false);
    }
}

/// A session's charge against its tenant's replay-byte budget, returned when
/// dropped (session end, panics included). Only the session's own recorder
/// moves it, under its `tx_state` lock.
pub struct TenantCharge {
    budget: Arc<TenantBudget>,
    replay_charged: AtomicUsize,
}

impl TenantCharge {
    pub fn new(budget: Arc<TenantBudget>) -> Self {
        Self {
            budget,
            replay_charged: AtomicUsize::new(0),
        }
    }

    pub fn budget(&self) -> &TenantBudget {
        &self.budget
    }

    /// Charge up to `retained` bytes only if the tenant stays within its
    /// budget (one CAS). A shrink always succeeds. Counts a refusal on
    /// failure. Without a replay budget: `true`, touching nothing.
    pub fn replay_reserve(&self, retained: usize) -> bool {
        let limit = self.budget.replay_limit;
        if limit == 0 {
            return true;
        }
        let charged = self.replay_charged.load(Ordering::Relaxed);
        if retained <= charged {
            self.replay_settle(retained);
            return true;
        }
        let delta = retained - charged;
        // One CAS (retried only under contention), growing only within the
        // limit.
        let in_use = &self.budget.replay_in_use;
        let mut used = in_use.load(Ordering::Relaxed);
        let grown = loop {
            let Some(next) = used.checked_add(delta).filter(|n| *n <= limit) else {
                break false;
            };
            match in_use.compare_exchange_weak(used, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break true,
                Err(actual) => used = actual,
            }
        };
        if grown {
            self.replay_charged.store(retained, Ordering::Relaxed);
            true
        } else {
            self.budget.replay_refusals.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Check only (the Flush projection): whether the tenant's other charges
    /// plus `retained` fit the budget. Counts nothing: a caller that refuses
    /// on `false` says so with [`Self::note_replay_refusal`]. Without a
    /// replay budget: `true`, touching nothing.
    pub fn replay_room(&self, retained: usize) -> bool {
        let limit = self.budget.replay_limit;
        if limit == 0 {
            return true;
        }
        let others = self
            .budget
            .replay_in_use
            .load(Ordering::Relaxed)
            .saturating_sub(self.replay_charged.load(Ordering::Relaxed));
        others.saturating_add(retained) <= limit
    }

    /// Count one refusal by the tenant's replay-byte budget.
    pub fn note_replay_refusal(&self) {
        self.budget.replay_refusals.fetch_add(1, Ordering::Relaxed);
    }

    /// Set this session's charge to `retained` bytes. Without a replay
    /// budget: a no-op.
    pub fn replay_settle(&self, retained: usize) {
        if self.budget.replay_limit == 0 {
            return;
        }
        let prev = self.replay_charged.swap(retained, Ordering::Relaxed);
        if retained > prev {
            self.budget
                .replay_in_use
                .fetch_add(retained - prev, Ordering::Relaxed);
        } else if prev > retained {
            self.budget
                .replay_in_use
                .fetch_sub(prev - retained, Ordering::Relaxed);
        }
    }
}

impl Drop for TenantCharge {
    fn drop(&mut self) {
        self.replay_settle(0);
    }
}

/// One session's wait at its tenant's active-query cap, registered under the
/// BackendKeyData relayed to its client so a CancelRequest can end it. Each
/// wait owns its state: whether a cancel claimed it is its own flag, never
/// inferred from the registry, so another wait registered under the same key
/// can neither cancel it nor be cancelled by it.
#[derive(Default)]
pub struct QueryWait {
    notify: Notify,
    claimed: AtomicBool,
}

impl QueryWait {
    /// Resolves once a CancelRequest claimed this wait (at once if it already
    /// has).
    pub async fn cancelled(&self) {
        self.notify.notified().await;
    }

    /// A CancelRequest claimed this wait.
    pub fn is_cancelled(&self) -> bool {
        self.claimed.load(Ordering::Acquire)
    }
}

/// The live sessions relayed one BackendKeyData, and those of their waits at
/// an active-query cap a CancelRequest naming the key may end.
#[derive(Default)]
struct KeyHolders {
    /// Live sessions relayed the key, of any tenant. Usually one; a backend
    /// that issues colliding keys relays one key to several.
    sessions: usize,
    /// Their registered waits (at most one per session).
    waits: Vec<Arc<QueryWait>>,
}

/// The caps of every known tenant with a client cap above 0, and the request
/// budgets of every known tenant with an active-query cap or a replay-byte
/// budget. Built once at startup; empty unless `[multi_tenancy] enabled =
/// true`.
#[derive(Default)]
pub struct TenantSlots {
    identify_by: String,
    tenants: HashMap<String, TenantSlot>,
    budgets: HashMap<String, Arc<TenantBudget>>,
    /// Some tenant has an active-query cap: only then can a session wait
    /// for a permit, so only then are the sessions behind each key counted.
    track_cancel_keys: bool,
    /// By BackendKeyData: the live sessions relayed it, and the waits at
    /// their tenant's active-query cap a CancelRequest naming it may end.
    /// A cancel can tell which session sent it only when exactly one live
    /// session holds the key, so only then does it end that session's wait.
    cancel_keys: dashmap::DashMap<(u32, u32), KeyHolders>,
}

impl TenantSlots {
    pub fn from_config(config: &ProxyConfig) -> Self {
        let mt = &config.multi_tenancy;
        if !mt.enabled {
            return Self::default();
        }
        let tenants = mt
            .tenants
            .iter()
            .filter_map(|t| {
                let limit = mt.client_cap(t);
                (limit > 0).then(|| (t.clone(), TenantSlot::new(limit)))
            })
            .collect();
        let query_wait =
            (mt.active_query_wait_ms > 0).then(|| Duration::from_millis(mt.active_query_wait_ms));
        let idle_permit_timeout = (mt.idle_permit_timeout_ms > 0)
            .then(|| Duration::from_millis(mt.idle_permit_timeout_ms));
        let budgets: HashMap<String, Arc<TenantBudget>> = mt
            .tenants
            .iter()
            .filter_map(|t| {
                let (queries, replay) = (mt.active_query_cap(t), mt.replay_byte_cap(t));
                (queries > 0 || replay > 0).then(|| {
                    (
                        t.clone(),
                        Arc::new(TenantBudget::new(
                            t.clone(),
                            queries,
                            query_wait,
                            idle_permit_timeout,
                            replay,
                        )),
                    )
                })
            })
            .collect();
        if budgets.values().any(|b| b.has_replay_budget())
            && !matches!(
                config.effective_tr_mode(),
                TrMode::Select | TrMode::Transaction
            )
        {
            tracing::warn!(
                "[multi_tenancy] replay-byte budgets have no effect unless tr_mode is \"select\" or \"transaction\""
            );
        }
        let track_cancel_keys = budgets.values().any(|b| b.has_query_cap());
        if track_cancel_keys && Self::vanished_clients_keep_permits(config) {
            tracing::warn!(
                "[multi_tenancy] active-query caps with [limits] client_tcp_keepalive_secs = 0: a client that vanishes without closing its connection while its session holds a permit keeps that permit until the proxy restarts unless a timeout ends the session; idle_permit_timeout_ms ends only one idle in a transaction or holding advisory locks, [limits] client_idle_timeout_secs also one in the middle of an extended cycle, and nothing ends one in a stalled COPY or with a message partly received"
            );
        }
        Self {
            identify_by: mt.identify_by.clone(),
            tenants,
            budgets,
            track_cancel_keys,
            cancel_keys: dashmap::DashMap::new(),
        }
    }

    /// Whether the session of a client that vanished without a FIN or RST
    /// while holding an active-query permit can wait on its dead client until
    /// the proxy restarts: TCP keepalive on client sockets is off (`[limits]
    /// client_tcp_keepalive_secs = 0`). No timeout covers every such session:
    /// `idle_permit_timeout_ms` ends only an idle permit holder (in a
    /// transaction, holding advisory locks), `[limits]
    /// client_idle_timeout_secs` also one in the middle of an extended cycle
    /// (after a `Flush`, owing its `Sync`), and both are disarmed in a COPY's
    /// copy-in and while a message is partly received. Logged at startup
    /// when a tenant has an active-query cap.
    fn vanished_clients_keep_permits(config: &ProxyConfig) -> bool {
        config.limits.client_tcp_keepalive_secs == 0
    }

    /// No tenant has a client cap.
    pub fn is_empty(&self) -> bool {
        self.tenants.is_empty()
    }

    /// The tenant id a startup packet names (`identify_by`); `identify_by =
    /// "database"` falls back to `user`, as PostgreSQL does for a startup
    /// packet without a database.
    fn key<'p>(&self, params: &'p HashMap<String, String>) -> Option<&'p String> {
        if self.identify_by == "database" {
            params.get("database").or_else(|| params.get("user"))
        } else {
            params.get(&self.identify_by)
        }
    }

    /// The capped tenant a startup packet names, if any. `identify_by =
    /// "database"` falls back to `user`, as PostgreSQL does for a startup
    /// packet without a database.
    pub fn tenant_of<'p>(&self, params: &'p HashMap<String, String>) -> Option<&'p str> {
        let value = self.key(params)?;
        self.tenants.contains_key(value).then_some(value.as_str())
    }

    /// Take a slot of `tenant` for a new client session. Uncapped or unknown
    /// tenant: `None` at once. Full: refuse at once without `wait`, else wait
    /// up to `wait` in FIFO order. Returns the wait in the second value when
    /// there was one.
    pub async fn acquire(
        &self,
        tenant: &str,
        wait: Option<Duration>,
    ) -> Result<(Option<OwnedSemaphorePermit>, Option<Duration>), TenantRefusal> {
        let Some(slot) = self.tenants.get(tenant) else {
            return Ok((None, None));
        };
        if let Ok(p) = slot.sem.clone().try_acquire_owned() {
            return Ok((Some(p), None));
        }
        match slot
            .wait_for(wait, None, || slot.sem.clone().acquire_owned())
            .await
        {
            Ok((p, waited)) => Ok((Some(p), Some(waited))),
            Err(_) => Err(TenantRefusal {
                tenant: tenant.to_string(),
                limit: slot.limit,
            }),
        }
    }

    /// Per capped tenant: limit, sessions holding a slot, sessions waiting,
    /// waits and refusals since start.
    pub fn stats(&self) -> Vec<TenantSlotStats> {
        let mut out: Vec<TenantSlotStats> = self
            .tenants
            .iter()
            .map(|(tenant, s)| TenantSlotStats {
                tenant: tenant.clone(),
                limit: s.limit,
                in_use: s.in_use(),
                waiting: s.waiting.load(Ordering::Relaxed),
                waits: s.waits.load(Ordering::Relaxed),
                refusals: s.refusals.load(Ordering::Relaxed),
            })
            .collect();
        out.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        out
    }

    /// Some tenant has a request budget (an active-query cap or a replay-byte
    /// budget).
    pub fn has_budgets(&self) -> bool {
        !self.budgets.is_empty()
    }

    /// Some tenant has a replay-byte budget.
    pub fn has_replay_budgets(&self) -> bool {
        self.budgets.values().any(|b| b.has_replay_budget())
    }

    /// The request budgets of the tenant a startup packet names, if it has
    /// any (identified like [`Self::tenant_of`]).
    pub fn budget_of(&self, params: &HashMap<String, String>) -> Option<&Arc<TenantBudget>> {
        if self.budgets.is_empty() {
            return None;
        }
        self.budgets.get(self.key(params)?)
    }

    /// The request budgets of `tenant`, if it has any.
    pub fn budget(&self, tenant: &str) -> Option<&Arc<TenantBudget>> {
        self.budgets.get(tenant)
    }

    /// Per tenant with a request budget, sorted by tenant.
    pub fn budget_stats(&self) -> Vec<TenantBudgetStats> {
        let mut out: Vec<TenantBudgetStats> = self.budgets.values().map(|b| b.stats()).collect();
        out.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        out
    }

    /// A live session was relayed the BackendKeyData `key` (once, at its
    /// startup); [`Self::session_key_ended`] when it ends. Counted only while
    /// some tenant has an active-query cap (sessions of every tenant, capped
    /// or not), so [`Self::cancel_query_wait`] knows whether the key names
    /// one session.
    pub fn session_key_relayed(&self, key: (u32, u32)) {
        if self.track_cancel_keys {
            self.cancel_keys.entry(key).or_default().sessions += 1;
        }
    }

    /// The live session relayed `key` ended.
    pub fn session_key_ended(&self, key: (u32, u32)) {
        if !self.track_cancel_keys {
            return;
        }
        if let dashmap::mapref::entry::Entry::Occupied(mut e) = self.cancel_keys.entry(key) {
            let holders = e.get_mut();
            holders.sessions = holders.sessions.saturating_sub(1);
            if holders.sessions == 0 && holders.waits.is_empty() {
                e.remove();
            }
        }
    }

    /// Register a session's wait at its tenant's active-query cap under its
    /// BackendKeyData `key`. The returned wait is the session's own: a
    /// CancelRequest naming the key claims and wakes it (see
    /// [`Self::cancel_query_wait`]), and the session ends it with
    /// [`Self::unregister_query_wait`].
    pub fn register_query_wait(&self, key: (u32, u32)) -> Arc<QueryWait> {
        let wait = Arc::new(QueryWait::default());
        self.cancel_keys
            .entry(key)
            .or_default()
            .waits
            .push(Arc::clone(&wait));
        wait
    }

    /// End the registered `wait`: remove its own entry only (another wait
    /// under the same key stays registered) and report whether a
    /// CancelRequest claimed it first.
    pub fn unregister_query_wait(&self, key: (u32, u32), wait: &Arc<QueryWait>) -> bool {
        if let dashmap::mapref::entry::Entry::Occupied(mut e) = self.cancel_keys.entry(key) {
            let holders = e.get_mut();
            if let Some(i) = holders.waits.iter().position(|w| Arc::ptr_eq(w, wait)) {
                holders.waits.swap_remove(i);
            }
            if holders.sessions == 0 && holders.waits.is_empty() {
                e.remove();
            }
        }
        // A claim happens under the same shard lock as the removal above:
        // either it took the wait first (and set the flag), or it can no
        // longer find it.
        wait.is_cancelled()
    }

    /// A CancelRequest for `(pid, key)`: claim the wait registered under it
    /// and wake it. Only when exactly one live session was relayed the key:
    /// a key shared by several (a backend that issues colliding keys) cannot
    /// tell which session sent the cancel, which may be one that is running
    /// a statement, not waiting. `false` then, and when no wait is
    /// registered under the key: the caller forwards the cancel to the
    /// backend as before. The claim removes the wait, so a cancel racing an
    /// admission is never lost nor applied twice.
    pub fn cancel_query_wait(&self, pid: u32, key: u32) -> bool {
        let dashmap::mapref::entry::Entry::Occupied(mut e) = self.cancel_keys.entry((pid, key))
        else {
            return false;
        };
        let holders = e.get_mut();
        if holders.sessions != 1 {
            return false;
        }
        let [wait] = holders.waits.as_slice() else {
            return false;
        };
        // Claimed before the wait goes, under the shard lock the waiter's
        // unregister takes too.
        wait.claimed.store(true, Ordering::Release);
        wait.notify.notify_one();
        holders.waits.clear();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slots(per_tenant: usize, overrides: &[(&str, usize)]) -> TenantSlots {
        let mut config = ProxyConfig::default();
        config.multi_tenancy.enabled = true;
        config.multi_tenancy.tenants = vec!["big".into(), "small".into(), "free".into()];
        config.multi_tenancy.max_clients_per_tenant = per_tenant;
        for (t, n) in overrides {
            config
                .multi_tenancy
                .tenant_max_clients
                .insert(t.to_string(), *n);
        }
        TenantSlots::from_config(&config)
    }

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn caps_come_from_the_default_and_the_overrides() {
        let s = slots(2, &[("big", 5), ("free", 0)]);
        let stats = s.stats();
        assert_eq!(
            stats
                .iter()
                .map(|t| (t.tenant.as_str(), t.limit))
                .collect::<Vec<_>>(),
            vec![("big", 5), ("small", 2)]
        );
        // Disabled multi-tenancy: no caps at all.
        let mut config = ProxyConfig::default();
        config.multi_tenancy.tenants = vec!["big".into()];
        config.multi_tenancy.max_clients_per_tenant = 1;
        assert!(TenantSlots::from_config(&config).is_empty());
    }

    #[test]
    fn the_tenant_is_read_from_the_startup_packet() {
        let s = slots(1, &[]);
        assert_eq!(
            s.tenant_of(&params(&[("application_name", "small")])),
            Some("small")
        );
        assert_eq!(
            s.tenant_of(&params(&[("application_name", "nobody")])),
            None
        );
        assert_eq!(s.tenant_of(&params(&[("user", "small")])), None);

        let mut config = ProxyConfig::default();
        config.multi_tenancy.enabled = true;
        config.multi_tenancy.identify_by = "database".into();
        config.multi_tenancy.tenants = vec!["acme".into()];
        config.multi_tenancy.max_clients_per_tenant = 1;
        let by_db = TenantSlots::from_config(&config);
        assert_eq!(
            by_db.tenant_of(&params(&[("database", "acme"), ("user", "x")])),
            Some("acme")
        );
        // No database in the packet: PostgreSQL uses the user name.
        assert_eq!(by_db.tenant_of(&params(&[("user", "acme")])), Some("acme"));
    }

    #[tokio::test]
    async fn a_full_tenant_waits_then_refuses_and_never_blocks_another() {
        let s = slots(1, &[]);
        let (big, _) = s.acquire("big", None).await.unwrap();
        assert!(big.is_some());
        // "big" is full: refused at once without a wait, after it with one.
        let err = s.acquire("big", None).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "too many connections for tenant \"big\" (limit 1)"
        );
        assert!(s
            .acquire("big", Some(Duration::from_millis(20)))
            .await
            .is_err());
        // "small" is unaffected by "big" being full.
        let (small, waited) = s.acquire("small", None).await.unwrap();
        assert!(small.is_some() && waited.is_none());
        // Unknown tenants take no slot.
        assert!(s.acquire("nobody", None).await.unwrap().0.is_none());

        // A waiter is admitted when the tenant's own session ends.
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(big);
        });
        let (again, waited) = s
            .acquire("big", Some(Duration::from_secs(5)))
            .await
            .unwrap();
        release.await.unwrap();
        assert!(again.is_some() && waited.is_some());
        let big_stats = &s.stats()[0];
        assert_eq!(
            (
                big_stats.in_use,
                big_stats.waiting,
                big_stats.waits,
                big_stats.refusals
            ),
            (1, 0, 2, 2)
        );
    }

    fn budgets(
        query_cap: usize,
        replay_cap: usize,
        overrides: &[(&str, usize, usize)],
        wait_ms: u64,
    ) -> TenantSlots {
        let mut config = ProxyConfig::default();
        config.multi_tenancy.enabled = true;
        config.multi_tenancy.tenants = vec!["big".into(), "small".into(), "free".into()];
        config.multi_tenancy.max_active_queries_per_tenant = query_cap;
        config.multi_tenancy.max_replay_bytes_per_tenant = replay_cap;
        config.multi_tenancy.active_query_wait_ms = wait_ms;
        for (t, q, r) in overrides {
            let mt = &mut config.multi_tenancy;
            mt.tenant_max_active_queries.insert(t.to_string(), *q);
            mt.tenant_max_replay_bytes.insert(t.to_string(), *r);
        }
        TenantSlots::from_config(&config)
    }

    /// A wait histogram with the default buckets.
    fn waits() -> LatencyHistogram {
        LatencyHistogram::new(&crate::metrics_hist::default_latency_buckets_ms())
    }

    #[test]
    fn budgets_come_from_the_defaults_and_the_overrides() {
        // "big": both budgets from the defaults; "small": a query cap only
        // (replay override 0); "free": neither (both overrides 0).
        let s = budgets(4, 100, &[("small", 2, 0), ("free", 0, 0)], 0);
        let stats = s.budget_stats();
        assert_eq!(
            stats
                .iter()
                .map(|b| (b.tenant.as_str(), b.query_limit, b.replay_byte_limit))
                .collect::<Vec<_>>(),
            vec![("big", 4, 100), ("small", 2, 0)]
        );
        assert!(s.has_budgets() && s.has_replay_budgets());
        assert!(s.budget("free").is_none());
        // A budget-only tenant has no client slot; the identity is the same.
        assert!(s.is_empty());
        assert_eq!(s.tenant_of(&params(&[("application_name", "big")])), None);
        let big = s
            .budget_of(&params(&[("application_name", "big")]))
            .unwrap();
        assert_eq!(big.tenant(), "big");
        assert!(big.has_query_cap() && big.has_replay_budget());
        // active_query_wait_ms 0 refuses at once; idle_permit_timeout_ms 0 is
        // never.
        assert_eq!(big.query_wait, None);
        assert_eq!(big.idle_permit_timeout(), None);
        assert!(!s.budget("small").unwrap().has_replay_budget());
        assert!(s
            .budget_of(&params(&[("application_name", "nobody")]))
            .is_none());

        // A client-cap-only tenant has no budget.
        let caps = slots(1, &[]);
        assert!(!caps.has_budgets() && !caps.has_replay_budgets());
        assert!(caps
            .budget_of(&params(&[("application_name", "big")]))
            .is_none());
        // A query cap alone is no replay budget.
        assert!(!budgets(1, 0, &[], 10).has_replay_budgets());

        // Disabled multi-tenancy: no budgets at all.
        let mut config = ProxyConfig::default();
        config.multi_tenancy.tenants = vec!["big".into()];
        config.multi_tenancy.max_active_queries_per_tenant = 1;
        assert!(!TenantSlots::from_config(&config).has_budgets());

        // Waits and the idle-permit timeout come from the config; the
        // database identity falls back to the user.
        let mut config = ProxyConfig::default();
        config.multi_tenancy.enabled = true;
        config.multi_tenancy.identify_by = "database".into();
        config.multi_tenancy.tenants = vec!["acme".into()];
        config.multi_tenancy.max_active_queries_per_tenant = 1;
        config.multi_tenancy.active_query_wait_ms = 250;
        config.multi_tenancy.idle_permit_timeout_ms = 900;
        let by_db = TenantSlots::from_config(&config);
        let acme = by_db.budget_of(&params(&[("user", "acme")])).unwrap();
        assert_eq!(acme.query_wait, Some(Duration::from_millis(250)));
        assert_eq!(acme.idle_permit_timeout(), Some(Duration::from_millis(900)));
        assert!(by_db
            .budget_of(&params(&[("database", "acme"), ("user", "x")]))
            .is_some());
    }

    #[tokio::test]
    async fn a_tenant_at_its_active_query_cap_waits_fifo_then_refuses() {
        let s = budgets(1, 0, &[("small", 1, 0)], 0);
        let hist = waits();
        let big = s.budget("big").unwrap();
        let first = big.try_acquire_query().expect("a free permit");
        assert!(big.try_acquire_query().is_none(), "the cap is reached");
        // No wait configured: refused at once, naming the tenant; nothing
        // waited, so nothing is recorded.
        let err = big.wait_query(&hist).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "too many active queries for tenant \"big\" (limit 1)"
        );
        assert_eq!((err.limit, err.nth), (1, 1));
        assert_eq!(hist.snapshot().count(), 0);
        // Another tenant is unaffected.
        assert!(s.budget("small").unwrap().try_acquire_query().is_some());

        drop(first);

        // With a wait, the waiter is admitted when the permit returns, and
        // its wait is recorded.
        let s = budgets(1, 0, &[], 5000);
        let big = s.budget("big").unwrap();
        let first = big.try_acquire_query().unwrap();
        let release = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            drop(first);
        };
        let ((permit, waited), ()) =
            tokio::join!(async { big.wait_query(&hist).await.unwrap() }, release);
        assert!(waited.is_some());
        assert_eq!(hist.snapshot().count(), 1);
        // The caller counts the admission once the request goes ahead.
        assert_eq!(s.budget_stats()[0].queries_admitted, 1);
        big.note_query_admitted();
        let stats = &s.budget_stats()[0];
        assert_eq!(
            (
                stats.queries_active,
                stats.queries_waiting,
                stats.query_waits,
                stats.query_refusals,
                stats.queries_admitted,
            ),
            (1, 0, 1, 0, 2)
        );
        drop(permit);
        assert_eq!(s.budget_stats()[0].queries_active, 0);

        // A replay-only budget has no query cap and refuses a wait.
        let r = budgets(0, 10, &[], 5000);
        let b = r.budget("big").unwrap();
        assert!(!b.has_query_cap() && b.try_acquire_query().is_none());
        assert_eq!(b.wait_query(&hist).await.unwrap_err().limit, 0);
        assert_eq!(hist.snapshot().count(), 1);
    }

    /// A wait that times out is refused, and recorded like an admitted one:
    /// the histogram counts every wait `query_waits` counts.
    #[tokio::test]
    async fn a_wait_that_times_out_is_recorded() {
        let s = budgets(1, 0, &[], 20);
        let hist = waits();
        let big = s.budget("big").unwrap();
        let _held = big.try_acquire_query().unwrap();
        let err = big.wait_query(&hist).await.unwrap_err();
        assert_eq!((err.limit, err.nth), (1, 1));
        let snap = hist.snapshot();
        assert_eq!(snap.count(), 1);
        assert!(snap.sum_us >= 20_000, "{snap:?}");
        let stats = &s.budget_stats()[0];
        assert_eq!(
            (
                stats.query_waits,
                stats.query_refusals,
                stats.queries_waiting
            ),
            (1, 1, 0)
        );
    }

    #[tokio::test]
    async fn a_dropped_query_wait_leaves_no_waiter_counted() {
        let s = budgets(1, 0, &[], 60_000);
        let hist = waits();
        let big = s.budget("big").unwrap();
        let held = big.try_acquire_query().unwrap();
        {
            let wait = big.wait_query(&hist);
            tokio::pin!(wait);
            // Poll the wait until it is parked on the semaphore, then drop it
            // the way a cancel or a client EOF does.
            assert!(
                tokio::time::timeout(Duration::from_millis(30), &mut wait)
                    .await
                    .is_err(),
                "the wait is still pending"
            );
            assert_eq!(s.budget_stats()[0].queries_waiting, 1);
        }
        let stats = &s.budget_stats()[0];
        assert_eq!(
            (
                stats.queries_waiting,
                stats.query_refusals,
                stats.query_waits
            ),
            (0, 0, 1)
        );
        // The dropped wait is recorded, like every wait.
        assert_eq!(hist.snapshot().count(), 1);
        drop(held);
        // No permit leaked: the cap is whole again.
        assert!(big.try_acquire_query().is_some());
        assert_eq!(s.budget_stats()[0].queries_active, 0);
    }

    #[test]
    fn query_hold_counts_idle_holders_exactly() {
        let s = budgets(2, 0, &[], 0);
        let big = s.budget("big").unwrap();
        let idle = || s.budget_stats()[0].queries_idle;
        let mut hold = QueryHold::new(big);
        assert!(!hold.is_held());
        // Without a permit nothing is ever idle.
        hold.set_idle(true);
        assert_eq!(idle(), 0);
        hold.hold(big.try_acquire_query().unwrap());
        assert!(hold.is_held());
        hold.set_idle(true);
        hold.set_idle(true);
        assert_eq!(idle(), 1, "counted once");
        hold.set_idle(false);
        assert_eq!(idle(), 0);
        hold.set_idle(true);
        hold.release();
        assert_eq!((idle(), hold.is_held()), (0, false));
        assert_eq!(s.budget_stats()[0].queries_active, 0);
        // Dropping an idle hold clears the mark, then returns the permit.
        hold.hold(big.try_acquire_query().unwrap());
        hold.set_idle(true);
        let mut other = QueryHold::new(big);
        other.hold(big.try_acquire_query().unwrap());
        other.set_idle(true);
        assert_eq!(idle(), 2);
        drop(hold);
        assert_eq!(idle(), 1);
        drop(other);
        let stats = &s.budget_stats()[0];
        assert_eq!((stats.queries_idle, stats.queries_active), (0, 0));
    }

    #[tokio::test]
    async fn a_cancel_claims_a_registered_wait_once() {
        let s = budgets(1, 0, &[], 0);
        assert!(!s.cancel_query_wait(7, 9), "nothing registered");
        s.session_key_relayed((7, 9));
        assert!(!s.cancel_query_wait(7, 9), "no wait registered");
        let wait = s.register_query_wait((7, 9));
        assert!(!wait.is_cancelled());
        assert!(s.cancel_query_wait(7, 9));
        assert!(!s.cancel_query_wait(7, 9), "claimed once");
        assert!(wait.is_cancelled());
        // The wake-up is stored for the waiter even if it polls later.
        tokio::time::timeout(Duration::from_secs(5), wait.cancelled())
            .await
            .expect("the cancel woke the wait");
        // The cancel claimed it first: the waiter learns it from its own flag.
        assert!(s.unregister_query_wait((7, 9), &wait));
        // Without a cancel the waiter's own unregister wins.
        let wait = s.register_query_wait((7, 9));
        assert!(!s.unregister_query_wait((7, 9), &wait));
        assert!(!s.cancel_query_wait(7, 9));
        assert!(!wait.is_cancelled());
        // The session's end forgets the key.
        s.session_key_ended((7, 9));
        assert!(s.cancel_keys.is_empty());
    }

    /// A key names one session only while exactly one live session was
    /// relayed it: a cancel naming a key several sessions share (backends
    /// that issue colliding keys) claims no wait, whether the others wait or
    /// run a statement; it goes on to the backend.
    #[tokio::test]
    async fn a_cancel_naming_a_key_several_sessions_hold_claims_no_wait() {
        let s = budgets(1, 0, &[], 0);
        // One session waits; another, running a statement, holds the key too.
        s.session_key_relayed((7, 9));
        s.session_key_relayed((7, 9));
        let wait = s.register_query_wait((7, 9));
        assert!(!s.cancel_query_wait(7, 9));
        assert!(!wait.is_cancelled());
        // The running session ends: the key names the waiting one alone.
        s.session_key_ended((7, 9));
        assert!(s.cancel_query_wait(7, 9));
        assert!(wait.is_cancelled());
        assert!(s.unregister_query_wait((7, 9), &wait));
        s.session_key_ended((7, 9));
        assert!(s.cancel_keys.is_empty());
        // Ending a key no session holds touches nothing.
        s.session_key_ended((7, 9));
        assert!(s.cancel_keys.is_empty());

        // Without an active-query cap no session can wait: keys are not
        // counted at all.
        let r = budgets(0, 10, &[], 0);
        r.session_key_relayed((7, 9));
        assert!(r.cancel_keys.is_empty());
        r.session_key_ended((7, 9));
    }

    /// Two sessions relayed the same BackendKeyData (backends that issue
    /// colliding keys): neither wait's end, nor a cancel naming the key, ever
    /// cancels the other session.
    #[tokio::test]
    async fn two_waits_sharing_a_key_never_cancel_each_other() {
        let s = budgets(1, 0, &[], 0);
        s.session_key_relayed((7, 9));
        s.session_key_relayed((7, 9));
        let registered = |s: &TenantSlots| s.cancel_keys.get(&(7, 9)).map(|h| h.waits.len());
        let first = s.register_query_wait((7, 9));
        let second = s.register_query_wait((7, 9));
        // A cancel naming the key cannot tell which session sent it: it
        // claims neither wait.
        assert!(!s.cancel_query_wait(7, 9));
        assert!(!first.is_cancelled() && !second.is_cancelled());
        // The first wait ends (admitted): only its own entry goes, and it was
        // not cancelled; the second stays registered.
        assert!(!s.unregister_query_wait((7, 9), &first));
        assert_eq!(registered(&s), Some(1));
        assert!(!second.is_cancelled());
        // Ending the first again (or a wait never registered) touches nothing.
        assert!(!s.unregister_query_wait((7, 9), &first));
        let stray = Arc::new(QueryWait::default());
        assert!(!s.unregister_query_wait((7, 9), &stray));
        assert_eq!(registered(&s), Some(1));
        // One wait is left, but both sessions still hold the key: still
        // ambiguous.
        assert!(!s.cancel_query_wait(7, 9));
        assert!(!second.is_cancelled());
        // The first session ends: the key now names the second alone.
        s.session_key_ended((7, 9));
        assert!(s.cancel_query_wait(7, 9));
        assert!(second.is_cancelled() && !first.is_cancelled());
        tokio::time::timeout(Duration::from_secs(5), second.cancelled())
            .await
            .expect("the cancel woke the second wait");
        assert!(s.unregister_query_wait((7, 9), &second));
        assert_eq!(registered(&s), Some(0));

        // In the other order: the later wait ends first, the earlier one is
        // still registered and uncancelled.
        s.session_key_relayed((7, 9));
        let first = s.register_query_wait((7, 9));
        let second = s.register_query_wait((7, 9));
        assert!(!s.unregister_query_wait((7, 9), &second));
        assert!(!first.is_cancelled());
        assert!(!s.unregister_query_wait((7, 9), &first));
        s.session_key_ended((7, 9));
        s.session_key_ended((7, 9));
        assert!(s.cancel_keys.is_empty());
    }

    #[test]
    fn replay_charges_stay_within_the_tenant_budget_and_are_returned() {
        let s = budgets(0, 100, &[], 0);
        let budget = s.budget("big").unwrap();
        let in_use = || s.budget_stats()[0].replay_bytes;
        let refusals = || s.budget_stats()[0].replay_refusals;
        let a = TenantCharge::new(Arc::clone(budget));
        let b = TenantCharge::new(Arc::clone(budget));
        assert!(a.replay_reserve(60));
        assert!(!b.replay_reserve(50), "60 + 50 exceeds 100");
        assert_eq!((refusals(), in_use()), (1, 60));
        // The check counts nothing; a refusal is noted by its caller.
        assert!(b.replay_room(40));
        assert!(!b.replay_room(41));
        assert_eq!(refusals(), 1);
        b.note_replay_refusal();
        assert_eq!(refusals(), 2);
        a.replay_settle(10);
        assert_eq!(in_use(), 10);
        assert!(b.replay_reserve(90));
        assert_eq!(in_use(), 100);
        // A shrink always succeeds.
        assert!(b.replay_reserve(80));
        assert_eq!(in_use(), 90);
        drop(b);
        assert_eq!(in_use(), 10);
        a.replay_settle(0);
        assert_eq!(in_use(), 0);

        // A query-only budget: no replay accounting at all.
        let q = budgets(1, 0, &[], 0);
        let c = TenantCharge::new(Arc::clone(q.budget("big").unwrap()));
        assert!(c.replay_reserve(1 << 40));
        assert!(c.replay_room(1 << 40));
        c.replay_settle(1 << 40);
        let stats = &q.budget_stats()[0];
        assert_eq!((stats.replay_bytes, stats.replay_refusals), (0, 0));
    }

    /// The startup warning's condition: the session of a client that
    /// vanished while holding a permit can outlive it whenever client TCP
    /// keepalive is off. The idle timeouts do not cover every such session
    /// (neither ends one in a stalled COPY), so setting them does not
    /// silence the warning.
    #[test]
    fn vanished_clients_keep_permits_whenever_keepalive_is_off() {
        let mut config = ProxyConfig::default();
        assert!(
            !TenantSlots::vanished_clients_keep_permits(&config),
            "client TCP keepalive is on by default"
        );
        config.limits.client_tcp_keepalive_secs = 0;
        assert!(TenantSlots::vanished_clients_keep_permits(&config));
        config.multi_tenancy.idle_permit_timeout_ms = 1000;
        assert!(
            TenantSlots::vanished_clients_keep_permits(&config),
            "idle_permit_timeout_ms never ends a session in a stalled COPY"
        );
        config.limits.client_idle_timeout_secs = 1;
        assert!(
            TenantSlots::vanished_clients_keep_permits(&config),
            "nor does client_idle_timeout_secs"
        );
        config.limits.client_tcp_keepalive_secs = 1;
        assert!(!TenantSlots::vanished_clients_keep_permits(&config));
    }
}
