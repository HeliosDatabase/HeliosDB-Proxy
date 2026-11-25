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
//!
//! Isolation and pools (W07): every known tenant has a [`TenantPlan`], which
//! a session is pinned to at admission ([`TenantSlots::identify`]): its
//! isolation (row, schema, database or branch), applied to the session's
//! startup parameters once, and, for a tenant with pool settings of its own,
//! its [`TenantPool`]: the `pool_max_connections` cap on its backend
//! connections (in use and parked) and the counters `/pools` reports.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{AcquireError, Notify, OwnedSemaphorePermit, Semaphore, SemaphorePermit};

use crate::config::{
    BranchRouteMode, MultiTenancyToml, ProxyConfig, SchemaApply, TenantIsolation, TrMode,
};
use crate::metrics_hist::LatencyHistogram;

/// How a pinned session is isolated, resolved from its tenant's entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IsolationPlan {
    /// Row filter on the shared tables (`[multi_tenancy] tenant_tables`).
    Row,
    /// `search_path` set to this schema (already double-quoted).
    Schema { search_path: String },
    /// Connect to this database.
    Database { database: String },
    /// Connect to this branch of the client's database.
    Branch { branch: String },
}

/// One known tenant as the data path applies it (W07). A session is pinned
/// to its tenant's plan at admission and keeps it for its life, whatever its
/// startup parameters are rewritten to.
#[derive(Debug)]
pub struct TenantPlan {
    id: String,
    isolation: IsolationPlan,
    schema_apply: SchemaApply,
    branch_mode: BranchRouteMode,
    lock_search_path: bool,
    /// `"\0t=<id>"` for a tenant pooled apart from the others.
    pool_suffix: Option<String>,
    /// `pool_max_idle`; 0 = `[pool_mode] max_pool_size`. Resized by SIGHUP.
    pool_max_idle: AtomicUsize,
    pool: Option<Arc<TenantPool>>,
}

impl TenantPlan {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn isolation(&self) -> &IsolationPlan {
        &self.isolation
    }

    /// The isolation pins what the session reaches beyond its rows (a
    /// schema, a database or a branch): a branch move would take it out.
    pub fn pins_database_or_schema(&self) -> bool {
        self.isolation != IsolationPlan::Row
    }

    /// A schema tenant whose `search_path` is set on every connection the
    /// session acquires: the `SET` to run, else `None`.
    pub fn set_on_acquire(&self) -> Option<String> {
        match (&self.isolation, self.schema_apply) {
            (IsolationPlan::Schema { search_path }, SchemaApply::SetOnAcquire) => {
                Some(format!("SET search_path TO {search_path}"))
            }
            _ => None,
        }
    }

    /// Statements changing `search_path` are refused for this session
    /// (`lock_search_path` with schema isolation).
    pub fn locks_search_path(&self) -> bool {
        self.lock_search_path && matches!(self.isolation, IsolationPlan::Schema { .. })
    }

    /// `RESET ALL` / `DISCARD ALL` also leave the tenant's schema (it was
    /// set by `SET`, not at startup).
    pub fn reset_leaves_schema(&self) -> bool {
        self.schema_apply == SchemaApply::SetOnAcquire
    }

    /// The pool-key suffix of a tenant pooled apart, else `None`.
    pub fn pool_suffix(&self) -> Option<&str> {
        self.pool_suffix.as_deref()
    }

    /// The idle cap per identity of this tenant's pool keys; `None` = the
    /// global `[pool_mode] max_pool_size`.
    pub fn pool_max_idle(&self) -> Option<usize> {
        match self.pool_max_idle.load(Ordering::Relaxed) {
            0 => None,
            n => Some(n),
        }
    }

    /// The tenant's pool (cap and counters), when it has pool settings.
    pub fn pool(&self) -> Option<&Arc<TenantPool>> {
        self.pool.as_ref()
    }

    /// Rewrite a session's startup parameters for the tenant's isolation:
    /// the `search_path` in `options` (schema, `schema_apply = "startup"`),
    /// the database (database), or the client's database's branch (branch).
    /// Row isolation and `set_on_acquire` change nothing.
    pub fn rewrite_startup(&self, params: &mut HashMap<String, String>) {
        match &self.isolation {
            IsolationPlan::Row => {}
            IsolationPlan::Schema { search_path } => {
                if self.schema_apply != SchemaApply::Startup {
                    return;
                }
                // A `search_path` startup parameter is applied after
                // `options`, so the client's own would win: it goes.
                params.retain(|k, _| !k.eq_ignore_ascii_case("search_path"));
                let setting = format!("-c search_path={search_path}");
                let options = match params.get("options").filter(|o| !o.trim().is_empty()) {
                    // A later `-c` of the same setting wins over the client's.
                    Some(existing) => format!("{existing} {setting}"),
                    None => setting,
                };
                params.insert("options".to_string(), options);
            }
            IsolationPlan::Database { database } => {
                params.insert("database".to_string(), database.clone());
            }
            IsolationPlan::Branch { branch } => {
                let client_db = params
                    .get("database")
                    .or_else(|| params.get("user"))
                    .map(String::as_str)
                    .unwrap_or_default();
                let base = crate::database_name::split_branch(client_db).0;
                let database = match self.branch_mode {
                    BranchRouteMode::Database => branch.clone(),
                    BranchRouteMode::Nano => format!("{base}@{branch}"),
                };
                params.insert("database".to_string(), database);
            }
        }
    }
}

/// A tenant pool's `pool_max_connections` is reached ([`TenantPool::try_acquire`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantPoolFull;

/// A tenant's backend connections (W07): the `pool_max_connections` cap,
/// held by each connection (in use or parked) like a node slot, and the
/// checkout and dial events of its sessions. `/pools` and `/metrics` report
/// it.
#[derive(Debug)]
pub struct TenantPool {
    tenant: String,
    /// `None`: no cap (the tenant has `pool_max_idle` only).
    cap: Option<TenantSlot>,
    /// Backend connections opened for the tenant's sessions.
    dials: AtomicU64,
    /// Parked connections its sessions borrowed.
    checkouts: AtomicU64,
}

/// What `/pools` and `/metrics` report for one tenant pool.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TenantPoolStats {
    pub tenant: String,
    /// `pool_max_connections`; `None` = uncapped.
    pub limit: Option<usize>,
    /// Backend connections (in use and parked) holding the tenant's slots;
    /// `None` without a cap.
    pub in_use: Option<usize>,
    pub waiting: usize,
    pub waits: u64,
    pub refusals: u64,
    pub dials: u64,
    pub checkouts: u64,
}

impl TenantPool {
    fn new(tenant: &str, limit: usize) -> Self {
        Self {
            tenant: tenant.to_string(),
            cap: (limit > 0).then(|| TenantSlot::new(limit)),
            dials: AtomicU64::new(0),
            checkouts: AtomicU64::new(0),
        }
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Count a backend connection opened for the tenant.
    pub fn note_dial(&self) {
        self.dials.fetch_add(1, Ordering::Relaxed);
    }

    /// Count a parked connection one of its sessions borrowed.
    pub fn note_checkout(&self) {
        self.checkouts.fetch_add(1, Ordering::Relaxed);
    }

    /// A slot for a new backend connection of the tenant, held for the
    /// connection's life (parked included). Uncapped: `Ok(None)`. Full: call
    /// `free_idle` once (it closes one of the tenant's parked connections,
    /// returning whether it did), retry, then wait up to `wait` in FIFO
    /// order; `Err` names the cap.
    pub async fn acquire(
        &self,
        wait: Duration,
        free_idle: impl FnOnce() -> bool,
    ) -> std::result::Result<(Option<OwnedSemaphorePermit>, Option<Duration>), String> {
        let Some(cap) = self.cap.as_ref() else {
            return Ok((None, None));
        };
        if let Ok(p) = cap.sem.clone().try_acquire_owned() {
            return Ok((Some(p), None));
        }
        // Counted as waiting before looking for an idle connection to close,
        // and until `wait_for` counts the wait itself (it registers before
        // calling the closure): a session parking a connection of the tenant
        // meanwhile either sees a waiter and closes it, or parks it before
        // `free_idle` looks.
        let announced = Waiting::new(&cap.waiting, None);
        if free_idle() {
            if let Ok(p) = cap.sem.clone().try_acquire_owned() {
                return Ok((Some(p), None));
            }
        }
        match cap
            .wait_for(Some(wait), None, || {
                drop(announced);
                cap.sem.clone().acquire_owned()
            })
            .await
        {
            Ok((p, waited)) => Ok((Some(p), Some(waited))),
            Err(_) => Err(format!(
                "tenant {} is at its pool_max_connections ({}): {} reached",
                self.tenant,
                cap.limit(),
                crate::node_slots::REFUSAL_MARKER
            )),
        }
    }

    /// A session of the tenant is waiting for a slot now.
    pub fn has_waiters(&self) -> bool {
        self.cap
            .as_ref()
            .is_some_and(|c| c.waiting.load(Ordering::Relaxed) > 0)
    }

    /// A slot without waiting and without closing a parked connection (the
    /// idle reaper's top-ups): `Err` when the tenant is full.
    pub fn try_acquire(&self) -> std::result::Result<Option<OwnedSemaphorePermit>, TenantPoolFull> {
        match self.cap.as_ref() {
            None => Ok(None),
            Some(cap) => cap
                .sem
                .clone()
                .try_acquire_owned()
                .map(Some)
                .map_err(|_| TenantPoolFull),
        }
    }

    pub fn stats(&self) -> TenantPoolStats {
        TenantPoolStats {
            tenant: self.tenant.clone(),
            limit: self.cap.as_ref().map(TenantSlot::limit),
            in_use: self.cap.as_ref().map(TenantSlot::in_use),
            waiting: self
                .cap
                .as_ref()
                .map_or(0, |c| c.waiting.load(Ordering::Relaxed)),
            waits: self
                .cap
                .as_ref()
                .map_or(0, |c| c.waits.load(Ordering::Relaxed)),
            refusals: self
                .cap
                .as_ref()
                .map_or(0, |c| c.refusals.load(Ordering::Relaxed)),
            dials: self.dials.load(Ordering::Relaxed),
            checkouts: self.checkouts.load(Ordering::Relaxed),
        }
    }
}

/// Shared accounting for a tenant's recovery-journal memory. Reservations are
/// acquired atomically across sessions and returned when their RAII charges
/// are dropped (rollback, eviction, or session teardown).
#[derive(Debug)]
pub struct TenantJournalBudget {
    tenant: String,
    /// Zero means unlimited, matching the other per-tenant byte budgets.
    /// Changed only by a SIGHUP ([`TenantSlots::reload_caps`]).
    limit: AtomicUsize,
    used: AtomicUsize,
    refusals: AtomicU64,
}

impl TenantJournalBudget {
    pub fn new(tenant: impl Into<String>, limit: usize) -> Arc<Self> {
        Arc::new(Self {
            tenant: tenant.into(),
            limit: AtomicUsize::new(limit),
            used: AtomicUsize::new(0),
            refusals: AtomicU64::new(0),
        })
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub fn refusals(&self) -> u64 {
        self.refusals.load(Ordering::Relaxed)
    }

    /// Reserve `bytes` across every session of this tenant. Overflow and
    /// configured-limit violations fail closed without changing usage.
    pub fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<TenantJournalCharge> {
        if !self.reserve(bytes) {
            self.refusals.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(TenantJournalCharge {
            budget: Arc::clone(self),
            bytes: AtomicUsize::new(bytes),
        })
    }

    fn reserve(&self, bytes: usize) -> bool {
        update(&self.used, |used| {
            let next = used.checked_add(bytes)?;
            let limit = self.limit();
            (limit == 0 || next <= limit).then_some(next)
        })
        .is_ok()
    }
}

/// One reservation against a shared tenant recovery-journal budget.
#[derive(Debug)]
pub struct TenantJournalCharge {
    budget: Arc<TenantJournalBudget>,
    bytes: AtomicUsize,
}

impl TenantJournalCharge {
    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Acquire)
    }

    /// Set this transaction's reservation to its current deterministic
    /// retained size. Growth is admitted atomically; shrinkage is returned
    /// immediately after rollback or savepoint truncation.
    pub fn settle(&self, retained: usize) -> bool {
        let current = self.bytes();
        if retained > current {
            let additional = retained - current;
            if !self.budget.reserve(additional) {
                self.budget.refusals.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            self.bytes.fetch_add(additional, Ordering::AcqRel);
        } else if current > retained {
            let released = current - retained;
            self.bytes.fetch_sub(released, Ordering::AcqRel);
            self.budget.used.fetch_sub(released, Ordering::AcqRel);
        }
        true
    }
}

impl Drop for TenantJournalCharge {
    fn drop(&mut self) {
        let bytes = self.bytes.load(Ordering::Acquire);
        let previous = self.budget.used.fetch_sub(bytes, Ordering::AcqRel);
        debug_assert!(previous >= bytes, "tenant journal charge underflow");
    }
}

#[derive(Debug)]
struct TenantSlot {
    /// The cap in force. Changed only by [`TenantSlot::resize`] (a SIGHUP).
    limit: AtomicUsize,
    sem: Arc<Semaphore>,
    /// Permits a lowered cap could not take back at once (see
    /// [`TenantSlot::resize`]).
    debt: Arc<SlotDebt>,
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

/// What a lowered cap still owes: permits that were held when the cap came
/// down, so they could not be taken out of the semaphore then. Every permit
/// of the semaphore is either within the cap or owed, so
/// `held + available == limit + owed` at rest.
#[derive(Default, Debug)]
struct SlotDebt {
    owed: AtomicUsize,
    /// A collector task is running for this slot.
    collecting: AtomicBool,
}

/// `AtomicUsize::fetch_update(AcqRel, Acquire, f)`, written out: that method
/// is deprecated from Rust 1.99, and its replacement (`try_update`) does not
/// exist on the 1.86 MSRV. `Ok` holds the value replaced, `Err` the value
/// `f` refused.
fn update(atomic: &AtomicUsize, mut f: impl FnMut(usize) -> Option<usize>) -> Result<usize, usize> {
    let mut current = atomic.load(Ordering::Acquire);
    while let Some(next) = f(current) {
        match atomic.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(previous) => return Ok(previous),
            Err(actual) => current = actual,
        }
    }
    Err(current)
}

impl SlotDebt {
    /// Take up to `most` permits off the debt; returns how many.
    fn cancel(&self, most: usize) -> usize {
        let mut cancelled = 0;
        let _ = update(&self.owed, |owed| {
            cancelled = owed.min(most);
            Some(owed - cancelled)
        });
        cancelled
    }
}

impl TenantSlot {
    fn new(limit: usize) -> Self {
        Self {
            limit: AtomicUsize::new(limit),
            sem: Arc::new(Semaphore::new(limit)),
            debt: Arc::default(),
            waiting: AtomicUsize::new(0),
            waits: AtomicU64::new(0),
            refusals: AtomicU64::new(0),
        }
    }

    /// The cap in force.
    fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    /// Permits taken right now: by sessions, or for a moment by the
    /// collector of a lowered cap.
    fn in_use(&self) -> usize {
        let total = self
            .limit()
            .saturating_add(self.debt.owed.load(Ordering::Relaxed));
        total.saturating_sub(self.sem.available_permits())
    }

    /// Change the cap of a running slot; returns the cap it replaces. No
    /// holder is ever revoked.
    ///
    /// Raising it first writes off what an earlier lowering still owes, then
    /// adds the rest as permits, which wake waiters in FIFO order. Lowering
    /// it removes free permits at once; the permits that are held become a
    /// debt, collected by one task that queues on the semaphore like a
    /// client (behind the waiters already there, ahead of later ones) and
    /// retires each permit it is handed. Until the debt is paid the tenant
    /// holds more than its new cap and admits nobody new.
    ///
    /// Callers serialize resizes of one slot and run on the tokio runtime
    /// (the collector is spawned on it); `new_limit` is within
    /// `Semaphore::MAX_PERMITS` and above 0.
    fn resize(&self, new_limit: usize) -> usize {
        let old = self.limit.swap(new_limit, Ordering::Relaxed);
        if new_limit > old {
            let raise = new_limit - old;
            let added = raise - self.debt.cancel(raise);
            if added > 0 {
                self.sem.add_permits(added);
            }
        } else if old > new_limit {
            let lower = old - new_limit;
            let owed = lower - self.sem.forget_permits(lower);
            if owed > 0 {
                self.debt.owed.fetch_add(owed, Ordering::AcqRel);
                self.collect_debt();
            }
        }
        old
    }

    /// Start the collector unless one is running.
    fn collect_debt(&self) {
        if self.debt.collecting.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.debt.collecting.store(false, Ordering::Release);
            tracing::error!(
                "tenant cap lowered outside the runtime: held permits above the new cap are not collected"
            );
            return;
        };
        let (sem, debt) = (Arc::clone(&self.sem), Arc::clone(&self.debt));
        runtime.spawn(async move {
            loop {
                while debt.owed.load(Ordering::Acquire) > 0 {
                    // Never closed; an error would leave nothing to collect.
                    let Ok(permit) = sem.acquire().await else {
                        return;
                    };
                    let owed = update(&debt.owed, |owed| owed.checked_sub(1));
                    if owed.is_ok() {
                        permit.forget();
                    }
                    // Else a raise wrote the debt off while this waited: the
                    // permit goes back to the tenant.
                }
                debt.collecting.store(false, Ordering::Release);
                // A lowering that found `collecting` set just before it was
                // cleared left its debt to this task.
                if debt.owed.load(Ordering::Acquire) == 0
                    || debt.collecting.swap(true, Ordering::AcqRel)
                {
                    return;
                }
            }
        });
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
    /// 0 = no replay budget. Changed only by a SIGHUP, never to or from 0
    /// ([`TenantSlots::reload_caps`]).
    replay_limit: AtomicUsize,
    replay_in_use: AtomicUsize,
    replay_refusals: AtomicU64,
    journal: Option<Arc<TenantJournalBudget>>,
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
    /// The recovery-journal byte budget; zero means not configured.
    pub journal_byte_limit: usize,
    /// Recovery-journal bytes reserved by this tenant now.
    pub journal_bytes: usize,
    /// Reservations refused by the recovery-journal byte budget.
    pub journal_refusals: u64,
}

impl TenantBudget {
    fn new(
        tenant: String,
        query_limit: usize,
        query_wait: Option<Duration>,
        idle_permit_timeout: Option<Duration>,
        replay_limit: usize,
        journal_limit: usize,
    ) -> Self {
        let journal =
            (journal_limit > 0).then(|| TenantJournalBudget::new(tenant.clone(), journal_limit));
        Self {
            tenant,
            queries: (query_limit > 0).then(|| TenantSlot::new(query_limit)),
            query_wait,
            idle_permit_timeout,
            queries_admitted: AtomicU64::new(0),
            queries_idle: AtomicUsize::new(0),
            query_wait_cancels: AtomicU64::new(0),
            idle_permit_timeouts: AtomicU64::new(0),
            replay_limit: AtomicUsize::new(replay_limit),
            replay_in_use: AtomicUsize::new(0),
            replay_refusals: AtomicU64::new(0),
            journal,
        }
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn has_query_cap(&self) -> bool {
        self.queries.is_some()
    }

    pub fn has_replay_budget(&self) -> bool {
        self.replay_limit() > 0
    }

    fn replay_limit(&self) -> usize {
        self.replay_limit.load(Ordering::Relaxed)
    }

    pub fn has_journal_budget(&self) -> bool {
        self.journal.is_some()
    }

    pub fn journal_budget(&self) -> Option<&Arc<TenantJournalBudget>> {
        self.journal.as_ref()
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
            Err(nth) => Err(self.refusal(slot.limit(), nth)),
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
            query_limit: q.map_or(0, TenantSlot::limit),
            queries_active: q.map_or(0, TenantSlot::in_use),
            queries_idle: self.queries_idle.load(Ordering::Relaxed),
            queries_waiting: q.map_or(0, |s| s.waiting.load(Ordering::Relaxed)),
            queries_admitted: self.queries_admitted.load(Ordering::Relaxed),
            query_waits: q.map_or(0, |s| s.waits.load(Ordering::Relaxed)),
            query_refusals: q.map_or(0, |s| s.refusals.load(Ordering::Relaxed)),
            query_wait_cancels: self.query_wait_cancels.load(Ordering::Relaxed),
            idle_permit_timeouts: self.idle_permit_timeouts.load(Ordering::Relaxed),
            replay_byte_limit: self.replay_limit(),
            replay_bytes: self.replay_in_use.load(Ordering::Relaxed),
            replay_refusals: self.replay_refusals.load(Ordering::Relaxed),
            journal_byte_limit: self.journal.as_ref().map_or(0, |budget| budget.limit()),
            journal_bytes: self.journal.as_ref().map_or(0, |budget| budget.used()),
            journal_refusals: self.journal.as_ref().map_or(0, |budget| budget.refusals()),
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
        let limit = self.budget.replay_limit();
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
        let limit = self.budget.replay_limit();
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
        if self.budget.replay_limit() == 0 {
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
    /// Every configured tenant (capped or not): the names `identify_by =
    /// "database"` resolves a startup database against, as TenantManager does.
    known: HashSet<String>,
    tenants: HashMap<String, TenantSlot>,
    budgets: HashMap<String, Arc<TenantBudget>>,
    /// Every known tenant's plan (W07), by id.
    plans: HashMap<String, Arc<TenantPlan>>,
    /// Some tenant has an active-query cap: only then can a session wait
    /// for a permit, so only then are the sessions behind each key counted.
    track_cancel_keys: bool,
    /// By BackendKeyData: the live sessions relayed it, and the waits at
    /// their tenant's active-query cap a CancelRequest naming it may end.
    /// A cancel can tell which session sent it only when exactly one live
    /// session holds the key, so only then does it end that session's wait.
    cancel_keys: dashmap::DashMap<(u32, u32), KeyHolders>,
    /// Serializes [`Self::reload_caps`].
    resizing: parking_lot::Mutex<()>,
}

/// Which per-tenant cap a reload changed ([`TenantSlots::reload_caps`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantCap {
    Clients,
    ActiveQueries,
    ReplayBytes,
    JournalBytes,
    PoolConnections,
    PoolIdle,
}

impl TenantCap {
    const ALL: [Self; 6] = [
        Self::Clients,
        Self::ActiveQueries,
        Self::ReplayBytes,
        Self::JournalBytes,
        Self::PoolConnections,
        Self::PoolIdle,
    ];

    /// The `[multi_tenancy]` keys that set it.
    pub fn keys(self) -> &'static str {
        match self {
            Self::Clients => "max_clients / max_clients_per_tenant / tenant_max_clients",
            Self::ActiveQueries => {
                "max_active_queries / max_active_queries_per_tenant / tenant_max_active_queries"
            }
            Self::ReplayBytes => {
                "max_replay_bytes / max_replay_bytes_per_tenant / tenant_max_replay_bytes"
            }
            Self::JournalBytes => {
                "max_journal_bytes / max_journal_bytes_per_tenant / tenant_max_journal_bytes"
            }
            Self::PoolConnections => "pool_max_connections",
            Self::PoolIdle => "pool_max_idle",
        }
    }

    fn configured(self, config: &MultiTenancyToml, tenant: &str) -> usize {
        match self {
            Self::Clients => config.client_cap(tenant),
            Self::ActiveQueries => config.active_query_cap(tenant),
            Self::ReplayBytes => config.replay_byte_cap(tenant),
            Self::JournalBytes => config.journal_byte_cap(tenant),
            Self::PoolConnections => config.pool_connection_cap(tenant),
            Self::PoolIdle => config.pool_idle_cap(tenant),
        }
    }

    /// A semaphore of permits, as opposed to a byte counter.
    fn counts_permits(self) -> bool {
        matches!(
            self,
            Self::Clients | Self::ActiveQueries | Self::PoolConnections
        )
    }
}

/// One cap a reload resized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantCapChange {
    pub tenant: String,
    pub cap: TenantCap,
    pub from: usize,
    pub to: usize,
}

impl TenantSlots {
    pub fn from_config(config: &ProxyConfig) -> Self {
        let mt = &config.multi_tenancy;
        if !mt.enabled {
            return Self::default();
        }
        let tenants = mt
            .tenant_ids()
            .filter_map(|t| {
                let limit = mt.client_cap(t);
                (limit > 0).then(|| (t.to_string(), TenantSlot::new(limit)))
            })
            .collect();
        let query_wait =
            (mt.active_query_wait_ms > 0).then(|| Duration::from_millis(mt.active_query_wait_ms));
        let idle_permit_timeout = (mt.idle_permit_timeout_ms > 0)
            .then(|| Duration::from_millis(mt.idle_permit_timeout_ms));
        let budgets: HashMap<String, Arc<TenantBudget>> = mt
            .tenant_ids()
            .filter_map(|t| {
                let (queries, replay, journal) = (
                    mt.active_query_cap(t),
                    mt.replay_byte_cap(t),
                    mt.journal_byte_cap(t),
                );
                (queries > 0 || replay > 0 || journal > 0).then(|| {
                    (
                        t.to_string(),
                        Arc::new(TenantBudget::new(
                            t.to_string(),
                            queries,
                            query_wait,
                            idle_permit_timeout,
                            replay,
                            journal,
                        )),
                    )
                })
            })
            .collect();
        let plans: HashMap<String, Arc<TenantPlan>> = mt
            .tenant_list()
            .iter()
            .map(|spec| {
                let isolation = match spec.isolation {
                    TenantIsolation::Row => IsolationPlan::Row,
                    TenantIsolation::Schema => IsolationPlan::Schema {
                        search_path: format!("\"{}\"", spec.schema_name()),
                    },
                    TenantIsolation::Database => IsolationPlan::Database {
                        database: spec.database_name().to_string(),
                    },
                    TenantIsolation::Branch => IsolationPlan::Branch {
                        branch: spec.branch_name().to_string(),
                    },
                };
                let plan = TenantPlan {
                    id: spec.id.clone(),
                    isolation,
                    schema_apply: mt.schema_apply,
                    branch_mode: config.branch.route_mode,
                    lock_search_path: mt.lock_search_path,
                    pool_suffix: spec.has_own_pool().then(|| format!("\0t={}", spec.id)),
                    pool_max_idle: AtomicUsize::new(spec.pool_max_idle.unwrap_or(0)),
                    pool: spec.has_own_pool().then(|| {
                        Arc::new(TenantPool::new(
                            &spec.id,
                            spec.pool_max_connections.unwrap_or(0),
                        ))
                    }),
                };
                (spec.id.clone(), Arc::new(plan))
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
                "[multi_tenancy] active-query caps with [limits] client_tcp_keepalive_secs = 0: a client that vanishes without closing its connection while its session holds a permit keeps that permit until the proxy restarts unless a timeout ends the session; idle_permit_timeout_ms ends only one idle in a transaction or holding advisory locks, [limits] client_idle_timeout_secs also one in the middle of an extended cycle, copy_idle_timeout_secs can bound a stalled COPY, but a partly received non-COPY message has no idle deadline"
            );
        }
        Self {
            identify_by: mt.identify_by.clone(),
            known: mt.tenant_ids().map(str::to_string).collect(),
            tenants,
            budgets,
            plans,
            track_cancel_keys,
            cancel_keys: dashmap::DashMap::new(),
            resizing: parking_lot::Mutex::new(()),
        }
    }

    /// Apply a reloaded `[multi_tenancy]` to the running caps (SIGHUP).
    ///
    /// A reload may resize a cap or byte budget that a tenant already has:
    /// its client cap, its active-query cap, its replay-byte budget and its
    /// recovery-journal budget. Everything else is built once at startup and
    /// is refused here, naming the first reason: any other key of the
    /// section, and a cap or budget that would be added to or removed from a
    /// tenant (0 on one side), since sessions decide at their start which of
    /// them they take part in. All or nothing: a refused reload changes no
    /// cap. Returns what changed, by tenant.
    ///
    /// A raised cap admits waiters at once, in FIFO order. A lowered cap
    /// never ends a session or a request: the tenant admits nobody new until
    /// enough of its holders have finished (see [`TenantSlot::resize`]); a
    /// lowered byte budget refuses growth until the tenant's use is back
    /// under it. Must run on the tokio runtime.
    pub fn reload_caps(
        &self,
        old: &MultiTenancyToml,
        new: &MultiTenancyToml,
    ) -> std::result::Result<Vec<TenantCapChange>, String> {
        if old.without_caps() != new.without_caps() {
            return Err("a key other than the per-tenant caps and byte budgets changed".into());
        }
        if !new.enabled {
            // Nothing is enforced, so there is nothing to resize.
            return Ok(Vec::new());
        }
        let _serialized = self.resizing.lock();
        let mut changes = Vec::new();
        for tenant in new.tenant_ids() {
            for cap in TenantCap::ALL {
                let (from, to) = (cap.configured(old, tenant), cap.configured(new, tenant));
                if from == to {
                    continue;
                }
                if from == 0 || to == 0 {
                    return Err(format!(
                        "tenant \"{tenant}\": {} cannot be added or removed while running ({from} -> {to})",
                        cap.keys()
                    ));
                }
                if cap.counts_permits() && to > Semaphore::MAX_PERMITS {
                    return Err(format!(
                        "tenant \"{tenant}\": {} must be <= {}",
                        cap.keys(),
                        Semaphore::MAX_PERMITS
                    ));
                }
                if !self.has_cap(tenant, cap) {
                    return Err(format!(
                        "tenant \"{tenant}\": {} is not enforced by this process",
                        cap.keys()
                    ));
                }
                changes.push(TenantCapChange {
                    tenant: tenant.to_string(),
                    cap,
                    from,
                    to,
                });
            }
        }
        for change in &changes {
            self.set_cap(&change.tenant, change.cap, change.to);
        }
        Ok(changes)
    }

    /// Whether `tenant` runs under `cap` (it was above 0 at startup).
    fn has_cap(&self, tenant: &str, cap: TenantCap) -> bool {
        let budget = self.budgets.get(tenant);
        let plan = self.plans.get(tenant);
        match cap {
            TenantCap::Clients => self.tenants.contains_key(tenant),
            TenantCap::ActiveQueries => budget.is_some_and(|b| b.has_query_cap()),
            TenantCap::ReplayBytes => budget.is_some_and(|b| b.has_replay_budget()),
            TenantCap::JournalBytes => budget.is_some_and(|b| b.has_journal_budget()),
            TenantCap::PoolConnections => {
                plan.and_then(|p| p.pool()).is_some_and(|p| p.cap.is_some())
            }
            TenantCap::PoolIdle => plan.is_some_and(|p| p.pool_max_idle().is_some()),
        }
    }

    /// Resize a cap [`Self::has_cap`] confirmed; `to` is above 0.
    fn set_cap(&self, tenant: &str, cap: TenantCap, to: usize) {
        let budget = self.budgets.get(tenant);
        match cap {
            TenantCap::Clients => {
                if let Some(slot) = self.tenants.get(tenant) {
                    slot.resize(to);
                }
            }
            TenantCap::ActiveQueries => {
                if let Some(slot) = budget.and_then(|b| b.queries.as_ref()) {
                    slot.resize(to);
                }
            }
            TenantCap::ReplayBytes => {
                if let Some(budget) = budget {
                    budget.replay_limit.store(to, Ordering::Relaxed);
                }
            }
            TenantCap::JournalBytes => {
                if let Some(journal) = budget.and_then(|b| b.journal.as_ref()) {
                    journal.limit.store(to, Ordering::Relaxed);
                }
            }
            TenantCap::PoolConnections => {
                let plan = self.plans.get(tenant);
                if let Some(slot) = plan.and_then(|p| p.pool()).and_then(|p| p.cap.as_ref()) {
                    slot.resize(to);
                }
            }
            TenantCap::PoolIdle => {
                if let Some(plan) = self.plans.get(tenant) {
                    plan.pool_max_idle.store(to, Ordering::Relaxed);
                }
            }
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

    /// The tenant id a startup packet names (`identify_by`). `identify_by =
    /// "database"` resolves the database as TenantManager does
    /// ([`crate::database_name::tenant_from_startup`]): the whole value if it
    /// is a configured tenant, else its base before the first `@`; without a
    /// database, the user name (as PostgreSQL does).
    fn key<'p>(&self, params: &'p HashMap<String, String>) -> Option<&'p str> {
        if self.identify_by == "database" {
            crate::database_name::tenant_from_startup(
                params.get("database").map(String::as_str),
                params.get("user").map(String::as_str),
                |t| self.known.contains(t),
            )
        } else {
            params.get(&self.identify_by).map(String::as_str)
        }
    }

    /// The capped tenant a startup packet names, if any. `identify_by =
    /// "database"` falls back to `user`, as PostgreSQL does for a startup
    /// packet without a database.
    pub fn tenant_of<'p>(&self, params: &'p HashMap<String, String>) -> Option<&'p str> {
        let value = self.key(params)?;
        self.tenants.contains_key(value).then_some(value)
    }

    /// The configured tenant (`[multi_tenancy] tenants`) a startup packet
    /// names, capped or not; `None` with multi-tenancy off. What plugin
    /// hooks are told the session's tenant is.
    pub fn configured_tenant_of<'p>(&self, params: &'p HashMap<String, String>) -> Option<&'p str> {
        let value = self.key(params)?;
        self.known.contains(value).then_some(value)
    }

    /// The plan of the known tenant a startup packet names, which the
    /// session is pinned to at admission (W07). A startup parameter matches a
    /// tenant id exactly, else in lower case (as the row filter's identifier
    /// reads it); `identify_by = "database"` resolves as [`Self::tenant_of`].
    pub fn identify(&self, params: &HashMap<String, String>) -> Option<&Arc<TenantPlan>> {
        if self.plans.is_empty() {
            return None;
        }
        let value = self.key(params)?;
        if let Some(plan) = self.plans.get(value) {
            return Some(plan);
        }
        if self.identify_by == "database" {
            return None;
        }
        self.plans.get(&value.to_lowercase())
    }

    /// The plan of the known tenant `tenant`.
    pub fn plan(&self, tenant: &str) -> Option<&Arc<TenantPlan>> {
        self.plans.get(tenant)
    }

    /// A known tenant uses schema, database or branch isolation.
    pub fn has_non_row_tenants(&self) -> bool {
        self.plans.values().any(|p| p.pins_database_or_schema())
    }

    /// Per tenant with pool settings of its own, sorted by tenant.
    pub fn pool_stats(&self) -> Vec<TenantPoolStats> {
        let mut out: Vec<TenantPoolStats> = self
            .plans
            .values()
            .filter_map(|p| p.pool().map(|pool| pool.stats()))
            .collect();
        out.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        out
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
                limit: slot.limit(),
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
                limit: s.limit(),
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

    #[test]
    fn update_behaves_as_fetch_update() {
        let a = AtomicUsize::new(5);
        assert_eq!(update(&a, |v| Some(v + 2)), Ok(5));
        assert_eq!(a.load(Ordering::SeqCst), 7);
        assert_eq!(update(&a, |v| v.checked_sub(8)), Err(7));
        assert_eq!(a.load(Ordering::SeqCst), 7);
        // Concurrent updaters lose nothing.
        let a = std::sync::Arc::new(AtomicUsize::new(0));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let a = std::sync::Arc::clone(&a);
                std::thread::spawn(move || {
                    for _ in 0..10_000 {
                        update(&a, |v| Some(v + 1)).unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(a.load(Ordering::SeqCst), 40_000);
    }

    #[test]
    fn journal_budget_reserves_exact_boundary_and_releases_with_charge() {
        let budget = TenantJournalBudget::new("acme", 100);
        let first = budget.try_reserve(60).unwrap();
        let second = budget.try_reserve(40).unwrap();
        assert_eq!(budget.used(), 100);
        assert!(budget.try_reserve(1).is_none());
        assert_eq!(budget.used(), 100);
        assert_eq!(budget.refusals(), 1);
        drop(first);
        assert_eq!(budget.used(), 40);
        drop(second);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn journal_budgets_are_isolated_and_concurrent_reservations_do_not_oversubscribe() {
        let acme = TenantJournalBudget::new("acme", 100);
        let other = TenantJournalBudget::new("other", 10);
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let budget = Arc::clone(&acme);
                std::thread::spawn(move || budget.try_reserve(16))
            })
            .collect();
        let charges: Vec<_> = workers
            .into_iter()
            .filter_map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(charges.len(), 6);
        assert_eq!(acme.used(), 96);
        assert_eq!(acme.refusals(), 10);
        let independent = other.try_reserve(10).unwrap();
        assert_eq!(other.used(), 10);
        drop(charges);
        assert_eq!(acme.used(), 0);
        drop(independent);
        assert_eq!(other.used(), 0);
    }

    #[test]
    fn unlimited_journal_budget_still_rejects_arithmetic_overflow() {
        let budget = TenantJournalBudget::new("uncapped", 0);
        let charge = budget.try_reserve(usize::MAX).unwrap();
        assert!(budget.try_reserve(1).is_none());
        assert_eq!(budget.used(), usize::MAX);
        drop(charge);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn journal_only_config_constructs_and_reports_a_tenant_budget() {
        let mut config = ProxyConfig::default();
        config.multi_tenancy.enabled = true;
        config.multi_tenancy.tenants = vec!["acme".into(), "other".into()];
        config.multi_tenancy.max_journal_bytes_per_tenant = 128;
        let slots = TenantSlots::from_config(&config);
        let budget = slots.budget("acme").unwrap();
        assert!(!budget.has_query_cap());
        assert!(!budget.has_replay_budget());
        assert!(budget.has_journal_budget());
        let charge = budget.journal_budget().unwrap().try_reserve(32).unwrap();
        let stats = slots.budget_stats();
        let row = stats.iter().find(|row| row.tenant == "acme").unwrap();
        assert_eq!((row.journal_byte_limit, row.journal_bytes), (128, 32));
        let other_budget = slots.budget("other").unwrap();
        let other_charge = other_budget
            .journal_budget()
            .unwrap()
            .try_reserve(128)
            .unwrap();
        assert_eq!(other_budget.journal_budget().unwrap().used(), 128);
        drop(charge);
        assert_eq!(slots.budget_stats()[0].journal_bytes, 0);
        drop(other_charge);
    }

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

    /// An uncapped tenant takes no slot but is still the session's
    /// configured tenant; with multi-tenancy off there is none.
    #[test]
    fn an_uncapped_tenant_is_still_a_configured_tenant() {
        let mut config = ProxyConfig::default();
        config.multi_tenancy.enabled = true;
        config.multi_tenancy.identify_by = "user".into();
        config.multi_tenancy.tenants = vec!["acme".into()];
        let s = TenantSlots::from_config(&config);
        let acme = params(&[("user", "acme")]);
        assert_eq!(s.tenant_of(&acme), None);
        assert_eq!(s.configured_tenant_of(&acme), Some("acme"));
        assert_eq!(s.configured_tenant_of(&params(&[("user", "x")])), None);
        config.multi_tenancy.enabled = false;
        let off = TenantSlots::from_config(&config);
        assert_eq!(off.configured_tenant_of(&acme), None);
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

    /// `<db>@<branch>` is the tenant `<db>` for the caps and the budgets, as
    /// for the row filter (crate::database_name); a whole value that is a
    /// tenant wins over its base.
    #[test]
    fn a_branch_database_takes_its_base_tenant_caps_and_budgets() {
        let mut config = ProxyConfig::default();
        config.multi_tenancy.enabled = true;
        config.multi_tenancy.identify_by = "database".into();
        config.multi_tenancy.tenants = vec!["acme".into(), "big@x".into()];
        config.multi_tenancy.max_clients_per_tenant = 1;
        config.multi_tenancy.max_active_queries_per_tenant = 1;
        let s = TenantSlots::from_config(&config);
        let db = |d: &str| params(&[("database", d), ("user", "u")]);
        assert_eq!(s.tenant_of(&db("acme@feat")), Some("acme"));
        assert_eq!(s.tenant_of(&db("acme")), Some("acme"));
        assert!(s.budget_of(&db("acme@feat")).is_some());
        assert_eq!(s.tenant_of(&db("big@x")), Some("big@x"));
        assert_eq!(s.tenant_of(&db("big@y")), None);
        assert_eq!(s.tenant_of(&db("nobody@feat")), None);
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

    /// Let a spawned debt collector run on the test's current-thread runtime.
    async fn settled() {
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    /// Lowering a cap below its holders revokes nobody: the holders keep
    /// their permits, nobody new is admitted until they are back under the
    /// new cap, and each permit they return while over it is retired.
    #[tokio::test]
    async fn a_lowered_cap_collects_held_permits_as_they_return() {
        let slot = TenantSlot::new(3);
        let mut held: Vec<_> = (0..3)
            .map(|_| slot.sem.clone().try_acquire_owned().unwrap())
            .collect();
        assert_eq!(slot.resize(1), 3);
        settled().await;
        assert_eq!((slot.limit(), slot.in_use()), (1, 3));
        assert_eq!(slot.debt.owed.load(Ordering::Relaxed), 2);

        drop(held.pop());
        settled().await;
        assert_eq!(slot.in_use(), 2);
        assert!(slot.sem.try_acquire().is_err(), "still over the new cap");
        drop(held.pop());
        settled().await;
        assert_eq!((slot.in_use(), slot.sem.available_permits()), (1, 0));
        assert_eq!(slot.debt.owed.load(Ordering::Relaxed), 0);
        assert!(slot.sem.try_acquire().is_err(), "at the new cap");
        assert!(!slot.debt.collecting.load(Ordering::Relaxed));

        drop(held.pop());
        settled().await;
        assert_eq!((slot.in_use(), slot.sem.available_permits()), (0, 1));

        // A second lowering starts a collector again; free permits go first.
        assert_eq!(slot.resize(4), 1);
        let held: Vec<_> = (0..3)
            .map(|_| slot.sem.clone().try_acquire_owned().unwrap())
            .collect();
        slot.resize(2);
        assert_eq!(slot.debt.owed.load(Ordering::Relaxed), 1);
        assert_eq!((slot.in_use(), slot.sem.available_permits()), (3, 0));
        drop(held);
        settled().await;
        assert_eq!((slot.in_use(), slot.sem.available_permits()), (0, 2));
    }

    /// Raising a cap writes off what a lowering still owes before it adds
    /// permits, so the semaphore never holds more than the cap in force.
    #[tokio::test]
    async fn a_raised_cap_writes_off_the_debt_first() {
        let slot = TenantSlot::new(4);
        let held: Vec<_> = (0..4)
            .map(|_| slot.sem.clone().try_acquire_owned().unwrap())
            .collect();
        slot.resize(1);
        assert_eq!(slot.debt.owed.load(Ordering::Relaxed), 3);
        // 1 -> 3 only forgives two of the three owed permits.
        slot.resize(3);
        settled().await;
        assert_eq!(slot.debt.owed.load(Ordering::Relaxed), 1);
        assert_eq!((slot.in_use(), slot.sem.available_permits()), (4, 0));
        // 3 -> 6 forgives the last one and adds two.
        slot.resize(6);
        settled().await;
        assert_eq!(slot.debt.owed.load(Ordering::Relaxed), 0);
        assert_eq!((slot.in_use(), slot.sem.available_permits()), (4, 2));
        assert!(!slot.debt.collecting.load(Ordering::Relaxed));
        drop(held);
        settled().await;
        assert_eq!((slot.in_use(), slot.sem.available_permits()), (0, 6));
    }

    /// A raised cap admits the waiters in FIFO order; the collector of a
    /// lowered cap queues behind the waiters that were already there.
    #[tokio::test]
    async fn resizing_keeps_the_fifo_order_of_waiters() {
        let slot = Arc::new(TenantSlot::new(2));
        let first = slot.sem.clone().try_acquire_owned().unwrap();
        let second = slot.sem.clone().try_acquire_owned().unwrap();
        let waiter = {
            let slot = Arc::clone(&slot);
            tokio::spawn(async move { slot.sem.clone().acquire_owned().await.unwrap() })
        };
        settled().await;
        slot.resize(1);
        settled().await;
        assert_eq!(slot.debt.owed.load(Ordering::Relaxed), 1);

        // The earlier waiter is served before the collector...
        drop(first);
        let admitted = waiter.await.unwrap();
        assert_eq!(slot.debt.owed.load(Ordering::Relaxed), 1);
        assert_eq!(slot.in_use(), 2);
        // ...which retires the next permit that returns.
        drop(second);
        settled().await;
        assert_eq!(slot.debt.owed.load(Ordering::Relaxed), 0);
        assert_eq!((slot.in_use(), slot.sem.available_permits()), (1, 0));

        let waiter = {
            let slot = Arc::clone(&slot);
            tokio::spawn(async move { slot.sem.clone().acquire_owned().await.unwrap() })
        };
        settled().await;
        assert!(!waiter.is_finished());
        slot.resize(2);
        let raised = waiter.await.unwrap();
        assert_eq!((slot.in_use(), slot.sem.available_permits()), (2, 0));
        drop((admitted, raised));
    }

    fn tenancy(edit: impl FnOnce(&mut MultiTenancyToml)) -> ProxyConfig {
        let mut config = ProxyConfig::default();
        let mt = &mut config.multi_tenancy;
        mt.enabled = true;
        mt.tenants = vec!["acme".into(), "globex".into()];
        mt.max_clients_per_tenant = 2;
        mt.max_active_queries_per_tenant = 2;
        mt.max_replay_bytes_per_tenant = 100;
        mt.max_journal_bytes_per_tenant = 100;
        mt.tenant_max_clients.insert("globex".into(), 0);
        edit(mt);
        config
    }

    /// A reload resizes the caps and byte budgets tenants already have, for
    /// the tenants it names, and reports each change.
    #[tokio::test]
    async fn a_reload_resizes_existing_caps_and_budgets() {
        let old = tenancy(|_| {});
        let slots = TenantSlots::from_config(&old);
        let new = tenancy(|mt| {
            mt.tenant_max_clients.insert("acme".into(), 5);
            mt.max_active_queries_per_tenant = 1;
            mt.tenant_max_replay_bytes.insert("acme".into(), 40);
            mt.tenant_max_journal_bytes.insert("globex".into(), 300);
        });

        // In use before the reload: both of acme's query permits, 60 of its
        // replay bytes.
        let acme = slots.budget("acme").unwrap().clone();
        let queries = (
            acme.try_acquire_query().unwrap(),
            acme.try_acquire_query().unwrap(),
        );
        let charge = TenantCharge::new(acme.clone());
        assert!(charge.replay_reserve(60));

        let mut changes = slots
            .reload_caps(&old.multi_tenancy, &new.multi_tenancy)
            .unwrap();
        changes.sort_by(|a, b| (&a.tenant, a.cap.keys()).cmp(&(&b.tenant, b.cap.keys())));
        let change = |tenant: &str, cap, from, to| TenantCapChange {
            tenant: tenant.into(),
            cap,
            from,
            to,
        };
        assert_eq!(
            changes,
            vec![
                change("acme", TenantCap::ActiveQueries, 2, 1),
                change("acme", TenantCap::Clients, 2, 5),
                change("acme", TenantCap::ReplayBytes, 100, 40),
                change("globex", TenantCap::ActiveQueries, 2, 1),
                change("globex", TenantCap::JournalBytes, 100, 300),
            ]
        );

        assert_eq!(
            slots
                .stats()
                .iter()
                .map(|t| (t.tenant.as_str(), t.limit))
                .collect::<Vec<_>>(),
            vec![("acme", 5)]
        );
        let mut held = Vec::new();
        for _ in 0..5 {
            held.push(slots.acquire("acme", None).await.unwrap().0.unwrap());
        }
        assert_eq!(slots.acquire("acme", None).await.unwrap_err().limit, 5);
        drop(held);

        // The lowered query cap: both holders stay, nobody new until one
        // permit has been retired and the other returned.
        let stats = |tenant: &str| {
            slots
                .budget_stats()
                .into_iter()
                .find(|row| row.tenant == tenant)
                .unwrap()
        };
        assert_eq!(
            (stats("acme").query_limit, stats("acme").queries_active),
            (1, 2)
        );
        drop(queries.0);
        settled().await;
        assert!(acme.try_acquire_query().is_none());
        assert_eq!(stats("acme").queries_active, 1);
        drop(queries.1);
        settled().await;
        let again = acme.try_acquire_query().unwrap();
        assert!(acme.try_acquire_query().is_none());
        drop(again);

        // The lowered replay budget: the 60 bytes in use stay charged, growth
        // is refused until the tenant is back under 40.
        assert_eq!(
            (stats("acme").replay_byte_limit, stats("acme").replay_bytes),
            (40, 60)
        );
        assert!(!charge.replay_reserve(61));
        assert!(charge.replay_reserve(30));
        assert!(charge.replay_reserve(40));
        assert!(!charge.replay_reserve(41));

        let globex = slots.budget("globex").unwrap().journal_budget().unwrap();
        assert_eq!(globex.limit(), 300);
        assert!(globex.try_reserve(300).is_some());
        assert_eq!(stats("acme").journal_byte_limit, 100);
    }

    /// Anything a running process cannot follow refuses the whole reload and
    /// changes nothing: another key of the section, and a cap that would be
    /// added to or removed from a tenant.
    #[tokio::test]
    async fn a_reload_that_is_more_than_a_resize_changes_nothing() {
        let old = tenancy(|_| {});
        let slots = TenantSlots::from_config(&old);
        let limits = || {
            let rows = slots.budget_stats();
            (
                slots.stats().iter().map(|t| t.limit).collect::<Vec<_>>(),
                rows.iter()
                    .map(|r| (r.query_limit, r.replay_byte_limit, r.journal_byte_limit))
                    .collect::<Vec<_>>(),
            )
        };
        let before = limits();
        let refused = |edit: fn(&mut MultiTenancyToml)| {
            // Each refused reload also asks for a resize that would apply.
            let new = tenancy(|mt| {
                mt.max_active_queries_per_tenant = 7;
                edit(mt);
            });
            let reason = slots
                .reload_caps(&old.multi_tenancy, &new.multi_tenancy)
                .unwrap_err();
            assert_eq!(limits(), before, "{reason}");
            reason
        };

        assert!(refused(|mt| mt.tenants.push("initech".into())).contains("other than"));
        assert!(refused(|mt| mt.identify_by = "database".into()).contains("other than"));
        assert!(refused(|mt| mt.active_query_wait_ms += 1).contains("other than"));
        assert!(refused(|mt| mt.idle_permit_timeout_ms += 1).contains("other than"));
        // globex has no client cap (override 0): one cannot be added...
        let added = refused(|mt| {
            mt.tenant_max_clients.insert("globex".into(), 3);
        });
        assert!(
            added.contains("globex") && added.contains("0 -> 3"),
            "{added}"
        );
        // ...and acme's replay budget cannot be removed.
        let removed = refused(|mt| {
            mt.tenant_max_replay_bytes.insert("acme".into(), 0);
        });
        assert!(
            removed.contains("acme") && removed.contains("100 -> 0"),
            "{removed}"
        );
        let over = refused(|mt| {
            mt.tenant_max_clients
                .insert("acme".into(), Semaphore::MAX_PERMITS + 1);
        });
        assert!(over.contains("must be <="), "{over}");

        // A process that enforces nothing (the section is off) accepts any
        // cap values and resizes nothing.
        let off = tenancy(|mt| mt.enabled = false);
        let off_slots = TenantSlots::from_config(&off);
        let off_new = tenancy(|mt| {
            mt.enabled = false;
            mt.max_clients_per_tenant = 9;
        });
        assert_eq!(
            off_slots.reload_caps(&off.multi_tenancy, &off_new.multi_tenancy),
            Ok(Vec::new())
        );
        // One that does not run the caps its config names refuses.
        assert!(TenantSlots::default()
            .reload_caps(
                &old.multi_tenancy,
                &tenancy(|mt| mt.max_clients_per_tenant = 3).multi_tenancy
            )
            .unwrap_err()
            .contains("not enforced"));
    }

    fn isolated(edit: impl FnOnce(&mut Vec<crate::config::TenantSpec>)) -> ProxyConfig {
        let mut config = ProxyConfig::default();
        let mt = &mut config.multi_tenancy;
        mt.enabled = true;
        mt.identify_by = "user".into();
        let mut specs = vec!["acme".into(), "Globex".into()];
        edit(&mut specs);
        mt.tenants = Vec::new();
        mt.tenant_settings = specs;
        config
    }

    fn with(id: &str, isolation: TenantIsolation) -> crate::config::TenantSpec {
        crate::config::TenantSpec {
            isolation,
            ..crate::config::TenantSpec::from(id)
        }
    }

    /// W07: a session is pinned to the tenant its startup packet names, the
    /// id exactly, else in lower case (as the row filter's identifier reads
    /// it); `identify_by = "database"` resolves the database exactly as the
    /// caps do, with no lower-case fallback.
    #[test]
    fn sessions_are_pinned_to_the_tenant_their_startup_names() {
        let slots = TenantSlots::from_config(&isolated(|_| {}));
        let id =
            |pairs: &[(&str, &str)]| slots.identify(&params(pairs)).map(|p| p.id().to_string());
        assert_eq!(id(&[("user", "acme")]).as_deref(), Some("acme"));
        assert_eq!(id(&[("user", "ACME")]).as_deref(), Some("acme"));
        assert_eq!(id(&[("user", "Globex")]).as_deref(), Some("Globex"));
        assert_eq!(
            id(&[("user", "globex")]),
            None,
            "an id is matched exactly first"
        );
        assert_eq!(id(&[("user", "nobody")]), None);
        assert_eq!(id(&[("database", "acme")]), None);

        let mut by_db = isolated(|_| {});
        by_db.multi_tenancy.identify_by = "database".into();
        let slots = TenantSlots::from_config(&by_db);
        let id =
            |pairs: &[(&str, &str)]| slots.identify(&params(pairs)).map(|p| p.id().to_string());
        assert_eq!(
            id(&[("user", "x"), ("database", "acme@b")]).as_deref(),
            Some("acme")
        );
        assert_eq!(id(&[("user", "x"), ("database", "ACME")]), None);
        assert_eq!(id(&[("user", "acme")]).as_deref(), Some("acme"));
        // Disabled: nobody is pinned.
        let mut off = isolated(|_| {});
        off.multi_tenancy.enabled = false;
        assert!(TenantSlots::from_config(&off)
            .identify(&params(&[("user", "acme")]))
            .is_none());
    }

    /// W07: the startup parameters each isolation gives a session.
    #[test]
    fn each_isolation_rewrites_the_startup_parameters() {
        let config = isolated(|specs| {
            let mut schema = with("s", TenantIsolation::Schema);
            schema.schema = Some("tenant_s".into());
            let mut db = with("d", TenantIsolation::Database);
            db.database = Some("tenant_db".into());
            let mut branch = with("b", TenantIsolation::Branch);
            branch.branch = Some("feat".into());
            specs.extend([schema, db, branch]);
        });
        let slots = TenantSlots::from_config(&config);
        let plan = |id: &str| slots.plan(id).expect("known").clone();
        let rewrite = |id: &str, pairs: &[(&str, &str)]| {
            let mut p = params(pairs);
            plan(id).rewrite_startup(&mut p);
            p
        };
        // Row: nothing.
        let row = params(&[("user", "acme"), ("database", "app")]);
        assert_eq!(
            rewrite("acme", &[("user", "acme"), ("database", "app")]),
            row
        );
        assert!(!plan("acme").pins_database_or_schema());
        // Schema: appended to the client's options; its own search_path goes.
        let s = rewrite(
            "s",
            &[
                ("user", "s"),
                ("options", "-c x=1"),
                ("SEARCH_PATH", "public"),
            ],
        );
        assert_eq!(
            s.get("options").map(String::as_str),
            Some("-c x=1 -c search_path=\"tenant_s\"")
        );
        assert!(!s.contains_key("SEARCH_PATH"));
        let s = rewrite("s", &[("user", "s"), ("options", "  ")]);
        assert_eq!(
            s.get("options").map(String::as_str),
            Some("-c search_path=\"tenant_s\"")
        );
        assert!(plan("s").locks_search_path());
        assert_eq!(plan("s").set_on_acquire(), None, "schema_apply = startup");
        // Database.
        let d = rewrite("d", &[("user", "d"), ("database", "app")]);
        assert_eq!(d.get("database").map(String::as_str), Some("tenant_db"));
        // Branch: the client's database's branch (by default the branch
        // database itself; `<base>@<branch>` on HeliosDB-Nano).
        let b = rewrite("b", &[("user", "b"), ("database", "app@x")]);
        assert_eq!(b.get("database").map(String::as_str), Some("feat"));
        let mut nano = config.clone();
        nano.branch.route_mode = BranchRouteMode::Nano;
        let nano = TenantSlots::from_config(&nano);
        let mut p = params(&[("user", "app")]);
        nano.plan("b").unwrap().rewrite_startup(&mut p);
        assert_eq!(
            p.get("database").map(String::as_str),
            Some("app@feat"),
            "no database: the user's"
        );

        // set_on_acquire leaves the startup alone and SETs instead.
        let mut late = config.clone();
        late.multi_tenancy.schema_apply = SchemaApply::SetOnAcquire;
        late.multi_tenancy.lock_search_path = false;
        let late = TenantSlots::from_config(&late);
        let plan = late.plan("s").unwrap();
        let mut p = params(&[("user", "s")]);
        plan.rewrite_startup(&mut p);
        assert_eq!(p, params(&[("user", "s")]));
        assert_eq!(
            plan.set_on_acquire().as_deref(),
            Some("SET search_path TO \"tenant_s\"")
        );
        assert!(plan.reset_leaves_schema());
        assert!(!plan.locks_search_path());
    }

    /// W07: a tenant with pool settings has a pool of its own: its key
    /// suffix, its idle cap, and `pool_max_connections` held by each
    /// connection; a full tenant frees one parked connection first, then
    /// waits, then is refused naming the cap. Tenants without pool settings
    /// have none.
    #[tokio::test]
    async fn tenant_pools_cap_their_connections() {
        let config = isolated(|specs| {
            specs[0].pool_max_connections = Some(1);
            specs[0].pool_max_idle = Some(2);
        });
        let slots = TenantSlots::from_config(&config);
        let acme = slots.plan("acme").unwrap();
        assert_eq!(acme.pool_suffix(), Some("\0t=acme"));
        assert_eq!(acme.pool_max_idle(), Some(2));
        assert!(slots.plan("Globex").unwrap().pool().is_none());
        let pool = acme.pool().unwrap();
        let wait = Duration::from_millis(20);
        let (held, waited) = pool.acquire(wait, || false).await.unwrap();
        assert!(held.is_some() && waited.is_none());
        assert!(pool.try_acquire().is_err(), "full");
        let refused = pool.acquire(wait, || false).await.unwrap_err();
        assert!(refused.contains("pool_max_connections (1)"), "{refused}");
        assert!(refused.contains(crate::node_slots::REFUSAL_MARKER));
        assert!(!pool.has_waiters());
        // Freeing a parked connection hands its slot over at once.
        let mut parked = held;
        let (again, waited) = pool
            .acquire(wait, || parked.take().is_some())
            .await
            .unwrap();
        assert!(again.is_some() && waited.is_none());
        pool.note_dial();
        pool.note_checkout();
        assert_eq!(
            slots.pool_stats(),
            vec![TenantPoolStats {
                tenant: "acme".into(),
                limit: Some(1),
                in_use: Some(1),
                waiting: 0,
                waits: 1,
                refusals: 1,
                dials: 1,
                checkouts: 1,
            }]
        );
    }
}
