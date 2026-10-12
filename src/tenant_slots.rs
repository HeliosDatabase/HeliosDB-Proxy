//! Per-tenant client admission (H-05 slice 2).
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::ProxyConfig;

struct TenantSlot {
    limit: usize,
    sem: Arc<Semaphore>,
    waiting: AtomicUsize,
    waits: AtomicU64,
    refusals: AtomicU64,
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

/// The caps of every known tenant with a client cap above 0. Built once at
/// startup; empty unless `[multi_tenancy] enabled = true`.
#[derive(Default)]
pub struct TenantSlots {
    identify_by: String,
    tenants: HashMap<String, TenantSlot>,
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
                (limit > 0).then(|| {
                    (
                        t.clone(),
                        TenantSlot {
                            limit,
                            sem: Arc::new(Semaphore::new(limit)),
                            waiting: AtomicUsize::new(0),
                            waits: AtomicU64::new(0),
                            refusals: AtomicU64::new(0),
                        },
                    )
                })
            })
            .collect();
        Self {
            identify_by: mt.identify_by.clone(),
            tenants,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tenants.is_empty()
    }

    /// The capped tenant a startup packet names, if any. `identify_by =
    /// "database"` falls back to `user`, as PostgreSQL does for a startup
    /// packet without a database.
    pub fn tenant_of<'p>(&self, params: &'p HashMap<String, String>) -> Option<&'p str> {
        let value = if self.identify_by == "database" {
            params.get("database").or_else(|| params.get("user"))
        } else {
            params.get(&self.identify_by)
        }?;
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
        let refused = || {
            slot.refusals.fetch_add(1, Ordering::Relaxed);
            TenantRefusal {
                tenant: tenant.to_string(),
                limit: slot.limit,
            }
        };
        let Some(wait) = wait else {
            return Err(refused());
        };
        slot.waits.fetch_add(1, Ordering::Relaxed);
        slot.waiting.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let acquired = tokio::time::timeout(wait, slot.sem.clone().acquire_owned()).await;
        slot.waiting.fetch_sub(1, Ordering::Relaxed);
        match acquired {
            Ok(Ok(p)) => Ok((Some(p), Some(started.elapsed()))),
            _ => Err(refused()),
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
                in_use: s.limit - s.sem.available_permits().min(s.limit),
                waiting: s.waiting.load(Ordering::Relaxed),
                waits: s.waits.load(Ordering::Relaxed),
                refusals: s.refusals.load(Ordering::Relaxed),
            })
            .collect();
        out.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        out
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
}
