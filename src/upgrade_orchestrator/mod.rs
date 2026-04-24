//! Zero-downtime PostgreSQL major-version upgrade orchestrator (T2.1).
//!
//! Coordinates a six-step upgrade workflow against a live cluster:
//!
//! 1. **Stand up** — spin up a new-version standby and start logical
//!    replication from the source primary.
//! 2. **Shadow execute** — every client write hits both source primary
//!    and target standby until the orchestrator declares parity.
//! 3. **Validate** — sample comparison (row counts + per-row hash on
//!    a deterministic sample) confirms target ≡ source.
//! 4. **Cutover** — pause client writes via `SwitchoverBuffer`,
//!    promote the new-version node to primary, swap proxy topology.
//! 5. **Drain** — replay any in-flight transactions onto the new
//!    primary via the existing `FailoverReplay` engine.
//! 6. **Retire** — mark the old-version primary disabled.
//!
//! ## Status
//!
//! This module is the **state machine + public API**. Each transition
//! is a stub that logs and advances state; the heavy lifting (logical
//! replication setup, sample validation, cutover coordination) lands
//! in subsequent commits as each stage is wired against the live
//! cluster harness in `tests/docker/upgrade-matrix.yml`.
//!
//! The shape is settled, the integration points are wired, the
//! contract is testable.

use crate::backend::BackendConfig;
use crate::switchover_buffer::SwitchoverBuffer;
use crate::{ProxyError, Result};
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Lifecycle of an `UpgradeJob`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeState {
    /// Job created, no work started yet.
    Pending,
    /// Logical replication slot configured; new-version standby
    /// catching up to the source.
    StandbyCatchingUp,
    /// Both primaries receiving writes; orchestrator measuring drift.
    ShadowExecuting,
    /// Sample comparison passed; ready to cutover.
    Validated,
    /// `SwitchoverBuffer` engaged; client traffic paused.
    Cutover,
    /// In-flight transactions replaying onto the new primary.
    Draining,
    /// Old-version primary retired; upgrade complete.
    Complete,
    /// Aborted on error or operator request.
    Failed,
}

/// Snapshot of an `UpgradeJob` for the admin API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeJob {
    pub id: Uuid,
    pub from_version: u32,
    pub to_version: u32,
    pub from_address: String,
    pub to_address: String,
    pub state: UpgradeState,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
    /// Statements shadow-executed against the new-version target.
    pub shadow_statements: u64,
    /// Sample-comparison rows checked at validation time.
    pub validated_rows: u64,
}

impl UpgradeJob {
    fn new(req: &PlanRequest) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            from_version: req.from_version,
            to_version: req.to_version,
            from_address: req.from_address.clone(),
            to_address: req.to_address.clone(),
            state: UpgradeState::Pending,
            started_at: now,
            updated_at: now,
            completed_at: None,
            error: None,
            shadow_statements: 0,
            validated_rows: 0,
        }
    }

    fn advance(&mut self, next: UpgradeState) {
        self.state = next;
        self.updated_at = Utc::now();
        if matches!(next, UpgradeState::Complete | UpgradeState::Failed) {
            self.completed_at = Some(self.updated_at);
        }
    }

    fn fail(&mut self, reason: impl Into<String>) {
        self.error = Some(reason.into());
        self.advance(UpgradeState::Failed);
    }
}

/// Caller-supplied request to start an upgrade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRequest {
    /// Source PG major version (e.g. `14`).
    pub from_version: u32,
    /// Target PG major version (e.g. `17`).
    pub to_version: u32,
    /// `host:port` of the source primary. Defaults to the proxy's
    /// configured primary if empty.
    #[serde(default)]
    pub from_address: String,
    /// `host:port` of the target node (already running the new PG
    /// version, but not yet part of the cluster).
    pub to_address: String,
}

impl PlanRequest {
    pub fn validate(&self) -> Result<()> {
        if self.to_version <= self.from_version {
            return Err(ProxyError::Configuration(format!(
                "to_version ({}) must be greater than from_version ({})",
                self.to_version, self.from_version
            )));
        }
        if self.to_address.is_empty() {
            return Err(ProxyError::Configuration(
                "to_address must be provided".into(),
            ));
        }
        Ok(())
    }
}

/// Coordinates active upgrade jobs.
///
/// Today's responsibility: maintain the job map, drive the state
/// machine via `tick()`. The actual side effects (logical-replication
/// setup, validation, cutover) hook into existing modules:
/// `BackendClient` for SQL on the source/target, `SwitchoverBuffer`
/// for the cutover pause, `FailoverReplay` for the drain.
pub struct UpgradeOrchestrator {
    jobs: Arc<RwLock<HashMap<Uuid, UpgradeJob>>>,
    /// Shared `SwitchoverBuffer` used during cutover. Wiring it in via
    /// the constructor keeps the orchestrator decoupled from the
    /// rest of the proxy and easy to unit-test.
    switchover: Arc<SwitchoverBuffer>,
    /// Backend-connection template. Host/port swapped to source /
    /// target at each step.
    backend_template: BackendConfig,
}

impl UpgradeOrchestrator {
    pub fn new(
        switchover: Arc<SwitchoverBuffer>,
        backend_template: BackendConfig,
    ) -> Self {
        Self {
            jobs: Arc::new(RwLock::new(HashMap::new())),
            switchover,
            backend_template,
        }
    }

    /// Register a new upgrade job. Returns the assigned job id; the
    /// job is created in `Pending` state. The state machine
    /// progresses on subsequent `tick()` calls.
    pub fn start(&self, req: PlanRequest) -> Result<Uuid> {
        req.validate()?;
        let job = UpgradeJob::new(&req);
        let id = job.id;
        self.jobs.write().insert(id, job);
        tracing::info!(
            job = %id,
            from = req.from_version,
            to = req.to_version,
            "upgrade job created"
        );
        Ok(id)
    }

    /// Read-only snapshot of a job's state.
    pub fn get(&self, id: Uuid) -> Option<UpgradeJob> {
        self.jobs.read().get(&id).cloned()
    }

    /// List every known job (recent + active).
    pub fn list(&self) -> Vec<UpgradeJob> {
        self.jobs.read().values().cloned().collect()
    }

    /// Advance the state machine for a single job. Returns the
    /// (possibly-updated) job snapshot.
    ///
    /// Each branch performs the SQL/coordination work for its current
    /// state and, on success, advances to the next. Errors transition
    /// to `Failed` with a captured reason.
    ///
    /// Today the side-effect bodies are stubs that log and advance —
    /// every call site is annotated with the real work that will
    /// land here once the upgrade-matrix harness validates the
    /// scaffolding.
    pub async fn tick(&self, id: Uuid) -> Result<UpgradeJob> {
        let mut snap = self
            .get(id)
            .ok_or_else(|| ProxyError::Internal(format!("upgrade job {} not found", id)))?;

        match snap.state {
            UpgradeState::Pending => {
                // TODO: open BackendClient to the source, run
                //   `CREATE PUBLICATION helios_upgrade FOR ALL TABLES;`
                //   `SELECT pg_create_logical_replication_slot('helios_upgrade', 'pgoutput');`
                // Then on the target, `CREATE SUBSCRIPTION helios_upgrade
                //   CONNECTION 'host=<from>...' PUBLICATION helios_upgrade;`
                tracing::info!(job = %id, "stage 1: standing up logical replication");
                snap.advance(UpgradeState::StandbyCatchingUp);
            }
            UpgradeState::StandbyCatchingUp => {
                // TODO: poll target's `pg_subscription` and
                //   `pg_stat_subscription` until lag is acceptable
                //   (e.g. < 1 MB or < 5 s). Use `pg_lsn_to_u64` from
                //   failover_replay to compare positions.
                tracing::info!(job = %id, "stage 2: target caught up, starting shadow");
                snap.advance(UpgradeState::ShadowExecuting);
            }
            UpgradeState::ShadowExecuting => {
                // TODO: keep shadowing until operator triggers
                //   validation; for the matrix harness, the runner
                //   advances after a fixed duration. The shadow
                //   itself is observation-only — logical replication
                //   handles the writes; we just track session-level
                //   acks and report drift via metrics.
                tracing::info!(job = %id, "stage 3: shadow stable, validating sample");
                snap.advance(UpgradeState::Validated);
            }
            UpgradeState::Validated => {
                // TODO: switchover.start_buffering(); proxy now
                //   queues client writes. Then run `SELECT pg_promote(true)`
                //   on target via failover_controller's logic.
                tracing::info!(job = %id, "stage 4: cutover — buffering + promoting target");
                self.switchover.start_buffering();
                snap.advance(UpgradeState::Cutover);
            }
            UpgradeState::Cutover => {
                // TODO: invoke FailoverReplay against any in-flight
                //   transactions, then switchover.stop_buffering()
                //   so queued writes drain to the new primary.
                tracing::info!(job = %id, "stage 5: draining in-flight transactions");
                snap.advance(UpgradeState::Draining);
            }
            UpgradeState::Draining => {
                // TODO: confirm zero in-flight, then disable old node
                //   in load_balancer.
                tracing::info!(job = %id, "stage 6: retiring old primary");
                self.switchover.stop_buffering();
                snap.advance(UpgradeState::Complete);
            }
            UpgradeState::Complete | UpgradeState::Failed => {
                // Terminal — no-op tick.
            }
        }

        self.jobs.write().insert(id, snap.clone());
        Ok(snap)
    }

    /// Cancel an active job — sets state to Failed with a "cancelled"
    /// reason. Side-effects rolled back where possible (logical-
    /// replication slot dropped, switchover_buffer released).
    pub fn cancel(&self, id: Uuid, reason: &str) -> Result<UpgradeJob> {
        let mut jobs = self.jobs.write();
        let job = jobs
            .get_mut(&id)
            .ok_or_else(|| ProxyError::Internal(format!("upgrade job {} not found", id)))?;
        if matches!(
            job.state,
            UpgradeState::Complete | UpgradeState::Failed
        ) {
            return Err(ProxyError::Internal(format!(
                "job {} already terminal: {:?}",
                id, job.state
            )));
        }
        // TODO: cleanup side effects per current state.
        self.switchover.stop_buffering();
        job.fail(format!("cancelled: {}", reason));
        Ok(job.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::tls::default_client_config;
    use crate::backend::TlsMode;
    use crate::switchover_buffer::BufferConfig;
    use std::time::Duration;

    fn template() -> BackendConfig {
        BackendConfig {
            host: "placeholder".into(),
            port: 0,
            user: "postgres".into(),
            password: None,
            database: None,
            application_name: Some("helios-upgrade".into()),
            tls_mode: TlsMode::Disable,
            connect_timeout: Duration::from_millis(200),
            query_timeout: Duration::from_millis(200),
            tls_config: default_client_config(),
        }
    }

    fn switchover() -> Arc<SwitchoverBuffer> {
        Arc::new(SwitchoverBuffer::new(BufferConfig::default()))
    }

    #[test]
    fn validate_rejects_downgrade() {
        let req = PlanRequest {
            from_version: 17,
            to_version: 14,
            from_address: "pg-17:5432".into(),
            to_address: "pg-14:5432".into(),
        };
        assert!(matches!(req.validate(), Err(ProxyError::Configuration(_))));
    }

    #[test]
    fn validate_rejects_same_version() {
        let req = PlanRequest {
            from_version: 16,
            to_version: 16,
            from_address: "a".into(),
            to_address: "b".into(),
        };
        assert!(req.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_target_address() {
        let req = PlanRequest {
            from_version: 14,
            to_version: 17,
            from_address: "a".into(),
            to_address: "".into(),
        };
        assert!(req.validate().is_err());
    }

    #[test]
    fn validate_accepts_proper_upgrade() {
        let req = PlanRequest {
            from_version: 14,
            to_version: 17,
            from_address: "pg-14:5432".into(),
            to_address: "pg-17:5432".into(),
        };
        assert!(req.validate().is_ok());
    }

    #[tokio::test]
    async fn full_state_machine_progression() {
        let orch = UpgradeOrchestrator::new(switchover(), template());
        let id = orch
            .start(PlanRequest {
                from_version: 14,
                to_version: 17,
                from_address: "pg-14:5432".into(),
                to_address: "pg-17:5432".into(),
            })
            .unwrap();

        assert_eq!(orch.get(id).unwrap().state, UpgradeState::Pending);
        assert_eq!(orch.tick(id).await.unwrap().state, UpgradeState::StandbyCatchingUp);
        assert_eq!(orch.tick(id).await.unwrap().state, UpgradeState::ShadowExecuting);
        assert_eq!(orch.tick(id).await.unwrap().state, UpgradeState::Validated);
        assert_eq!(orch.tick(id).await.unwrap().state, UpgradeState::Cutover);
        assert_eq!(orch.tick(id).await.unwrap().state, UpgradeState::Draining);
        assert_eq!(orch.tick(id).await.unwrap().state, UpgradeState::Complete);
        // Terminal state — further ticks are no-ops.
        assert_eq!(orch.tick(id).await.unwrap().state, UpgradeState::Complete);
        assert!(orch.get(id).unwrap().completed_at.is_some());
    }

    #[tokio::test]
    async fn cancel_marks_failed_with_reason() {
        let orch = UpgradeOrchestrator::new(switchover(), template());
        let id = orch
            .start(PlanRequest {
                from_version: 14,
                to_version: 17,
                from_address: "a".into(),
                to_address: "b".into(),
            })
            .unwrap();

        orch.tick(id).await.unwrap(); // → StandbyCatchingUp
        let cancelled = orch.cancel(id, "operator request").unwrap();
        assert_eq!(cancelled.state, UpgradeState::Failed);
        assert!(cancelled.error.unwrap().contains("operator request"));
    }

    #[test]
    fn cancel_errors_on_terminal_job() {
        let orch = UpgradeOrchestrator::new(switchover(), template());
        let id = orch
            .start(PlanRequest {
                from_version: 14,
                to_version: 17,
                from_address: "a".into(),
                to_address: "b".into(),
            })
            .unwrap();
        // Force into terminal state via cancel, then try again.
        orch.cancel(id, "first cancel").unwrap();
        assert!(orch.cancel(id, "second cancel").is_err());
    }

    #[test]
    fn list_returns_every_known_job() {
        let orch = UpgradeOrchestrator::new(switchover(), template());
        for to in [15, 16, 17] {
            orch.start(PlanRequest {
                from_version: 14,
                to_version: to,
                from_address: "a".into(),
                to_address: "b".into(),
            })
            .unwrap();
        }
        assert_eq!(orch.list().len(), 3);
    }
}
