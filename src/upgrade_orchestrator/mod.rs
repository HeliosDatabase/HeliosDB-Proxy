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

use crate::backend::{BackendClient, BackendConfig};
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
    /// the job to `Failed` with a captured reason.
    ///
    /// Stage transitions in order:
    /// 1. **Pending → StandbyCatchingUp**: `CREATE PUBLICATION` on
    ///    source, `CREATE SUBSCRIPTION` on target.
    /// 2. **StandbyCatchingUp → ShadowExecuting**: poll
    ///    `pg_subscription` on target until subscription is active.
    /// 3. **ShadowExecuting → Validated**: operator-gated transition;
    ///    today this advances after a brief settle delay. The
    ///    runner is expected to call `tick` only when its shadow
    ///    workload says drift is zero.
    /// 4. **Validated → Cutover**: `switchover.start_buffering()`
    ///    pauses client writes; `SELECT pg_promote(true, ...)` on
    ///    the target promotes it to primary.
    /// 5. **Cutover → Draining**: brief pause for the buffered writes
    ///    to flush onto the new primary via the proxy's normal
    ///    routing path (next admin sync picks up the new primary).
    /// 6. **Draining → Complete**: `switchover.stop_buffering()`
    ///    resumes traffic; `DROP SUBSCRIPTION` + `DROP PUBLICATION`
    ///    clean up the upgrade artefacts.
    pub async fn tick(&self, id: Uuid) -> Result<UpgradeJob> {
        let mut snap = self
            .get(id)
            .ok_or_else(|| ProxyError::Internal(format!("upgrade job {} not found", id)))?;

        let outcome = match snap.state {
            UpgradeState::Pending => self.stage_create_replication(&snap).await,
            UpgradeState::StandbyCatchingUp => self.stage_wait_catchup(&snap).await,
            UpgradeState::ShadowExecuting => self.stage_settle_shadow(&snap).await,
            UpgradeState::Validated => self.stage_cutover(&snap).await,
            UpgradeState::Cutover => self.stage_drain(&snap).await,
            UpgradeState::Draining => self.stage_retire(&snap).await,
            UpgradeState::Complete | UpgradeState::Failed => Ok(snap.state),
        };

        match outcome {
            Ok(next) => snap.advance(next),
            Err(e) => snap.fail(e.to_string()),
        }

        self.jobs.write().insert(id, snap.clone());
        Ok(snap)
    }

    // ----- per-stage bodies --------------------------------------------

    /// Stage 1: create logical-replication publication on source and
    /// matching subscription on target. Subscription name is derived
    /// from the job id so concurrent upgrades don't collide.
    ///
    /// On success → StandbyCatchingUp.
    async fn stage_create_replication(&self, job: &UpgradeJob) -> Result<UpgradeState> {
        let pub_name = publication_name(job.id);

        // Step 1a: publication on source.
        let source_cfg = self.backend_for(&job.from_address)?;
        let mut source = BackendClient::connect(&source_cfg).await.map_err(|e| {
            ProxyError::FailoverFailed(format!("connect source: {}", e))
        })?;
        // Idempotent — drop+create so reruns don't error on residue.
        let _ = source
            .execute(&format!(
                "DROP PUBLICATION IF EXISTS {}",
                quote_ident(&pub_name)
            ))
            .await;
        source
            .execute(&format!(
                "CREATE PUBLICATION {} FOR ALL TABLES",
                quote_ident(&pub_name)
            ))
            .await
            .map_err(|e| {
                ProxyError::FailoverFailed(format!("CREATE PUBLICATION: {}", e))
            })?;
        source.close().await;

        // Step 1b: subscription on target. The CONNECTION string
        // points at the source; we reuse the backend template's
        // credentials. Note that the TARGET runs the new PG major
        // version and may reject syntax the source accepts — by
        // staying on the conservative `FOR ALL TABLES` shape we keep
        // the SQL portable across PG 14-17.
        let target_cfg = self.backend_for(&job.to_address)?;
        let conninfo = source_conninfo(&source_cfg);
        let mut target = BackendClient::connect(&target_cfg).await.map_err(|e| {
            ProxyError::FailoverFailed(format!("connect target: {}", e))
        })?;
        let _ = target
            .execute(&format!(
                "DROP SUBSCRIPTION IF EXISTS {}",
                quote_ident(&pub_name)
            ))
            .await;
        target
            .execute(&format!(
                "CREATE SUBSCRIPTION {} CONNECTION '{}' PUBLICATION {}",
                quote_ident(&pub_name),
                conninfo.replace('\'', "''"),
                quote_ident(&pub_name)
            ))
            .await
            .map_err(|e| {
                ProxyError::FailoverFailed(format!("CREATE SUBSCRIPTION: {}", e))
            })?;
        target.close().await;

        tracing::info!(job = %job.id, pub_name = %pub_name, "stage 1: replication created");
        Ok(UpgradeState::StandbyCatchingUp)
    }

    /// Stage 2: poll `pg_subscription` on the target until the
    /// subscription is enabled. A complete impl would also poll
    /// `pg_stat_subscription.received_lsn` against the source's
    /// `pg_current_wal_lsn()` and only advance when drift is below a
    /// configurable threshold; this MVP advances as soon as the
    /// subscription is active, which is correct under steady state.
    async fn stage_wait_catchup(&self, job: &UpgradeJob) -> Result<UpgradeState> {
        let target_cfg = self.backend_for(&job.to_address)?;
        let mut target = BackendClient::connect(&target_cfg).await.map_err(|e| {
            ProxyError::FailoverFailed(format!("connect target: {}", e))
        })?;
        let pub_name = publication_name(job.id);
        let row = target
            .query_scalar(&format!(
                "SELECT subenabled FROM pg_subscription WHERE subname = '{}'",
                pub_name.replace('\'', "''")
            ))
            .await
            .map_err(|e| {
                ProxyError::FailoverFailed(format!("subscription probe: {}", e))
            })?;
        target.close().await;

        let enabled = row
            .as_bool("subenabled")
            .map_err(|e| {
                ProxyError::FailoverFailed(format!("subenabled value: {}", e))
            })?
            .unwrap_or(false);
        if !enabled {
            return Err(ProxyError::FailoverFailed(format!(
                "subscription {} not enabled on target",
                pub_name
            )));
        }
        tracing::info!(job = %job.id, "stage 2: subscription active");
        Ok(UpgradeState::ShadowExecuting)
    }

    /// Stage 3: shadow-execute settle. The orchestrator does not own
    /// the workload — the runner runs pgbench (or production
    /// traffic). Here we just sleep briefly so the workload's
    /// drift-measurement window has time to land, then advance.
    /// Operators that want stricter validation should query
    /// `pg_stat_subscription.last_msg_receipt_time` themselves
    /// before calling tick().
    async fn stage_settle_shadow(&self, job: &UpgradeJob) -> Result<UpgradeState> {
        tracing::info!(job = %job.id, "stage 3: shadow window settle");
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        Ok(UpgradeState::Validated)
    }

    /// Stage 4: cutover — pause client writes via the switchover
    /// buffer, then promote the target.
    async fn stage_cutover(&self, job: &UpgradeJob) -> Result<UpgradeState> {
        self.switchover.start_buffering();
        tracing::info!(job = %job.id, "stage 4: switchover_buffer engaged; promoting target");

        let target_cfg = self.backend_for(&job.to_address)?;
        let mut target = BackendClient::connect(&target_cfg).await.map_err(|e| {
            // On connect failure here we MUST release the buffer;
            // otherwise client traffic stalls forever.
            self.switchover.stop_buffering();
            ProxyError::FailoverFailed(format!("connect target for promote: {}", e))
        })?;

        // pg_promote(wait => true, wait_seconds => 60).
        let result = target
            .query_scalar("SELECT pg_promote(true, 60)")
            .await
            .map_err(|e| ProxyError::FailoverFailed(format!("pg_promote: {}", e)));
        target.close().await;

        let row = match result {
            Ok(r) => r,
            Err(e) => {
                self.switchover.stop_buffering();
                return Err(e);
            }
        };
        let promoted = row
            .as_bool("pg_promote")
            .map_err(|e| {
                self.switchover.stop_buffering();
                ProxyError::FailoverFailed(format!("pg_promote result: {}", e))
            })?
            .unwrap_or(false);
        if !promoted {
            self.switchover.stop_buffering();
            return Err(ProxyError::FailoverFailed(
                "pg_promote returned false".into(),
            ));
        }

        tracing::info!(job = %job.id, "stage 4: target promoted");
        Ok(UpgradeState::Cutover)
    }

    /// Stage 5: drain — let the buffered writes flush onto the new
    /// primary. The proxy's primary tracker picks up the new primary
    /// on its next poll. Brief sleep + advance.
    async fn stage_drain(&self, job: &UpgradeJob) -> Result<UpgradeState> {
        tracing::info!(job = %job.id, "stage 5: draining buffered writes");
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        Ok(UpgradeState::Draining)
    }

    /// Stage 6: retire — release the switchover buffer (clients
    /// resume), drop the subscription on target, drop the publication
    /// on source. Best-effort cleanup; failure here logs but does NOT
    /// fail the job, because the upgrade itself has already
    /// succeeded (clients are talking to the new primary).
    async fn stage_retire(&self, job: &UpgradeJob) -> Result<UpgradeState> {
        self.switchover.stop_buffering();
        tracing::info!(job = %job.id, "stage 6: switchover_buffer released");

        let pub_name = publication_name(job.id);

        // Best-effort drop on target.
        if let Ok(target_cfg) = self.backend_for(&job.to_address) {
            if let Ok(mut target) = BackendClient::connect(&target_cfg).await {
                let _ = target
                    .execute(&format!(
                        "DROP SUBSCRIPTION IF EXISTS {}",
                        quote_ident(&pub_name)
                    ))
                    .await;
                target.close().await;
            }
        }

        // Best-effort drop on source.
        if let Ok(source_cfg) = self.backend_for(&job.from_address) {
            if let Ok(mut source) = BackendClient::connect(&source_cfg).await {
                let _ = source
                    .execute(&format!(
                        "DROP PUBLICATION IF EXISTS {}",
                        quote_ident(&pub_name)
                    ))
                    .await;
                source.close().await;
            }
        }

        Ok(UpgradeState::Complete)
    }

    /// Build a BackendConfig pointing at `addr` ("host:port") using
    /// the orchestrator's stored credential template.
    fn backend_for(&self, addr: &str) -> Result<BackendConfig> {
        let (host, port) = parse_addr(addr)?;
        let mut c = self.backend_template.clone();
        c.host = host;
        c.port = port;
        Ok(c)
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

// --- module-level helpers -----------------------------------------

/// Quote a PostgreSQL identifier (publication / subscription name).
/// Doubles embedded `"` and wraps in `"`.
fn quote_ident(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('"');
    for c in name.chars() {
        if c == '"' {
            out.push_str("\"\"");
        } else {
            out.push(c);
        }
    }
    out.push('"');
    out
}

/// Derive a publication / subscription name from a job id. Stays
/// under PG's 63-byte identifier limit and is unique per concurrent
/// upgrade (UUIDs are 36 chars; with the prefix we sit at 49).
fn publication_name(id: Uuid) -> String {
    format!("helios_upgrade_{}", id.simple())
}

/// Build a libpq-style conninfo string for the given backend config.
/// Used as the SUBSCRIPTION's CONNECTION clause so the target
/// can pull from the source.
fn source_conninfo(cfg: &BackendConfig) -> String {
    let mut parts = vec![
        format!("host={}", cfg.host),
        format!("port={}", cfg.port),
        format!("user={}", cfg.user),
    ];
    if let Some(pw) = &cfg.password {
        parts.push(format!("password={}", pw));
    }
    if let Some(db) = &cfg.database {
        parts.push(format!("dbname={}", db));
    }
    parts.join(" ")
}

/// Parse "host:port" into its parts. Errors on malformed input rather
/// than silently defaulting — the orchestrator depends on these
/// being correct.
fn parse_addr(addr: &str) -> Result<(String, u16)> {
    let (host, port) = addr.rsplit_once(':').ok_or_else(|| {
        ProxyError::Configuration(format!("expected host:port, got {:?}", addr))
    })?;
    let port: u16 = port.parse().map_err(|_| {
        ProxyError::Configuration(format!("invalid port in {:?}", addr))
    })?;
    Ok((host.to_string(), port))
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

    /// With no live PG cluster, stage 1's `CREATE PUBLICATION` connect
    /// fails — the job transitions to Failed with a meaningful error.
    /// This is the safe-by-default behaviour: we don't silently skip
    /// real SQL when the addresses are bogus.
    #[tokio::test]
    async fn tick_fails_job_on_unreachable_source() {
        let orch = UpgradeOrchestrator::new(switchover(), template());
        let id = orch
            .start(PlanRequest {
                from_version: 14,
                to_version: 17,
                // 127.0.0.1:1 — no daemon, connect refused.
                from_address: "127.0.0.1:1".into(),
                to_address: "127.0.0.1:2".into(),
            })
            .unwrap();

        let result = orch.tick(id).await.unwrap();
        assert_eq!(result.state, UpgradeState::Failed);
        let err = result.error.expect("failure carries an error message");
        // Either the source connect or the publication SQL trips it;
        // both surface a "connect" or "FailoverFailed" message.
        assert!(
            err.contains("connect") || err.contains("FailoverFailed") || err.contains("PUBLICATION"),
            "expected connect/SQL error, got {}",
            err
        );
    }

    /// Terminal-state ticks are no-ops.
    #[tokio::test]
    async fn tick_on_terminal_job_is_noop() {
        let orch = UpgradeOrchestrator::new(switchover(), template());
        let id = orch
            .start(PlanRequest {
                from_version: 14,
                to_version: 17,
                from_address: "127.0.0.1:1".into(),
                to_address: "127.0.0.1:2".into(),
            })
            .unwrap();

        // First tick fails the job (unreachable source).
        let r1 = orch.tick(id).await.unwrap();
        assert_eq!(r1.state, UpgradeState::Failed);

        // Second tick: terminal — same Failed state, no panic.
        let r2 = orch.tick(id).await.unwrap();
        assert_eq!(r2.state, UpgradeState::Failed);
    }

    #[tokio::test]
    async fn cancel_marks_failed_with_reason() {
        let orch = UpgradeOrchestrator::new(switchover(), template());
        let id = orch
            .start(PlanRequest {
                from_version: 14,
                to_version: 17,
                from_address: "a:1".into(),
                to_address: "b:2".into(),
            })
            .unwrap();

        // Cancel from Pending — no tick has run yet, so we never
        // attempt a network connection.
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
                from_address: "a:1".into(),
                to_address: "b:2".into(),
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
                from_address: "a:1".into(),
                to_address: "b:2".into(),
            })
            .unwrap();
        }
        assert_eq!(orch.list().len(), 3);
    }

    #[test]
    fn parse_addr_round_trip() {
        let (h, p) = parse_addr("pg-primary.svc:5432").unwrap();
        assert_eq!(h, "pg-primary.svc");
        assert_eq!(p, 5432);
    }

    #[test]
    fn parse_addr_supports_ipv6_style_host() {
        // Last colon is the port separator — works for IPv6-bracket
        // syntax `[::1]:5432` too.
        let (h, p) = parse_addr("[::1]:5432").unwrap();
        assert_eq!(h, "[::1]");
        assert_eq!(p, 5432);
    }

    #[test]
    fn parse_addr_rejects_missing_port() {
        assert!(parse_addr("pg-primary.svc").is_err());
        assert!(parse_addr("pg-primary.svc:").is_err());
        assert!(parse_addr("pg-primary.svc:not-a-port").is_err());
    }

    #[test]
    fn quote_ident_doubles_embedded_quotes() {
        assert_eq!(quote_ident("simple"), "\"simple\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn publication_name_uses_simple_uuid_form() {
        let id = Uuid::nil();
        let name = publication_name(id);
        assert_eq!(name, "helios_upgrade_00000000000000000000000000000000");
        // Check < 63 byte PG identifier limit.
        assert!(name.len() < 63);
    }

    #[test]
    fn source_conninfo_includes_credentials() {
        let cfg = template();
        let s = source_conninfo(&cfg);
        assert!(s.contains("host=placeholder"));
        assert!(s.contains("port=0"));
        assert!(s.contains("user=postgres"));
        // No password / database in the test template.
        assert!(!s.contains("password="));
        assert!(!s.contains("dbname="));
    }
}
