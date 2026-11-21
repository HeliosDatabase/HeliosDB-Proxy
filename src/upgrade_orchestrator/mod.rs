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
//! 5. **Drain** — with writes paused, wait until the target has
//!    applied everything the source committed.
//! 6. **Retire** — resume traffic, replay the writes buffered during the
//!    pause onto the new primary, drop the replication artefacts.
//!
//! ## Status
//!
//! Every stage performs real work against the live cluster:
//!
//! - **Stage 1** issues `CREATE PUBLICATION` on the source and
//!   `CREATE SUBSCRIPTION` on the target over the backend client.
//! - **Stage 2** polls `pg_subscription.subenabled` on the target.
//! - **Stage 3** validates parity by comparing the row counts of a
//!   deterministic sample of user tables between source and target,
//!   accumulating the rows checked into `validated_rows`; a mismatch
//!   fails the job.
//! - **Stage 4** engages the `SwitchoverBuffer` and runs `pg_promote`.
//! - **Stage 5** compares the source's `pg_current_wal_lsn()` with the
//!   `confirmed_flush_lsn` of the subscription's replication slot; the
//!   job stays in `Cutover` until the target has confirmed it.
//! - **Stage 6** releases the buffer, replays the buffered writes onto
//!   the new primary, and drops the replication artefacts.
//!
//! Any stage error fails the job; once the buffer is engaged, a failure
//! also releases it and fails every buffered write explicitly, so client
//! traffic never stalls and no buffered write is silently dropped.
//!
//! The orchestrator does not own the shadow workload (the runner drives
//! pgbench / production traffic); a per-row checksum on top of the
//! row-count parity check is a stricter follow-on.

use crate::backend::{BackendClient, BackendConfig, BackendResult, TextValue};
use crate::failover_replay::pg_lsn_to_u64;
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
    pub fn new(switchover: Arc<SwitchoverBuffer>, backend_template: BackendConfig) -> Self {
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
    /// 3. **ShadowExecuting → Validated**: row-count parity of a
    ///    deterministic table sample between source and target. The
    ///    runner is expected to call `tick` only when its shadow
    ///    workload says drift is zero.
    /// 4. **Validated → Cutover**: `switchover.start_buffering()`
    ///    pauses client writes; `SELECT pg_promote(true, ...)` on
    ///    the target promotes it to primary.
    /// 5. **Cutover → Draining**: once the subscription's replication
    ///    slot on the source has confirmed the source's current WAL
    ///    position (writes are paused, so it is final). While the target
    ///    lags, the job stays in `Cutover`; tick again (or `cancel`).
    /// 6. **Draining → Complete**: `switchover.stop_buffering()`
    ///    resumes traffic and the buffered writes are replayed onto the
    ///    new primary; `DROP SUBSCRIPTION` + `DROP PUBLICATION` clean up
    ///    the upgrade artefacts.
    pub async fn tick(&self, id: Uuid) -> Result<UpgradeJob> {
        let mut snap = self
            .get(id)
            .ok_or_else(|| ProxyError::Internal(format!("upgrade job {} not found", id)))?;

        let outcome = match snap.state {
            UpgradeState::Pending => self.stage_create_replication(&snap).await,
            UpgradeState::StandbyCatchingUp => self.stage_wait_catchup(&snap).await,
            UpgradeState::ShadowExecuting => match self.stage_validate(&snap).await {
                // Record how many rows the parity check actually compared.
                Ok((next, rows)) => {
                    snap.validated_rows = rows;
                    Ok(next)
                }
                Err(e) => Err(e),
            },
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
        let mut source = BackendClient::connect(&source_cfg)
            .await
            .map_err(|e| ProxyError::FailoverFailed(format!("connect source: {}", e)))?;
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
            .map_err(|e| ProxyError::FailoverFailed(format!("CREATE PUBLICATION: {}", e)))?;
        source.close().await;

        // Step 1b: subscription on target. The CONNECTION string
        // points at the source; we reuse the backend template's
        // credentials. Note that the TARGET runs the new PG major
        // version and may reject syntax the source accepts — by
        // staying on the conservative `FOR ALL TABLES` shape we keep
        // the SQL portable across PG 14-17.
        let target_cfg = self.backend_for(&job.to_address)?;
        let conninfo = source_conninfo(&source_cfg);
        let mut target = BackendClient::connect(&target_cfg)
            .await
            .map_err(|e| ProxyError::FailoverFailed(format!("connect target: {}", e)))?;
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
            .map_err(|e| ProxyError::FailoverFailed(format!("CREATE SUBSCRIPTION: {}", e)))?;
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
        let mut target = BackendClient::connect(&target_cfg)
            .await
            .map_err(|e| ProxyError::FailoverFailed(format!("connect target: {}", e)))?;
        let pub_name = publication_name(job.id);
        let row = target
            .query_scalar(&format!(
                "SELECT subenabled FROM pg_subscription WHERE subname = '{}'",
                pub_name.replace('\'', "''")
            ))
            .await
            .map_err(|e| ProxyError::FailoverFailed(format!("subscription probe: {}", e)))?;
        target.close().await;

        let enabled = row
            .as_bool("subenabled")
            .map_err(|e| ProxyError::FailoverFailed(format!("subenabled value: {}", e)))?
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

    /// Stage 3: validate — confirm the target has reached parity with
    /// the source before we cut over. Lists a deterministic sample of
    /// the source's user tables and compares `count(*)` on each between
    /// source and target; a mismatch fails the job (replication has not
    /// caught up or has diverged). Returns the next state plus the total
    /// rows compared, which the caller records as `validated_rows`.
    ///
    /// Row-count parity is the portable, catalog-version-independent
    /// check that behaves identically across PG 14-17. A per-row
    /// checksum is a stricter follow-on; counts already catch the common
    /// divergence modes (missing/extra rows from lag or a broken
    /// subscription). The orchestrator does not own the workload, so the
    /// runner is expected to quiesce writes (or accept eventual parity)
    /// before driving this transition.
    async fn stage_validate(&self, job: &UpgradeJob) -> Result<(UpgradeState, u64)> {
        // Cap the sample so validation stays bounded on wide schemas.
        const SAMPLE_TABLES: usize = 64;

        let source_cfg = self.backend_for(&job.from_address)?;
        let target_cfg = self.backend_for(&job.to_address)?;

        let mut source = BackendClient::connect(&source_cfg)
            .await
            .map_err(|e| ProxyError::FailoverFailed(format!("connect source: {}", e)))?;
        let mut target = BackendClient::connect(&target_cfg)
            .await
            .map_err(|e| ProxyError::FailoverFailed(format!("connect target: {}", e)))?;

        // Deterministic table sample (ordered identically on both nodes).
        let tables = match source
            .simple_query(
                "SELECT schemaname, tablename FROM pg_tables \
                 WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY schemaname, tablename",
            )
            .await
        {
            Ok(t) => t,
            Err(e) => {
                source.close().await;
                target.close().await;
                return Err(ProxyError::FailoverFailed(format!("list tables: {}", e)));
            }
        };

        let mut total_rows: u64 = 0;
        let mut checked = 0usize;
        for row in tables.rows.iter().take(SAMPLE_TABLES) {
            let schema = row.first().and_then(|v| v.as_str()).unwrap_or("public");
            let table = match row.get(1).and_then(|v| v.as_str()) {
                Some(t) => t,
                None => continue,
            };
            let qualified = format!("{}.{}", quote_ident(schema), quote_ident(table));
            let count_sql = format!("SELECT count(*) AS n FROM {}", qualified);

            let read_count = |res: BackendResult<TextValue>, side: &str| -> Result<i64> {
                res.map_err(|e| {
                    ProxyError::FailoverFailed(format!("{} count {}: {}", side, qualified, e))
                })?
                .as_i64("n")
                .map_err(|e| ProxyError::FailoverFailed(format!("{}: {}", side, e)))
                .map(|o| o.unwrap_or(0))
            };

            let s = match read_count(source.query_scalar(&count_sql).await, "source") {
                Ok(v) => v,
                Err(e) => {
                    source.close().await;
                    target.close().await;
                    return Err(e);
                }
            };
            let t = match read_count(target.query_scalar(&count_sql).await, "target") {
                Ok(v) => v,
                Err(e) => {
                    source.close().await;
                    target.close().await;
                    return Err(e);
                }
            };

            if s != t {
                source.close().await;
                target.close().await;
                return Err(ProxyError::FailoverFailed(format!(
                    "validation failed: {} row count source={} target={}",
                    qualified, s, t
                )));
            }
            total_rows += s.max(0) as u64;
            checked += 1;
        }

        source.close().await;
        target.close().await;
        tracing::info!(
            job = %job.id,
            tables = checked,
            rows = total_rows,
            "stage 3: validation passed (row-count parity)"
        );
        Ok((UpgradeState::Validated, total_rows))
    }

    /// Stage 4: cutover — pause client writes via the switchover
    /// buffer, then promote the target.
    async fn stage_cutover(&self, job: &UpgradeJob) -> Result<UpgradeState> {
        self.switchover.start_buffering();
        tracing::info!(job = %job.id, "stage 4: switchover_buffer engaged; promoting target");

        let target_cfg = match self.backend_for(&job.to_address) {
            Ok(c) => c,
            Err(e) => {
                self.abort_buffer("upgrade cutover failed");
                return Err(e);
            }
        };
        let mut target = BackendClient::connect(&target_cfg).await.map_err(|e| {
            // On connect failure here we MUST release the buffer;
            // otherwise client traffic stalls forever.
            self.abort_buffer("upgrade cutover failed");
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
                self.abort_buffer("upgrade cutover failed");
                return Err(e);
            }
        };
        let promoted = row
            .as_bool("pg_promote")
            .map_err(|e| {
                self.abort_buffer("upgrade cutover failed");
                ProxyError::FailoverFailed(format!("pg_promote result: {}", e))
            })?
            .unwrap_or(false);
        if !promoted {
            self.abort_buffer("upgrade cutover failed");
            return Err(ProxyError::FailoverFailed(
                "pg_promote returned false".into(),
            ));
        }

        tracing::info!(job = %job.id, "stage 4: target promoted");
        Ok(UpgradeState::Cutover)
    }

    /// Stage 5: drain — writes are paused, so the source's current WAL
    /// position is final; the target has applied everything once the
    /// subscription's replication slot on the source has confirmed it.
    /// Returns `Draining` when it has, `Cutover` (unchanged: tick again)
    /// while the target lags. Any error releases the buffer, failing the
    /// buffered writes, and fails the job.
    async fn stage_drain(&self, job: &UpgradeJob) -> Result<UpgradeState> {
        match self.replication_lag(job).await {
            Ok(0) => {
                tracing::info!(job = %job.id, "stage 5: target confirmed the source's WAL");
                Ok(UpgradeState::Draining)
            }
            Ok(lag) => {
                tracing::info!(job = %job.id, lag_bytes = lag, "stage 5: target still catching up");
                Ok(UpgradeState::Cutover)
            }
            Err(e) => {
                self.abort_buffer(&format!("upgrade drain failed: {}", e));
                Err(e)
            }
        }
    }

    /// Bytes of source WAL the subscription has not yet confirmed.
    async fn replication_lag(&self, job: &UpgradeJob) -> Result<u64> {
        let slot = publication_name(job.id);
        let source_cfg = self.backend_for(&job.from_address)?;
        let mut source = BackendClient::connect(&source_cfg)
            .await
            .map_err(|e| ProxyError::FailoverFailed(format!("connect source for drain: {}", e)))?;
        let probe = async {
            let current = source
                .query_scalar("SELECT pg_current_wal_lsn()::text")
                .await
                .map_err(|e| ProxyError::FailoverFailed(format!("source WAL position: {}", e)))?;
            let confirmed = source
                .query_scalar(&format!(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                     WHERE slot_name = '{}'",
                    slot.replace('\'', "''")
                ))
                .await
                .map_err(|e| {
                    ProxyError::FailoverFailed(format!(
                        "replication slot {} on source: {}",
                        slot, e
                    ))
                })?;
            Ok::<_, ProxyError>((current, confirmed))
        }
        .await;
        source.close().await;
        let (current, confirmed) = probe?;
        replication_lag(&slot, current.as_str(), confirmed.as_str())
    }

    /// Stage 6: retire — release the switchover buffer (clients
    /// resume), replay the writes buffered during the pause onto the new
    /// primary, then drop the subscription on target and the publication
    /// on source. A buffered write the new primary refuses is answered
    /// with that error. If the new primary cannot be reached, every
    /// buffered write fails explicitly and so does the job. Dropping the
    /// artefacts is best-effort: a failure is logged (a leftover
    /// subscription keeps a replication slot, and WAL, on the source)
    /// but does not fail the job, because the upgrade itself has
    /// succeeded.
    async fn stage_retire(&self, job: &UpgradeJob) -> Result<UpgradeState> {
        self.switchover.stop_buffering();
        tracing::info!(job = %job.id, "stage 6: switchover_buffer released");

        let target = match self.backend_for(&job.to_address) {
            Ok(cfg) => BackendClient::connect(&cfg)
                .await
                .map_err(|e| ProxyError::FailoverFailed(format!("connect new primary: {}", e))),
            Err(e) => Err(e),
        };
        let target = match target {
            Ok(t) => t,
            Err(e) => {
                self.abort_buffer(&format!("cannot replay buffered writes: {}", e));
                return Err(e);
            }
        };

        let before = self.switchover.stats();
        let target = Arc::new(tokio::sync::Mutex::new(target));
        self.switchover
            .drain(|sql, params| {
                let target = target.clone();
                async move { replay_buffered(&target, &sql, &params).await }
            })
            .await;
        let after = self.switchover.stats();
        let failed = (after.failed_replays - before.failed_replays)
            + (after.timed_out_queries - before.timed_out_queries);
        if failed > 0 {
            tracing::warn!(
                job = %job.id,
                failed,
                "stage 6: buffered writes not applied on the new primary (their clients got the error)"
            );
        }
        if let Ok(target) = Arc::try_unwrap(target) {
            target.into_inner().close().await;
        }

        let pub_name = publication_name(job.id);
        let drop_sub = format!("DROP SUBSCRIPTION IF EXISTS {}", quote_ident(&pub_name));
        if let Err(e) = self.run_on(&job.to_address, &drop_sub).await {
            tracing::warn!(job = %job.id, "stage 6: {} on target failed: {}", drop_sub, e);
        }
        let drop_pub = format!("DROP PUBLICATION IF EXISTS {}", quote_ident(&pub_name));
        if let Err(e) = self.run_on(&job.from_address, &drop_pub).await {
            tracing::warn!(job = %job.id, "stage 6: {} on source failed: {}", drop_pub, e);
        }

        Ok(UpgradeState::Complete)
    }

    /// Run one statement on a fresh connection to `addr`.
    async fn run_on(&self, addr: &str, sql: &str) -> Result<()> {
        let cfg = self.backend_for(addr)?;
        let mut client = BackendClient::connect(&cfg)
            .await
            .map_err(|e| ProxyError::FailoverFailed(format!("connect {}: {}", addr, e)))?;
        let outcome = client.execute(sql).await;
        client.close().await;
        outcome
            .map(|_| ())
            .map_err(|e| ProxyError::FailoverFailed(e.to_string()))
    }

    /// Release the switchover buffer after a failure, answering every
    /// buffered write with `SwitchoverFailed` instead of leaving it
    /// waiting.
    fn abort_buffer(&self, reason: &str) {
        self.switchover.stop_buffering();
        self.switchover.fail_all(reason);
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
        if matches!(job.state, UpgradeState::Complete | UpgradeState::Failed) {
            return Err(ProxyError::Internal(format!(
                "job {} already terminal: {:?}",
                id, job.state
            )));
        }
        // Release the safety-critical side effect synchronously so client
        // traffic can never stall after a cancel, answering each buffered
        // write with `SwitchoverFailed`. Dropping the replication
        // publication/subscription artefacts is best-effort and left to the
        // operator (or a follow-up retire tick) since this entry point is
        // sync; a Failed job's artefacts are harmless and idempotently
        // re-creatable on a fresh run.
        self.abort_buffer(&format!("upgrade cancelled: {}", reason));
        job.fail(format!("cancelled: {}", reason));
        Ok(job.clone())
    }
}

// --- module-level helpers -----------------------------------------

/// Bytes of WAL between the source's current position and the slot's
/// confirmed flush position (0 once confirmed). A missing (NULL) or
/// malformed position is an error: catch-up cannot be confirmed.
fn replication_lag(slot: &str, current: Option<&str>, confirmed: Option<&str>) -> Result<u64> {
    let current = current.and_then(pg_lsn_to_u64).ok_or_else(|| {
        ProxyError::FailoverFailed(format!(
            "source reported no usable WAL position ({:?})",
            current
        ))
    })?;
    let confirmed = confirmed.and_then(pg_lsn_to_u64).ok_or_else(|| {
        ProxyError::FailoverFailed(format!(
            "replication slot {} has no confirmed flush position ({:?})",
            slot, confirmed
        ))
    })?;
    Ok(current.saturating_sub(confirmed))
}

/// Replay one write buffered during the cutover onto the new primary.
/// Buffered bound parameters carry no type or format information, so a
/// parameterised write is refused rather than guessed at.
async fn replay_buffered(
    target: &tokio::sync::Mutex<BackendClient>,
    sql: &str,
    params: &[Vec<u8>],
) -> Result<()> {
    if !params.is_empty() {
        return Err(ProxyError::FailoverFailed(format!(
            "buffered write has {} bound parameters; the upgrade orchestrator replays only \
             parameterless statements",
            params.len()
        )));
    }
    target
        .lock()
        .await
        .execute(sql)
        .await
        .map(|_| ())
        .map_err(|e| ProxyError::FailoverFailed(format!("replay buffered write: {}", e)))
}

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
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| ProxyError::Configuration(format!("expected host:port, got {:?}", addr)))?;
    let port: u16 = port
        .parse()
        .map_err(|_| ProxyError::Configuration(format!("invalid port in {:?}", addr)))?;
    Ok((host.to_string(), port))
}

#[cfg(test)]
pub(crate) mod test_backend;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::tls::default_client_config;
    use crate::backend::TlsMode;
    use crate::switchover_buffer::{BufferConfig, BufferResult};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use test_backend::{Reply, ScriptedPg};

    fn template() -> BackendConfig {
        BackendConfig {
            host: "template-host".into(),
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
            err.contains("connect")
                || err.contains("FailoverFailed")
                || err.contains("PUBLICATION"),
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

    /// A job forced into `state`, as if earlier ticks had got it there.
    fn job_in(orch: &UpgradeOrchestrator, state: UpgradeState, from: &str, to: &str) -> Uuid {
        let id = orch
            .start(PlanRequest {
                from_version: 14,
                to_version: 17,
                from_address: from.into(),
                to_address: to.into(),
            })
            .unwrap();
        orch.jobs.write().get_mut(&id).unwrap().state = state;
        id
    }

    #[test]
    fn replication_lag_compares_wal_positions() {
        assert_eq!(replication_lag("s", Some("0/20"), Some("0/20")).unwrap(), 0);
        assert_eq!(
            replication_lag("s", Some("1/0"), Some("0/FFFFFFF0")).unwrap(),
            0x10
        );
        // A slot ahead of the sampled position has caught up.
        assert_eq!(replication_lag("s", Some("0/10"), Some("0/20")).unwrap(), 0);
        let err = replication_lag("slot_x", Some("0/10"), None).unwrap_err();
        assert!(err.to_string().contains("slot_x"), "{err}");
        assert!(replication_lag("s", Some("garbage"), Some("0/10")).is_err());
        assert!(replication_lag("s", None, Some("0/10")).is_err());
    }

    /// Stage 5 stays in `Cutover` while the slot lags and advances once
    /// it has confirmed the source's WAL position.
    #[tokio::test]
    async fn drain_waits_for_the_slot_to_confirm_the_source_wal() {
        let caught_up = Arc::new(AtomicBool::new(false));
        let flag = caught_up.clone();
        let pg = ScriptedPg::spawn(move |q| {
            if q.contains("pg_current_wal_lsn") {
                Reply::Rows(vec![Some("0/20".into())])
            } else if q.contains("confirmed_flush_lsn") {
                let lsn = if flag.load(Ordering::SeqCst) {
                    "0/20"
                } else {
                    "0/10"
                };
                Reply::Rows(vec![Some(lsn.into())])
            } else {
                Reply::Error(format!("unexpected: {q}"))
            }
        })
        .await;
        let orch = UpgradeOrchestrator::new(switchover(), template());
        let id = job_in(&orch, UpgradeState::Cutover, &pg.address(), &pg.address());

        let job = orch.tick(id).await.unwrap();
        assert_eq!(job.state, UpgradeState::Cutover, "{:?}", job.error);
        caught_up.store(true, Ordering::SeqCst);
        let job = orch.tick(id).await.unwrap();
        assert_eq!(job.state, UpgradeState::Draining, "{:?}", job.error);
        let slot = publication_name(id);
        assert!(pg.seen().iter().any(|q| q.contains(&slot)));
    }

    /// Stage 5 no longer advances on a timer: without a reachable source
    /// it fails the job, releases the buffer and fails the buffered write.
    #[tokio::test]
    async fn drain_without_reachable_source_fails_closed() {
        let buffer = switchover();
        let orch = UpgradeOrchestrator::new(buffer.clone(), template());
        let id = job_in(&orch, UpgradeState::Cutover, "127.0.0.1:1", "127.0.0.1:2");
        buffer.start_buffering();
        let rx = buffer
            .buffer_query("INSERT INTO t VALUES (1)".into(), vec![], 1)
            .unwrap();

        let job = orch.tick(id).await.unwrap();
        assert_eq!(job.state, UpgradeState::Failed);
        assert!(job.error.unwrap().contains("connect source for drain"));
        assert!(!buffer.is_buffering());
        assert!(matches!(rx.await.unwrap(), BufferResult::SwitchoverFailed));
    }

    /// Stage 6 replays the buffered writes onto the new primary (refusing
    /// a parameterised one explicitly) and drops the artefacts.
    #[tokio::test]
    async fn retire_replays_buffered_writes_on_the_new_primary() {
        let pg = ScriptedPg::spawn(|_| Reply::Command("OK")).await;
        let buffer = switchover();
        let orch = UpgradeOrchestrator::new(buffer.clone(), template());
        let id = job_in(&orch, UpgradeState::Draining, &pg.address(), &pg.address());
        buffer.start_buffering();
        let plain = buffer
            .buffer_query("INSERT INTO t VALUES (1)".into(), vec![], 1)
            .unwrap();
        let bound = buffer
            .buffer_query("INSERT INTO t VALUES ($1)".into(), vec![b"2".to_vec()], 2)
            .unwrap();

        let job = orch.tick(id).await.unwrap();
        assert_eq!(job.state, UpgradeState::Complete, "{:?}", job.error);
        assert!(!buffer.is_buffering());
        assert!(matches!(plain.await.unwrap(), BufferResult::Success));
        match bound.await.unwrap() {
            BufferResult::Error(e) => assert!(e.contains("bound parameters"), "{e}"),
            other => panic!("expected an explicit error, got {other:?}"),
        }
        let name = publication_name(id);
        assert_eq!(
            pg.seen(),
            vec![
                "INSERT INTO t VALUES (1)".to_string(),
                format!("DROP SUBSCRIPTION IF EXISTS \"{name}\""),
                format!("DROP PUBLICATION IF EXISTS \"{name}\""),
            ]
        );
    }

    /// Stage 6 with the new primary unreachable fails every buffered
    /// write and the job instead of reporting completion.
    #[tokio::test]
    async fn retire_without_reachable_target_fails_closed() {
        let buffer = switchover();
        let orch = UpgradeOrchestrator::new(buffer.clone(), template());
        let id = job_in(&orch, UpgradeState::Draining, "127.0.0.1:1", "127.0.0.1:2");
        buffer.start_buffering();
        let rx = buffer
            .buffer_query("INSERT INTO t VALUES (1)".into(), vec![], 1)
            .unwrap();

        let job = orch.tick(id).await.unwrap();
        assert_eq!(job.state, UpgradeState::Failed);
        assert!(job.error.unwrap().contains("connect new primary"));
        assert!(matches!(rx.await.unwrap(), BufferResult::SwitchoverFailed));
    }

    #[tokio::test]
    async fn cancel_fails_buffered_writes() {
        let buffer = switchover();
        let orch = UpgradeOrchestrator::new(buffer.clone(), template());
        let id = job_in(&orch, UpgradeState::Cutover, "a:1", "b:2");
        buffer.start_buffering();
        let rx = buffer.buffer_query("INSERT 1".into(), vec![], 1).unwrap();
        orch.cancel(id, "operator request").unwrap();
        assert!(!buffer.is_buffering());
        assert!(matches!(rx.await.unwrap(), BufferResult::SwitchoverFailed));
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
        assert!(s.contains("host=template-host"));
        assert!(s.contains("port=0"));
        assert!(s.contains("user=postgres"));
        // No password / database in the test template.
        assert!(!s.contains("password="));
        assert!(!s.contains("dbname="));
    }

    /// Live exercise of the real stage-3 row-count parity validation
    /// against an actual PostgreSQL backend. Gated on `HELIOS_LIVE_PG`
    /// (`host:port`, e.g. `127.0.0.1:25433`) so CI without a backend
    /// stays green. Source and target point at the SAME node, so every
    /// table's count equals itself and the stage must pass — proving the
    /// real path (list `pg_tables`, `count(*)` on both connections,
    /// compare, accumulate `validated_rows`) works end-to-end over the
    /// wire. Run with:
    ///   HELIOS_LIVE_PG=127.0.0.1:25433 cargo test --lib \
    ///     upgrade_orchestrator::tests::stage_validate_live -- --nocapture
    #[tokio::test]
    async fn stage_validate_live_row_count_parity() {
        let addr = match std::env::var("HELIOS_LIVE_PG") {
            Ok(a) if !a.is_empty() => a,
            _ => {
                eprintln!("skipping stage_validate_live: set HELIOS_LIVE_PG=host:port");
                return;
            }
        };
        let user = std::env::var("HELIOS_LIVE_USER").unwrap_or_else(|_| "bench".into());
        let pass = std::env::var("HELIOS_LIVE_PASS").unwrap_or_else(|_| "benchpass".into());
        let db = std::env::var("HELIOS_LIVE_DB").unwrap_or_else(|_| "benchdb".into());

        let mut tmpl = template();
        tmpl.user = user;
        tmpl.password = Some(pass);
        tmpl.database = Some(db);
        tmpl.connect_timeout = Duration::from_secs(5);
        tmpl.query_timeout = Duration::from_secs(5);

        // Seed a deterministic probe table with 3 rows.
        let (host, port) = parse_addr(&addr).unwrap();
        let mut seed_cfg = tmpl.clone();
        seed_cfg.host = host;
        seed_cfg.port = port;
        let mut seed = BackendClient::connect(&seed_cfg)
            .await
            .expect("connect to seed backend");
        seed.execute("DROP TABLE IF EXISTS upgrade_validate_probe")
            .await
            .unwrap();
        seed.execute("CREATE TABLE upgrade_validate_probe(id int)")
            .await
            .unwrap();
        seed.execute("INSERT INTO upgrade_validate_probe VALUES (1),(2),(3)")
            .await
            .unwrap();
        seed.close().await;

        // Self-compare: source == target, so parity is guaranteed and the
        // stage must report at least the 3 rows we seeded.
        let orch = UpgradeOrchestrator::new(switchover(), tmpl);
        let req = PlanRequest {
            from_version: 14,
            to_version: 17,
            from_address: addr.clone(),
            to_address: addr.clone(),
        };
        let job = UpgradeJob::new(&req);
        let (next, rows) = orch
            .stage_validate(&job)
            .await
            .expect("live validation should pass on a self-compare");
        assert_eq!(next, UpgradeState::Validated);
        assert!(rows >= 3, "expected >= 3 validated rows, got {}", rows);

        // Cleanup.
        if let Ok(mut c) = BackendClient::connect(&seed_cfg).await {
            let _ = c
                .execute("DROP TABLE IF EXISTS upgrade_validate_probe")
                .await;
            c.close().await;
        }
    }
}
