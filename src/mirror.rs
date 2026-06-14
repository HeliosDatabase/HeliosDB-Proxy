//! Continuous traffic mirroring.
//!
//! Replays a sampled share of live (simple-query) write statements to a
//! secondary backend, **asynchronously and off the client hot path**: the
//! data path does a non-blocking `try_send` into a bounded queue and moves
//! on; a background worker drains the queue and applies each statement to the
//! mirror backend. When the queue is full, statements are dropped (and
//! counted) rather than slowing the client — mirroring is best-effort.
//!
//! This is the on-ramp to the PG->Nano migration mirror (Batch G2): point the
//! mirror at a HeliosDB-Nano instance and its write set tracks the primary.
//! (Result diffing for blue/green validation already lives in
//! `shadow_execute`; this module is the continuous write tail.)

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::mpsc;

use crate::backend::types::TextValue;
use crate::backend::{tls::default_client_config, BackendClient, BackendConfig, ParamValue, TlsMode};
use crate::config::MirrorConfig;

/// Counters surfaced for observability.
#[derive(Default)]
pub struct MirrorMetrics {
    /// Statements accepted into the queue.
    pub enqueued: AtomicU64,
    /// Statements successfully applied to the mirror backend.
    pub mirrored: AtomicU64,
    /// Statements dropped because the queue was full.
    pub dropped: AtomicU64,
    /// Apply/connect failures.
    pub errors: AtomicU64,
}

/// Operator-facing migration status (served at `/api/migration/status`).
#[derive(Debug, Clone, Serialize)]
pub struct MigrationStatus {
    pub enabled: bool,
    pub target: String,
    pub writes_only: bool,
    pub enqueued: u64,
    pub mirrored: u64,
    pub dropped: u64,
    pub errors: u64,
    /// Statements accepted but not yet applied (queue backlog).
    pub lag: u64,
    /// True when the mirror is enabled, the backlog is drained, and nothing
    /// has been dropped — i.e. the secondary is caught up and a cutover is
    /// safe with respect to the mirrored write set.
    pub migration_ready: bool,
}

/// Compute a migration status snapshot from the live counters.
pub fn status(target: &str, writes_only: bool, m: &MirrorMetrics) -> MigrationStatus {
    let enqueued = m.enqueued.load(Ordering::Relaxed);
    let mirrored = m.mirrored.load(Ordering::Relaxed);
    let dropped = m.dropped.load(Ordering::Relaxed);
    let errors = m.errors.load(Ordering::Relaxed);
    let lag = enqueued.saturating_sub(mirrored).saturating_sub(errors);
    MigrationStatus {
        enabled: true,
        target: target.to_string(),
        writes_only,
        enqueued,
        mirrored,
        dropped,
        errors,
        lag,
        migration_ready: lag == 0 && dropped == 0,
    }
}

/// Handle held by the server: a bounded sender plus the sampling policy.
pub struct MirrorHandle {
    tx: mpsc::Sender<String>,
    sample_rate: f64,
    writes_only: bool,
    target: String,
    pub metrics: Arc<MirrorMetrics>,
}

impl MirrorHandle {
    /// A snapshot of migration status for the admin API.
    pub fn status(&self) -> MigrationStatus {
        status(&self.target, self.writes_only, &self.metrics)
    }
    pub fn target(&self) -> &str {
        &self.target
    }
    pub fn writes_only(&self) -> bool {
        self.writes_only
    }
}

impl MirrorHandle {
    /// Offer one statement to the mirror. `is_write` is the data path's
    /// already-computed verb classification (avoids re-parsing). Non-blocking:
    /// drops (and counts) when the queue is full. Returns immediately.
    pub fn offer(&self, sql: &str, is_write: bool) {
        if self.writes_only && !is_write {
            return;
        }
        if self.sample_rate < 1.0 {
            // Cheap per-call sample without locking a shared RNG.
            use rand::Rng;
            if rand::thread_rng().gen::<f64>() >= self.sample_rate {
                return;
            }
        }
        match self.tx.try_send(sql.to_string()) {
            Ok(()) => {
                self.metrics.enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

/// Spawn the mirror worker. Returns a handle for the data path to feed.
pub fn spawn(config: MirrorConfig) -> MirrorHandle {
    let (tx, rx) = mpsc::channel::<String>(config.queue_size.max(1));
    let metrics = Arc::new(MirrorMetrics::default());
    let handle = MirrorHandle {
        tx,
        sample_rate: config.sample_rate.clamp(0.0, 1.0),
        writes_only: config.writes_only,
        target: format!("{}:{}", config.backend_host, config.backend_port),
        metrics: metrics.clone(),
    };
    tokio::spawn(worker(config, rx, metrics));
    handle
}

/// Target the proxy redirects client traffic to after a migration cutover.
/// New connections route here (with these credentials/database substituted
/// for the client's), making the cutover transparent to the application.
#[derive(Debug, Clone)]
pub struct CutoverTarget {
    pub addr: String,
    pub user: String,
    pub password: Option<String>,
    pub database: Option<String>,
}

/// Per-table snapshot result.
#[derive(Debug, Clone, Serialize)]
pub struct TableSnapshot {
    pub table: String,
    pub source_rows: u64,
    pub copied: u64,
}

fn backend_cfg(host: &str, port: u16, user: &str, pass: Option<&str>, db: Option<&str>, app: &str) -> BackendConfig {
    BackendConfig {
        host: host.to_string(),
        port,
        user: user.to_string(),
        password: pass.map(|s| s.to_string()),
        database: db.map(|s| s.to_string()),
        application_name: Some(app.to_string()),
        tls_mode: TlsMode::Disable,
        connect_timeout: Duration::from_secs(5),
        query_timeout: Duration::from_secs(60),
        tls_config: default_client_config(),
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Encode one value into a buffer in PostgreSQL `COPY ... FROM STDIN` text
/// format: `NULL` is `\N`; backslash/tab/newline/carriage-return are escaped.
/// Source values arrive as their text representation already, so this is just
/// the COPY-level escaping.
fn encode_copy_field(out: &mut Vec<u8>, v: &TextValue) {
    match v {
        TextValue::Null => out.extend_from_slice(b"\\N"),
        TextValue::Text(s) => {
            for &b in s.as_bytes() {
                match b {
                    b'\\' => out.extend_from_slice(b"\\\\"),
                    b'\t' => out.extend_from_slice(b"\\t"),
                    b'\n' => out.extend_from_slice(b"\\n"),
                    b'\r' => out.extend_from_slice(b"\\r"),
                    _ => out.push(b),
                }
            }
        }
    }
}

/// Map a PostgreSQL type OID to a portable column type for the snapshot's
/// `CREATE TABLE` on the secondary. Unknown OIDs fall back to `text`.
fn oid_type(oid: u32) -> &'static str {
    match oid {
        16 => "boolean",
        20 => "bigint",
        21 | 23 => "integer",
        700 => "real",
        701 => "double precision",
        1700 => "numeric",
        1082 => "date",
        1114 | 1184 => "timestamp",
        2950 => "uuid",
        114 | 3802 => "jsonb",
        _ => "text",
    }
}

/// Snapshot-bootstrap the secondary: for each table, read all existing rows
/// from the source (primary) and copy them into the mirror target, creating
/// the table there if needed. Returns a per-table report. Used by
/// `POST /api/migration/snapshot` to seed a migration with existing data
/// before/alongside the continuous write tail.
pub async fn snapshot_tables(cfg: &MirrorConfig, tables: &[String]) -> Result<Vec<TableSnapshot>, String> {
    let src_cfg = backend_cfg(
        &cfg.source_host, cfg.source_port, &cfg.source_user,
        cfg.source_password.as_deref(), cfg.source_database.as_deref(), "heliosproxy-snapshot-src",
    );
    let tgt_cfg = backend_cfg(
        &cfg.backend_host, cfg.backend_port, &cfg.backend_user,
        cfg.backend_password.as_deref(), cfg.backend_database.as_deref(), "heliosproxy-snapshot-tgt",
    );
    let mut src = BackendClient::connect(&src_cfg).await.map_err(|e| format!("source connect: {}", e))?;
    let mut tgt = BackendClient::connect(&tgt_cfg).await.map_err(|e| format!("target connect: {}", e))?;

    let mut report = Vec::new();
    for table in tables {
        let qt = quote_ident(table);
        let res = src
            .simple_query(&format!("SELECT * FROM {}", qt))
            .await
            .map_err(|e| format!("read {}: {}", table, e))?;

        // CREATE TABLE IF NOT EXISTS on the target from the source columns.
        let cols_ddl: Vec<String> = res
            .columns
            .iter()
            .map(|c| format!("{} {}", quote_ident(&c.name), oid_type(c.type_oid)))
            .collect();
        let create = format!("CREATE TABLE IF NOT EXISTS {} ({})", qt, cols_ddl.join(", "));
        tgt.execute(&create).await.map_err(|e| format!("create {} on target: {}", table, e))?;

        let col_list = res.columns.iter().map(|c| quote_ident(&c.name)).collect::<Vec<_>>().join(", ");

        // Primary path: a single COPY ... FROM STDIN bulk-load. `HELIOS_SNAPSHOT_USE_COPY=0`
        // forces the INSERT path (ops kill-switch / fallback test).
        let use_copy =
            std::env::var("HELIOS_SNAPSHOT_USE_COPY").map(|v| v != "0").unwrap_or(true);

        let mut copied: Option<u64> = None;
        if use_copy {
            let mut copy_buf: Vec<u8> = Vec::new();
            for row in &res.rows {
                for (i, v) in row.iter().enumerate() {
                    if i > 0 {
                        copy_buf.push(b'\t');
                    }
                    encode_copy_field(&mut copy_buf, v);
                }
                copy_buf.push(b'\n');
            }
            let copy_sql = format!("COPY {} ({}) FROM STDIN", qt, col_list);
            match tgt.copy_in(&copy_sql, &copy_buf).await {
                Ok(n) => copied = Some(n),
                Err(e) => {
                    // COPY rejected/unsupported leaves the connection clean and
                    // zero rows loaded (COPY is atomic) — fall through to INSERT.
                    tracing::warn!(
                        table = %table,
                        error = %e,
                        "COPY snapshot failed; falling back to per-row INSERT"
                    );
                }
            }
        }

        // Fallback path (preserved): per-row parameterised INSERTs.
        let copied = match copied {
            Some(n) => n,
            None => {
                let placeholders =
                    (1..=res.columns.len()).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ");
                let insert = format!("INSERT INTO {} ({}) VALUES ({})", qt, col_list, placeholders);
                let mut copied = 0u64;
                for row in &res.rows {
                    let params: Vec<ParamValue> = row
                        .iter()
                        .map(|v| match v {
                            TextValue::Null => ParamValue::Null,
                            TextValue::Text(s) => ParamValue::Text(s.clone()),
                        })
                        .collect();
                    tgt.query_with_params(&insert, &params)
                        .await
                        .map_err(|e| format!("insert into {}: {}", table, e))?;
                    copied += 1;
                }
                copied
            }
        };
        report.push(TableSnapshot { table: table.clone(), source_rows: res.rows.len() as u64, copied });
    }
    src.close().await;
    tgt.close().await;
    Ok(report)
}

async fn worker(config: MirrorConfig, mut rx: mpsc::Receiver<String>, metrics: Arc<MirrorMetrics>) {
    let bcfg = BackendConfig {
        host: config.backend_host.clone(),
        port: config.backend_port,
        user: config.backend_user.clone(),
        password: config.backend_password.clone(),
        database: config.backend_database.clone(),
        application_name: Some("heliosproxy-mirror".to_string()),
        tls_mode: TlsMode::Disable,
        connect_timeout: Duration::from_secs(5),
        query_timeout: Duration::from_secs(30),
        tls_config: default_client_config(),
    };
    tracing::info!(target = %bcfg.address(), "traffic mirror worker started");

    let mut client: Option<BackendClient> = None;
    while let Some(sql) = rx.recv().await {
        // (Re)connect lazily so a temporarily-down mirror doesn't crash-loop.
        if client.is_none() {
            match BackendClient::connect(&bcfg).await {
                Ok(c) => client = Some(c),
                Err(e) => {
                    metrics.errors.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(error = %e, "mirror connect failed; dropping statement");
                    continue;
                }
            }
        }
        let c = client.as_mut().unwrap();
        if let Err(e) = c.simple_query(&sql).await {
            metrics.errors.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(error = %e, "mirror apply failed; will reconnect");
            // Drop the connection so the next statement reconnects.
            if let Some(c) = client.take() {
                c.close().await;
            }
        } else {
            metrics.mirrored.fetch_add(1, Ordering::Relaxed);
        }
    }
    tracing::info!("traffic mirror worker stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(v: &TextValue) -> String {
        let mut out = Vec::new();
        encode_copy_field(&mut out, v);
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn copy_field_encoding() {
        // NULL -> \N (distinct from an empty string).
        assert_eq!(enc(&TextValue::Null), "\\N");
        assert_eq!(enc(&TextValue::Text(String::new())), "");
        // Plain text passes through.
        assert_eq!(enc(&TextValue::Text("alice".into())), "alice");
        // Tab / newline / CR / backslash are escaped so they can't break the
        // tab-delimited, newline-terminated COPY framing.
        assert_eq!(enc(&TextValue::Text("a\tb".into())), "a\\tb");
        assert_eq!(enc(&TextValue::Text("a\nb".into())), "a\\nb");
        assert_eq!(enc(&TextValue::Text("a\rb".into())), "a\\rb");
        assert_eq!(enc(&TextValue::Text("a\\b".into())), "a\\\\b");
        // A literal backslash-N in data must not be confused with NULL.
        assert_eq!(enc(&TextValue::Text("\\N".into())), "\\\\N");
    }
}
