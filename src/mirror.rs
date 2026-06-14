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

use tokio::sync::mpsc;

use crate::backend::{tls::default_client_config, BackendClient, BackendConfig, TlsMode};
use crate::config::MirrorConfig;

/// Counters surfaced for observability.
#[derive(Default)]
pub struct MirrorMetrics {
    pub mirrored: AtomicU64,
    pub dropped: AtomicU64,
    pub errors: AtomicU64,
}

/// Handle held by the server: a bounded sender plus the sampling policy.
pub struct MirrorHandle {
    tx: mpsc::Sender<String>,
    sample_rate: f64,
    writes_only: bool,
    pub metrics: Arc<MirrorMetrics>,
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
            Ok(()) => {}
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
        metrics: metrics.clone(),
    };
    tokio::spawn(worker(config, rx, metrics));
    handle
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
