//! The idle reaper's tick (W02): the data-path pool's idle TTL, lifetime and
//! floors.
//!
//! Every `[limits] pool_reap_interval_secs` the reaper:
//!
//! - closes parked connections idle longer than the idle TTL (the smaller
//!   non-zero of `[pool] idle_timeout_secs` and `[pool_mode]
//!   idle_timeout_secs`), keeping the newest `[pool_mode] min_idle` of each
//!   identity and, per node, the `[pool] min_connections` it must keep open;
//! - closes every parked connection older than the max lifetime (the
//!   smaller non-zero of the two `max_lifetime_secs`), floors or not;
//! - opens what the floors miss, ahead of use: up to `min_idle` parked
//!   connections for every identity a session parked one of within the idle
//!   TTL, and up to `min_connections` open connections per enabled, healthy
//!   node, with the identity it served most recently.
//!
//! An ahead-of-use connection never waits for, or takes away, what a
//! session needs: its node and tenant slots and its reconnect permit are
//! taken only when free, and the pool's idle caps still apply. It
//! authenticates as the identity's user with the proxy's own credential
//! (`[auth] mode = "scram"` with a plaintext `auth_file` entry), or none for
//! a trust backend. With pass-through auth and a backend that asks for a
//! password the identity is never opened ahead of use again, a warning is
//! logged once, and no node fault is recorded: nothing is wrong with the
//! node.

use super::*;

impl ProxyServer {
    /// One reaper tick over the data-path pool (transaction / statement
    /// pooling), under the live configuration.
    pub(super) async fn pool_maintenance(state: &Arc<ServerState>) {
        let Some(pool) = state.backend_pool.as_ref() else {
            return;
        };
        let config = state.live_config.load_full();
        let min_idle = config.pool_min_idle();
        let min_connections = config.pool.min_connections;
        // Each node may lose idle connections only down to its floor.
        let mut closable: Option<HashMap<String, usize>> = (min_connections > 0).then(|| {
            config
                .nodes
                .iter()
                .map(|n| {
                    let addr = n.address();
                    (
                        addr.to_string(),
                        state.node_slots.open(addr).saturating_sub(min_connections),
                    )
                })
                .collect()
        });
        let closed = pool.reap(
            config.pool_idle_ttl(),
            config.pool_max_lifetime(),
            min_idle,
            closable.as_mut(),
        );
        if closed > 0 {
            tracing::debug!(
                target: "helios::pool",
                closed,
                idle_remaining = pool.idle_count(),
                "reaped idle backend connections (idle TTL, max lifetime)"
            );
        }
        if min_idle == 0 && min_connections == 0 {
            return;
        }
        pool.expire_specs(config.pool_idle_ttl());
        if min_idle > 0 {
            for (key, spec) in pool.specs() {
                let want = min_idle.min(spec.max_idle);
                let have = pool.idle_count_for(&key);
                for _ in have..want {
                    if !Self::open_ahead(state, pool, &key, &spec, &config).await {
                        break;
                    }
                }
            }
        }
        if min_connections > 0 {
            for node in config.nodes.iter().filter(|n| n.enabled) {
                let addr = node.address();
                let open = state.node_slots.open(addr);
                // Never above the node's own cap (`pool.min_connections` may
                // exceed it; the configuration only warns).
                let floor = match config.node_connection_cap(node) {
                    0 => min_connections,
                    cap => min_connections.min(cap),
                };
                if open >= floor {
                    continue;
                }
                let Some((key, spec)) = pool.latest_spec_for(addr) else {
                    continue;
                };
                for _ in open..floor {
                    if !Self::open_ahead(state, pool, &key, &spec, &config).await {
                        break;
                    }
                }
            }
        }
    }

    /// Open one connection of `spec`'s identity ahead of use and park it
    /// under `key`. `false` when none was parked: a slot or the reconnect
    /// permit is not free, the node is not healthy, the identity's idle cap
    /// is reached, or the dial failed.
    async fn open_ahead(
        state: &Arc<ServerState>,
        pool: &crate::pool::BackendIdlePool,
        key: &str,
        spec: &crate::pool::DialSpec,
        config: &ProxyConfig,
    ) -> bool {
        let healthy = state
            .health
            .load()
            .get(&spec.node)
            .is_some_and(|h| h.healthy);
        if !healthy {
            return false;
        }
        let Some(slot) = state.node_slots.try_acquire(&spec.node) else {
            return false;
        };
        let tenant_slot = match spec.tenant_pool.as_ref().map(|p| p.try_acquire()) {
            None => None,
            Some(Ok(slot)) => slot,
            Some(Err(crate::tenant_slots::TenantPoolFull)) => return false,
        };
        let Some(_dialing) = state.node_slots.try_acquire_reconnect(&spec.node) else {
            return false;
        };
        let credential = state
            .auth_file
            .as_ref()
            .and_then(|af| af.password(&spec.user))
            .map(str::to_string);
        let opened = async {
            let mut stream = tokio::time::timeout(
                config.pool.acquire_timeout(),
                TcpStream::connect(&spec.node),
            )
            .await
            .map_err(|_| ProxyError::Connection(format!("Connection timeout to {}", spec.node)))?
            .map_err(|e| {
                ProxyError::Connection(format!("Failed to connect to {}: {e}", spec.node))
            })?;
            let _ = stream.set_nodelay(true);
            stream
                .write_all(&spec.startup)
                .await
                .map_err(|e| ProxyError::Network(format!("Backend startup error: {e}")))?;
            let frames = Self::complete_backend_auth(
                &mut stream,
                state.limits.max_pending_bytes,
                &spec.user,
                credential.as_deref(),
            )
            .await?;
            Ok::<_, ProxyError>((stream, frames))
        }
        .await;
        match opened {
            Ok((stream, frames)) => {
                Self::note_backend_startup(state, &spec.node, &frames);
                let conn = crate::pool::PooledConn {
                    stream,
                    slot,
                    tenant_slot,
                    backend_key: Self::backend_key_data(&frames),
                    settings: crate::pool::ParkedSettings::default(),
                    opened_at: std::time::Instant::now(),
                };
                let parked = pool.checkin_conn_capped(key, conn, spec.max_idle);
                if parked {
                    pool.note_topup(true);
                    if let Some(tenant_pool) = spec.tenant_pool.as_ref() {
                        tenant_pool.note_dial();
                    }
                }
                parked
            }
            Err(ProxyError::Auth(reason)) => {
                // The backend refused the proxy's own authentication for this
                // identity (pass-through auth holds no password, or the
                // auth_file entry is not the backend's): not a node fault,
                // and never tried again for it, so the backend sees no
                // stream of failed logins.
                pool.mark_no_credential(key);
                pool.note_topup(false);
                if pool.first_no_credential_warning() {
                    tracing::warn!(
                        target: "helios::pool",
                        node = %spec.node,
                        user = %spec.user,
                        "[pool_mode] min_idle / [pool] min_connections cannot open connections ahead of use for this identity, which is not tried again: {reason}. With pass-through auth the proxy holds no password: use [auth] mode = \"scram\" with a plaintext auth_file entry, or a trust backend. Logged once."
                    );
                }
                false
            }
            Err(e) => {
                pool.note_topup(false);
                tracing::debug!(
                    target: "helios::pool",
                    node = %spec.node,
                    error = %e,
                    "could not open a backend connection ahead of use"
                );
                false
            }
        }
    }
}
