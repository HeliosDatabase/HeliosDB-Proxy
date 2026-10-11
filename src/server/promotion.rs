use super::*;
use crate::auto_promote::{self, OutageClock, PromoNode, Refusal};

impl ProxyServer {
    /// Spawn the opt-in automatic-promotion loop (`[auto_promote]`). Returns
    /// `None` unless `enabled = true`; validation already guarantees the
    /// postgres topology provider, so the tracker below is authoritative.
    pub(super) fn spawn_auto_promoter(&self) -> Option<tokio::task::JoinHandle<()>> {
        let cfg = self.state.live_config.load();
        if !cfg.auto_promote.enabled {
            return None;
        }
        {
            let mut st = self.state.promoter.lock();
            st.enabled = true;
            st.state = "idle".into();
        }
        let state = self.state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        Some(tokio::spawn(async move {
            let mut clock = OutageClock::default();
            let mut last_leader: Option<String> = None;
            let mut attempts: u32 = 0;
            let mut cooldown_until: Option<tokio::time::Instant> = None;
            loop {
                // Re-read each tick so a SIGHUP retunes the loop.
                let config = state.live_config.load_full();
                let ap = &config.auto_promote;
                let sleep = Duration::from_secs(ap.check_interval_secs.max(1));
                tokio::select! {
                    _ = tokio::time::sleep(sleep) => {}
                    _ = shutdown_rx.recv() => break,
                }
                if !ap.enabled {
                    continue;
                }
                let leader = Self::authoritative_leader(&config, &state.primary_tracker);
                if leader.is_some() {
                    last_leader = leader.clone();
                    attempts = 0;
                }
                let absent = clock.observe(std::time::Instant::now(), leader.is_some());
                {
                    let mut st = state.promoter.lock();
                    st.primary_absent_secs = absent.map_or(0, |d| d.as_secs());
                    st.attempts_this_outage = attempts;
                    st.state = if absent.is_none() {
                        "idle"
                    } else {
                        "primary_absent"
                    }
                    .into();
                }
                let Some(absent) = absent else { continue };
                if absent < Duration::from_secs(ap.down_after_secs) {
                    continue;
                }
                if attempts >= ap.max_attempts {
                    state.promoter.lock().state = "gave_up".into();
                    continue;
                }
                if cooldown_until.is_some_and(|t| tokio::time::Instant::now() < t) {
                    state.promoter.lock().state = "cooldown".into();
                    continue;
                }

                let health = state.health.load_full();
                let nodes: Vec<PromoNode> = config
                    .nodes
                    .iter()
                    .map(|n| PromoNode {
                        host: n.host.clone(),
                        port: n.port,
                        role: n.role,
                        enabled: n.enabled,
                        lag_bytes: health
                            .get(n.address())
                            .and_then(|h| h.replication_lag_bytes),
                    })
                    .collect();
                let old_primary = last_leader.clone().unwrap_or_else(|| {
                    config
                        .nodes
                        .iter()
                        .find(|n| n.role == NodeRole::Primary)
                        .map(|n| n.address().to_string())
                        .unwrap_or_default()
                });

                attempts += 1;
                {
                    let mut st = state.promoter.lock();
                    st.state = "promoting".into();
                    st.attempts_this_outage = attempts;
                    st.last_attempt_at = Some(chrono::Utc::now());
                }
                tracing::warn!(
                    absent_secs = absent.as_secs(),
                    attempt = attempts,
                    old_primary = %old_primary,
                    "auto_promote: primary absent past down_after_secs, attempting promotion"
                );
                let outcome =
                    auto_promote::attempt(ap, &config.topology, &nodes, &old_primary).await;
                let mut st = state.promoter.lock();
                match outcome {
                    Ok(p) => {
                        tracing::warn!(target = %p.target, fenced = p.fenced, "auto_promote: standby promoted");
                        st.promotions_total += 1;
                        st.last_target = Some(p.target);
                        st.last_result = Some("promoted".into());
                        st.state = "cooldown".into();
                        cooldown_until = Some(
                            tokio::time::Instant::now() + Duration::from_secs(ap.cooldown_secs),
                        );
                        // The topology provider rediscovers the new primary;
                        // restart the outage clock so we do not re-fire.
                        clock = OutageClock::default();
                    }
                    Err(r) => {
                        // A live primary is not a failure of the promoter:
                        // do not burn the attempt budget on it.
                        if matches!(r, Refusal::PrimaryAlive(_)) {
                            attempts = attempts.saturating_sub(1);
                        }
                        tracing::error!(refusal = %r, "auto_promote: promotion refused");
                        st.last_result = Some(r.to_string());
                        st.state = "primary_absent".into();
                    }
                }
            }
        }))
    }
}
