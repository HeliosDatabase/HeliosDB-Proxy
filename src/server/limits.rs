use super::*;

/// Operational limits/timeouts resolved once at startup from the `[limits]`
/// TOML section ([`crate::config::LimitsToml`]). The `*_secs` config keys are
/// converted to [`Duration`] here (at construction, not per-use) so the hot
/// path reads a ready `Duration`/`usize` with no conversion. Defaults are the
/// exact prior compiled-in constants, so a default config is unchanged.
#[derive(Debug, Clone)]
pub(super) struct ResolvedLimits {
    pub(super) max_cancel_keys: usize,
    pub(super) startup_timeout: Duration,
    pub(super) backend_write_timeout: Duration,
    pub(super) backend_read_timeout: Duration,
    pub(super) client_write_timeout: Duration,
    pub(super) reprepare_timeout: Duration,
    pub(super) max_prepared_statements: usize,
    pub(super) max_prepared_bytes: usize,
    pub(super) max_pending_bytes: usize,
    /// Cap on one backend response frame's declared length on every streaming
    /// relay (H-07); `[limits] max_backend_frame_bytes`.
    pub(super) max_backend_frame_bytes: usize,
    /// Whole-response deadline on the streaming relays (H-07 slow-drip);
    /// `None` = off. `[limits] backend_response_timeout_secs`.
    pub(super) backend_response_timeout: Option<Duration>,
    /// TR-06 observation digest byte budget per recorded statement.
    pub(super) tr_max_observation_bytes: usize,
    /// Overall operator-replay deadline (O-04); `None` when disabled (`0`).
    pub(super) replay_deadline: Option<Duration>,
    /// Only read on the pool-modes data path; gated to avoid a dead-field
    /// warning on feature-off builds.
    #[cfg(feature = "pool-modes")]
    pub(super) max_total_idle_backend_conns: usize,
    pub(super) pool_reap_interval: Duration,
    /// Ceiling on concurrently-served client connections. `0` = unlimited (the
    /// default), which is the pre-cap behaviour. Read once here at startup —
    /// see `ServerState::client_slots`.
    pub(super) max_client_connections: usize,
    /// Idle-session deadline for an authenticated client. `None` when
    /// `client_idle_timeout_secs = 0` (the default), which is the pre-timeout
    /// behaviour: the query loop then waits on the client forever.
    pub(super) client_idle_timeout: Option<Duration>,
    /// Bounded admission wait for the `max_client_connections` cap (H-05).
    /// `None` when `client_admission_wait_secs = 0` (refuse immediately).
    pub(super) client_admission_wait: Option<Duration>,
    /// Idle authenticated clients kept per backend identity by the non-PG-wire
    /// gateways (H-06).
    pub(super) gateway_pool_max_idle: usize,
    /// In-session TR: cap on recorded statements per explicit transaction.
    pub(super) tr_max_replay_statements: usize,
    /// In-session TR: cap on recorded bytes per explicit transaction.
    pub(super) tr_max_replay_bytes: usize,
    /// In-session TR: cap on tracked session `SET`/`RESET` statements.
    pub(super) tr_max_session_set_statements: usize,
}

impl ResolvedLimits {
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) fn relay(&self) -> RelayLimits {
        RelayLimits {
            client_write_timeout: self.client_write_timeout,
            backend_read_timeout: self.backend_read_timeout,
            max_frame_bytes: self.max_backend_frame_bytes,
            response_timeout: self.backend_response_timeout,
            observation_bytes: self.tr_max_observation_bytes,
        }
    }

    pub(super) fn from_toml(l: &crate::config::LimitsToml) -> Self {
        Self {
            max_cancel_keys: l.max_cancel_keys,
            startup_timeout: Duration::from_secs(l.startup_timeout_secs),
            backend_write_timeout: Duration::from_secs(l.backend_write_timeout_secs),
            backend_read_timeout: Duration::from_secs(l.backend_read_timeout_secs),
            client_write_timeout: Duration::from_secs(l.client_write_timeout_secs),
            reprepare_timeout: Duration::from_secs(l.reprepare_timeout_secs),
            max_prepared_statements: l.max_prepared_statements,
            max_prepared_bytes: l.max_prepared_bytes,
            max_pending_bytes: l.max_pending_bytes,
            max_backend_frame_bytes: l.max_backend_frame_bytes,
            backend_response_timeout: (l.backend_response_timeout_secs > 0)
                .then(|| Duration::from_secs(l.backend_response_timeout_secs)),
            tr_max_observation_bytes: l.tr_max_observation_bytes,
            replay_deadline: (l.replay_deadline_secs > 0)
                .then(|| Duration::from_secs(l.replay_deadline_secs)),
            #[cfg(feature = "pool-modes")]
            max_total_idle_backend_conns: l.max_total_idle_backend_conns,
            pool_reap_interval: Duration::from_secs(l.pool_reap_interval_secs),
            max_client_connections: l.max_client_connections,
            client_idle_timeout: match l.client_idle_timeout_secs {
                0 => None,
                secs => Some(Duration::from_secs(secs)),
            },
            client_admission_wait: match l.client_admission_wait_secs {
                0 => None,
                secs => Some(Duration::from_secs(secs)),
            },
            gateway_pool_max_idle: l.gateway_pool_max_idle,
            tr_max_replay_statements: l.tr_max_replay_statements,
            tr_max_replay_bytes: l.tr_max_replay_bytes,
            tr_max_session_set_statements: l.tr_max_session_set_statements,
        }
    }
}

impl Default for ResolvedLimits {
    fn default() -> Self {
        Self::from_toml(&crate::config::LimitsToml::default())
    }
}
