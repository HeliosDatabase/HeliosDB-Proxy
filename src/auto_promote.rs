//! Opt-in automatic standby promotion (`[auto_promote]`).
//!
//! Decision core + actions. The daemon loop that feeds it (primary-absence
//! clock, cooldown, attempt budget) lives in `server/promotion.rs`; everything
//! here is either pure (and unit-tested) or a bounded, single-shot action.
//!
//! Safety model (see [`crate::config::AutoPromoteConfig`]): the promoter acts
//! only when the authoritative topology has had no primary for
//! `down_after_secs` AND a fresh direct probe of every configured node finds
//! no writable primary. It then picks the standby with the highest replay LSN,
//! refuses it when its last-sampled lag is above `max_lag_bytes` (or unknown,
//! unless `allow_unknown_lag`), runs the optional fence hook, promotes, and
//! verifies the target left recovery. Single-proxy only; it can never fence
//! an old primary that is unreachable from here except through `fence_command`.

use crate::backend::{
    tls::default_client_config, BackendClient, BackendConfig, TextValue, TlsMode,
};
use crate::config::{AutoPromoteConfig, NodeRole, PromoteMethod, TopologyConfig};
use serde::Serialize;
use std::time::Duration;

/// One configured node as the promoter sees it.
#[derive(Debug, Clone)]
pub struct PromoNode {
    pub host: String,
    pub port: u16,
    pub role: NodeRole,
    pub enabled: bool,
    /// Last health-sampled replication lag in bytes (`None` = unknown).
    pub lag_bytes: Option<u64>,
}

impl PromoNode {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Result of probing one node directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeProbe {
    Unreachable,
    /// `pg_is_in_recovery() = false`: a writable primary.
    Primary,
    /// In recovery; `replay_lsn` is 0 when the backend cannot report it.
    Standby {
        replay_lsn: u64,
    },
}

/// A standby eligible in principle for promotion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub index: usize,
    pub replay_lsn: u64,
    pub lag_bytes: Option<u64>,
}

/// Why an attempt did not promote.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum Refusal {
    /// A node answered as a writable primary; the topology will find it.
    PrimaryAlive(String),
    NoCandidate,
    LagTooHigh {
        node: String,
        lag_bytes: u64,
    },
    LagUnknown {
        node: String,
    },
    FenceFailed(String),
    PromoteFailed(String),
    VerifyFailed(String),
}

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Refusal::PrimaryAlive(a) => write!(f, "{a} answers as a writable primary"),
            Refusal::NoCandidate => write!(f, "no reachable standby to promote"),
            Refusal::LagTooHigh { node, lag_bytes } => {
                write!(f, "best standby {node} lags {lag_bytes} bytes (over max_lag_bytes)")
            }
            Refusal::LagUnknown { node } => write!(
                f,
                "best standby {node} has no lag sample (set [health] user + [lag_routing], or allow_unknown_lag)"
            ),
            Refusal::FenceFailed(e) => write!(f, "fence failed: {e}"),
            Refusal::PromoteFailed(e) => write!(f, "promote failed: {e}"),
            Refusal::VerifyFailed(e) => write!(f, "post-promote verification failed: {e}"),
        }
    }
}

/// Successful promotion.
#[derive(Debug, Clone)]
pub struct Promoted {
    pub target: String,
    pub fenced: bool,
}

/// Tracks how long the primary has been continuously absent.
#[derive(Debug, Default)]
pub struct OutageClock {
    since: Option<std::time::Instant>,
}

impl OutageClock {
    /// Feed one observation; returns how long the primary has been absent
    /// (`None` while present).
    pub fn observe(&mut self, now: std::time::Instant, primary_present: bool) -> Option<Duration> {
        if primary_present {
            self.since = None;
            return None;
        }
        let since = *self.since.get_or_insert(now);
        Some(now.saturating_duration_since(since))
    }
}

/// Choose the standby with the highest replay LSN (ties: configured order)
/// and apply the lag gate to it. Pure.
pub fn pick_candidate(
    nodes: &[PromoNode],
    cands: &[Candidate],
    max_lag_bytes: u64,
    allow_unknown_lag: bool,
) -> Result<Candidate, Refusal> {
    let best = cands
        .iter()
        .max_by(|a, b| a.replay_lsn.cmp(&b.replay_lsn).then(b.index.cmp(&a.index)))
        .ok_or(Refusal::NoCandidate)?;
    let node = nodes[best.index].address();
    match best.lag_bytes {
        Some(l) if l > max_lag_bytes => Err(Refusal::LagTooHigh { node, lag_bytes: l }),
        None if !allow_unknown_lag => Err(Refusal::LagUnknown { node }),
        _ => Ok(best.clone()),
    }
}

fn backend_cfg(topo: &TopologyConfig, host: &str, port: u16, timeout: Duration) -> BackendConfig {
    BackendConfig {
        host: host.to_string(),
        port,
        user: topo.user.clone(),
        password: topo.password.clone(),
        database: Some(topo.database.clone()),
        application_name: Some("heliosdb-proxy-promoter".into()),
        tls_mode: TlsMode::Prefer,
        connect_timeout: timeout,
        query_timeout: timeout,
        tls_config: default_client_config(),
    }
}

fn first_text(res: &crate::backend::QueryResult) -> Option<&str> {
    match res.rows.first().and_then(|r| r.first()) {
        Some(TextValue::Text(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Probe one node directly (not through the health snapshot).
pub async fn probe_node(topo: &TopologyConfig, node: &PromoNode, timeout: Duration) -> NodeProbe {
    let cfg = backend_cfg(topo, &node.host, node.port, timeout);
    let Ok(Ok(mut c)) = tokio::time::timeout(timeout, BackendClient::connect(&cfg)).await else {
        return NodeProbe::Unreachable;
    };
    let rec = tokio::time::timeout(timeout, c.simple_query("SELECT pg_is_in_recovery()::text"))
        .await
        .ok()
        .and_then(|r| r.ok());
    let Some(rec) = rec else {
        c.close().await;
        return NodeProbe::Unreachable;
    };
    let in_recovery = matches!(first_text(&rec), Some("true") | Some("t"));
    if !in_recovery {
        c.close().await;
        return NodeProbe::Primary;
    }
    let lsn = tokio::time::timeout(
        timeout,
        c.simple_query("SELECT pg_last_wal_replay_lsn()::text"),
    )
    .await
    .ok()
    .and_then(|r| r.ok())
    .and_then(|r| first_text(&r).and_then(crate::server::parse_pg_lsn))
    .unwrap_or(0);
    c.close().await;
    NodeProbe::Standby { replay_lsn: lsn }
}

/// Run a hook argv (no shell) with a bounded runtime and the promote env.
async fn run_hook(
    argv: &[String],
    target: &str,
    old_primary: &str,
    timeout: Duration,
) -> Result<(), String> {
    let mut cmd = tokio::process::Command::new(&argv[0]);
    cmd.args(&argv[1..])
        .env("HELIOS_PROMOTE_TARGET", target)
        .env("HELIOS_PROMOTE_OLD_PRIMARY", old_primary)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    let out = tokio::time::timeout(timeout, cmd.output())
        .await
        .map_err(|_| format!("`{}` timed out after {:?}", argv[0], timeout))?
        .map_err(|e| format!("`{}`: {e}", argv[0]))?;
    if out.status.success() {
        Ok(())
    } else {
        let tail = String::from_utf8_lossy(&out.stderr);
        Err(format!(
            "`{}` exited {}: {}",
            argv[0],
            out.status,
            tail.trim().chars().take(200).collect::<String>()
        ))
    }
}

async fn do_promote(
    cfg: &AutoPromoteConfig,
    topo: &TopologyConfig,
    target: &PromoNode,
    old_primary: &str,
) -> Result<(), String> {
    let timeout = Duration::from_secs(cfg.promote_timeout_secs);
    let addr = target.address();
    match cfg.method {
        PromoteMethod::PgPromote => {
            let bc = backend_cfg(topo, &target.host, target.port, timeout);
            let mut c = tokio::time::timeout(timeout, BackendClient::connect(&bc))
                .await
                .map_err(|_| "connect timed out".to_string())?
                .map_err(|e| format!("connect: {e}"))?;
            let sql = format!(
                "SELECT pg_promote(true, {})",
                cfg.promote_timeout_secs.max(1)
            );
            let r = tokio::time::timeout(timeout + Duration::from_secs(5), c.simple_query(&sql))
                .await
                .map_err(|_| "pg_promote timed out".to_string())?
                .map_err(|e| format!("pg_promote: {e}"));
            c.close().await;
            let r = r?;
            match first_text(&r) {
                Some("t") | Some("true") => Ok(()),
                other => Err(format!("pg_promote returned {other:?}")),
            }
        }
        PromoteMethod::Command => run_hook(&cfg.promote_command, &addr, old_primary, timeout).await,
        PromoteMethod::Http => {
            let url = cfg.promote_url.as_deref().unwrap_or_default();
            let client = reqwest::Client::builder()
                .timeout(timeout)
                .build()
                .map_err(|e| e.to_string())?;
            let resp = client
                .post(url)
                .json(&serde_json::json!({ "target": addr, "old_primary": old_primary }))
                .send()
                .await
                .map_err(|e| format!("POST {url}: {e}"))?;
            if resp.status().is_success() {
                Ok(())
            } else {
                Err(format!("POST {url}: HTTP {}", resp.status()))
            }
        }
    }
}

/// One full promotion attempt: probe, choose, fence, promote, verify.
pub async fn attempt(
    cfg: &AutoPromoteConfig,
    topo: &TopologyConfig,
    nodes: &[PromoNode],
    old_primary: &str,
) -> Result<Promoted, Refusal> {
    let probe_timeout = Duration::from_secs(cfg.promote_timeout_secs.clamp(1, 10));
    let mut cands = Vec::new();
    for (i, n) in nodes.iter().enumerate().filter(|(_, n)| n.enabled) {
        match probe_node(topo, n, probe_timeout).await {
            NodeProbe::Primary => return Err(Refusal::PrimaryAlive(n.address())),
            NodeProbe::Standby { replay_lsn } if n.role == NodeRole::Standby => {
                cands.push(Candidate {
                    index: i,
                    replay_lsn,
                    lag_bytes: n.lag_bytes,
                });
            }
            _ => {}
        }
    }
    let best = pick_candidate(nodes, &cands, cfg.max_lag_bytes, cfg.allow_unknown_lag)?;
    let target = &nodes[best.index];
    let addr = target.address();

    let fenced = !cfg.fence_command.is_empty();
    if fenced {
        run_hook(
            &cfg.fence_command,
            &addr,
            old_primary,
            Duration::from_secs(cfg.fence_timeout_secs),
        )
        .await
        .map_err(Refusal::FenceFailed)?;
    }

    do_promote(cfg, topo, target, old_primary)
        .await
        .map_err(Refusal::PromoteFailed)?;

    // Verify the target really left recovery (bounded by promote_timeout).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(cfg.promote_timeout_secs);
    loop {
        if probe_node(topo, target, probe_timeout).await == NodeProbe::Primary {
            return Ok(Promoted {
                target: addr,
                fenced,
            });
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Refusal::VerifyFailed(format!(
                "{addr} still in recovery after {}s",
                cfg.promote_timeout_secs
            )));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Live promoter state for `GET /api/promotion`.
#[derive(Debug, Clone, Serialize, Default)]
pub struct PromoterStatus {
    pub enabled: bool,
    /// `idle` | `primary_absent` | `promoting` | `cooldown` | `gave_up`
    pub state: String,
    pub primary_absent_secs: u64,
    pub attempts_this_outage: u32,
    pub promotions_total: u64,
    pub last_attempt_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_target: Option<String>,
    pub last_result: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(port: u16, role: NodeRole) -> PromoNode {
        PromoNode {
            host: "h".into(),
            port,
            role,
            enabled: true,
            lag_bytes: Some(0),
        }
    }

    fn cand(index: usize, lsn: u64, lag: Option<u64>) -> Candidate {
        Candidate {
            index,
            replay_lsn: lsn,
            lag_bytes: lag,
        }
    }

    #[test]
    fn picks_highest_lsn_ties_to_config_order() {
        let nodes = vec![
            node(1, NodeRole::Standby),
            node(2, NodeRole::Standby),
            node(3, NodeRole::Standby),
        ];
        let c = [
            cand(0, 10, Some(0)),
            cand(1, 50, Some(0)),
            cand(2, 50, Some(0)),
        ];
        assert_eq!(pick_candidate(&nodes, &c, 100, false).unwrap().index, 1);
    }

    #[test]
    fn refuses_when_best_lags_too_much_even_if_worse_is_ok() {
        let nodes = vec![node(1, NodeRole::Standby), node(2, NodeRole::Standby)];
        let c = [cand(0, 10, Some(5)), cand(1, 99, Some(500))];
        assert!(matches!(
            pick_candidate(&nodes, &c, 100, false),
            Err(Refusal::LagTooHigh { lag_bytes: 500, .. })
        ));
    }

    #[test]
    fn unknown_lag_refused_unless_allowed() {
        let nodes = vec![node(1, NodeRole::Standby)];
        let c = [cand(0, 1, None)];
        assert!(matches!(
            pick_candidate(&nodes, &c, 100, false),
            Err(Refusal::LagUnknown { .. })
        ));
        assert!(pick_candidate(&nodes, &c, 100, true).is_ok());
    }

    #[test]
    fn no_candidates_is_refused() {
        assert_eq!(pick_candidate(&[], &[], 1, true), Err(Refusal::NoCandidate));
    }

    #[test]
    fn outage_clock_resets_when_primary_returns() {
        let mut c = OutageClock::default();
        let t0 = std::time::Instant::now();
        assert_eq!(c.observe(t0, false), Some(Duration::ZERO));
        let t1 = t0 + Duration::from_secs(40);
        assert_eq!(c.observe(t1, false), Some(Duration::from_secs(40)));
        assert_eq!(c.observe(t1, true), None);
        let t2 = t1 + Duration::from_secs(1);
        assert_eq!(c.observe(t2, false), Some(Duration::ZERO));
    }

    #[tokio::test]
    async fn hook_success_failure_and_timeout() {
        let t = Duration::from_secs(5);
        assert!(run_hook(&["true".into()], "a:1", "", t).await.is_ok());
        assert!(run_hook(&["false".into()], "a:1", "", t).await.is_err());
        let slow = run_hook(
            &["sleep".into(), "5".into()],
            "a:1",
            "",
            Duration::from_millis(100),
        )
        .await;
        assert!(slow.unwrap_err().contains("timed out"));
    }

    #[tokio::test]
    async fn hook_receives_target_env() {
        let argv = vec![
            "sh".to_string(),
            "-c".to_string(),
            "test \"$HELIOS_PROMOTE_TARGET\" = 10.0.0.2:5432 && test \"$HELIOS_PROMOTE_OLD_PRIMARY\" = old:1"
                .to_string(),
        ];
        assert!(
            run_hook(&argv, "10.0.0.2:5432", "old:1", Duration::from_secs(5))
                .await
                .is_ok()
        );
    }

    #[test]
    fn config_validation() {
        use crate::config::{ProxyConfig, TopologyProviderKind};
        let mut c = ProxyConfig::default();
        let mk = |host: &str, port: u16, role: NodeRole| {
            let mut n = crate::config::NodeConfig::default();
            n.host = host.into();
            n.port = port;
            n.role = role;
            n.enabled = true;
            n
        };
        c.nodes = vec![mk("p", 1, NodeRole::Primary), mk("s", 2, NodeRole::Standby)];
        // disabled: always fine
        assert!(c.auto_promote.validate(&c).is_ok());
        c.auto_promote.enabled = true;
        // needs the postgres provider
        assert!(c.auto_promote.validate(&c).is_err());
        c.topology.provider = TopologyProviderKind::Postgres;
        assert!(c.auto_promote.validate(&c).is_ok());
        c.auto_promote.method = PromoteMethod::Command;
        assert!(c.auto_promote.validate(&c).is_err());
        c.auto_promote.promote_command = vec!["/bin/true".into()];
        assert!(c.auto_promote.validate(&c).is_ok());
        c.auto_promote.method = PromoteMethod::Http;
        assert!(c.auto_promote.validate(&c).is_err());
        c.auto_promote.promote_url = Some("https://x/promote".into());
        assert!(c.auto_promote.validate(&c).is_ok());
        c.auto_promote.down_after_secs = 0;
        assert!(c.auto_promote.validate(&c).is_err());
    }

    #[test]
    fn toml_section_parses_with_defaults_and_overrides() {
        let d: crate::config::ProxyConfig = toml::from_str("").unwrap_or_default();
        assert!(!d.auto_promote.enabled);
        assert_eq!(d.auto_promote.down_after_secs, 30);
        assert_eq!(d.auto_promote.cooldown_secs, 300);
        let a: AutoPromoteConfig = toml::from_str(
            r#"
enabled = true
down_after_secs = 12
method = "command"
promote_command = ["/usr/local/bin/promote", "--fast"]
fence_command = ["/usr/local/bin/fence"]
allow_unknown_lag = true
"#,
        )
        .unwrap();
        assert!(a.enabled && a.allow_unknown_lag);
        assert_eq!(a.down_after_secs, 12);
        assert_eq!(a.method, PromoteMethod::Command);
        assert_eq!(a.promote_command.len(), 2);
        assert_eq!(a.max_attempts, 3);
    }
}
