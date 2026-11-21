//! Branch routing: a `/*helios:branch=<name>*/` routing hint, or a route
//! plugin's `branch` answer, moves the session to a branch database before
//! its statement runs (`[branch] route_enabled`).
//!
//! A branch is a database: on PostgreSQL one of the branch's name (what
//! `/api/branch` provisions with `CREATE DATABASE ... TEMPLATE`), on
//! HeliosDB-Nano `<base>@<name>` (`[branch] route_mode`). Moving is changing
//! the session's startup `database` and giving it new backend connections,
//! so it happens only where nothing of the session lives on its current
//! connections:
//!
//! - at a request boundary, outside a transaction block, with no extended
//!   cycle open (25001 otherwise);
//! - with none of the state a connection to another database would lack:
//!   temporary objects, cursors `WITH HOLD`, SQL `PREPARE`d statements,
//!   `LISTEN`, session advisory locks or session settings (0A000; see
//!   `session_bound`). Protocol-level prepared statements are fine: the
//!   proxy prepares them again on the new connection when they are used.
//!
//! The session then stays on the branch: later statements run there until
//! another branch request (`branch=` with no name returns to the database
//! the session started on). The `[[hba]]` rules are checked for the new
//! database (28000). The session's per-connection result cache is dropped;
//! the shared cache keys on the database. Each move is logged.

use super::*;

/// Why a branch request was refused: SQLSTATE and message.
pub(super) type BranchRefusal = (&'static str, String);

impl ProxyServer {
    /// The branch a request asks its session to move to: a `branch=`
    /// routing hint (`hinted`) wins over the route plugin's `branch` answer.
    #[cfg(feature = "routing-hints")]
    pub(super) fn branch_request(hinted: Option<String>, plugin: &RouteOverride) -> Option<String> {
        hinted.or_else(|| Self::plugin_branch(plugin))
    }

    /// The database branch `name` of a session that started on `home`
    /// (`""`: `home` itself).
    pub(super) fn branch_database(
        name: &str,
        home: &str,
        config: &crate::config::BranchConfig,
    ) -> std::result::Result<String, BranchRefusal> {
        if name.is_empty() {
            return Ok(home.to_string());
        }
        if !crate::branch::valid_ident(name) {
            return Err((
                "42602",
                format!(
                    "invalid branch name {name:?}: letters, digits and '_', not starting with \
                     a digit, at most 63"
                ),
            ));
        }
        if !config.route_allow.is_empty() && !config.route_allow.iter().any(|b| b == name) {
            return Err((
                "42501",
                format!("branch {name:?} is not one this proxy routes to ([branch] route_allow)"),
            ));
        }
        Ok(match config.route_mode {
            crate::config::BranchRouteMode::Database => name.to_string(),
            crate::config::BranchRouteMode::Nano => {
                let base = home.split_once('@').map_or(home, |(base, _)| base);
                format!("{base}@{name}")
            }
        })
    }

    /// Move the session to branch `name` (see the module comment) before a
    /// request runs. `at_boundary`: the request starts a cycle (no extended
    /// cycle or COPY open). A request for the database the session is on
    /// already is no move. `Err`: the refusal to answer the request with;
    /// nothing changed.
    pub(super) async fn switch_branch(
        name: &str,
        at_boundary: bool,
        conns: &mut ConnMap,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> std::result::Result<(), BranchRefusal> {
        let branches = &config.branch;
        if !branches.route_enabled {
            return Err((
                "42000",
                format!(
                    "this statement asks for branch {name:?}, but branch routing is off \
                     ([branch] route_enabled); it was not run"
                ),
            ));
        }
        let (current, user) = {
            let vars = session.variables.read().await;
            (
                vars.get("database").cloned().unwrap_or_default(),
                vars.get("user").cloned().unwrap_or_default(),
            )
        };
        let home = session
            .branch_home
            .get()
            .cloned()
            .unwrap_or_else(|| current.clone());
        let target = Self::branch_database(name, &home, branches)?;
        if target == current {
            return Ok(());
        }
        if !at_boundary
            || session.in_transaction.load(Ordering::Relaxed)
            || session.last_rfq_status.load(Ordering::Relaxed) != b'I'
        {
            return Err((
                "25001",
                format!(
                    "cannot move the session to branch {name:?} inside a transaction block or \
                     an open extended-protocol cycle"
                ),
            ));
        }
        let blockers = session.branch_blockers.load(Ordering::Relaxed);
        if blockers != 0 {
            return Err((
                "0A000",
                format!(
                    "cannot move the session to branch {name:?}: it holds {}, which a \
                     connection to another database would not have (DISCARD ALL first, or \
                     move before creating them)",
                    super::session_bound::describe(blockers)
                ),
            ));
        }
        if !Self::hba_admits(&config.hba, session.client_addr.ip(), &user, &target) {
            return Err((
                "28000",
                format!("no [[hba]] rule admits user {user:?} to database {target:?}"),
            ));
        }
        // The move: the next connection is dialed for the new database.
        session.branch_home.get_or_init(|| current.clone());
        session
            .variables
            .write()
            .await
            .insert("database".to_string(), target.clone());
        let nodes: Vec<String> = conns.keys().cloned().collect();
        for node in nodes {
            Self::abandon_conn(conns, &node, session, state).await;
        }
        // The per-connection result cache keys on the statement text alone.
        #[cfg(feature = "query-cache")]
        if let Some(cache) = state.query_cache.as_ref() {
            cache.remove_l1_cache(session.id.as_u64_pair().0);
            *session
                .cache_effective_namespace
                .lock()
                .expect("cache namespace mutex poisoned") = None;
        }
        state.metrics.branch_moves.fetch_add(1, Ordering::Relaxed);
        tracing::info!(
            session = %session.session_id_str,
            from = %current,
            to = %target,
            "session moved to another branch"
        );
        Ok(())
    }

    /// The reply refusing a request's branch move: the error, then
    /// ReadyForQuery with the session's status when the request ends with
    /// one (`wait_ready`).
    pub(super) fn branch_refusal_reply(
        refusal: &BranchRefusal,
        session: &ClientSession,
        wait_ready: bool,
    ) -> Vec<u8> {
        tracing::info!(sqlstate = refusal.0, reason = %refusal.1, "branch move refused");
        let mut reply = Self::create_error_response(refusal.0, &refusal.1);
        if wait_ready {
            reply.extend_from_slice(&Self::create_ready_for_query(
                session.last_rfq_status.load(Ordering::Relaxed),
            ));
        }
        reply
    }
}
