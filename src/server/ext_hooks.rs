//! Statement hooks on the extended protocol.
//!
//! A simple `Query` passes the pre-query and route plugin hooks, the
//! `[query_rewrite]` rules and the `[multi_tenancy]` row filter on its way to
//! a backend. The same statement sent as `Parse`/`Bind`/`Execute`, which is
//! how most drivers send every statement, passes them here:
//!
//! - every `Parse` runs the pre-query plugin hook, the route plugin hook,
//!   the rewrite rules and the tenant row filter on its SQL, in the simple
//!   path's order, before the proxy records anything of it. A rewrite
//!   replaces the SQL of the `Parse` the proxy registers and forwards, so
//!   routing, the caches, Transaction Replay and a later re-prepare all see
//!   the statement that runs; a refusal refuses the cycle
//!   ([`StmtGate::Refuse`]).
//! - a `Bind` that executes a statement Parsed in an earlier cycle runs the
//!   two plugin hooks again, on the client's text of that statement (not the
//!   text the rewrite rules or the row filter registered), for their refusal:
//!   a plugin that counts or limits executions sees each one, not only the
//!   `Parse` of a prepared statement. A pre-query plugin that now rewrites
//!   the statement differently than at its `Parse` refuses the execution
//!   with `0A000` (the statement must be prepared again): what runs on the
//!   backend is the earlier rewrite, and running it would ignore the plugin.
//! - a statement whose text is not valid UTF-8 (a client encoding such as
//!   LATIN1) cannot be read by any of these hooks: while one of them is
//!   active it is refused with `22021`, on both protocols, instead of passing
//!   them unread.
//!
//! A pre-query plugin's cached result answers an extended cycle too
//! ([`ProxyServer::plugin_cycle_reply`]) when the cycle is one statement the
//! answer can stand for: a `Bind` of the unnamed portal with no parameters
//! and text results, a `Describe` of that portal (the client learns the
//! plugin's columns from it), an `Execute` without a row limit and a `Sync`,
//! starting a cycle, outside a failed transaction. The statement is the one
//! the `Parse` of the cycle defines (held by the unnamed-`Parse` promotion
//! or not), a named statement prepared earlier, or the unnamed statement a
//! plugin answered before. The reply is `ParseComplete` (when the client
//! sent the `Parse`), `BindComplete`, the plugin's rows, `CommandComplete`
//! and `ReadyForQuery` with the session's status; no backend sees the cycle.
//! A plugin answer at a `Parse` has not seen parameter values, hence no
//! parameters. Any other shape runs on the backend as before. An unnamed
//! statement a plugin answered exists for the client only: a later cycle
//! that binds it without a `Parse` gives the backend that statement first
//! (`forward_extended_batch`, out of band), unless the plugin answers that
//! execution too.

use super::*;

/// SQLSTATE of a statement refused because no hook can read its text
/// (`character_not_in_repertoire`).
#[cfg(any(
    feature = "wasm-plugins",
    feature = "multi-tenancy",
    feature = "query-rewriting"
))]
pub(super) const UNREADABLE_SQLSTATE: &str = "22021";
/// The message of that refusal.
#[cfg(any(
    feature = "wasm-plugins",
    feature = "multi-tenancy",
    feature = "query-rewriting"
))]
pub(super) const UNREADABLE_STATEMENT: &str =
    "statement text is not valid UTF-8: this proxy applies statement hooks (plugins, query \
     rewrite rules or the tenant row filter) that read UTF-8 only; use client_encoding UTF8";
/// SQLSTATE of an execution refused because a plugin's rewrite of the
/// prepared statement changed since its `Parse` (PostgreSQL's code for a
/// prepared statement that must be prepared again).
#[cfg(feature = "wasm-plugins")]
const REPREPARE_SQLSTATE: &str = "0A000";

/// What the statement hooks decide for one extended-protocol message.
pub(super) enum StmtGate {
    /// Forward the client's message.
    Pass,
    /// A `Parse`: register and forward it with this SQL instead.
    Rewrite(String),
    /// Refuse the cycle with this ErrorResponse: nothing of it that the
    /// proxy still holds reaches a backend. A plugin, the tenant row filter,
    /// or any active hook for a statement it cannot read refuses.
    #[cfg(any(
        feature = "wasm-plugins",
        feature = "multi-tenancy",
        feature = "query-rewriting"
    ))]
    Refuse {
        sqlstate: &'static str,
        reason: String,
    },
}

/// What the plugin hooks saw of one prepared statement at its `Parse`, kept
/// to run them on the same text when a later cycle executes it.
#[cfg(feature = "wasm-plugins")]
struct StmtTexts {
    /// The client's statement text: what the pre-query hook reads.
    client: Box<str>,
    /// The text the pre-query plugin rewrote it to, when it did: what the
    /// route hook reads. `None`: the client's text.
    after_plugin: Option<Box<str>>,
}

#[cfg(feature = "wasm-plugins")]
impl StmtTexts {
    fn new(client: &str, after_plugin: Option<String>) -> Self {
        Self {
            client: client.into(),
            after_plugin: after_plugin.map(String::into_boxed_str),
        }
    }
}

/// The texts an execution of a prepared statement is checked on.
#[cfg(feature = "wasm-plugins")]
struct ExecTexts<'a> {
    client: &'a str,
    after_plugin: &'a str,
}

#[cfg(feature = "wasm-plugins")]
impl<'a> ExecTexts<'a> {
    fn of(texts: &'a StmtTexts) -> Self {
        Self {
            client: &texts.client,
            after_plugin: texts.after_plugin.as_deref().unwrap_or(&texts.client),
        }
    }

    fn same(sql: &'a str) -> Self {
        Self {
            client: sql,
            after_plugin: sql,
        }
    }
}

/// What [`ProxyServer::gate_parse`] decided for one `Parse`.
struct ParseVerdict {
    gate: StmtGate,
    route: RouteOverride,
    /// The pre-query plugin's rewrite of the client's text, if any.
    #[cfg(feature = "wasm-plugins")]
    plugin_rewrite: Option<String>,
    /// The pre-query plugin's cached result for the statement, if any.
    #[cfg(feature = "wasm-plugins")]
    cached: Option<Vec<u8>>,
}

/// What the two plugin hooks decided for a `Parse`'s statement.
#[cfg(feature = "wasm-plugins")]
struct PluginVerdict {
    /// The pre-query plugin's rewrite.
    rewritten: Option<String>,
    /// The route plugin's override.
    route: RouteOverride,
    /// The pre-query plugin's cached result.
    cached: Option<Vec<u8>>,
}

#[cfg(feature = "wasm-plugins")]
impl Default for PluginVerdict {
    fn default() -> Self {
        Self {
            rewritten: None,
            route: RouteOverride::None,
            cached: None,
        }
    }
}

/// A plugin's answer to an extended cycle as the client loop keeps it: the
/// whole reply, and the unnamed `Parse` the client sent with the cycle.
pub(super) type PluginCycleAnswer = (Vec<u8>, Option<(bytes::Bytes, bytes::Bytes)>);

/// How a batch routes: `(is_write, forced node, standby hint)`.
pub(super) type BatchRoute = (bool, Option<String>, bool);

/// An extended cycle a plugin's cached result answered
/// ([`ProxyServer::plugin_cycle_reply`]).
#[cfg(feature = "wasm-plugins")]
pub(super) struct PluginCycleReply {
    /// The whole reply, `ReadyForQuery` included.
    pub(super) reply: Vec<u8>,
    /// The unnamed `Parse` the client sent with the cycle (and its
    /// signature): the client's unnamed statement is now one no backend
    /// holds. `None` when the cycle executed a statement prepared before.
    pub(super) parsed_unnamed: Option<(bytes::Bytes, bytes::Bytes)>,
}

/// The session's hook state between the messages of an extended cycle.
pub(super) struct ExtHooks {
    /// The route plugin's answer for the first `Parse` of the open cycle,
    /// which the cycle is routed by (`pending_route_sql`).
    pub(super) route: RouteOverride,
    /// The open cycle Parsed the unnamed statement: its hooks ran then.
    parsed_unnamed: bool,
    /// The texts of the unnamed statement as last Parsed, for a `Bind` of it
    /// in a later cycle (also when a plugin is loaded only after the Parse).
    #[cfg(feature = "wasm-plugins")]
    unnamed: Option<StmtTexts>,
    /// Named statements whose registered text is not the client's: the
    /// registered text, and what the plugin hooks saw at the `Parse`.
    #[cfg(feature = "wasm-plugins")]
    originals: HashMap<String, (Box<str>, StmtTexts)>,
    /// The pre-query plugin's cached result for a statement of the open
    /// cycle (at its `Parse`, or at the `Bind` executing it): what
    /// [`ProxyServer::plugin_cycle_reply`] may answer the cycle with.
    #[cfg(feature = "wasm-plugins")]
    cached: Option<Vec<u8>>,
}

impl ExtHooks {
    pub(super) fn new() -> Self {
        Self {
            route: RouteOverride::None,
            parsed_unnamed: false,
            #[cfg(feature = "wasm-plugins")]
            unnamed: None,
            #[cfg(feature = "wasm-plugins")]
            originals: HashMap::new(),
            #[cfg(feature = "wasm-plugins")]
            cached: None,
        }
    }

    /// The pre-query plugin's cached result for the open cycle's statement.
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn cached(&self) -> Option<&[u8]> {
        self.cached.as_deref()
    }

    /// Whether the route plugin answered for a statement of the open cycle
    /// (its answer, [`ExtHooks::route`], routes the cycle).
    pub(super) fn routes(&self) -> bool {
        !matches!(self.route, RouteOverride::None)
    }

    /// The cycle ended (its `Sync` was answered).
    pub(super) fn end_cycle(&mut self) {
        self.route = RouteOverride::None;
        self.parsed_unnamed = false;
        #[cfg(feature = "wasm-plugins")]
        {
            self.cached = None;
        }
    }

    /// The named statement `name` was deallocated.
    pub(super) fn forget(&mut self, name: &str) {
        #[cfg(feature = "wasm-plugins")]
        if !self.originals.is_empty() {
            self.originals.remove(name);
        }
        let _ = name;
    }

    /// Run the hooks on `msg`. `defines` and `registry` are the named
    /// statements the open cycle Parsed and the session's registered ones.
    pub(super) async fn on_message(
        &mut self,
        msg: &Message,
        defines: &[String],
        registry: &HashMap<String, bytes::Bytes>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> StmtGate {
        match msg.msg_type {
            MessageType::Parse => {
                // A malformed Parse is the backend's to answer.
                let Some((name, text)) = ProxyServer::parse_name_and_text(&msg.payload) else {
                    return StmtGate::Pass;
                };
                let Ok(sql) = std::str::from_utf8(text) else {
                    return ProxyServer::unreadable_statement(state);
                };
                let mut verdict = ProxyServer::gate_parse(sql, session, state).await;
                #[cfg(any(
                    feature = "wasm-plugins",
                    feature = "multi-tenancy",
                    feature = "query-rewriting"
                ))]
                if matches!(verdict.gate, StmtGate::Refuse { .. }) {
                    return verdict.gate;
                }
                // Every statement of the cycle has its say in where the cycle
                // runs, not only the first one.
                let route = std::mem::replace(&mut verdict.route, RouteOverride::None);
                #[cfg(feature = "wasm-plugins")]
                if let Err(conflict) = self.combine_route(route) {
                    return conflict;
                }
                #[cfg(not(feature = "wasm-plugins"))]
                let _ = route;
                if name.is_empty() {
                    self.parsed_unnamed = true;
                }
                #[cfg(feature = "wasm-plugins")]
                {
                    self.cached = verdict.cached.take();
                    self.remember_texts(name, sql, &verdict, state);
                }
                verdict.gate
            }
            #[cfg(feature = "wasm-plugins")]
            MessageType::Bind if ProxyServer::execution_hooks_on(state) => {
                let texts = match ProxyServer::bind_stmt_ref(&msg.payload) {
                    // Parsed in this cycle: its hooks ran at the Parse.
                    Some(name) if defines.iter().any(|d| d == name) => return StmtGate::Pass,
                    Some(name) => {
                        // No statement of that name: the backend answers 26000.
                        let Some(parse) = registry.get(name) else {
                            return StmtGate::Pass;
                        };
                        let Some(registered) = ProxyServer::parse_msg_sql(parse) else {
                            return ProxyServer::unreadable_statement(state);
                        };
                        match self.originals.get(name) {
                            Some((text, texts)) if &**text == registered => ExecTexts::of(texts),
                            _ => ExecTexts::same(registered),
                        }
                    }
                    None if self.parsed_unnamed => return StmtGate::Pass,
                    None => match &self.unnamed {
                        Some(texts) => ExecTexts::of(texts),
                        // Nothing Parsed: the backend answers the Bind.
                        None => return StmtGate::Pass,
                    },
                };
                match ProxyServer::gate_execution(&texts, session, state) {
                    // The route plugin's answer for this execution routes the
                    // cycle with the others'.
                    Ok((route, cached)) => {
                        self.cached = cached;
                        match self.combine_route(route) {
                            Ok(()) => StmtGate::Pass,
                            Err(conflict) => conflict,
                        }
                    }
                    Err(refusal) => refusal,
                }
            }
            _ => {
                let _ = (defines, registry);
                StmtGate::Pass
            }
        }
    }

    /// Fold one statement's route plugin answer into the cycle's: a cycle
    /// runs on one connection, so the answers must agree. `primary` wins over
    /// `standby`; a `node` wins over `standby` and must be the same node as
    /// any other `node`; a `node` next to `primary` (or another node) cannot
    /// be honoured and refuses the cycle (42000) rather than run a statement
    /// where its plugin did not send it.
    #[cfg(feature = "wasm-plugins")]
    fn combine_route(&mut self, next: RouteOverride) -> std::result::Result<(), StmtGate> {
        use RouteOverride as R;
        let combined = match (std::mem::replace(&mut self.route, R::None), next) {
            (R::None, next) => next,
            (current, R::None) => current,
            (R::Primary, R::Primary | R::Standby) | (R::Standby, R::Primary) => R::Primary,
            (R::Standby, R::Standby) => R::Standby,
            (R::Node(a), R::Node(b)) if a == b => R::Node(a),
            (R::Node(node), R::Standby) | (R::Standby, R::Node(node)) => R::Node(node),
            (R::Branch(a), R::Branch(b)) if a == b => R::Branch(a),
            (current, next) => {
                tracing::info!(
                    first = ?current,
                    then = ?next,
                    "route plugin sent statements of one extended cycle to different targets"
                );
                return Err(StmtGate::Refuse {
                    sqlstate: "42000",
                    reason: format!(
                        "Query blocked by route plugin: the statements of one extended-protocol \
                         cycle were routed to different targets ({current:?}, then {next:?}); \
                         send them in separate cycles"
                    ),
                });
            }
        };
        self.route = combined;
        Ok(())
    }

    /// Keep what the plugin hooks saw of the statement a `Parse` that passed
    /// them defines: always for the unnamed statement (the proxy holds no
    /// other copy of its client text), for a named one only when the text
    /// registered for it is not the client's (else the registry has it).
    #[cfg(feature = "wasm-plugins")]
    fn remember_texts(
        &mut self,
        name: &[u8],
        sql: &str,
        verdict: &ParseVerdict,
        state: &ServerState,
    ) {
        if name.is_empty() {
            self.unnamed = Some(StmtTexts::new(sql, verdict.plugin_rewrite.clone()));
            return;
        }
        let name = String::from_utf8_lossy(name);
        match &verdict.gate {
            StmtGate::Rewrite(registered) => {
                // Bounded with the registry it shadows.
                if self.originals.len() < state.limits.max_prepared_statements
                    || self.originals.contains_key(name.as_ref())
                {
                    self.originals.insert(
                        name.into_owned(),
                        (
                            registered.as_str().into(),
                            StmtTexts::new(sql, verdict.plugin_rewrite.clone()),
                        ),
                    );
                }
            }
            _ => {
                if !self.originals.is_empty() {
                    self.originals.remove(name.as_ref());
                }
            }
        }
    }
}

impl ProxyServer {
    /// Whether any statement hook is configured, so the extended path runs
    /// [`ExtHooks::on_message`] at all. Decided once per session: the plugin
    /// manager, the rewriter and the tenant manager exist from startup or
    /// never.
    pub(super) fn extended_hooks_on(state: &ServerState) -> bool {
        let on = false;
        #[cfg(feature = "wasm-plugins")]
        let on = on || state.plugin_manager.is_some();
        #[cfg(feature = "query-rewriting")]
        let on = on || state.rewriter.is_some();
        #[cfg(feature = "multi-tenancy")]
        let on = on || state.tenant_manager.is_some();
        let _ = state;
        on
    }

    /// Whether a plugin hook that decides per execution (pre-query, route)
    /// is loaded now.
    #[cfg(any(
        feature = "wasm-plugins",
        feature = "multi-tenancy",
        feature = "query-rewriting"
    ))]
    fn execution_hooks_on(state: &ServerState) -> bool {
        #[cfg(feature = "wasm-plugins")]
        if let Some(pm) = state.plugin_manager.as_ref() {
            return pm.has_hook(HookType::PreQuery) || pm.has_hook(HookType::Route);
        }
        let _ = state;
        false
    }

    /// Whether a hook that reads statement text is active now: a pre-query
    /// or route plugin, the query rewrite rules, the tenant row filter. A
    /// statement none of them can read (not UTF-8) is refused while one is.
    #[cfg(any(
        feature = "wasm-plugins",
        feature = "multi-tenancy",
        feature = "query-rewriting"
    ))]
    pub(super) fn statement_hooks_active(state: &ServerState) -> bool {
        #[cfg(feature = "query-rewriting")]
        if state.rewriter.is_some() {
            return true;
        }
        #[cfg(feature = "multi-tenancy")]
        if state.tenant_manager.is_some() {
            return true;
        }
        Self::execution_hooks_on(state)
    }

    /// The verdict on a statement whose text is not UTF-8: refused while a
    /// hook that reads statement text is active, else forwarded as before.
    fn unreadable_statement(state: &ServerState) -> StmtGate {
        #[cfg(any(
            feature = "wasm-plugins",
            feature = "multi-tenancy",
            feature = "query-rewriting"
        ))]
        if Self::statement_hooks_active(state) {
            tracing::info!("statement text is not UTF-8 while statement hooks are active; refused");
            return StmtGate::Refuse {
                sqlstate: UNREADABLE_SQLSTATE,
                reason: UNREADABLE_STATEMENT.to_string(),
            };
        }
        let _ = state;
        StmtGate::Pass
    }

    /// The statement name and text of a `Parse` payload, `None` when either
    /// C string is unterminated.
    pub(super) fn parse_name_and_text(payload: &[u8]) -> Option<(&[u8], &[u8])> {
        let name_end = memchr::memchr(0, payload)?;
        let rest = &payload[name_end + 1..];
        let text_end = memchr::memchr(0, rest)?;
        Some((&payload[..name_end], &rest[..text_end]))
    }

    /// The `Parse` message `msg` with `sql` as its statement text: same
    /// name, same declared parameter types.
    pub(super) fn parse_with_sql(msg: &Message, sql: &str) -> Message {
        let name_end = msg.payload.iter().position(|&b| b == 0).unwrap_or(0);
        let rest = &msg.payload[(name_end + 1).min(msg.payload.len())..];
        let params = rest
            .iter()
            .position(|&b| b == 0)
            .map_or(&[][..], |end| &rest[end + 1..]);
        let mut payload = BytesMut::with_capacity(name_end + 1 + sql.len() + 1 + params.len());
        payload.extend_from_slice(&msg.payload[..name_end]);
        payload.put_u8(0);
        payload.extend_from_slice(sql.as_bytes());
        payload.put_u8(0);
        payload.extend_from_slice(params);
        Message::new(MessageType::Parse, payload)
    }

    /// The hooks of a statement entering through a `Parse`, in the simple
    /// path's order: pre-query plugin, route plugin, rewrite rules, tenant
    /// row filter. Also returns the route plugin's override and the
    /// pre-query plugin's rewrite.
    async fn gate_parse(
        sql: &str,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> ParseVerdict {
        let text = std::borrow::Cow::Borrowed(sql);

        #[cfg(feature = "wasm-plugins")]
        let (text, route, plugin_rewrite, cached) = match Self::gate_plugins(&text, session, state)
        {
            Ok(PluginVerdict {
                rewritten: Some(rewritten),
                route,
                cached,
            }) => (
                std::borrow::Cow::Owned(rewritten.clone()),
                route,
                Some(rewritten),
                cached,
            ),
            Ok(PluginVerdict {
                rewritten: None,
                route,
                cached,
            }) => (text, route, None, cached),
            Err(refusal) => {
                return ParseVerdict {
                    gate: refusal,
                    route: RouteOverride::None,
                    plugin_rewrite: None,
                    cached: None,
                }
            }
        };
        #[cfg(not(feature = "wasm-plugins"))]
        let route = RouteOverride::None;

        #[cfg(feature = "query-rewriting")]
        let text = {
            let rewritten = state.rewriter.as_ref().and_then(|rw| match rw.rewrite(&text) {
                Ok(res) if res.was_rewritten() => {
                    tracing::debug!(target: "helios::rewrite", rules = ?res.rules_applied, "extended statement rewritten");
                    Some(res.query().to_string())
                }
                _ => None,
            });
            rewritten.map_or(text, std::borrow::Cow::Owned)
        };

        #[cfg(feature = "multi-tenancy")]
        let text = match state.tenant_manager.as_ref() {
            Some(tm) => {
                let verdict = Self::tenant_statement(tm, &text, session).await;
                match verdict {
                    TenantStatement::Refused(reason) => {
                        tracing::info!(target: "helios::tenant", reason = %reason, "tenant statement refused");
                        return ParseVerdict {
                            gate: StmtGate::Refuse {
                                sqlstate: crate::multi_tenancy::transformer::REFUSAL_SQLSTATE,
                                reason: reason.into_owned(),
                            },
                            route: RouteOverride::None,
                            #[cfg(feature = "wasm-plugins")]
                            plugin_rewrite: None,
                            #[cfg(feature = "wasm-plugins")]
                            cached: None,
                        };
                    }
                    TenantStatement::Filtered(filtered) => std::borrow::Cow::Owned(filtered),
                    TenantStatement::Unchanged => text,
                }
            }
            None => text,
        };

        let _ = (session, state);
        let gate = match text {
            std::borrow::Cow::Borrowed(_) => StmtGate::Pass,
            std::borrow::Cow::Owned(rewritten) => StmtGate::Rewrite(rewritten),
        };
        // A cached result answers the statement the client sent; one the
        // rewrite rules or the row filter changed is not that statement.
        #[cfg(feature = "wasm-plugins")]
        let cached = cached.filter(|_| matches!(gate, StmtGate::Pass));
        ParseVerdict {
            gate,
            route,
            #[cfg(feature = "wasm-plugins")]
            plugin_rewrite,
            #[cfg(feature = "wasm-plugins")]
            cached,
        }
    }

    /// The two plugin hooks on a `Parse`'s SQL: the pre-query hook (its
    /// rewrite or cached result is returned, its Block refuses), then the
    /// route hook on the statement the first one left.
    #[cfg(feature = "wasm-plugins")]
    fn gate_plugins(
        sql: &str,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> std::result::Result<PluginVerdict, StmtGate> {
        let Some(pm) = state.plugin_manager.as_ref() else {
            return Ok(PluginVerdict::default());
        };
        let mut rewritten = None;
        let mut cached = None;
        if pm.has_hook(HookType::PreQuery) {
            match pm.execute_pre_query(&Self::build_query_context(sql, session)) {
                PreQueryResult::Block(reason) => {
                    return Err(Self::plugin_refusal("plugin", &reason))
                }
                PreQueryResult::Rewrite(new_sql) => rewritten = Some(new_sql),
                // See the module comment for a cached result.
                PreQueryResult::Cached(bytes) => cached = Some(bytes),
                PreQueryResult::Continue => {}
            }
        }
        let route = if pm.has_hook(HookType::Route) {
            let ctx = Self::build_query_context(rewritten.as_deref().unwrap_or(sql), session);
            match Self::route_result_override(pm.execute_route(&ctx)) {
                RouteOverride::Block(reason) => {
                    return Err(Self::plugin_refusal("route plugin", &reason))
                }
                route => route,
            }
        } else {
            RouteOverride::None
        };
        Ok(PluginVerdict {
            rewritten,
            route,
            cached,
        })
    }

    /// The two plugin hooks on a prepared statement a `Bind` executes again,
    /// each on the text it read at the `Parse`: a Block refuses; a pre-query
    /// rewrite other than the one the statement was prepared with refuses
    /// with `0A000` (the backend holds the earlier one). Returns the route
    /// plugin's answer, which routes the cycle with its other statements',
    /// and the pre-query plugin's cached result for this execution.
    #[cfg(feature = "wasm-plugins")]
    fn gate_execution(
        texts: &ExecTexts<'_>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> std::result::Result<(RouteOverride, Option<Vec<u8>>), StmtGate> {
        let mut route = RouteOverride::None;
        let mut cached = None;
        if let Some(pm) = state.plugin_manager.as_ref() {
            if pm.has_hook(HookType::PreQuery) {
                match pm.execute_pre_query(&Self::build_query_context(texts.client, session)) {
                    PreQueryResult::Block(reason) => {
                        return Err(Self::plugin_refusal("plugin", &reason))
                    }
                    // A rewrite the backend does not hold: running the
                    // statement as prepared would ignore it. (A plugin that
                    // no longer rewrites it lets it run as prepared.)
                    PreQueryResult::Rewrite(sql) if sql != texts.after_plugin => {
                        return Err(Self::plugin_rewrite_changed())
                    }
                    PreQueryResult::Cached(bytes) => cached = Some(bytes),
                    _ => {}
                }
            }
            if pm.has_hook(HookType::Route) {
                let ctx = Self::build_query_context(texts.after_plugin, session);
                match Self::route_result_override(pm.execute_route(&ctx)) {
                    RouteOverride::Block(reason) => {
                        return Err(Self::plugin_refusal("route plugin", &reason))
                    }
                    answer => route = answer,
                }
            }
        }
        Ok((route, cached))
    }

    /// A pre-query plugin's cached result (`answer`) as the whole reply to
    /// the extended cycle `batch` (its frames up to and including the
    /// `Sync`), when the cycle is one execution the answer can stand for
    /// (see the module comment). `held`: the unnamed `Parse` the promotion
    /// keeps ahead of the batch; `registry`: the session's named statements;
    /// `status`: the session's transaction status, reported unchanged.
    /// `None`: the cycle runs on the backend. (A `Bind` of an unnamed
    /// statement Parsed in an earlier cycle got its answer only if the hooks
    /// knew that statement, at its `Parse`.)
    #[cfg(feature = "wasm-plugins")]
    pub(super) fn plugin_cycle_reply(
        answer: &[u8],
        batch: &[u8],
        held: Option<&(bytes::Bytes, bytes::Bytes)>,
        registry: &HashMap<String, bytes::Bytes>,
        status: u8,
    ) -> Option<PluginCycleReply> {
        use super::frames::{next_frontend_frame as frame, split_cstring as cstring};
        // A failed transaction answers every statement with 25P02.
        if status == b'E' {
            return None;
        }
        let mut rest = batch;
        let mut next = frame(&mut rest)?;
        // An unnamed Parse inside the batch (the promotion is off or did not
        // hold it).
        let mut in_batch: Option<(bytes::Bytes, bytes::Bytes)> = None;
        if next.0 == b'P' {
            let (name, _) = cstring(next.1)?;
            if !name.is_empty() || held.is_some() {
                return None;
            }
            let frame_len = 5 + next.1.len();
            in_batch = Some((
                bytes::Bytes::copy_from_slice(&batch[..frame_len]),
                bytes::Bytes::copy_from_slice(&next.1[1..]),
            ));
            next = frame(&mut rest)?;
        }
        let (b'B', bind) = next else {
            return None;
        };
        let (portal, bind) = cstring(bind)?;
        let (statement, tail) = cstring(bind)?;
        if !portal.is_empty() {
            return None;
        }
        let parsed = in_batch.or_else(|| held.cloned());
        if !statement.is_empty()
            && (parsed.is_some() || !registry.contains_key(std::str::from_utf8(statement).ok()?))
        {
            // Two statements, or one the session never prepared.
            return None;
        }
        // The Bind after its names: parameter format codes, no parameter
        // values (a Parse-time answer saw none), text results only.
        let count = |b: &[u8]| -> Option<usize> {
            let n = i16::from_be_bytes([*b.first()?, *b.get(1)?]);
            usize::try_from(n).ok()
        };
        let formats = count(tail)?;
        let tail = tail.get(2 + 2 * formats..)?;
        if count(tail)? != 0 {
            return None;
        }
        let tail = tail.get(2..)?;
        let results = count(tail)?;
        let codes = tail.get(2..)?;
        if codes.len() != 2 * results || codes.iter().any(|b| *b != 0) {
            return None;
        }
        // The portal described (the plugin's columns reach the client), run
        // to completion, and the cycle ended.
        if frame(&mut rest)? != (b'D', &b"P\0"[..])
            || frame(&mut rest)? != (b'E', &[0, 0, 0, 0, 0][..])
            || frame(&mut rest)? != (b'S', &[][..])
            || !rest.is_empty()
        {
            return None;
        }
        let rows = match Self::synthesise_cached_response(answer, status) {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "plugin cached result unusable; the extended cycle runs on the backend");
                return None;
            }
        };
        let mut reply = Vec::with_capacity(10 + rows.len());
        if parsed.is_some() {
            reply.extend_from_slice(&[b'1', 0, 0, 0, 4]);
        }
        reply.extend_from_slice(&[b'2', 0, 0, 0, 4]);
        reply.extend_from_slice(&rows);
        Some(PluginCycleReply {
            reply,
            parsed_unnamed: parsed,
        })
    }

    /// The refusal of an execution whose prepared text a plugin no longer
    /// rewrites the same way.
    #[cfg(feature = "wasm-plugins")]
    fn plugin_rewrite_changed() -> StmtGate {
        tracing::info!(
            "a pre-query plugin rewrites a prepared statement differently than at its Parse; \
             execution refused"
        );
        StmtGate::Refuse {
            sqlstate: REPREPARE_SQLSTATE,
            reason: "prepared statement must be prepared again: a plugin now rewrites it \
                     differently than when it was prepared"
                .to_string(),
        }
    }

    /// A plugin's Block as the refusal the simple path answers with.
    #[cfg(feature = "wasm-plugins")]
    fn plugin_refusal(by: &str, reason: &str) -> StmtGate {
        tracing::info!(reason = %reason, "{by} blocked an extended-protocol statement");
        StmtGate::Refuse {
            sqlstate: "42000",
            reason: format!("Query blocked by {by}: {reason}"),
        }
    }

    /// How an extended batch that routes by `sql` is routed: `(is_write,
    /// forced node, standby hint)`. A routing hint in the SQL wins over the
    /// route plugin's override (`plugin`), as on the simple path; the write
    /// flag is taken from the hint-stripped SQL.
    ///
    /// Also returns the batch's branch request: a `branch=` hint, else the
    /// route plugin's `branch` answer (`switch_branch`).
    pub(super) fn extended_route(
        state: &Arc<ServerState>,
        sql: &str,
        plugin: &RouteOverride,
    ) -> (BatchRoute, Option<String>) {
        #[cfg(feature = "routing-hints")]
        if let Some((route, hinted)) = Self::extended_hint_route(state, sql, plugin) {
            return (route, Self::branch_request(hinted, plugin));
        }
        let _ = state;
        (
            Self::route_by_override(plugin, Self::is_write_query(sql)),
            Self::plugin_branch(plugin),
        )
    }

    /// The route plugin's `branch` answer, if that is what `plugin` is.
    pub(super) fn plugin_branch(plugin: &RouteOverride) -> Option<String> {
        match plugin {
            #[cfg(feature = "wasm-plugins")]
            RouteOverride::Branch(name) => Some(name.clone()),
            _ => None,
        }
    }

    /// `(is_write, forced node, standby hint)` of a statement classified
    /// `is_write` under `route`.
    pub(super) fn route_by_override(
        route: &RouteOverride,
        is_write: bool,
    ) -> (bool, Option<String>, bool) {
        match route {
            RouteOverride::None => (is_write, None, false),
            #[cfg(any(feature = "wasm-plugins", feature = "routing-hints"))]
            RouteOverride::Primary => (true, None, false),
            // A standby override never moves a write or transaction control.
            #[cfg(any(feature = "wasm-plugins", feature = "routing-hints"))]
            RouteOverride::Standby => (false, None, !is_write),
            #[cfg(any(feature = "wasm-plugins", feature = "routing-hints"))]
            RouteOverride::Node(node) => (is_write, Some(node.clone()), false),
            // Answered at the Parse: a blocked statement routes nowhere.
            #[cfg(feature = "wasm-plugins")]
            RouteOverride::Block(_) => (is_write, None, false),
            // The session moves to the branch; the statement routes there as
            // any other.
            #[cfg(feature = "wasm-plugins")]
            RouteOverride::Branch(_) => (is_write, None, false),
        }
    }
}
