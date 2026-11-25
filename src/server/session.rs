use super::*;

/// Client session
pub struct ClientSession {
    /// Session ID
    pub id: Uuid,
    /// Client address
    pub client_addr: SocketAddr,
    /// `client_addr.ip()` rendered once at session creation. Formatting an
    /// `IpAddr` allocates; the analytics path needed it on EVERY query.
    pub client_ip_str: String,
    /// `id` rendered once at session creation. Formatting a `Uuid` allocates
    /// and hex-encodes 16 bytes; same reason as `client_ip_str`.
    pub session_id_str: String,
    /// Current backend node
    pub current_node: RwLock<Option<String>>,
    /// Fast, lock-free "in a transaction" flag — the single per-query hot-path
    /// read/write of transaction state. Written from the ReadyForQuery status
    /// byte at each response boundary; read by pool-release, read-node
    /// selection, and cache-eligibility checks. This is authoritative on the
    /// data path; `tx_state` (below) retains the richer structure for TR/replay
    /// consumers but is no longer touched per query, so the relay pays no
    /// `RwLock` acquisition just to test in-transaction.
    pub in_transaction: std::sync::atomic::AtomicBool,
    /// Set while the session is mid-COPY (the backend sent CopyInResponse /
    /// CopyBothResponse and is awaiting CopyData from the client). A COPY is
    /// NOT a clean transaction boundary even though no ReadyForQuery has been
    /// seen yet, so Transaction/Statement pool release must be suppressed while
    /// it is set — otherwise the connection would be reset (`DISCARD ALL`) and
    /// parked in the middle of a copy, aborting it and hanging the client.
    /// Cleared once the COPY drains to ReadyForQuery, and at any
    /// ReadyForQuery the backend relays (one that ended copy-in at a message
    /// other than CopyData/CopyDone/CopyFail: `note_copy_left`).
    pub copy_in_progress: std::sync::atomic::AtomicBool,
    /// Status byte of the most recent `ReadyForQuery` relayed to the client
    /// (`b'I'` idle, `b'T'` in transaction, `b'E'` failed transaction).
    /// Written alongside `in_transaction`; read by the in-session TR
    /// bookkeeping to detect the Idle→InTx transition and a failed
    /// transaction (which is never replayable).
    pub last_rfq_status: std::sync::atomic::AtomicU8,
    /// Whether the most recent fully-relayed response carried an
    /// `ErrorResponse` frame. Lets the TR bookkeeping skip recording a
    /// `SET` that the backend rejected.
    pub last_response_error: std::sync::atomic::AtomicBool,
    /// The current request's statements that completed (CommandComplete,
    /// EmptyQueryResponse, PortalSuspended) across every response relayed
    /// for it, Flush output included; whether any of those carried an
    /// ErrorResponse; and whether the tally is whole (no fault interrupted
    /// the request). Settings tracking places a failure with it. Reset when a
    /// request starts and when it ends.
    pub cycle_completions: std::sync::atomic::AtomicU32,
    pub cycle_errored: std::sync::atomic::AtomicBool,
    pub cycle_counted: std::sync::atomic::AtomicBool,
    /// TR-06 observation digest of the most recent fully-relayed response
    /// (RowDescription/DataRow/CommandComplete/EmptyQueryResponse frames),
    /// `0` = none computed. Hashed only while inside an explicit transaction
    /// under a recording `tr_mode`, so the autocommit hot path pays nothing.
    pub last_response_digest: std::sync::atomic::AtomicU64,
    /// The most recent response exceeded `tr_max_observation_bytes`: it has no
    /// verifiable observation, so its transaction must not be replayed.
    pub last_response_unverifiable: std::sync::atomic::AtomicBool,
    /// Set by the forward path when the statement it just sent was
    /// transformed on the way to the backend (query-rewrite rule fired,
    /// tenant filter injected): the client text the TR bookkeeping records is
    /// then NOT what executed, so the enclosing transaction must not be
    /// blindly replayed. Consumed (swapped to false) by the recorder.
    pub tr_replay_tainted: std::sync::atomic::AtomicBool,
    /// Secret the proxy may use to authenticate its OWN backend connections
    /// for this session (SCRAM-SHA-256 / MD5 / cleartext challenges on a
    /// redial, route switch or in-session failover). Populated only when the
    /// proxy is the auth boundary (`[auth] mode = "scram"`) and the user's
    /// `auth_file` entry is plaintext. `None` in pass-through mode: the
    /// proxy never sees the client's password there, so a fresh connection
    /// can only be opened to a backend that does not challenge (trust).
    pub backend_credential: RwLock<Option<String>>,
    /// Tables written earlier in the CURRENT explicit transaction, so the
    /// query cache can re-invalidate them when the backend reports the commit
    /// (C-02): a reader may refill an entry between the write statement's
    /// response and the commit, and only a commit-time pass closes that
    /// window. Driven by the journal capture's ops on both protocols; cleared
    /// on rollback and at session end.
    #[cfg(feature = "query-cache")]
    pub(crate) tx_cache_stage: std::sync::Mutex<TxCacheStage>,
    /// The simple-query string in flight may run a statement the journal
    /// capture does not see (a `;` in its backend-ambiguous rest,
    /// `sql_text::ambiguous_rest`): the next `ReadyForQuery` invalidates every
    /// cached result before it reaches the client (C-02), and inside a
    /// transaction the commit does again.
    #[cfg(feature = "query-cache")]
    pub(crate) cache_unseen_write: std::sync::atomic::AtomicBool,
    /// Fail-closed result-cache eligibility. Any statement whose effects on
    /// execution context are not provably neutral makes this sticky.
    #[cfg(feature = "query-cache")]
    pub(crate) cache_ineligible: std::sync::atomic::AtomicBool,
    /// ParameterStatus messages relayed to this session so far: a reused
    /// settings proof ([`CacheScopeMemo`]) is valid only while none arrived.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(crate) parameter_changes: std::sync::atomic::AtomicU64,
    /// Complete startup identity, initialized once before the first lookup.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(crate) cache_startup_namespace: std::sync::OnceLock<[u8; 32]>,
    /// Kinds of state ([`super::session_bound`]) the session holds on its
    /// backend connection, refreshed before each request: a branch move
    /// (which gives the session a connection to another database) is refused
    /// while any is held.
    pub(crate) branch_blockers: std::sync::atomic::AtomicU8,
    /// The database the session started on, recorded at its first branch
    /// move: `branch=` with no name moves it back there, and a Nano branch
    /// is named after its base.
    pub(crate) branch_home: std::sync::OnceLock<String>,
    /// Last effective scope used by this client's raw-SQL L1 cache.
    #[cfg(feature = "query-cache")]
    pub(crate) cache_effective_namespace: std::sync::Mutex<Option<[u8; 32]>>,
    /// Journal metadata already relayed by a Flush or COPY yield; never rows.
    pub(crate) journal_response_prefix:
        std::sync::Mutex<Option<(crate::journal_capture::ResponseOutcome, usize)>>,
    pub(crate) journal_response_pending: std::sync::atomic::AtomicBool,
    /// TR-07 recovery-journal capture: the per-session state machine that
    /// turns registered statements + observed backend responses into
    /// committed transactions (`journal_capture`). Locked briefly on the
    /// forward path (register) and at each response boundary (observe).
    pub journal: std::sync::Mutex<crate::journal_capture::SessionCapture>,
    /// Set by the forward path when the current cycle registered a statement
    /// whose outcome the relay must collect (writes / COPY / control). Reads
    /// never arm it, so an autocommit read pays no capture work in the relay.
    pub journal_armed: std::sync::atomic::AtomicBool,
    /// Mirror of "the capture is inside an explicit transaction": with it
    /// clear and the response idle, the relay skips the capture mutex.
    pub journal_open: std::sync::atomic::AtomicBool,
    /// Rich transaction state (tx id, statement log, savepoints) for
    /// Transaction-Replay/library consumers. Only touched on the per-query
    /// path while the session is inside an explicit transaction AND
    /// `tr_mode` is `select`/`transaction` (the statement log is the replay
    /// source for in-session failover) — see `in_transaction` above.
    pub tx_state: RwLock<TransactionState>,
    /// Session variables
    pub variables: RwLock<HashMap<String, String>>,
    /// Created at
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// TR mode for this session
    pub tr_mode: TrMode,
    /// Wall-clock instant of this session's most recent write, for
    /// read-your-writes routing: reads within the configured window after a
    /// write are pinned to the primary so the client observes its own writes
    /// despite replica lag.
    #[cfg(feature = "lag-routing")]
    pub last_write_at: RwLock<Option<std::time::Instant>>,
    /// Client ID for pool-modes lease tracking
    #[cfg(feature = "pool-modes")]
    pub pool_client_id: ClientId,
    /// Identity returned by an `Authenticate` plugin, if any. Downstream
    /// plugins (masking, residency routing, cost governor) read this to
    /// gate per-user policy. `None` when no plugin ran or every plugin
    /// deferred to the default auth flow.
    #[cfg(feature = "wasm-plugins")]
    pub plugin_identity: RwLock<Option<Identity>>,
    /// What every plugin hook call of this session carries in its
    /// `hook_context` (identity, database, branch, attributes), built once
    /// after authentication ([`ProxyServer::plugin_session_context`]).
    #[cfg(feature = "wasm-plugins")]
    pub plugin_context: std::sync::OnceLock<crate::plugins::HookContext>,
    /// Sticky edge-cache ineligibility: set once the session executes any
    /// statement that alters execution context (SET/SET ROLE/search_path,
    /// temp tables, ...). The shared edge cache keys on (fingerprint,
    /// params, db/user, startup vars) — it cannot see session-local GUC
    /// state, so a session that mutates it must never read from or store
    /// into the shared cache again (cross-session wrong-rows otherwise).
    #[cfg(feature = "edge-proxy")]
    pub edge_ineligible: std::sync::atomic::AtomicBool,
    /// Tables of an in-flight `COPY ... FROM` awaiting its CopyDone drain.
    /// COPY rows become visible at drain time, so the edge invalidation is
    /// deferred until then (never held across an await).
    #[cfg(feature = "edge-proxy")]
    pub pending_edge_copy_tables: std::sync::Mutex<Option<Vec<String>>>,
    /// Rate-limit bucket key for this session, resolved once and reused.
    /// The keying dimension (`[rate_limit] key_by`) is fixed for the life of a
    /// connection, and so are the startup parameters it reads (`user`,
    /// `database`), so the gate no longer rebuilds the key — nor takes the
    /// `variables` lock, nor re-renders the metrics key string — per query.
    /// Populated lazily on the first gated query, once the startup parameters
    /// are present; see `ProxyServer::rate_limit_key`.
    #[cfg(feature = "rate-limiting")]
    pub rate_limit_key: std::sync::OnceLock<crate::rate_limit::CachedLimiterKey>,
    /// The in-flight slot (`[rate_limit] max_concurrent`) of the request this
    /// session is running. Taken at the request boundary by
    /// `ProxyServer::rate_limit_check`; returned when the client loop finds
    /// the session idle again (its request's ReadyForQuery relayed, or none
    /// in flight), and when the session ends.
    #[cfg(feature = "rate-limiting")]
    pub(crate) rate_permit: parking_lot::Mutex<Option<crate::rate_limit::BucketPermit>>,
    /// Whether `rate_permit` holds a permit. Only the task serving the
    /// session stores and takes the permit, so the per-request release reads
    /// this flag and leaves the lock alone when there is nothing to give back
    /// (always, without `[rate_limit] max_concurrent`).
    #[cfg(feature = "rate-limiting")]
    pub(crate) rate_permit_held: std::sync::atomic::AtomicBool,
    /// The journal's source identity of this session, resolved once and
    /// reused by every journaled cycle. User, database, tenant and client
    /// address are fixed after startup; only `backend` follows the session's
    /// current node, and `ProxyServer::journal_source` refreshes it in place.
    pub(super) journal_identity: parking_lot::Mutex<crate::transaction_journal::SourceIdentity>,
    /// Bumped whenever an input of `journal_identity` other than the current
    /// node changes ([`ClientSession::invalidate_journal_identity`]).
    pub(super) journal_identity_generation: std::sync::atomic::AtomicU64,
    /// The generation `journal_identity` was resolved at; 0 before the first.
    pub(super) journal_identity_resolved: std::sync::atomic::AtomicU64,
    /// The node slot (H-05) of the backend connection opened during startup,
    /// handed to that connection's `BackendConn` once the session starts.
    pub(crate) startup_slot: parking_lot::Mutex<crate::node_slots::Slot>,
    /// W07: the known tenant the startup packet named, pinned at admission:
    /// its isolation, pool and caps apply for the session's life, whatever
    /// its startup parameters are rewritten to. Unset for a session of no
    /// known tenant (and without `[multi_tenancy] enabled`).
    pub(crate) tenant: std::sync::OnceLock<Arc<crate::tenant_slots::TenantPlan>>,
    /// W07: the startup connection's slot under its tenant's
    /// `pool_max_connections`, taken at admission and handed to that
    /// connection's `BackendConn` once the session starts.
    pub(crate) startup_tenant_slot: parking_lot::Mutex<crate::node_slots::Slot>,
    /// W02: a connection leased from the pool without its liveness probe
    /// (`[pool] test_on_acquire = false`) refused the first write of a
    /// request, which therefore reached no backend: the forward path dials
    /// a fresh connection and sends the request once more.
    #[cfg(feature = "pool-modes")]
    pub(crate) stale_lease: std::sync::atomic::AtomicBool,
    /// W02: the next connection this session opens is dialed fresh, never
    /// leased from the pool (set for the one retry after a stale lease).
    #[cfg(feature = "pool-modes")]
    pub(crate) fresh_dial: std::sync::atomic::AtomicBool,
    /// W02 (`[pool_mode] prepared_statement_mode = "track"`): the statements
    /// the session prepared with SQL `PREPARE`, prepared again on a pooled
    /// connection that lacks one before a statement uses it.
    pub(crate) sql_prepared: parking_lot::Mutex<super::sql_prepared::SqlPrepared>,
    /// `sql_prepared` holds a statement: lets the request path skip it
    /// without the lock.
    pub(crate) sql_prepared_live: std::sync::atomic::AtomicBool,
    /// The session's reads stay on its home node (the primary) from now on:
    /// it created state that only that connection holds (temp tables,
    /// cursors, SQL `PREPARE`, `LISTEN`, untracked GUC changes), or mirroring
    /// its tracked `SET`s onto a standby failed. Only consulted while
    /// `[load_balancer]` offloads reads.
    pub(crate) reads_pinned: std::sync::atomic::AtomicBool,
    /// The standby the session's offloaded reads last ran on. Reads keep
    /// using it while it stays eligible, so one session does not read from
    /// standbys at different replay positions in turn.
    pub(crate) read_node: parking_lot::Mutex<Option<String>>,
    /// When the session last sent a write (or `SET`, or transaction end)
    /// while reads are offloaded: `[load_balancer] read_your_writes_ms`
    /// keeps its reads on the primary for that long afterwards.
    pub(crate) last_write: parking_lot::Mutex<Option<std::time::Instant>>,
    /// ac858d61400b: the session's tenant request budgets, set once at
    /// admission and only for a tenant with one. Its Drop returns the replay
    /// bytes still charged.
    pub(crate) tenant_charge: std::sync::OnceLock<crate::tenant_slots::TenantCharge>,
    /// The BackendKeyData (pid, secret) relayed to this client at startup, so
    /// a CancelRequest can end a wait at its tenant's active-query cap. Set
    /// together with `TenantSlots::session_key_relayed`; the `SessionGuard`
    /// ends that at the session's end. It is also the startup backend
    /// connection's own (`BackendConn::backend_key`).
    pub(crate) cancel_key: std::sync::OnceLock<(u32, u32)>,
    /// The waker of the task serving this session while it waits for its
    /// client at a point where a shutdown drain may end it ([`DrainSignal`]).
    pub(super) drain_waker: parking_lot::Mutex<Option<std::task::Waker>>,
}

impl ClientSession {
    /// Give back the in-flight slot the session's request holds, if any.
    #[cfg(feature = "rate-limiting")]
    pub(crate) fn release_rate_permit(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        if !self.rate_permit_held.load(Relaxed) {
            return;
        }
        let permit = self.rate_permit.lock().take();
        self.rate_permit_held.store(false, Relaxed);
        drop(permit);
    }

    /// Hold `permit` until the session's request ends.
    #[cfg(feature = "rate-limiting")]
    pub(crate) fn hold_rate_permit(&self, permit: crate::rate_limit::BucketPermit) {
        *self.rate_permit.lock() = Some(permit);
        self.rate_permit_held
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether the session's request holds an in-flight slot.
    #[cfg(feature = "rate-limiting")]
    pub(crate) fn holds_rate_permit(&self) -> bool {
        self.rate_permit_held
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The session's startup variables or tenant changed: the next journaled
    /// cycle resolves the journal source identity again.
    pub(crate) fn invalidate_journal_identity(&self) {
        self.journal_identity_generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }

    /// The session's tenant request budgets, if its tenant has any.
    pub(crate) fn tenant_budget(&self) -> Option<&crate::tenant_slots::TenantBudget> {
        self.tenant_charge
            .get()
            .map(crate::tenant_slots::TenantCharge::budget)
    }

    /// Whether the session's tenant has an active-query cap, so the session
    /// holds one of its permits while a request runs.
    pub(crate) fn tenant_query_capped(&self) -> bool {
        self.tenant_budget()
            .is_some_and(crate::tenant_slots::TenantBudget::has_query_cap)
    }

    /// The session's charge, only when its tenant has a replay-byte budget.
    fn tenant_replay(&self) -> Option<&crate::tenant_slots::TenantCharge> {
        self.tenant_charge
            .get()
            .filter(|c| c.budget().has_replay_budget())
    }

    /// The replay bytes of `ts` charged to the tenant: a `select`-mode
    /// transaction with writes is never replayed, so it is charged nothing.
    fn tenant_chargeable(&self, ts: &TransactionState, bytes: usize) -> usize {
        if self.tr_mode == TrMode::Select && ts.has_writes {
            0
        } else {
            bytes
        }
    }

    /// Charge the tenant up to `retained` replay bytes for `ts`; `false`
    /// when that would take the tenant over its budget.
    pub(crate) fn tenant_replay_reserve(&self, ts: &TransactionState, retained: usize) -> bool {
        self.tenant_replay()
            .is_none_or(|c| c.replay_reserve(self.tenant_chargeable(ts, retained)))
    }

    /// Whether `projected` replay bytes for `ts` would fit the tenant's
    /// budget (a check only: the Flush projection); a `false` is counted as
    /// a tenant refusal. `writes_unrecorded` tells whether the part not yet
    /// recorded (the Flush chunks the Sync will record) may write. It may
    /// scan the open cycle, so it is asked last: only when the projection
    /// does not fit, in `select` mode, for a transaction with no recorded
    /// write. One that writes is charged nothing there, so such a projection
    /// fits (and is no refusal).
    pub(crate) fn tenant_replay_fits(
        &self,
        ts: &TransactionState,
        projected: usize,
        writes_unrecorded: impl FnOnce() -> bool,
    ) -> bool {
        let Some(c) = self.tenant_replay() else {
            return true;
        };
        if c.replay_room(self.tenant_chargeable(ts, projected))
            || (self.tr_mode == TrMode::Select && !ts.has_writes && writes_unrecorded())
        {
            return true;
        }
        c.note_replay_refusal();
        false
    }

    /// Set the tenant charge to the chargeable `ts.replay_bytes`.
    pub(crate) fn tenant_replay_settle(&self, ts: &TransactionState) {
        if let Some(c) = self.tenant_replay() {
            c.replay_settle(self.tenant_chargeable(ts, ts.replay_bytes));
        }
    }

    /// Return every replay byte the session holds to its tenant.
    pub(crate) fn tenant_replay_release(&self) {
        if let Some(c) = self.tenant_replay() {
            c.replay_settle(0);
        }
    }
}

/// Transaction state
#[derive(Debug, Clone, Default)]
pub struct TransactionState {
    /// Whether in a transaction
    pub in_transaction: bool,
    /// Transaction ID
    pub tx_id: Option<Uuid>,
    /// Statements executed in current transaction, in wire order, starting
    /// with the statement that opened it (the `BEGIN`). This is the source
    /// for `tr_mode = "transaction"` replay after an in-session failover.
    pub statements: Vec<StatementLog>,
    /// Read-only transaction — no recorded statement was classified as a
    /// write (`!has_writes`).
    pub read_only: bool,
    /// Savepoints
    pub savepoints: Vec<String>,
    /// Bytes retained in `statements` (SQL text + raw extended frames),
    /// checked against `[limits] tr_max_replay_bytes`.
    pub replay_bytes: usize,
    /// Any recorded statement was a write (or an opaque statement that may
    /// write). Read-only transactions may be replayed in `select` mode.
    pub has_writes: bool,
    /// The transaction can no longer be replayed: it exceeded a replay cap,
    /// entered the failed state, contained a COPY, or a statement was
    /// transformed on the way to the backend (query-rewrite / tenant filter)
    /// so its recorded client text is not what executed. `transaction` mode
    /// degrades to `session` behaviour for it.
    pub non_replayable: bool,
    /// `non_replayable` because the transaction outgrew `[limits]
    /// tr_max_replay_statements` / `tr_max_replay_bytes`, or its tenant's
    /// `[multi_tenancy]` replay-byte budget (P-03 refusal reason).
    pub replay_cap_exceeded: bool,
}

/// Logged statement for TR replay
#[derive(Debug, Clone)]
pub struct StatementLog {
    /// Statement SQL. For an extended-protocol batch this is the routing SQL
    /// (its first `Parse`, or the referenced named statement's text) and the
    /// replayable form lives in `extended`.
    pub sql: String,
    /// Parameters
    pub params: Vec<String>,
    /// Result checksum
    pub result_checksum: Option<u64>,
    /// Execution time
    pub executed_at: chrono::DateTime<chrono::Utc>,
    /// Raw extended-protocol form (`None` for a simple-protocol `Query`).
    pub extended: Option<ExtendedBatchLog>,
}

/// Raw extended-protocol frames recorded for one Sync-terminated cycle so it
/// can be re-sent verbatim to a replacement backend.
#[derive(Debug, Clone)]
pub struct ExtendedBatchLog {
    /// Every frame forwarded for the cycle (all Flush-terminated batches plus
    /// the terminating Sync batch), in wire order.
    pub frames: bytes::Bytes,
    /// The unnamed `Parse` held aside by the unnamed-Parse promotion, if the
    /// cycle had one — always re-sent first on a fresh connection.
    pub unnamed_parse: Option<bytes::Bytes>,
    /// Named statements the cycle's own `Parse`s define.
    pub defines: Vec<String>,
    /// Named statements the cycle references (Bind / Describe-S) — re-prepared
    /// from the session registry if the replacement connection lacks them.
    pub refs: Vec<String>,
    /// TR-08: statements EARLIER cycles prepared that this cycle uses, with
    /// the `Parse` the backend had acknowledged for each when the cycle ran.
    /// Replay redefines them (Close + Parse) before re-sending the cycle, so a
    /// later redefinition of the name in the session cannot leak into it.
    pub outside: Vec<(String, bytes::Bytes)>,
}

/// The backend connections a session holds, by node address.
pub(super) type ConnMap = NodeMap<BackendConn>;

/// A map from node address to `V` for the few entries one session has: its
/// home connection and, with read offload or a moved home, one or two more.
/// The relay looks a connection up several times per statement, so a lookup
/// is a scan that compares addresses, with no hashing; the entries have no
/// order a caller may rely on.
pub(super) struct NodeMap<V> {
    entries: Vec<(String, V)>,
}

impl<V> Default for NodeMap<V> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<V> NodeMap<V> {
    #[inline]
    fn position(&self, node: &str) -> Option<usize> {
        self.entries.iter().position(|(addr, _)| addr == node)
    }

    #[inline]
    pub(super) fn contains_key<K: AsRef<str> + ?Sized>(&self, node: &K) -> bool {
        self.position(node.as_ref()).is_some()
    }

    #[inline]
    pub(super) fn get<K: AsRef<str> + ?Sized>(&self, node: &K) -> Option<&V> {
        let node = node.as_ref();
        self.entries
            .iter()
            .find(|(addr, _)| addr == node)
            .map(|(_, value)| value)
    }

    #[inline]
    pub(super) fn get_mut<K: AsRef<str> + ?Sized>(&mut self, node: &K) -> Option<&mut V> {
        let node = node.as_ref();
        self.entries
            .iter_mut()
            .find(|(addr, _)| addr == node)
            .map(|(_, value)| value)
    }

    /// Store `value` for `node`; the value it replaces, if any.
    pub(super) fn insert(&mut self, node: String, value: V) -> Option<V> {
        match self.position(&node) {
            Some(at) => Some(std::mem::replace(&mut self.entries[at].1, value)),
            None => {
                self.entries.push((node, value));
                None
            }
        }
    }

    pub(super) fn remove<K: AsRef<str> + ?Sized>(&mut self, node: &K) -> Option<V> {
        let at = self.position(node.as_ref())?;
        Some(self.entries.swap_remove(at).1)
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn keys(&self) -> impl Iterator<Item = &String> {
        self.entries.iter().map(|(addr, _)| addr)
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.iter().map(|(_, value)| value)
    }

    pub(super) fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.entries.iter_mut().map(|(_, value)| value)
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut V)> {
        self.entries.iter_mut().map(|(addr, value)| (&*addr, value))
    }
}

/// `map[node]`, panicking when the node has no entry, as a `HashMap` does.
impl<V, K: AsRef<str> + ?Sized> std::ops::Index<&K> for NodeMap<V> {
    type Output = V;

    fn index(&self, node: &K) -> &V {
        self.get(node).expect("no entry for the node")
    }
}

impl<V> FromIterator<(String, V)> for NodeMap<V> {
    fn from_iter<I: IntoIterator<Item = (String, V)>>(iter: I) -> Self {
        let mut map = Self::default();
        for (node, value) in iter {
            map.insert(node, value);
        }
        map
    }
}

/// A cached per-session backend connection plus the set of *named* prepared
/// statements known to be live on **this** socket.
///
/// Tying the prepared-statement set to the socket (rather than to the node
/// address) is what makes prepared statements survive a backend switch: when a
/// connection is dropped and redialed, or when a session is routed to a
/// different node, the fresh `BackendConn` starts with an empty set, so the
/// proxy transparently re-issues the original `Parse` for any named statement
/// the target connection is missing before forwarding a `Bind`/`Describe` that
/// references it (Batch F.4). The session keeps the canonical `Parse` bytes in
/// a separate registry; this set is just "what does *this* socket already
/// know".
///
/// A `Close` reaches only the connection its batch is routed to, so another
/// cached connection of the session can still hold the closed definition. Such
/// names move from `prepared` to `stale` and are closed on that socket before
/// a later batch uses or redefines them (F2).
pub(super) struct BackendConn {
    /// A cleanly drained backend that cannot provide the settings proof.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) cache_scope_unsupported: bool,
    /// Reused only under the explicit immutable-backend-context contract
    /// (for the physical connection's life, so it is parked with it).
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) cache_scope_verified: Option<crate::pool::SettingsProof>,
    /// The last settings proof of this connection, reused for later reads
    /// within `[cache] proof_ttl_ms` while nothing but proven cacheable reads
    /// ran on it ([`Self::forget_cache_scope`]).
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) cache_scope_memo: Option<CacheScopeMemo>,
    /// The connection still holds its reset settings: it was opened for
    /// this session, or leased from the pool parked with them
    /// (`ParkedSettings::baseline`), and nothing that may change settings
    /// ran on it since. A proof taken while this holds describes the
    /// settings `DISCARD ALL` restores, so it survives that reset when the
    /// connection is parked (`ProxyServer::parked_settings`).
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    pub(super) settings_baseline: bool,
    /// Explicit proof for synthetic protocol fixtures; absent from production.
    #[cfg(all(test, any(feature = "query-cache", feature = "edge-proxy")))]
    pub(super) cache_scope_test_override: Option<[u8; 32]>,
    pub(super) stream: TcpStream,
    /// Unpublished backend frame tail, owned by this socket across Flush,
    /// idle watch and Sync. Dropped with the socket, never moved to another node.
    pub(super) response_buf: BytesMut,
    pub(super) prepared: HashSet<String>,
    /// Named statements the session closed through another connection that
    /// this socket may still hold. Never also in `prepared`.
    pub(super) stale: HashSet<String>,
    /// The session's tracked-`SET` epoch this socket was last brought up to
    /// date with (offloaded reads mirror the primary's session GUCs onto a
    /// standby connection). `None`: never synchronised.
    pub(super) gucs_synced: Option<u64>,
    /// The mirrored set this socket took changed the role or the session
    /// authorization, which `RESET ALL` leaves in place: resetting the socket
    /// then also needs `RESET SESSION AUTHORIZATION` and `RESET ROLE`.
    pub(super) gucs_role: bool,
    /// Signature (query text + parameter-type OIDs) of the *unnamed* prepared
    /// statement currently established on this socket, if any. When the client
    /// re-sends an identical unnamed `Parse`, the proxy can skip forwarding it
    /// (the backend's unnamed statement already holds that SQL) and synthesize
    /// the `ParseComplete` locally — the unnamed-Parse promotion (Batch H).
    pub(super) unnamed_sig: Option<bytes::Bytes>,
    /// The socket may hold an unnamed statement of the client's that
    /// `unnamed_sig` does not describe (the promotion is off, or its `Parse`
    /// did not open its batch): the proxy could not re-establish it, so
    /// nothing that destroys the unnamed statement (the result cache's
    /// settings probe, a simple Query of the proxy's own) may run ahead of a
    /// batch of the client's that does not replace it.
    pub(super) unnamed_untracked: bool,
    /// The unnamed statement and portal this socket may still hold although
    /// the session's were destroyed without it (see [`UnnamedStale`]). Never
    /// set on a fresh or pooled connection.
    pub(super) unnamed_stale: UnnamedStale,
    /// Whether a simple-query statement forwarded on this socket may have left
    /// session-level state behind (a `SET`, temp table, `LISTEN`, advisory
    /// lock, …). Used only by the conditional-reset optimisation
    /// (`pool_mode.skip_clean_reset`): a connection is eligible to be parked
    /// WITHOUT running the reset query only when it is provably clean —
    /// `!dirty && prepared.is_empty() && unnamed_sig.is_none()`. Set
    /// conservatively (any statement not provably session-neutral sets it), so
    /// the worst outcome of a misclassification is an unnecessary reset, never
    /// leaked state. A batch that closed a named statement sets it too: the
    /// `Close` may have been skipped after an error in the batch. Always
    /// `false` on a fresh/reused connection.
    #[cfg(feature = "pool-modes")]
    pub(super) dirty: bool,
    /// The server reported `in_hot_standby = on` when this socket started
    /// (PostgreSQL 14+): it was a standby then, and writes on it fail until a
    /// promotion completes. `false` when not reported (other PG-wire
    /// backends, pooled sockets).
    pub(super) hot_standby: bool,
    /// ac858d61400b: a statement that takes a session-level advisory lock
    /// ran on this socket (`takes_session_lock`, see `note_session_locks`), and no
    /// `pg_advisory_unlock_all()` or `DISCARD ALL` has succeeded on it since.
    /// PostgreSQL keeps such locks after ReadyForQuery 'I', so under session
    /// pooling a capped session keeps its tenant's active-query permit while
    /// any of its cached sockets is marked. The locks end with the backend
    /// session, so the mark goes with the socket: a connection that is lost,
    /// or dropped for a replacement, no longer pins the permit. Read only for
    /// capped tenants under session pooling; `false` on a fresh or pooled
    /// connection.
    pub(super) session_locks: bool,
    /// The BackendKeyData (process id, secret key) the backend behind this
    /// socket reported at its startup (the session's startup connection, a
    /// fresh dial, or one leased from the pool, which keeps it while the
    /// socket is parked). The proxy cancels with it what it abandons on the
    /// socket while the backend may still be running it (`abandon_conn`).
    /// `None` when the backend sent none.
    pub(super) backend_key: Option<(u32, u32)>,
    /// This socket's slot on its node's `max_connections` cap (H-05), which
    /// also counts the node's open connections. Held for its `Drop`:
    /// released when the socket is dropped, or moved into the idle pool with
    /// it (`pool-modes`).
    pub(super) _slot: crate::node_slots::Slot,
    /// This socket's slot under its tenant's `pool_max_connections` (W07);
    /// `None` for a tenant without one. Held and moved like `_slot`.
    pub(super) _tenant_slot: crate::node_slots::Slot,
    /// When the backend connection was opened: `[pool] / [pool_mode]
    /// max_lifetime_secs` count from here (a pooled connection keeps it).
    #[cfg_attr(not(feature = "pool-modes"), allow(dead_code))]
    pub(super) opened_at: std::time::Instant,
    /// Leased from the idle pool without the liveness probe (`[pool]
    /// test_on_acquire = false`), and no request written on it yet: a first
    /// write it refuses reached no backend (`ProxyServer::send_failed`).
    pub(super) unprobed: bool,
    /// The SQL `PREPARE`d statements (folded names) this socket holds, as
    /// far as the proxy saw (`[pool_mode] prepared_statement_mode =
    /// "track"`).
    pub(super) sql_prepared: HashSet<String>,
}

impl BackendConn {
    pub(super) fn new(stream: TcpStream) -> Self {
        Self::opened(stream, std::time::Instant::now())
    }

    /// A connection to a backend opened at `opened_at`.
    fn opened(stream: TcpStream, opened_at: std::time::Instant) -> Self {
        Self {
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            cache_scope_unsupported: false,
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            cache_scope_verified: None,
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            cache_scope_memo: None,
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            settings_baseline: true,
            #[cfg(all(test, any(feature = "query-cache", feature = "edge-proxy")))]
            cache_scope_test_override: None,
            stream,
            response_buf: BytesMut::new(),
            prepared: HashSet::new(),
            stale: HashSet::new(),
            gucs_synced: None,
            gucs_role: false,
            unnamed_sig: None,
            unnamed_untracked: false,
            unnamed_stale: UnnamedStale::default(),
            #[cfg(feature = "pool-modes")]
            dirty: false,
            hot_standby: false,
            session_locks: false,
            backend_key: None,
            _slot: None,
            _tenant_slot: None,
            opened_at,
            unprobed: false,
            sql_prepared: HashSet::new(),
        }
    }

    /// A connection that holds a node slot (H-05).
    pub(super) fn with_slot(stream: TcpStream, slot: crate::node_slots::Slot) -> Self {
        Self {
            _slot: slot,
            ..Self::new(stream)
        }
    }

    /// A connection leased from the idle pool: it keeps its slots, its
    /// BackendKeyData and when it was opened. `unprobed`: leased without
    /// the liveness probe.
    #[cfg(feature = "pool-modes")]
    pub(super) fn leased(conn: crate::pool::PooledConn, unprobed: bool) -> Self {
        Self {
            _slot: conn.slot,
            _tenant_slot: conn.tenant_slot,
            backend_key: conn.backend_key,
            unprobed,
            ..Self::opened(conn.stream, conn.opened_at)
        }
    }

    /// The pool's form of this connection, to park it.
    #[cfg(feature = "pool-modes")]
    pub(super) fn into_pooled(
        self,
        settings: crate::pool::ParkedSettings,
    ) -> crate::pool::PooledConn {
        crate::pool::PooledConn {
            stream: self.stream,
            slot: self._slot,
            tenant_slot: self._tenant_slot,
            backend_key: self.backend_key,
            settings,
            opened_at: self.opened_at,
        }
    }

    /// A simple Query reached this socket (the client's, or one the proxy
    /// runs itself: mirrored or restored settings, a replayed statement):
    /// the backend destroyed its unnamed statement and portal.
    pub(super) fn unnamed_destroyed(&mut self) {
        self.unnamed_sig = None;
        self.unnamed_untracked = false;
        self.unnamed_stale = UnnamedStale::default();
        // A simple Query reached the socket: it may have changed settings.
        self.forget_cache_scope();
    }

    /// Something other than a proven cacheable read reached the socket (a
    /// statement that may change its settings): the next cached read proves
    /// them again.
    pub(super) fn forget_cache_scope(&mut self) {
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        {
            self.cache_scope_memo = None;
            self.settings_baseline = false;
        }
    }

    /// A statement that leaves the settings as they were (the settings proof
    /// itself, a proven cacheable read) reached the socket through a path
    /// that calls [`Self::unnamed_destroyed`]: keep what was known about the
    /// settings before it.
    pub(super) fn unnamed_destroyed_keeping_settings(&mut self) {
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        let kept = (self.cache_scope_memo.take(), self.settings_baseline);
        self.unnamed_destroyed();
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        {
            (self.cache_scope_memo, self.settings_baseline) = kept;
        }
    }
}

/// A settings proof kept for reuse ([`BackendConn::cache_scope_memo`]).
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
#[derive(Debug, Clone, Copy)]
pub(super) struct CacheScopeMemo {
    /// The proven namespace.
    pub(super) namespace: [u8; 32],
    /// The session startup digest it was computed with: a pooled
    /// connection's proof carries over only to a borrower with the same one.
    pub(super) startup: [u8; 32],
    /// When it was proven.
    pub(super) at: std::time::Instant,
    /// The session's ParameterStatus count then
    /// ([`ClientSession::parameter_changes`]): any ParameterStatus since
    /// retires the memo.
    pub(super) parameter_changes: u64,
}

/// Which unnamed objects a backend socket may still hold after the session's
/// were destroyed without it. PostgreSQL destroys the unnamed statement at
/// every simple `Query` and every unnamed `Parse` before it runs or parses
/// anything, so even one that fails leaves none, and the unnamed portal at
/// every simple `Query` (it outlives its cycle only inside a transaction
/// block). A request the proxy refuses or answers without that socket (the
/// rate limiter, an open circuit, the tenant's active-query cap or a cancel
/// of its wait, a cache or plugin answer, the aborted-transaction emulation),
/// or a Query another of the session's sockets runs, leaves the older ones
/// in place there: a later Bind or Describe of the unnamed statement, or an
/// Execute of the unnamed portal, with no Parse or Bind of its own would run
/// them where PostgreSQL answers 26000 or 34000. They are closed before a
/// batch could use them (`forward_extended_batch`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct UnnamedStale {
    pub(super) statement: bool,
    pub(super) portal: bool,
}

impl UnnamedStale {
    /// What an unnamed `Parse` destroys.
    pub(super) const STATEMENT: Self = Self {
        statement: true,
        portal: false,
    };

    /// What a simple `Query` destroys, the session in a transaction block
    /// (`in_block`) or not.
    pub(super) fn query(in_block: bool) -> Self {
        Self {
            statement: true,
            portal: in_block,
        }
    }

    pub(super) fn any(self) -> bool {
        self.statement || self.portal
    }
}

/// RAII teardown for one client connection. Its `Drop` deregisters the session
/// from `state.sessions`, bumps the connections-closed metric, and reclaims the
/// session's L1 query cache — running on a normal return AND on a panic unwind,
/// so a panic in negotiation/startup/the query loop can never leak the session
/// entry (which would inflate the active-session gauge and stall graceful
/// drains). All operations are synchronous, so they are valid inside `Drop`.
pub(super) struct SessionGuard {
    pub(super) state: Arc<ServerState>,
    pub(super) session_id: Uuid,
    /// Owned client-connection permit taken by admission control once the
    /// connection's first startup message is classified (`None` when
    /// `[limits] max_client_connections = 0`, i.e. no cap, or for a
    /// `CancelRequest`, which never consumes one). Held here purely so that
    /// dropping the guard returns the slot — on a normal return AND on a panic
    /// unwind, which a release at the end of `handle_client` would miss.
    pub(super) _client_slot: Option<tokio::sync::OwnedSemaphorePermit>,
    /// H-05: the session's slot under its tenant's client cap, released the
    /// same way (`None` when the tenant has no cap or is not known).
    pub(super) _tenant_slot: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        if let Some((_, session)) = self.state.sessions.remove(&self.session_id) {
            // The BackendKeyData relayed to it no longer names this session
            // for a CancelRequest that may end a tenant active-query wait.
            if let Some(key) = session.cancel_key.get() {
                self.state.tenant_slots.session_key_ended(*key);
            }
        }
        self.state
            .metrics
            .connections_closed
            .fetch_add(1, Ordering::Relaxed);
        // Reclaim the per-connection L1 query cache (keyed by the session's
        // first u64); without this an abandoned cache leaks under churn.
        #[cfg(feature = "query-cache")]
        if let Some(ref qc) = self.state.query_cache {
            qc.remove_l1_cache(self.session_id.as_u64_pair().0);
        }
    }
}

/// Resolves once the server has begun its shutdown drain
/// ([`ServerState::draining`], set by `ProxyServer::begin_drain` on SIGTERM
/// or SIGINT). A session awaits it next to its client read only while it
/// waits at a point where ending it loses nothing (no transaction, cycle,
/// COPY or partly received request), so a drain closes idle sessions at once
/// instead of waiting for their clients to leave.
///
/// The session's task waker is left in [`ClientSession::drain_waker`];
/// `begin_drain` sets the flag first and wakes it afterwards, and the flag is
/// read again here after the waker is stored, so a drain that begins between
/// the two is never missed.
pub(super) struct DrainSignal<'a> {
    pub(super) state: &'a ServerState,
    pub(super) session: &'a ClientSession,
}

impl std::future::Future for DrainSignal<'_> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        use std::sync::atomic::Ordering;
        if self.state.draining.load(Ordering::Relaxed) {
            return std::task::Poll::Ready(());
        }
        {
            let mut slot = self.session.drain_waker.lock();
            if !slot.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                *slot = Some(cx.waker().clone());
            }
        }
        if self.state.draining.load(Ordering::SeqCst) {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }
}
