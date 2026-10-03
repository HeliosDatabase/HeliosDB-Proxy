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
    /// Cleared once the COPY drains to ReadyForQuery.
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
pub(super) struct BackendConn {
    pub(super) stream: TcpStream,
    pub(super) prepared: HashSet<String>,
    /// Signature (query text + parameter-type OIDs) of the *unnamed* prepared
    /// statement currently established on this socket, if any. When the client
    /// re-sends an identical unnamed `Parse`, the proxy can skip forwarding it
    /// (the backend's unnamed statement already holds that SQL) and synthesize
    /// the `ParseComplete` locally — the unnamed-Parse promotion (Batch H).
    pub(super) unnamed_sig: Option<bytes::Bytes>,
    /// Whether a simple-query statement forwarded on this socket may have left
    /// session-level state behind (a `SET`, temp table, `LISTEN`, advisory
    /// lock, …). Used only by the conditional-reset optimisation
    /// (`pool_mode.skip_clean_reset`): a connection is eligible to be parked
    /// WITHOUT running the reset query only when it is provably clean —
    /// `!dirty && prepared.is_empty() && unnamed_sig.is_none()`. Set
    /// conservatively (any statement not provably session-neutral sets it), so
    /// the worst outcome of a misclassification is an unnecessary reset, never
    /// leaked state. Always `false` on a fresh/reused connection.
    #[cfg(feature = "pool-modes")]
    pub(super) dirty: bool,
}

impl BackendConn {
    pub(super) fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            prepared: HashSet::new(),
            unnamed_sig: None,
            #[cfg(feature = "pool-modes")]
            dirty: false,
        }
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
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.state.sessions.remove(&self.session_id);
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
