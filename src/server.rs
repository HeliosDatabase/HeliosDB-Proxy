//! Proxy Server Implementation
//!
//! Main server that accepts client connections and routes them to backends.
//! Implements PostgreSQL wire protocol forwarding with TWR (Transparent Write Routing).

use crate::admin::{AdminServer, AdminState, ConfigSnapshot, NodeSnapshot};
#[cfg(feature = "ha-tr")]
use crate::backend::{tls::default_client_config, BackendConfig, TlsMode};
use crate::client_tls::{build_tls_acceptor, ClientStream};
use crate::config::{HbaAction, HbaRule, NodeConfig, NodeRole, ProxyConfig, TrMode};
#[cfg(feature = "wasm-plugins")]
use crate::protocol::QueryMessage;
use crate::protocol::{
    ErrorResponse, Message, MessageType, ProtocolCodec, StartupMessage, TransactionStatus,
};
use crate::{ProxyError, Result};
use arc_swap::ArcSwap;
use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

// Pool-modes feature imports
#[cfg(feature = "pool-modes")]
use crate::pool::lease::ClientId;
#[cfg(feature = "pool-modes")]
use crate::pool::{ConnectionPoolManager, PoolModeConfig, PoolingMode};
#[cfg(feature = "pool-modes")]
use crate::NodeEndpoint;

// WASM plugin system imports
#[cfg(feature = "wasm-plugins")]
use crate::plugins::{
    AuthRequest as PluginAuthRequest, AuthResult, HookContext, HookType, Identity, PluginManager,
    PostQueryOutcome, PreQueryResult, QueryContext, RouteResult,
};

/// Proxy server
pub struct ProxyServer {
    config: ProxyConfig,
    state: Arc<ServerState>,
    shutdown_tx: broadcast::Sender<()>,
    /// Path the config was loaded from, retained so `SIGHUP` can re-read it
    /// for a zero-downtime reload (Batch H). `None` when the config was built
    /// from CLI flags/defaults rather than a file.
    config_path: Option<String>,
}

/// Stand-in "signal stream" on platforms without Unix signals: its `recv()`
/// never resolves, so the `SIGHUP` select arm is simply inert there.
#[cfg(not(unix))]
struct HangupNever;
#[cfg(not(unix))]
impl HangupNever {
    async fn recv(&mut self) -> Option<()> {
        std::future::pending().await
    }
}

/// Build the BackendConfig template the time-travel replay engine
/// uses for its target connection. The replay handler swaps in
/// `target_host` / `target_port` per request; everything else
/// (auth, TLS policy, timeouts) comes from this template.
///
/// Auth defaults to the bare PostgreSQL `postgres` superuser without
/// a password — sensible for local development against `trust` auth,
/// never for production. Per-call credential overrides on
/// ReplayRequestBody land in FU-21.
///
/// `_config` is kept in the signature so future iterations can pull
/// shared TLS / timeout settings from the proxy config without
/// changing the call site.
#[cfg(feature = "ha-tr")]
fn build_replay_backend_template(_config: &ProxyConfig) -> BackendConfig {
    BackendConfig {
        host: "placeholder".to_string(),
        port: 0,
        user: "postgres".to_string(),
        password: None,
        database: None,
        application_name: Some("heliosdb-proxy-replay".to_string()),
        tls_mode: TlsMode::Disable,
        connect_timeout: Duration::from_secs(5),
        query_timeout: Duration::from_secs(30),
        tls_config: default_client_config(),
    }
}

/// Cheap query-shape fingerprint for the anomaly detector. Replaces
/// numeric and string literals with `?` placeholders, lower-cases
/// keywords, and collapses whitespace. Same shape regardless of
/// literal values — `SELECT * FROM users WHERE id = 1` and
/// `SELECT * FROM users WHERE id = 99` map to the same fingerprint.
///
/// Not a parser. The analytics module has the canonical normaliser
/// when query-analytics is on; this is a lightweight standalone so
/// the anomaly detector works even when analytics is off.
///
/// Writes into `out` (cleared first) rather than returning a fresh
/// `String` so the per-query hot path can hand it a reusable buffer
/// and allocate nothing once the buffer has grown.
#[cfg(feature = "anomaly-detection")]
fn anomaly_fingerprint_into(sql: &str, out: &mut String) {
    out.clear();
    out.reserve(sql.len());
    let mut in_single = false;
    let mut prev_space = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            in_single = !in_single;
            // Replace the entire string literal (open + body +
            // close) with a single ?.
            if in_single {
                out.push('?');
                while let Some(&n) = chars.peek() {
                    chars.next();
                    if n == '\'' {
                        in_single = false;
                        break;
                    }
                }
                prev_space = false;
                continue;
            }
        }
        if c.is_ascii_digit() {
            if !out.ends_with('?') {
                out.push('?');
            }
            // Skip the rest of the number.
            while matches!(chars.peek(), Some(c) if c.is_ascii_digit() || *c == '.') {
                chars.next();
            }
            prev_space = false;
            continue;
        }
        if c.is_ascii_whitespace() {
            if !prev_space && !out.is_empty() {
                out.push(' ');
                prev_space = true;
            }
            continue;
        }
        out.push(c.to_ascii_lowercase());
        prev_space = false;
    }
    // Same as the old `out.trim_end().to_string()`, in place.
    let trimmed_len = out.trim_end().len();
    out.truncate(trimmed_len);
}

/// Owning wrapper around [`anomaly_fingerprint_into`]. Test-only —
/// the hot path uses the buffer form.
#[cfg(all(test, feature = "anomaly-detection"))]
fn anomaly_fingerprint(sql: &str) -> String {
    let mut out = String::new();
    anomaly_fingerprint_into(sql, &mut out);
    out
}

/// Validate a backend-declared frame length against the configured cap
/// before it is used to size a read/accumulation buffer.
///
/// The backend auth-phase scanners (`proxy_authentication`,
/// `complete_backend_auth`) hand-parse the raw wire header — `len` is the
/// 4-byte big-endian length field straight off a byte a hostile or
/// compromised backend controls. Without this check a declared
/// `len = 0xFFFFFFFF` makes the scanner wait for ~4 GiB to accumulate in
/// `backend_buffer`/`buffer` before ever reaching `ProtocolCodec`, which
/// enforces its own `max_message_size` only on the already-decoded path.
/// `max` is `state.limits.max_pending_bytes` — the same configured cap the
/// client-facing buffers in this file already use to bound frame/pending
/// accumulation (see the `max_pending_bytes` checks above in the data
/// path), reused here rather than inventing a second size limit.
/// Read one backend frame header out of `rem` and validate it (H-07).
///
/// `Ok(None)` when fewer than the 5 header bytes are present. `Err` when the
/// declared length is below the 4-byte protocol minimum (no further bytes can
/// make it a valid frame, so waiting is a hang) or above `max` (the accumulator
/// would otherwise grow toward whatever the backend advertises). `Ok(Some(len))`
/// is the declared length; the frame occupies `len + 1` bytes, and because
/// `len <= max <= usize::MAX - 1` is enforced here that addition cannot overflow.
fn backend_frame_len(rem: &[u8], max: usize) -> Result<Option<usize>> {
    if rem.len() < 5 {
        return Ok(None);
    }
    let len = u32::from_be_bytes([rem[1], rem[2], rem[3], rem[4]]) as usize;
    if len < 4 {
        return Err(ProxyError::Protocol(format!(
            "backend frame '{}' declares length {} below the 4-byte minimum",
            rem[0] as char, len
        )));
    }
    validate_backend_frame_len(len, max.min(usize::MAX - 1))?;
    Ok(Some(len))
}

/// PostgreSQL built-in functions that have no side effects when re-executed
/// (TR-03). This is the whole basis for calling an interrupted read
/// re-executable on an unknown outcome: a read calling anything NOT listed here
/// — a user-defined function, `nextval`, `pg_notify`, `set_config`, advisory
/// locks, large-object or replication functions — may already have run once and
/// is never run again by the proxy. Nondeterminism (`random`, `now`, `pg_sleep`)
/// is deliberately allowed: an unknown-outcome autocommit read published nothing
/// to the client, so only side effects matter. Sorted; looked up by binary
/// search on the ASCII-lowercased call name.
const TR_PURE_BUILTINS: &[&str] = &[
    "abs",
    "acos",
    "acosh",
    "age",
    "array_agg",
    "array_append",
    "array_cat",
    "array_dims",
    "array_fill",
    "array_length",
    "array_lower",
    "array_ndims",
    "array_position",
    "array_positions",
    "array_prepend",
    "array_remove",
    "array_replace",
    "array_to_json",
    "array_to_string",
    "array_upper",
    "ascii",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "avg",
    "bit_and",
    "bit_count",
    "bit_length",
    "bit_or",
    "bit_xor",
    "bool_and",
    "bool_or",
    "btrim",
    "cardinality",
    "cbrt",
    "ceil",
    "ceiling",
    "char_length",
    "character_length",
    "chr",
    "clock_timestamp",
    "coalesce",
    "col_description",
    "concat",
    "concat_ws",
    "convert",
    "convert_from",
    "convert_to",
    "corr",
    "cos",
    "cosh",
    "cot",
    "count",
    "covar_pop",
    "covar_samp",
    "cume_dist",
    "current_database",
    "current_query",
    "current_schema",
    "current_schemas",
    "current_setting",
    "date_bin",
    "date_part",
    "date_trunc",
    "decode",
    "degrees",
    "dense_rank",
    "div",
    "encode",
    "every",
    "exp",
    "extract",
    "factorial",
    "first_value",
    "floor",
    "format",
    "gcd",
    "gen_random_uuid",
    "generate_series",
    "generate_subscripts",
    "get_bit",
    "get_byte",
    "greatest",
    "has_column_privilege",
    "has_database_privilege",
    "has_function_privilege",
    "has_schema_privilege",
    "has_table_privilege",
    "inet_client_addr",
    "inet_client_port",
    "inet_server_addr",
    "inet_server_port",
    "initcap",
    "isfinite",
    "json_agg",
    "json_array_elements",
    "json_array_elements_text",
    "json_array_length",
    "json_build_array",
    "json_build_object",
    "json_each",
    "json_each_text",
    "json_extract_path",
    "json_extract_path_text",
    "json_object",
    "json_object_agg",
    "json_object_keys",
    "json_populate_record",
    "json_populate_recordset",
    "json_strip_nulls",
    "json_to_record",
    "json_to_recordset",
    "json_typeof",
    "jsonb_agg",
    "jsonb_array_elements",
    "jsonb_array_elements_text",
    "jsonb_array_length",
    "jsonb_build_array",
    "jsonb_build_object",
    "jsonb_each",
    "jsonb_each_text",
    "jsonb_extract_path",
    "jsonb_extract_path_text",
    "jsonb_insert",
    "jsonb_object",
    "jsonb_object_agg",
    "jsonb_object_keys",
    "jsonb_path_exists",
    "jsonb_path_match",
    "jsonb_path_query",
    "jsonb_path_query_array",
    "jsonb_path_query_first",
    "jsonb_populate_record",
    "jsonb_populate_recordset",
    "jsonb_pretty",
    "jsonb_set",
    "jsonb_set_lax",
    "jsonb_strip_nulls",
    "jsonb_to_record",
    "jsonb_to_recordset",
    "jsonb_typeof",
    "justify_days",
    "justify_hours",
    "justify_interval",
    "lag",
    "last_value",
    "lcm",
    "lead",
    "least",
    "left",
    "length",
    "ln",
    "localtime",
    "localtimestamp",
    "log",
    "log10",
    "lower",
    "lpad",
    "ltrim",
    "make_date",
    "make_interval",
    "make_time",
    "make_timestamp",
    "make_timestamptz",
    "max",
    "md5",
    "min",
    "mod",
    "mode",
    "nlevel",
    "now",
    "nth_value",
    "ntile",
    "nullif",
    "num_nonnulls",
    "num_nulls",
    "obj_description",
    "octet_length",
    "overlay",
    "parse_ident",
    "percent_rank",
    "percentile_cont",
    "percentile_disc",
    "pg_backend_pid",
    "pg_client_encoding",
    "pg_column_size",
    "pg_conf_load_time",
    "pg_current_wal_flush_lsn",
    "pg_current_wal_insert_lsn",
    "pg_current_wal_lsn",
    "pg_database_size",
    "pg_get_constraintdef",
    "pg_get_expr",
    "pg_get_functiondef",
    "pg_get_indexdef",
    "pg_get_userbyid",
    "pg_get_viewdef",
    "pg_has_role",
    "pg_indexes_size",
    "pg_is_in_recovery",
    "pg_last_wal_receive_lsn",
    "pg_last_wal_replay_lsn",
    "pg_last_xact_replay_timestamp",
    "pg_postmaster_start_time",
    "pg_relation_size",
    "pg_size_bytes",
    "pg_size_pretty",
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    "pg_table_size",
    "pg_total_relation_size",
    "pg_typeof",
    "pi",
    "position",
    "power",
    "quote_ident",
    "quote_literal",
    "quote_nullable",
    "radians",
    "random",
    "rank",
    "regexp_count",
    "regexp_instr",
    "regexp_like",
    "regexp_match",
    "regexp_matches",
    "regexp_replace",
    "regexp_split_to_array",
    "regexp_split_to_table",
    "regexp_substr",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "repeat",
    "replace",
    "reverse",
    "right",
    "round",
    "row_number",
    "row_to_json",
    "rpad",
    "rtrim",
    "scale",
    "set_bit",
    "set_byte",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "sign",
    "sin",
    "sinh",
    "split_part",
    "sqrt",
    "starts_with",
    "statement_timestamp",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "string_agg",
    "string_to_array",
    "string_to_table",
    "strpos",
    "substr",
    "substring",
    "sum",
    "tan",
    "tanh",
    "timeofday",
    "to_ascii",
    "to_char",
    "to_date",
    "to_hex",
    "to_json",
    "to_jsonb",
    "to_number",
    "to_timestamp",
    "to_tsquery",
    "to_tsvector",
    "transaction_timestamp",
    "translate",
    "trim",
    "trim_scale",
    "trunc",
    "unnest",
    "upper",
    "var_pop",
    "var_samp",
    "variance",
    "version",
    "width_bucket",
    "xmlagg",
];

/// SQL keywords that can be followed by `(` without being a function call.
const TR_CALL_KEYWORDS: &[&str] = &[
    "all",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "between",
    "by",
    "case",
    "cast",
    "collate",
    "cross",
    "date",
    "desc",
    "distinct",
    "else",
    "end",
    "except",
    "exists",
    "filter",
    "first",
    "following",
    "for",
    "from",
    "full",
    "group",
    "having",
    "in",
    "inner",
    "intersect",
    "interval",
    "is",
    "join",
    "lateral",
    "least",
    "left",
    "limit",
    "natural",
    "not",
    "null",
    "nulls",
    "offset",
    "on",
    "or",
    "order",
    "outer",
    "over",
    "partition",
    "preceding",
    "range",
    "returning",
    "right",
    "row",
    "rows",
    "select",
    "some",
    "table",
    "then",
    "time",
    "timestamp",
    "unbounded",
    "union",
    "using",
    "values",
    "when",
    "where",
    "window",
    "with",
    "within",
];

/// Operator-extended read-eligibility policy (`tr_read_functions`).
#[derive(Debug, Default)]
pub struct TrReadPolicy {
    /// Lowercased extra function names treated as side-effect-free.
    extra: std::collections::HashSet<String>,
}

impl TrReadPolicy {
    pub fn from_config(names: &[String]) -> Self {
        Self {
            extra: names.iter().map(|n| n.to_ascii_lowercase()).collect(),
        }
    }

    fn allows_call(&self, name: &str) -> bool {
        // Names are ASCII identifiers by validation; fold without allocating
        // for the overwhelmingly common short case.
        let mut buf = [0u8; 64];
        let lower: &str = if name.len() <= buf.len() && name.is_ascii() {
            let b = &mut buf[..name.len()];
            b.copy_from_slice(name.as_bytes());
            b.make_ascii_lowercase();
            std::str::from_utf8(b).unwrap_or(name)
        } else {
            return self.extra.contains(&name.to_ascii_lowercase());
        };
        TR_CALL_KEYWORDS.binary_search(&lower).is_ok()
            || TR_PURE_BUILTINS.binary_search(&lower).is_ok()
            || self.extra.contains(lower)
    }
}

/// The budgets one streaming relay needs, resolved from `[limits]`. Only the
/// cache-capture relay takes them as a bundle (clippy's argument cap); the plain
/// relays read `state.limits` directly, so this is gated with that relay.
#[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
#[derive(Debug, Clone, Copy)]
struct RelayLimits {
    client_write_timeout: Duration,
    backend_read_timeout: Duration,
    /// H-07 backend frame budget (`[limits] max_backend_frame_bytes`).
    max_frame_bytes: usize,
}

fn validate_backend_frame_len(len: usize, max: usize) -> Result<()> {
    if len > max {
        return Err(ProxyError::Protocol(format!(
            "backend frame length {} exceeds max {}",
            len, max
        )));
    }
    Ok(())
}

/// Operational limits/timeouts resolved once at startup from the `[limits]`
/// TOML section ([`crate::config::LimitsToml`]). The `*_secs` config keys are
/// converted to [`Duration`] here (at construction, not per-use) so the hot
/// path reads a ready `Duration`/`usize` with no conversion. Defaults are the
/// exact prior compiled-in constants, so a default config is unchanged.
#[derive(Debug, Clone)]
struct ResolvedLimits {
    max_cancel_keys: usize,
    startup_timeout: Duration,
    backend_write_timeout: Duration,
    backend_read_timeout: Duration,
    client_write_timeout: Duration,
    reprepare_timeout: Duration,
    max_prepared_statements: usize,
    max_prepared_bytes: usize,
    max_pending_bytes: usize,
    /// Cap on one backend response frame's declared length on every streaming
    /// relay (H-07); `[limits] max_backend_frame_bytes`.
    max_backend_frame_bytes: usize,
    /// Only read on the pool-modes data path; gated to avoid a dead-field
    /// warning on feature-off builds.
    #[cfg(feature = "pool-modes")]
    max_total_idle_backend_conns: usize,
    pool_reap_interval: Duration,
    /// Ceiling on concurrently-served client connections. `0` = unlimited (the
    /// default), which is the pre-cap behaviour. Read once here at startup —
    /// see `ServerState::client_slots`.
    max_client_connections: usize,
    /// Idle-session deadline for an authenticated client. `None` when
    /// `client_idle_timeout_secs = 0` (the default), which is the pre-timeout
    /// behaviour: the query loop then waits on the client forever.
    client_idle_timeout: Option<Duration>,
    /// In-session TR: cap on recorded statements per explicit transaction.
    tr_max_replay_statements: usize,
    /// In-session TR: cap on recorded bytes per explicit transaction.
    tr_max_replay_bytes: usize,
    /// In-session TR: cap on tracked session `SET`/`RESET` statements.
    tr_max_session_set_statements: usize,
}

impl ResolvedLimits {
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    fn relay(&self) -> RelayLimits {
        RelayLimits {
            client_write_timeout: self.client_write_timeout,
            backend_read_timeout: self.backend_read_timeout,
            max_frame_bytes: self.max_backend_frame_bytes,
        }
    }

    fn from_toml(l: &crate::config::LimitsToml) -> Self {
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
            #[cfg(feature = "pool-modes")]
            max_total_idle_backend_conns: l.max_total_idle_backend_conns,
            pool_reap_interval: Duration::from_secs(l.pool_reap_interval_secs),
            max_client_connections: l.max_client_connections,
            client_idle_timeout: match l.client_idle_timeout_secs {
                0 => None,
                secs => Some(Duration::from_secs(secs)),
            },
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

/// Server runtime state
struct ServerState {
    /// Operational limits/timeouts resolved from `[limits]` config at startup.
    limits: ResolvedLimits,
    /// Permit pool bounding concurrently-served client connections, sized from
    /// `[limits] max_client_connections` when that is > 0 (`None` = unlimited,
    /// the default and the historical behaviour). One owned permit is taken per
    /// connection by `admit_client_slot` — after its first startup message is
    /// classified, so a `CancelRequest` is never refused — and handed to the
    /// connection's `SessionGuard`, so the permit is returned on every exit
    /// path including a panic unwind. Sized ONCE at startup: a SIGHUP reload
    /// cannot shrink it (permits already held by in-flight sessions could not
    /// be revoked), so the reload path logs and ignores a change to the key.
    client_slots: Option<Arc<tokio::sync::Semaphore>>,
    /// Active client sessions. A `DashMap` (not a `tokio::RwLock<HashMap>`) so
    /// register/deregister are synchronous and lock-free-sharded — which lets a
    /// `SessionGuard`'s `Drop` remove the entry on ANY exit, including a panic
    /// unwind, and removes a per-connection async write-lock from the accept and
    /// teardown paths.
    sessions: DashMap<Uuid, Arc<ClientSession>>,
    /// Node health status
    // Read-mostly: only the periodic health checker writes (a full-map
    // swap), every query reads. ArcSwap makes the per-query read a single
    // lock-free atomic load with no await, no semaphore, no guard held
    // across the routing awaits.
    health: ArcSwap<HashMap<String, NodeHealth>>,
    /// Write-serialization lock for `health`. Every reader stays lock-free on
    /// the ArcSwap; every *writer* (periodic checker, in-band demotion, SIGHUP
    /// reconcile) holds this across its load → clone → mutate → store so the
    /// non-atomic read-modify-write cannot lose updates under concurrency.
    health_write: parking_lot::Mutex<()>,
    /// Live, reloadable proxy configuration (Batch H). The accept loop snapshots
    /// this per new connection and the health checker reads it each tick, so a
    /// SIGHUP that swaps it takes effect for new connections and node health
    /// without dropping any in-flight session. The fields that can only be
    /// applied at startup (listen/admin socket addresses) are ignored on reload
    /// with a warning. Existing connections keep the snapshot they started with.
    live_config: ArcSwap<ProxyConfig>,
    /// Metrics
    metrics: ServerMetrics,
    /// Query-cancellation routing. Maps the BackendKeyData (pid, secret)
    /// the backend handed to the client onto the backend address that
    /// issued it, so a later out-of-band CancelRequest (which arrives on a
    /// fresh connection) can be forwarded to the right backend instead of
    /// being dropped. Bounded; best-effort.
    cancel_map: Arc<DashMap<(u32, u32), String>>,
    /// Insertion order of `cancel_map` keys, so an overflow evicts the OLDEST
    /// entries (FIFO) instead of clearing the whole map — a busy proxy no
    /// longer loses every in-flight cancel registration at once.
    cancel_order: Arc<parking_lot::Mutex<std::collections::VecDeque<(u32, u32)>>>,
    /// Client-facing TLS acceptor, built from `[tls]` config when enabled.
    /// `None` => the proxy rejects SSLRequests with `N` (plaintext only).
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    /// Proxy-terminated SCRAM auth state. `Some` when `[auth] mode = "scram"`:
    /// the proxy authenticates clients itself against this user list instead
    /// of relaying their credentials to the backend.
    auth_file: Option<Arc<crate::auth_scram::AuthFile>>,
    /// Traffic-mirror handle. `Some` when `[mirror] enabled`: the data path
    /// offers write statements to a background mirror worker.
    mirror: Option<crate::mirror::MirrorHandle>,
    /// Migration cutover switch. When `Some`, NEW client connections are
    /// transparently redirected to the promoted target (the former mirror)
    /// instead of the configured primary. Set via POST /api/migration/cutover.
    cutover: Arc<ArcSwap<Option<Arc<crate::mirror::CutoverTarget>>>>,
    /// Load balancer state
    lb_state: LoadBalancerState,
    /// SQL-comment routing-hint parser. `Some` when `[routing_hints] enabled`
    /// and the `routing-hints` feature is compiled in; the parser's own
    /// `strip_hints` flag records whether to rewrite the SQL before forwarding.
    /// Applied per query, taking precedence over default verb routing.
    #[cfg(feature = "routing-hints")]
    hint_parser: Option<crate::routing::HintParser>,
    /// Multi-dimensional rate limiter. `Some` when `[rate_limit] enabled`;
    /// every query is checked against it before being forwarded to a backend.
    #[cfg(feature = "rate-limiting")]
    rate_limiter: Option<Arc<crate::rate_limit::RateLimiter>>,
    /// Per-node circuit breaker manager. `Some` when `[circuit_breaker]
    /// enabled`. Records per-node success/failure on the forward path, excludes
    /// open nodes from read selection, and fast-fails queries to an open node.
    #[cfg(feature = "circuit-breaker")]
    circuit_breaker: Option<Arc<crate::circuit_breaker::CircuitBreakerManager>>,
    /// Query analytics engine. `Some` when `[analytics] enabled`. Every
    /// forwarded query is recorded (fingerprint, latency, slow-query log).
    #[cfg(feature = "query-analytics")]
    analytics: Option<Arc<crate::analytics::QueryAnalytics>>,
    /// Query-result cache (L1 hot / L2 warm). `Some` when `[cache] enabled`.
    /// Read SELECTs are served from it; writes invalidate referenced tables.
    #[cfg(feature = "query-cache")]
    query_cache: Option<Arc<crate::cache::QueryCache>>,
    /// SQL query rewriter. `Some` when `[query_rewrite] enabled` with rules.
    /// Rewrites the query SQL on the path before forwarding.
    #[cfg(feature = "query-rewriting")]
    rewriter: Option<Arc<crate::rewriter::QueryRewriter>>,
    /// Multi-tenancy manager. `Some` when `[multi_tenancy] enabled`. Identifies
    /// the tenant for a session and injects a row-level tenant filter.
    #[cfg(feature = "multi-tenancy")]
    tenant_manager: Option<Arc<crate::multi_tenancy::TenantManager>>,
    /// Schema/workload query analyzer. `Some` when `[schema_routing] enabled`;
    /// analytical (OLAP) queries are routed to the configured analytics node.
    #[cfg(feature = "schema-routing")]
    schema_analyzer: Option<Arc<crate::schema_routing::QueryAnalyzer>>,
    /// Pool manager for Session/Transaction/Statement modes
    #[cfg(feature = "pool-modes")]
    pool_manager: Option<Arc<ConnectionPoolManager>>,
    /// Data-path idle backend-connection pool. `Some` only when pooling is
    /// active (mode is Transaction or Statement); `None` leaves the 1:1
    /// session-pinned path completely unchanged. This is the raw-stream pool
    /// the data path actually leases from, keyed by `(node, user, database)`.
    #[cfg(feature = "pool-modes")]
    backend_pool: Option<Arc<crate::pool::BackendIdlePool>>,
    /// WASM plugin manager. `None` means no plugins loaded — the per-query
    /// hook path becomes a fast no-op. When `Some`, `PreQuery` / `PostQuery`
    /// hooks fire on every simple-query message.
    #[cfg(feature = "wasm-plugins")]
    plugin_manager: Option<Arc<PluginManager>>,
    /// Shared transaction journal — single sink for per-session
    /// statement journaling. The replay engine reads windows from
    /// this directly. Always present when the `ha-tr` feature is on;
    /// journaling self-disables internally when not configured.
    #[cfg(feature = "ha-tr")]
    transaction_journal: Arc<crate::transaction_journal::TransactionJournal>,
    /// TR-03 read re-execution eligibility (built-ins + `tr_read_functions`).
    tr_read_policy: Arc<TrReadPolicy>,
    /// Anomaly detector (T3.1). Records every query and every
    /// auth outcome; surfaces detections via /api/anomalies.
    #[cfg(feature = "anomaly-detection")]
    anomaly_detector: Arc<crate::anomaly::AnomalyDetector>,
    /// Edge cache + home registry (T3.2). Both always-present even
    /// in Home mode (the cache is a no-op there); avoids an extra
    /// Option in the hot path.
    #[cfg(feature = "edge-proxy")]
    edge_cache: Arc<crate::edge::EdgeCache>,
    #[cfg(feature = "edge-proxy")]
    edge_registry: Arc<crate::edge::EdgeRegistry>,
}

/// Node health status
#[derive(Debug, Clone)]
pub struct NodeHealth {
    /// Node address
    pub address: String,
    /// Whether node is healthy
    pub healthy: bool,
    /// Last check time
    pub last_check: chrono::DateTime<chrono::Utc>,
    /// Consecutive failures
    pub failure_count: u32,
    /// Last error message
    pub last_error: Option<String>,
    /// Average latency (ms)
    pub latency_ms: f64,
    /// Replication lag (if applicable)
    pub replication_lag_bytes: Option<u64>,
}

/// Server metrics
#[derive(Default)]
struct ServerMetrics {
    /// Total connections accepted
    connections_accepted: AtomicU64,
    /// Total connections refused because the `[limits] max_client_connections`
    /// cap was already saturated. The refusal happens after the socket is
    /// accepted and its first startup message classified (so a `CancelRequest`
    /// is never refused), hence a rejected connection IS also counted in
    /// `connections_accepted` — and in `connections_closed` when it ends, so
    /// their difference stays a correct active-session gauge. This counter is
    /// the separate "how often did the cap bite" signal.
    connections_rejected: AtomicU64,
    /// Total connections closed
    connections_closed: AtomicU64,
    /// Total queries processed
    queries_processed: AtomicU64,
    /// Total bytes received from clients
    bytes_received: AtomicU64,
    /// Total bytes sent to clients
    bytes_sent: AtomicU64,
    /// Failover count
    failovers: AtomicU64,
    /// Responses whose capture-for-caching was abandoned because the response
    /// exceeded `[cache] max_cacheable_response_bytes`. Non-zero means reads
    /// are bypassing the response caches on size grounds (the client still
    /// receives every byte) — either the workload returns huge result sets or
    /// the ceiling is set too low.
    cache_capture_oversize: AtomicU64,
    /// In-session Transaction Replay (`tr_mode`) counters.
    tr: TrMetrics,
}

/// In-session Transaction Replay counters (see `TrMode` / `tr_decide`).
#[derive(Default)]
struct TrMetrics {
    /// Sessions re-homed onto a replacement backend after a fault.
    failovers: AtomicU64,
    /// In-flight statements transparently re-executed on the new backend.
    statements_reexecuted: AtomicU64,
    /// Explicit transactions successfully replayed on the new backend.
    transactions_replayed: AtomicU64,
    /// Transaction replays that failed (client received SQLSTATE 40001).
    replay_failures: AtomicU64,
    /// SQLSTATE 08007 `transaction_resolution_unknown` errors returned.
    unknown_outcome_errors: AtomicU64,
    /// Transactions marked non-replayable because they exceeded
    /// `[limits] tr_max_replay_statements` / `tr_max_replay_bytes`.
    replay_cap_exceeded: AtomicU64,
    /// Sessions whose `SET` tracking stopped at
    /// `[limits] tr_max_session_set_statements`.
    session_set_cap_exceeded: AtomicU64,
}

impl TrMetrics {
    fn snapshot(&self) -> TrMetricsSnapshot {
        TrMetricsSnapshot {
            failovers: self.failovers.load(Ordering::Relaxed),
            statements_reexecuted: self.statements_reexecuted.load(Ordering::Relaxed),
            transactions_replayed: self.transactions_replayed.load(Ordering::Relaxed),
            replay_failures: self.replay_failures.load(Ordering::Relaxed),
            unknown_outcome_errors: self.unknown_outcome_errors.load(Ordering::Relaxed),
            replay_cap_exceeded: self.replay_cap_exceeded.load(Ordering::Relaxed),
            session_set_cap_exceeded: self.session_set_cap_exceeded.load(Ordering::Relaxed),
        }
    }
}

/// Load balancer state
struct LoadBalancerState {
    /// Round-robin counter. Atomic so the read-routing path never
    /// takes a write lock just to advance the rotation.
    rr_counter: AtomicU64,
}

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
struct BackendConn {
    stream: TcpStream,
    prepared: HashSet<String>,
    /// Signature (query text + parameter-type OIDs) of the *unnamed* prepared
    /// statement currently established on this socket, if any. When the client
    /// re-sends an identical unnamed `Parse`, the proxy can skip forwarding it
    /// (the backend's unnamed statement already holds that SQL) and synthesize
    /// the `ParseComplete` locally — the unnamed-Parse promotion (Batch H).
    unnamed_sig: Option<bytes::Bytes>,
    /// Whether a simple-query statement forwarded on this socket may have left
    /// session-level state behind (a `SET`, temp table, `LISTEN`, advisory
    /// lock, …). Used only by the conditional-reset optimisation
    /// (`pool_mode.skip_clean_reset`): a connection is eligible to be parked
    /// WITHOUT running the reset query only when it is provably clean —
    /// `!dirty && prepared.is_empty() && unnamed_sig.is_none()`. Set
    /// conservatively (any statement not provably session-neutral sets it), so
    /// the worst outcome of a misclassification is an unnecessary reset, never
    /// leaked state. Always `false` on a fresh/reused connection.
    dirty: bool,
}

impl BackendConn {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            prepared: HashSet::new(),
            unnamed_sig: None,
            dirty: false,
        }
    }
}

/// Bind a TCP listener with `SO_REUSEADDR` + `SO_REUSEPORT` so a second process
/// can bind the same address concurrently (the kernel then load-balances new
/// connections across both). This is what lets a new binary take over new
/// connections while the old one drains — used for both the client and admin
/// listeners so a binary handoff can re-bind every address (Batch H).
pub(crate) fn bind_reuseport(addr: &str) -> Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let sockaddr: SocketAddr = addr
        .parse()
        .map_err(|e| ProxyError::Config(format!("invalid listen address '{}': {}", addr, e)))?;
    let domain = if sockaddr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .map_err(|e| ProxyError::Network(format!("socket(): {}", e)))?;
    socket
        .set_reuse_address(true)
        .map_err(|e| ProxyError::Network(format!("SO_REUSEADDR: {}", e)))?;
    #[cfg(all(unix, not(target_os = "solaris")))]
    socket
        .set_reuse_port(true)
        .map_err(|e| ProxyError::Network(format!("SO_REUSEPORT: {}", e)))?;
    socket
        .set_nonblocking(true)
        .map_err(|e| ProxyError::Network(format!("set_nonblocking: {}", e)))?;
    socket
        .bind(&sockaddr.into())
        .map_err(|e| ProxyError::Network(format!("Failed to bind {}: {}", addr, e)))?;
    socket
        .listen(1024)
        .map_err(|e| ProxyError::Network(format!("listen(): {}", e)))?;
    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
        .map_err(|e| ProxyError::Network(format!("from_std listener: {}", e)))
}

/// Disposition produced by the pre-query plugin hook stage.
///
/// When the `wasm-plugins` feature is off, only `Forward` is ever produced —
/// the hook dispatch is compiled out entirely and the variant list exists
/// purely for pattern-match symmetry.
#[derive(Debug)]
#[allow(dead_code)] // Block/Cached only constructed under wasm-plugins
enum PreQueryAction {
    /// Send the message to the backend as usual.
    Forward,
    /// A plugin blocked the query. The caller sends an error + ReadyForQuery
    /// to the client and skips backend forwarding.
    Block(String),
    /// A plugin returned a cached response. Not yet wired — response
    /// synthesis from raw bytes requires building a full protocol reply
    /// (RowDescription + DataRow(s) + CommandComplete + ReadyForQuery),
    /// which is the next step of T0-a. For now the caller falls back to
    /// `Forward` and logs a warning.
    Cached(Vec<u8>),
}

/// Override produced by the Route plugin hook. Consumed by `route_and_forward`
/// when deciding which backend to talk to.
///
/// As with `PreQueryAction`, only `None` is ever produced when the
/// `wasm-plugins` feature is off.
#[derive(Debug)]
#[allow(dead_code)] // Primary/Standby/Node/Block only constructed under wasm-plugins
enum RouteOverride {
    /// No override — use the default SQL-verb-based routing.
    None,
    /// Force the write path (use `select_primary_with_timeout`).
    Primary,
    /// Force the read path (use `select_read_node`).
    Standby,
    /// Use this exact node address. Takes precedence over the is_write
    /// heuristic; the proxy will still verify the node is healthy before
    /// connecting (via the normal switch-vs-reuse flow).
    Node(String),
    /// Reject the query: write a PG ErrorResponse + ReadyForQuery to
    /// the client and skip the forward. Carries the reason the plugin
    /// supplied. Takes precedence over every other field — the proxy
    /// short-circuits before any backend selection.
    Block(String),
}

/// Outcome of waiting for the client's next protocol message.
#[derive(Debug, PartialEq, Eq)]
enum ClientRead {
    /// Bytes were appended to the session buffer (`0` = the client closed).
    Bytes(usize),
    /// The `[limits] client_idle_timeout_secs` deadline expired while the
    /// session sat idle between statements.
    IdleTimeout,
}

/// RAII teardown for one client connection. Its `Drop` deregisters the session
/// from `state.sessions`, bumps the connections-closed metric, and reclaims the
/// session's L1 query cache — running on a normal return AND on a panic unwind,
/// so a panic in negotiation/startup/the query loop can never leak the session
/// entry (which would inflate the active-session gauge and stall graceful
/// drains). All operations are synchronous, so they are valid inside `Drop`.
struct SessionGuard {
    state: Arc<ServerState>,
    session_id: Uuid,
    /// Owned client-connection permit taken by admission control once the
    /// connection's first startup message is classified (`None` when
    /// `[limits] max_client_connections = 0`, i.e. no cap, or for a
    /// `CancelRequest`, which never consumes one). Held here purely so that
    /// dropping the guard returns the slot — on a normal return AND on a panic
    /// unwind, which a release at the end of `handle_client` would miss.
    _client_slot: Option<tokio::sync::OwnedSemaphorePermit>,
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

// Test-only tally of classifier evaluations performed on the CURRENT thread.
// `libtest` runs each test on its own thread, so the count is per-test and
// race-free; `StmtFacts` bumps it every time it actually walks the SQL, which
// is what lets the laziness contract be asserted rather than assumed.
#[cfg(test)]
thread_local! {
    static STMT_FACT_CLASSIFICATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Classifier evaluations performed on this thread so far (test-only).
#[cfg(test)]
fn stmt_fact_classifications() -> usize {
    STMT_FACT_CLASSIFICATIONS.with(std::cell::Cell::get)
}

/// The cheap lexical facts about ONE simple-query statement, memoized so each
/// fact is derived AT MOST ONCE per message — and ONLY if a gate that is
/// actually enabled asks for it.
///
/// Every getter delegates to the classifier that already owned that decision
/// (`is_write_query`, `stmt_leaves_session_state`, `is_cacheable_read_sql`,
/// …), so the semantics are byte-identical. What changes is the number of
/// passes over the SQL: `forward_simple_query` used to re-derive several of
/// these two or three times for the same string (once per *call site*); it now
/// derives each at most once (once per *fact*).
///
/// The memo is deliberately LAZY. Each of these classifiers sat behind a
/// runtime gate that is OFF in the stock configuration —
/// `[pool_mode] skip_clean_reset = false`, `[cache] enabled = false`,
/// `[edge] enabled = false` — so a default proxy classified the leading
/// keyword once and nothing else. Computing the whole set up front would make
/// that default hot path do strictly MORE work than before, worst of all for
/// the big statements (bulk INSERT, large SELECT text) these classifiers scan
/// end to end. Every getter is therefore called from inside the very gate that
/// used to guard the classification, and short-circuits with it.
///
/// Facts describe the text they were computed from: `sql` is borrowed from the
/// message payload. When a routing-hint strip, a rewrite rule, or a tenant
/// transform replaces the SQL, the caller rebuilds the whole value on the final
/// text — which resets every memo cell — before any gate consults it.
///
/// Cells are `cfg`-gated to the features that consume them so the struct
/// carries no dead state in a minimal build.
#[derive(Debug)]
struct StmtFacts<'a> {
    /// The statement text every cell is derived from. `""` when the payload
    /// carried no valid query cstring — the same fallback the individual call
    /// sites used (`unwrap_or("")`, or a skipped `if let Some(sql)`: all four
    /// classifiers answer `false` for the empty string, so the two agree).
    sql: &'a str,
    /// `is_write_query`: routing-relevant write / transaction-control / SET.
    is_write: Option<bool>,
    /// A `;` before the (optional) trailing one — i.e. the simple-query string
    /// carries more than one statement, so no leading-keyword classification
    /// can vouch for what follows.
    #[cfg(feature = "edge-proxy")]
    has_interior_semicolon: Option<bool>,
    /// `stmt_leaves_session_state`: not provably session-neutral.
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    leaves_session_state: Option<bool>,
    /// `is_cacheable_read_sql`: plain, deterministic, single-statement SELECT.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    is_cacheable_read: Option<bool>,
}

impl<'a> StmtFacts<'a> {
    /// Facts for `sql`. Nothing is classified here — every cell is empty
    /// until a getter asks for it.
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            is_write: None,
            #[cfg(feature = "edge-proxy")]
            has_interior_semicolon: None,
            #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
            leaves_session_state: None,
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            is_cacheable_read: None,
        }
    }

    /// Facts for a simple `Query` message, borrowing the SQL straight out of
    /// the payload (the message is forwarded verbatim, so no copy is needed).
    fn of_query(msg: &'a Message) -> Self {
        Self::new(crate::protocol::query_text(&msg.payload).unwrap_or(""))
    }

    /// The statement text the facts describe — so a gate that also needs the
    /// SQL itself does not re-walk the payload for its own copy.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    fn sql(&self) -> &'a str {
        self.sql
    }

    /// Compute-on-first-use: run `classify` over the statement the first time
    /// a cell is read, remember the answer, never run it again.
    fn memo<F: FnOnce(&str) -> bool>(cell: &mut Option<bool>, sql: &'a str, classify: F) -> bool {
        match *cell {
            Some(v) => v,
            None => {
                Self::note_classification();
                let v = classify(sql);
                *cell = Some(v);
                v
            }
        }
    }

    #[cfg(test)]
    fn note_classification() {
        STMT_FACT_CLASSIFICATIONS.with(|c| c.set(c.get() + 1));
    }

    #[cfg(not(test))]
    #[inline(always)]
    fn note_classification() {}

    /// See [`ProxyServer::is_write_query`].
    fn is_write(&mut self) -> bool {
        Self::memo(&mut self.is_write, self.sql, ProxyServer::is_write_query)
    }

    /// See [`ProxyServer::stmt_has_interior_semicolon`].
    #[cfg(feature = "edge-proxy")]
    fn has_interior_semicolon(&mut self) -> bool {
        Self::memo(
            &mut self.has_interior_semicolon,
            self.sql,
            ProxyServer::stmt_has_interior_semicolon,
        )
    }

    /// See [`ProxyServer::stmt_leaves_session_state`].
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    fn leaves_session_state(&mut self) -> bool {
        Self::memo(
            &mut self.leaves_session_state,
            self.sql,
            ProxyServer::stmt_leaves_session_state,
        )
    }

    /// See [`ProxyServer::is_cacheable_read_sql`].
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    fn is_cacheable_read(&mut self) -> bool {
        Self::memo(
            &mut self.is_cacheable_read,
            self.sql,
            ProxyServer::is_cacheable_read_sql,
        )
    }
}

impl ProxyServer {
    /// Build a `PluginManager` from config and preload plugins from disk.
    ///
    /// Returns `None` when plugins are disabled in config, when the
    /// runtime fails to initialise, or when the plugin directory is
    /// missing. Individual per-file load failures are logged but do not
    /// abort startup — the remaining plugins load normally and the
    /// proxy stays up.
    #[cfg(feature = "wasm-plugins")]
    fn init_plugin_manager(
        toml_cfg: &crate::config::PluginToml,
    ) -> Option<Arc<crate::plugins::PluginManager>> {
        if !toml_cfg.enabled {
            return None;
        }

        let runtime_cfg = crate::plugins::PluginRuntimeConfig::from(toml_cfg);
        let plugin_dir = runtime_cfg.plugin_dir.clone();

        let pm = match crate::plugins::PluginManager::new(runtime_cfg) {
            Ok(pm) => Arc::new(pm),
            Err(e) => {
                tracing::error!(error = %e, "Failed to create plugin manager; plugins disabled");
                return None;
            }
        };

        match std::fs::read_dir(&plugin_dir) {
            Ok(entries) => {
                let mut loaded = 0usize;
                let mut failed = 0usize;
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) != Some("wasm") {
                        continue;
                    }
                    match pm.load_plugin(&path) {
                        Ok(()) => loaded += 1,
                        Err(e) => {
                            failed += 1;
                            tracing::warn!(
                                path = %path.display(),
                                error = %e,
                                "Failed to load plugin"
                            );
                        }
                    }
                }
                tracing::info!(
                    dir = %plugin_dir.display(),
                    loaded = loaded,
                    failed = failed,
                    "Plugin loading complete"
                );
            }
            Err(e) => {
                tracing::warn!(
                    dir = %plugin_dir.display(),
                    error = %e,
                    "Plugin directory not readable; no plugins loaded"
                );
            }
        }

        Some(pm)
    }

    /// Create a new proxy server
    pub fn new(config: ProxyConfig) -> Result<Self> {
        let (shutdown_tx, _) = broadcast::channel(1);

        // Resolve the [limits] section once (secs -> Duration) so every hot-path
        // use site reads a ready value; stored on ServerState below.
        let resolved_limits = ResolvedLimits::from_toml(&config.limits);

        // Initialize health status
        let mut health = HashMap::new();
        for node in &config.nodes {
            health.insert(
                node.address().to_string(),
                NodeHealth {
                    address: node.address().to_string(),
                    healthy: true, // Assume healthy until proven otherwise
                    last_check: chrono::Utc::now(),
                    failure_count: 0,
                    last_error: None,
                    latency_ms: 0.0,
                    replication_lag_bytes: None,
                },
            );
        }

        // Initialize pool manager if pool-modes feature is enabled
        #[cfg(feature = "pool-modes")]
        let pool_manager = {
            use crate::pool::PreparedStatementMode as PoolPreparedStatementMode;

            let pool_config = PoolModeConfig {
                default_mode: match config.pool_mode.mode {
                    crate::config::PoolingMode::Session => PoolingMode::Session,
                    crate::config::PoolingMode::Transaction => PoolingMode::Transaction,
                    crate::config::PoolingMode::Statement => PoolingMode::Statement,
                },
                max_pool_size: config.pool_mode.max_pool_size,
                min_idle: config.pool_mode.min_idle,
                idle_timeout_secs: config.pool_mode.idle_timeout_secs,
                max_lifetime_secs: config.pool_mode.max_lifetime_secs,
                acquire_timeout_secs: config.pool_mode.acquire_timeout_secs,
                reset_query: config.pool_mode.reset_query.clone(),
                prepared_statement_mode: match config.pool_mode.prepared_statement_mode {
                    crate::config::PreparedStatementMode::Disable => {
                        PoolPreparedStatementMode::Disable
                    }
                    crate::config::PreparedStatementMode::Track => PoolPreparedStatementMode::Track,
                    crate::config::PreparedStatementMode::Named => PoolPreparedStatementMode::Named,
                },
                test_on_acquire: config.pool.test_on_acquire,
                validation_query: "SELECT 1".to_string(),
                queue_timeout_secs: 30,
                max_queue_size: 0,
            };
            Some(Arc::new(ConnectionPoolManager::new(pool_config)))
        };

        // The raw-stream data-path pool is only built when pooling is active
        // (Transaction/Statement). Session mode leaves it `None` so the hot
        // path is byte-for-byte unchanged.
        #[cfg(feature = "pool-modes")]
        let backend_pool = match config.pool_mode.mode {
            crate::config::PoolingMode::Transaction | crate::config::PoolingMode::Statement => {
                tracing::info!(
                    mode = ?config.pool_mode.mode,
                    max_idle_per_identity = config.pool_mode.max_pool_size,
                    "pool-modes: data-path connection pooling enabled"
                );
                Some(Arc::new(crate::pool::BackendIdlePool::new(
                    config.pool_mode.max_pool_size as usize,
                    resolved_limits.max_total_idle_backend_conns,
                )))
            }
            crate::config::PoolingMode::Session => None,
        };

        // Initialize plugin manager if the wasm-plugins feature is enabled
        // AND plugins are turned on in config. Scans plugin_dir for `.wasm`
        // files and loads each; a missing directory is non-fatal and logs
        // a warning so empty deployments don't fail startup.
        #[cfg(feature = "wasm-plugins")]
        let plugin_manager = Self::init_plugin_manager(&config.plugins);

        // Build the client TLS acceptor if [tls] is configured + enabled.
        // A bad cert/key is fatal at startup (fail fast, don't silently
        // fall back to plaintext for a deployment that asked for TLS).
        let tls_acceptor = match config.tls.as_ref() {
            Some(tls) if tls.enabled => match build_tls_acceptor(tls) {
                Ok(acc) => {
                    tracing::info!(
                        mtls = tls.require_client_cert,
                        "client TLS termination enabled"
                    );
                    Some(acc)
                }
                Err(e) => {
                    return Err(ProxyError::Config(format!("TLS init failed: {}", e)));
                }
            },
            _ => None,
        };

        // Load the SCRAM auth_file when proxy-terminated auth is requested.
        // Misconfiguration is fatal at startup (fail fast).
        let auth_file = if config.auth.mode == crate::config::AuthMode::Scram {
            let path = config.auth.auth_file.as_ref().ok_or_else(|| {
                ProxyError::Config("auth mode 'scram' requires auth_file".to_string())
            })?;
            let af = crate::auth_scram::AuthFile::load(path)
                .map_err(|e| ProxyError::Config(format!("auth_file: {}", e)))?;
            tracing::info!(users = %(!af.is_empty()), "proxy SCRAM auth enabled");
            Some(Arc::new(af))
        } else {
            None
        };

        // Spawn the traffic-mirror worker when enabled (we are inside the
        // tokio runtime here — main is #[tokio::main]).
        let mirror = if config.mirror.enabled {
            tracing::info!(target = %format!("{}:{}", config.mirror.backend_host, config.mirror.backend_port),
                writes_only = config.mirror.writes_only, "traffic mirroring enabled");
            Some(crate::mirror::spawn(config.mirror.clone()))
        } else {
            None
        };

        // Build the rate limiter from the TOML config when enabled.
        #[cfg(feature = "rate-limiting")]
        let rate_limiter = if config.rate_limit.enabled {
            let rl = &config.rate_limit;
            tracing::info!(
                qps = rl.default_qps,
                burst = rl.default_burst,
                key_by = ?rl.key_by,
                "rate limiting enabled"
            );
            let rlc = crate::rate_limit::RateLimitConfig {
                enabled: true,
                default_qps: rl.default_qps,
                default_burst: rl.default_burst,
                default_concurrency: if rl.max_concurrent > 0 {
                    rl.max_concurrent
                } else {
                    crate::rate_limit::RateLimitConfig::default().default_concurrency
                },
                ..Default::default()
            };
            Some(Arc::new(crate::rate_limit::RateLimiter::new(rlc)))
        } else {
            None
        };

        // Build the per-node circuit breaker manager when enabled.
        #[cfg(feature = "circuit-breaker")]
        let circuit_breaker = if config.circuit_breaker.enabled {
            let cb = &config.circuit_breaker;
            tracing::info!(
                failure_threshold = cb.failure_threshold,
                open_secs = cb.open_secs,
                "circuit breaker enabled"
            );
            let cbc = crate::circuit_breaker::CircuitBreakerConfig {
                failure_threshold: cb.failure_threshold,
                cooldown: Duration::from_secs(cb.open_secs),
                half_open_success_threshold: cb.success_threshold,
                ..Default::default()
            };
            let mgr = crate::circuit_breaker::CircuitBreakerManager::new(
                crate::circuit_breaker::ManagerConfig::new(cbc),
            );
            Some(Arc::new(mgr))
        } else {
            None
        };

        // Build the query-analytics engine when enabled.
        #[cfg(feature = "query-analytics")]
        let analytics = if config.analytics.enabled {
            let a = &config.analytics;
            tracing::info!(
                slow_query_ms = a.slow_query_ms,
                max_fingerprints = a.max_fingerprints,
                queue_capacity = a.queue_capacity,
                fingerprint_cache_size = a.fingerprint_cache_size,
                fingerprint_cache_max_sql_bytes = a.fingerprint_cache_max_sql_bytes,
                "query analytics enabled"
            );
            let ac = crate::analytics::AnalyticsConfig {
                enabled: true,
                max_fingerprints: a.max_fingerprints as usize,
                queue_capacity: a.queue_capacity as usize,
                fingerprint_cache_size: a.fingerprint_cache_size as usize,
                fingerprint_cache_max_sql_bytes: a.fingerprint_cache_max_sql_bytes as usize,
                slow_query: crate::analytics::SlowQueryConfig {
                    threshold: Duration::from_millis(a.slow_query_ms),
                    ..Default::default()
                },
                ..Default::default()
            };
            Some(Arc::new(crate::analytics::QueryAnalytics::new(ac)))
        } else {
            None
        };

        // Build the query-result cache when enabled.
        #[cfg(feature = "query-cache")]
        let query_cache = if config.cache.enabled {
            let c = &config.cache;
            tracing::info!(
                ttl_secs = c.ttl_secs,
                max_result_bytes = c.max_result_bytes,
                "query cache enabled (L1 hot + L2 warm)"
            );
            let ttl = Duration::from_secs(c.ttl_secs);
            let cc = crate::cache::CacheConfig {
                enabled: true,
                default_ttl: ttl,
                max_result_size: c.max_result_bytes,
                l1: crate::cache::L1Config {
                    ttl,
                    ..Default::default()
                },
                l2: crate::cache::L2Config {
                    ttl,
                    ..Default::default()
                },
                ..Default::default()
            };
            Some(Arc::new(crate::cache::QueryCache::new(cc)))
        } else {
            None
        };

        // Build the SQL query rewriter from the configured rules.
        #[cfg(feature = "query-rewriting")]
        let rewriter = if config.query_rewrite.enabled && !config.query_rewrite.rules.is_empty() {
            use crate::rewriter::{
                QueryPattern, QueryRewriter, RewriteRule, RewriterConfig, Transformation,
            };
            let rw = QueryRewriter::new(RewriterConfig {
                enabled: true,
                ..Default::default()
            });
            let mut n = 0usize;
            for (i, r) in config.query_rewrite.rules.iter().enumerate() {
                let transformation =
                    if let (Some(from), Some(to)) = (&r.match_table, &r.replace_table_with) {
                        Transformation::ReplaceTable {
                            from: from.clone(),
                            to: to.clone(),
                        }
                    } else if let Some(w) = &r.append_where {
                        Transformation::AppendWhereAnd(w.clone())
                    } else if let Some(limit) = r.add_limit {
                        Transformation::AddLimit(limit)
                    } else {
                        continue; // no transformation specified — skip
                    };
                let pattern = if let Some(t) = &r.match_table {
                    QueryPattern::Table(t.clone())
                } else if let Some(re) = &r.match_regex {
                    QueryPattern::regex(re.clone())
                } else {
                    QueryPattern::All
                };
                rw.add_rule(
                    RewriteRule::build(format!("rule-{i}"))
                        .pattern(pattern)
                        .transform(transformation)
                        .build(),
                );
                n += 1;
            }
            tracing::info!(rules = n, "query rewriting enabled");
            Some(Arc::new(rw))
        } else {
            None
        };

        // Build the multi-tenancy manager from the configured tenants.
        #[cfg(feature = "multi-tenancy")]
        let tenant_manager =
            if config.multi_tenancy.enabled && !config.multi_tenancy.tenants.is_empty() {
                use crate::multi_tenancy::{
                    IdentificationMethod, IsolationStrategy, MultiTenancyConfig, TenantConfig,
                    TenantId, TenantManagerBuilder, TenantQueryTransformer,
                };
                let mt = &config.multi_tenancy;
                let identification = match mt.identify_by.as_str() {
                    "database" => IdentificationMethod::DatabaseName,
                    param => IdentificationMethod::Header {
                        header_name: param.to_string(),
                    },
                };
                let mtc = MultiTenancyConfig {
                    enabled: true,
                    identification,
                    ..Default::default()
                };
                // Configure which tables are tenant-scoped + the filter column.
                let table_refs: Vec<&str> = mt.tenant_tables.iter().map(|s| s.as_str()).collect();
                let transformer = TenantQueryTransformer::new()
                    .register_tables(&table_refs, mt.tenant_column.clone());
                let tm = TenantManagerBuilder::new()
                    .config(mtc)
                    .query_transformer(transformer)
                    .build();
                for id in &mt.tenants {
                    tm.register_tenant(TenantConfig::new(
                        TenantId::new(id.clone()),
                        IsolationStrategy::row("public", mt.tenant_column.clone()),
                    ));
                }
                tracing::info!(
                    tenants = mt.tenants.len(),
                    identify_by = %mt.identify_by,
                    "multi-tenancy enabled"
                );
                Some(Arc::new(tm))
            } else {
                None
            };

        // Build the schema/workload query analyzer when enabled.
        #[cfg(feature = "schema-routing")]
        let schema_analyzer =
            if config.schema_routing.enabled && !config.schema_routing.analytics_node.is_empty() {
                tracing::info!(
                    analytics_node = %config.schema_routing.analytics_node,
                    "schema/workload routing enabled (OLAP -> analytics node)"
                );
                let registry = Arc::new(crate::schema_routing::SchemaRegistry::new());
                Some(Arc::new(crate::schema_routing::QueryAnalyzer::new(
                    registry,
                )))
            } else {
                None
            };

        // Size the client-connection permit pool once, here at startup. A `0`
        // cap (the default) leaves it `None` so the accept path is unchanged.
        let client_slots = match resolved_limits.max_client_connections {
            0 => None,
            n => {
                tracing::info!(
                    max_client_connections = n,
                    "client connection cap enabled; excess connections are refused with SQLSTATE 53300"
                );
                Some(Arc::new(tokio::sync::Semaphore::new(n)))
            }
        };
        let state = Arc::new(ServerState {
            limits: resolved_limits,
            client_slots,
            sessions: DashMap::new(),
            health: ArcSwap::from_pointee(health),
            health_write: parking_lot::Mutex::new(()),
            live_config: ArcSwap::from_pointee(config.clone()),
            metrics: ServerMetrics::default(),
            cancel_map: Arc::new(DashMap::new()),
            cancel_order: Arc::new(parking_lot::Mutex::new(std::collections::VecDeque::new())),
            tls_acceptor,
            auth_file,
            mirror,
            cutover: Arc::new(ArcSwap::from_pointee(None)),
            lb_state: LoadBalancerState {
                rr_counter: AtomicU64::new(0),
            },
            #[cfg(feature = "routing-hints")]
            hint_parser: if config.routing_hints.enabled {
                tracing::info!(
                    strip = config.routing_hints.strip_hints,
                    "SQL-comment routing hints enabled"
                );
                Some(if config.routing_hints.strip_hints {
                    crate::routing::HintParser::new()
                } else {
                    crate::routing::HintParser::without_stripping()
                })
            } else {
                None
            },
            #[cfg(feature = "rate-limiting")]
            rate_limiter,
            #[cfg(feature = "circuit-breaker")]
            circuit_breaker,
            #[cfg(feature = "query-analytics")]
            analytics,
            #[cfg(feature = "query-cache")]
            query_cache,
            #[cfg(feature = "query-rewriting")]
            rewriter,
            #[cfg(feature = "multi-tenancy")]
            tenant_manager,
            #[cfg(feature = "schema-routing")]
            schema_analyzer,
            #[cfg(feature = "pool-modes")]
            pool_manager,
            #[cfg(feature = "pool-modes")]
            backend_pool,
            #[cfg(feature = "wasm-plugins")]
            plugin_manager,
            #[cfg(feature = "ha-tr")]
            transaction_journal: Arc::new(crate::transaction_journal::TransactionJournal::new()),
            tr_read_policy: Arc::new(TrReadPolicy::from_config(&config.tr_read_functions)),
            #[cfg(feature = "anomaly-detection")]
            anomaly_detector: Arc::new(crate::anomaly::AnomalyDetector::new(
                config.anomaly.to_anomaly_config(),
            )),
            #[cfg(feature = "edge-proxy")]
            edge_cache: Arc::new(crate::edge::EdgeCache::with_limits(
                config.edge.max_entries.max(1),
                config.cache.max_cacheable_response_bytes,
            )),
            #[cfg(feature = "edge-proxy")]
            edge_registry: Arc::new(crate::edge::EdgeRegistry::new(
                config.edge.max_edges,
                std::time::Duration::from_secs(config.edge.liveness_window_secs),
            )),
        });

        Ok(Self {
            config,
            state,
            shutdown_tx,
            config_path: None,
        })
    }

    /// Record the config file path so `SIGHUP` can re-read it for a live
    /// reload (Batch H). Without a path (config built from CLI flags/defaults)
    /// a `SIGHUP` is logged and ignored — there is nothing to re-read.
    pub fn with_config_path(mut self, path: Option<String>) -> Self {
        self.config_path = path;
        self
    }

    /// A stream that yields once per `SIGHUP`. On non-Unix platforms it never
    /// yields (config reload is Unix-signal driven).
    #[cfg(unix)]
    fn hangup_stream() -> tokio::signal::unix::Signal {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
            .expect("failed to install SIGHUP handler")
    }
    #[cfg(not(unix))]
    fn hangup_stream() -> HangupNever {
        HangupNever
    }

    /// A stream that yields once per `SIGUSR2` — the graceful binary-handoff
    /// drain trigger. Never yields on non-Unix platforms.
    #[cfg(unix)]
    fn usr2_stream() -> tokio::signal::unix::Signal {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2())
            .expect("failed to install SIGUSR2 handler")
    }
    #[cfg(not(unix))]
    fn usr2_stream() -> HangupNever {
        HangupNever
    }

    /// Wait for in-flight client connections to finish, up to `timeout`. Used by
    /// the graceful drain after the listener is closed — the session map is the
    /// live active-connection gauge (one entry per accepted connection).
    async fn drain_connections(state: &Arc<ServerState>, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let active = state.sessions.len();
            if active == 0 {
                tracing::info!("drain complete — all in-flight connections finished");
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    active,
                    "drain timeout reached — exiting with connections still open"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Graceful-drain timeout: how long to keep serving in-flight connections
    /// after SIGUSR2 before exiting. Sourced from `shutdown_drain_timeout_secs`
    /// in the live config, with the `HELIOS_DRAIN_TIMEOUT_SECS` env var as a
    /// runtime override.
    fn drain_timeout(config_secs: u64) -> Duration {
        let secs = std::env::var("HELIOS_DRAIN_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(config_secs);
        Duration::from_secs(secs)
    }

    /// Re-read the config file and hot-swap the live config (Batch H).
    ///
    /// New connections immediately use the reloaded config; in-flight sessions
    /// keep the snapshot they began with, so nothing is dropped. A parse error
    /// keeps the running config untouched. Socket-bound fields (listen/admin
    /// address) cannot change on an already-bound listener and are reported but
    /// not applied. The node set is reconciled into the health map so routing
    /// sees additions/removals at once.
    async fn reload_config(&self) {
        let Some(path) = self.config_path.as_deref() else {
            tracing::warn!(
                "SIGHUP received but config was not loaded from a file — nothing to reload"
            );
            return;
        };
        tracing::info!(path, "SIGHUP: reloading configuration");
        let mut new_config = match ProxyConfig::from_file(path) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(path, error = %e, "SIGHUP reload failed to parse — keeping current config");
                return;
            }
        };
        let old = self.state.live_config.load_full();
        // [edge] is applied at startup only: the EdgeClient subscription and
        // the registry GC task are spawned once in run() from the boot
        // config. Letting the per-connection data path follow a reloaded
        // [edge] would half-enable the feature (e.g. role=edge serving
        // cached reads with no invalidation subscription ever spawned), so
        // carry the running section forward — same warn-and-keep treatment
        // as listen_address/admin_address.
        if new_config.edge != old.edge {
            tracing::warn!(
                "[edge] configuration changed on SIGHUP but is applied at startup only — \
                 keeping the running edge settings (restart to apply)"
            );
            new_config.edge = old.edge.clone();
        }
        // `[limits]` is resolved once at startup into `state.limits` (and, for
        // max_client_connections, into the already-sized `client_slots`
        // semaphore whose outstanding permits could not be revoked anyway), so
        // a reload cannot apply it. Warn on the two opt-in stability bounds
        // rather than let an operator believe a SIGHUP armed them.
        if new_config.limits.max_client_connections != old.limits.max_client_connections
            || new_config.limits.client_idle_timeout_secs != old.limits.client_idle_timeout_secs
        {
            tracing::warn!(
                old_max_client_connections = old.limits.max_client_connections,
                new_max_client_connections = new_config.limits.max_client_connections,
                old_client_idle_timeout_secs = old.limits.client_idle_timeout_secs,
                new_client_idle_timeout_secs = new_config.limits.client_idle_timeout_secs,
                "[limits] max_client_connections / client_idle_timeout_secs changed on SIGHUP but are applied at startup only — keeping the running values (restart to apply)"
            );
            // Keep the published config truthful: `/config` must not advertise
            // a cap/idle timeout that is not the one in effect (same treatment
            // as `[edge]` above).
            new_config.limits.max_client_connections = old.limits.max_client_connections;
            new_config.limits.client_idle_timeout_secs = old.limits.client_idle_timeout_secs;
        }
        if new_config.listen_address != old.listen_address {
            tracing::warn!(old = %old.listen_address, new = %new_config.listen_address,
                "listen_address change needs a restart/handoff; the bound socket is kept");
        }
        if new_config.admin_address != old.admin_address {
            tracing::warn!(old = %old.admin_address, new = %new_config.admin_address,
                "admin_address change needs a restart; the bound socket is kept");
        }
        // Reconcile node health to the new node set before publishing the
        // config, so the first connection on the new config can route to it.
        Self::reconcile_health(&self.state, &new_config);
        let nodes = new_config.nodes.len();
        let hba_rules = new_config.hba.len();
        let pool_max = new_config.pool.max_connections;
        self.state.live_config.store(Arc::new(new_config));
        tracing::info!(
            nodes,
            hba_rules,
            pool_max,
            "SIGHUP: configuration reloaded — applies to new connections"
        );
    }

    /// Rebuild the health map for `config`'s node set: surviving nodes keep
    /// their current health; new nodes are seeded healthy (immediately
    /// routable, the next check confirms); removed nodes are dropped.
    fn reconcile_health(state: &Arc<ServerState>, config: &ProxyConfig) {
        // Serialize against the periodic checker and in-band demotions so this
        // rebuild neither clobbers nor is clobbered by a concurrent write.
        let _writers = state.health_write.lock();
        let current = state.health.load_full();
        let mut next: HashMap<String, NodeHealth> = HashMap::new();
        for node in &config.nodes {
            let addr = node.address().to_string();
            match current.get(&addr) {
                Some(existing) => {
                    next.insert(addr, existing.clone());
                }
                None => {
                    tracing::info!(node = %addr, "SIGHUP: new node added — seeding healthy");
                    next.insert(
                        addr.clone(),
                        NodeHealth {
                            address: addr,
                            healthy: true,
                            last_check: chrono::Utc::now(),
                            failure_count: 0,
                            last_error: None,
                            latency_ms: 0.0,
                            replication_lag_bytes: None,
                        },
                    );
                }
            }
        }
        for gone in current.keys().filter(|k| !next.contains_key(*k)) {
            tracing::info!(node = %gone, "SIGHUP: node removed from config");
        }
        state.health.store(Arc::new(next));
    }

    /// Run the proxy server
    pub async fn run(&self) -> Result<()> {
        // Bind with SO_REUSEPORT so a freshly-started binary can bind the SAME
        // listen address concurrently — the kernel load-balances new
        // connections across both processes. That is the mechanism behind the
        // zero-downtime binary handoff: start the new binary, then SIGUSR2 the
        // old one to close its listener and drain (Batch H, item 84).
        let listener = bind_reuseport(&self.config.listen_address)?;

        tracing::info!(
            "Proxy listening on {} (SO_REUSEPORT)",
            self.config.listen_address
        );

        // Start background tasks
        let health_task = self.spawn_health_checker();
        let pool_task = self.spawn_pool_manager();

        // Single background analytics consumer. Everything expensive about
        // recording a query (fingerprint normalization, statistics, slow-query
        // log, pattern detection, cost attribution) runs here instead of on the
        // connection task that served the query; the relay only pays a
        // `try_send` onto a bounded queue. Started exactly once, here, and
        // aborted with the other background tasks at shutdown.
        #[cfg(feature = "query-analytics")]
        let analytics_task = self
            .state
            .analytics
            .as_ref()
            .and_then(|a| a.start_consumer());

        // Edge registry GC — prunes edges not seen within the liveness window.
        #[cfg(feature = "edge-proxy")]
        let _edge_maintenance_task = if self.config.edge.enabled {
            Some(self.spawn_edge_maintenance())
        } else {
            None
        };

        // Start admin server
        let admin_task = self.spawn_admin_server();

        // Start the MCP agent gateway when enabled.
        let mcp_task = if self.config.mcp.enabled {
            let mcp_cfg = self.config.mcp.clone();
            // Resolve the configured agent contract (scoped grants) by id.
            let contract = mcp_cfg.contract.as_ref().and_then(|id| {
                let found = self.config.agent_contracts.iter().find(|c| &c.id == id).cloned();
                if found.is_none() {
                    tracing::warn!(%id, "mcp.contract names an unknown agent_contract; gateway runs with only the read-only guardrail");
                }
                found
            });
            Some(tokio::spawn(async move {
                if let Err(e) = crate::mcp::McpServer::new(mcp_cfg, contract).run().await {
                    tracing::error!("MCP gateway error: {}", e);
                }
            }))
        } else {
            None
        };

        // Start the HTTP SQL gateway (Neon-serverless compatible) when enabled.
        let http_gw_task = if self.config.http_gateway.enabled {
            let gw_cfg = self.config.http_gateway.clone();
            Some(tokio::spawn(async move {
                if let Err(e) = crate::http_gateway::HttpGateway::new(gw_cfg).run().await {
                    tracing::error!("HTTP gateway error: {}", e);
                }
            }))
        } else {
            None
        };

        // Start the GraphQL-to-SQL gateway when enabled.
        #[cfg(feature = "graphql-gateway")]
        let _graphql_gw_task = if self.config.graphql_gateway.enabled {
            let gw_cfg = self.config.graphql_gateway.clone();
            Some(tokio::spawn(async move {
                if let Err(e) = crate::graphql_gateway::GraphqlGateway::new(gw_cfg)
                    .run()
                    .await
                {
                    tracing::error!("GraphQL gateway error: {}", e);
                }
            }))
        } else {
            None
        };

        // Edge role: hold an SSE subscription against the home's admin API so
        // invalidations drop matching local cache entries as they arrive.
        #[cfg(feature = "edge-proxy")]
        let _edge_client_task =
            if self.config.edge.enabled && self.config.edge.role == crate::edge::EdgeRole::Edge {
                tracing::info!(
                    home_url = %self.config.edge.home_url,
                    region = %self.config.edge.region,
                    "edge role: subscribing to home invalidation stream"
                );
                Some(crate::edge::client::EdgeClient::spawn(
                    self.config.edge.clone(),
                    self.state.edge_cache.clone(),
                ))
            } else {
                None
            };

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // SIGHUP -> zero-downtime config reload; SIGUSR2 -> graceful drain for
        // binary handoff (Batch H). On platforms without Unix signals these are
        // simply never readable.
        let mut sighup = Self::hangup_stream();
        let mut sigusr2 = Self::usr2_stream();
        let mut graceful = false;

        loop {
            tokio::select! {
                _ = sighup.recv() => {
                    self.reload_config().await;
                }
                _ = sigusr2.recv() => {
                    tracing::info!(
                        "SIGUSR2: graceful binary-handoff drain — closing the listener so new \
                         connections route to the sibling process; finishing in-flight connections"
                    );
                    graceful = true;
                    break;
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            // PG wire traffic is small request/response
                            // frames; Nagle + delayed-ACK costs tens of
                            // ms per round-trip if left on.
                            let _ = stream.set_nodelay(true);
                            // NOTE: the `[limits] max_client_connections` cap is
                            // NOT applied here. A slot is taken inside
                            // `handle_client`, once the first startup message has
                            // been classified — a `CancelRequest` arrives as its
                            // own fresh connection and must still be served when
                            // the proxy is saturated (that is exactly when an
                            // operator needs to cancel a query), just as
                            // PostgreSQL handles cancels in the postmaster
                            // without consuming a `max_connections` slot.
                            self.state.metrics.connections_accepted.fetch_add(1, Ordering::Relaxed);
                            let state = self.state.clone();
                            // Snapshot the *live* config so a SIGHUP reload
                            // applies to new connections; in-flight sessions
                            // keep the snapshot they began with (Batch H). This
                            // is an Arc clone (cheap, atomic refcount bump), not
                            // a deep clone of ProxyConfig — see handle_client.
                            let config = self.state.live_config.load_full();
                            let shutdown_tx = self.shutdown_tx.clone();

                            tokio::spawn(async move {
                                if let Err(e) = Self::handle_client(stream, addr, state, config, shutdown_tx).await {
                                    tracing::error!("Client handler error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("Accept error: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!("Shutdown signal received");
                    break;
                }
            }
        }

        // Close the listening socket so the kernel stops routing new connections
        // to this process's accept queue (with SO_REUSEPORT they would otherwise
        // sit unaccepted) — all new connections now go to the sibling listener.
        drop(listener);

        // On a graceful handoff, keep serving in-flight connections until they
        // finish (or the drain deadline), so nothing in flight is dropped.
        if graceful {
            let timeout =
                Self::drain_timeout(self.state.live_config.load().shutdown_drain_timeout_secs);
            tracing::info!(
                timeout_secs = timeout.as_secs(),
                "draining in-flight connections"
            );
            Self::drain_connections(&self.state, timeout).await;
        }

        // Wait for background tasks
        health_task.abort();
        pool_task.abort();
        admin_task.abort();
        // Drain what the connection tasks already queued before killing the
        // consumer, so a graceful handoff does not silently lose the last few
        // hundred samples. Bounded by the same `shutdown_drain_timeout_secs`
        // budget as the connection drain — the queue is small and this is
        // normally instant, but a wedged consumer must not hold up exit.
        #[cfg(feature = "query-analytics")]
        if let Some(t) = analytics_task {
            if let Some(a) = self.state.analytics.as_ref() {
                let budget =
                    Self::drain_timeout(self.state.live_config.load().shutdown_drain_timeout_secs);
                if tokio::time::timeout(budget, a.flush()).await.is_err() {
                    tracing::warn!("analytics ingest queue did not drain before shutdown");
                }
            }
            t.abort();
        }
        if let Some(t) = mcp_task {
            t.abort();
        }
        if let Some(t) = http_gw_task {
            t.abort();
        }

        Ok(())
    }

    /// Spawn admin API server
    fn spawn_admin_server(&self) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        let state = self.state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Create admin state
            let admin_state = Arc::new(AdminState::new());

            // Initialize config snapshot
            {
                let mut snapshot = admin_state.config_snapshot.write().await;
                *snapshot = ConfigSnapshot {
                    listen_address: config.listen_address.clone(),
                    admin_address: config.admin_address.clone(),
                    tr_enabled: config.tr_enabled,
                    tr_mode: format!("{:?}", config.tr_mode),
                    pool_min_connections: config.pool.min_connections,
                    pool_max_connections: config.pool.max_connections,
                    nodes: config
                        .nodes
                        .iter()
                        .map(|n| NodeSnapshot {
                            address: n.address().to_string(),
                            role: format!("{:?}", n.role),
                            weight: n.weight,
                            enabled: n.enabled,
                        })
                        .collect(),
                };
            }

            // Set proxy config for SQL routing
            admin_state.set_proxy_config(config.clone()).await;

            // Require a Bearer token on admin requests when configured.
            admin_state
                .with_auth_token(config.admin_token.clone())
                .await;

            // Branch-database provisioning surface.
            if config.branch.enabled {
                admin_state.with_branch(config.branch.clone()).await;
            }

            // Surface traffic-mirror / migration status when mirroring is on.
            if let Some(ref mirror) = state.mirror {
                admin_state
                    .with_migration(crate::admin::MigrationInfo {
                        target: mirror.target().to_string(),
                        writes_only: mirror.writes_only(),
                        metrics: mirror.metrics.clone(),
                        config: config.mirror.clone(),
                        cutover: state.cutover.clone(),
                        cutover_target: crate::mirror::CutoverTarget {
                            addr: format!(
                                "{}:{}",
                                config.mirror.backend_host, config.mirror.backend_port
                            ),
                            user: config.mirror.backend_user.clone(),
                            password: config.mirror.backend_password.clone(),
                            database: config.mirror.backend_database.clone(),
                        },
                    })
                    .await;
            }

            // Attach the plugin manager so /plugins + the admin UI
            // surface real loaded modules. Cheap Arc-clone — no
            // duplicate state, both AdminState and ServerState hold
            // the same manager.
            #[cfg(feature = "wasm-plugins")]
            if let Some(ref pm) = state.plugin_manager {
                admin_state.with_plugin_manager(pm.clone()).await;
            }

            // Attach the pool manager so /api/pools surfaces real per-node
            // pool statistics instead of an empty list.
            #[cfg(feature = "pool-modes")]
            if let Some(ref pm) = state.pool_manager {
                admin_state.with_pool_manager(pm.clone()).await;
            }

            // Attach the circuit-breaker manager so /api/circuit reports live
            // per-node breaker state.
            #[cfg(feature = "circuit-breaker")]
            if let Some(ref cb) = state.circuit_breaker {
                admin_state.with_circuit_breaker(cb.clone()).await;
            }

            // Attach the time-travel replay engine. The engine reads
            // windows from the shared TransactionJournal and replays
            // statements against a target backend supplied per-request.
            // Per-call credential overrides land via FU-21's
            // ReplayRequestBody.target_user / target_password /
            // target_database fields.
            #[cfg(feature = "ha-tr")]
            {
                let template = build_replay_backend_template(&config);
                let engine = Arc::new(crate::replay::ReplayEngine::new(
                    state.transaction_journal.clone(),
                    template,
                ));
                admin_state.with_replay_engine(engine).await;
            }

            // Attach the anomaly detector — same Arc the server
            // populates from the query path. /api/anomalies polls
            // this for surfaced detections.
            #[cfg(feature = "anomaly-detection")]
            admin_state
                .with_anomaly_detector(state.anomaly_detector.clone())
                .await;

            // Attach the query-analytics engine so /api/analytics can read it.
            #[cfg(feature = "query-analytics")]
            if let Some(a) = state.analytics.as_ref() {
                admin_state.with_analytics(a.clone()).await;
            }

            // Attach the edge cache + registry. Both surfaced via
            // /api/edge/* admin routes.
            #[cfg(feature = "edge-proxy")]
            admin_state
                .with_edge(state.edge_cache.clone(), state.edge_registry.clone())
                .await;

            // Create admin server
            let admin_server = AdminServer::new(config.admin_address.clone(), admin_state.clone());

            // Spawn state sync task
            let admin_state_sync = admin_state.clone();
            let server_state = state.clone();
            let sync_task = tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
                loop {
                    interval.tick().await;

                    // Sync health status
                    {
                        let health = server_state.health.load_full();
                        let mut admin_health = admin_state_sync.node_health.write().await;
                        *admin_health = (*health).clone();
                    }

                    // Sync metrics
                    {
                        let metrics = ServerMetricsSnapshot {
                            connections_accepted: server_state
                                .metrics
                                .connections_accepted
                                .load(Ordering::Relaxed),
                            connections_rejected: server_state
                                .metrics
                                .connections_rejected
                                .load(Ordering::Relaxed),
                            connections_closed: server_state
                                .metrics
                                .connections_closed
                                .load(Ordering::Relaxed),
                            queries_processed: server_state
                                .metrics
                                .queries_processed
                                .load(Ordering::Relaxed),
                            bytes_received: server_state
                                .metrics
                                .bytes_received
                                .load(Ordering::Relaxed),
                            bytes_sent: server_state.metrics.bytes_sent.load(Ordering::Relaxed),
                            failovers: server_state.metrics.failovers.load(Ordering::Relaxed),
                            cache_capture_oversize: server_state
                                .metrics
                                .cache_capture_oversize
                                .load(Ordering::Relaxed),
                            tr: server_state.metrics.tr.snapshot(),
                        };
                        let mut admin_metrics = admin_state_sync.metrics.write().await;
                        *admin_metrics = metrics;
                    }

                    // Sync session count
                    {
                        let mut admin_sessions = admin_state_sync.active_sessions.write().await;
                        *admin_sessions = server_state.sessions.len() as u64;
                    }
                }
            });

            // Run admin server
            tokio::select! {
                result = admin_server.run() => {
                    if let Err(e) = result {
                        tracing::error!("Admin server error: {}", e);
                    }
                }
                _ = shutdown_rx.recv() => {
                    tracing::info!("Admin server shutting down");
                }
            }

            sync_task.abort();
        })
    }

    /// PostgreSQL `ErrorResponse` bytes for a connection refused because the
    /// `[limits] max_client_connections` cap is saturated.
    ///
    /// SQLSTATE `53300` (`too_many_connections`) with PostgreSQL's own severity
    /// and wording (`FATAL`, `sorry, too many clients already`), so an
    /// off-the-shelf driver marks the connection dead and reports the same
    /// condition it would report against a PostgreSQL server at
    /// `max_connections` instead of seeing a bare TCP reset. Factored out so it
    /// can be asserted on without a socket.
    fn over_capacity_error_bytes() -> Vec<u8> {
        Self::create_fatal_response("53300", "sorry, too many clients already")
    }

    /// Admission control for a client connection whose first startup-phase
    /// message has just been classified.
    ///
    /// `Ok(slot)` admits the connection (`None` = no cap configured, the
    /// default); `Err(())` means the cap is saturated and the caller must
    /// answer with [`Self::over_capacity_error_bytes`] and close.
    ///
    /// A `CancelRequest` NEVER consumes a slot: it always arrives as its own
    /// throwaway connection, it is answered without ever becoming a session,
    /// and refusing it would make query cancellation impossible exactly when
    /// the proxy is saturated — PostgreSQL likewise handles cancels in the
    /// postmaster without taking a `max_connections` slot.
    fn admit_client_slot(
        state: &Arc<ServerState>,
        first: &StartupMessage,
    ) -> std::result::Result<Option<tokio::sync::OwnedSemaphorePermit>, ()> {
        if matches!(first, StartupMessage::CancelRequest { .. }) {
            return Ok(None);
        }
        let Some(sem) = state.client_slots.as_ref() else {
            return Ok(None);
        };
        match Arc::clone(sem).try_acquire_owned() {
            Ok(permit) => Ok(Some(permit)),
            Err(_) => {
                state
                    .metrics
                    .connections_rejected
                    .fetch_add(1, Ordering::Relaxed);
                Err(())
            }
        }
    }

    /// Tell a client the proxy is at its connection cap, then close the stream.
    ///
    /// Bounded by the configured client write timeout, so a client that never
    /// reads cannot pin this task and the session slot it is being refused:
    /// worst case the frame is dropped and the connection closed anyway.
    async fn refuse_over_capacity(stream: &mut ClientStream, write_timeout: Duration) {
        let msg = Self::over_capacity_error_bytes();
        let _ = tokio::time::timeout(write_timeout, async {
            let _ = stream.write_all(&msg).await;
            let _ = stream.flush().await;
            let _ = stream.shutdown().await;
        })
        .await;
    }

    /// PostgreSQL `ErrorResponse` bytes for a session terminated by the
    /// `[limits] client_idle_timeout_secs` idle-session timeout.
    ///
    /// SQLSTATE `57P05` (`idle_session_timeout`) with PostgreSQL's own severity
    /// and wording (`FATAL`), so a driver marks the connection dead instead of
    /// trying to continue on it and then hitting a bare EOF.
    fn idle_session_timeout_error_bytes() -> Vec<u8> {
        Self::create_fatal_response(
            "57P05",
            "terminating connection due to idle-session timeout",
        )
    }

    /// Tell a client its session is being reclaimed by the idle-session
    /// timeout. The caller then leaves the query loop, which closes the
    /// connection through the normal teardown path.
    ///
    /// The write is bounded by the configured client write timeout: a client
    /// that has stopped reading (a zero receive window, a vanished host —
    /// precisely the stall this timeout exists to reclaim) must not be able to
    /// pin the connection task, its session-map entry and its connection slot
    /// forever on the goodbye frame.
    async fn terminate_idle_session(
        stream: &mut ClientStream,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) {
        tracing::debug!(
            client = %session.client_addr,
            in_transaction = session
                .in_transaction
                .load(std::sync::atomic::Ordering::Relaxed),
            timeout_secs = state
                .limits
                .client_idle_timeout
                .map(|d| d.as_secs())
                .unwrap_or(0),
            "terminating idle client session (idle-session timeout)"
        );
        let emsg = Self::idle_session_timeout_error_bytes();
        let _ = tokio::time::timeout(state.limits.client_write_timeout, async {
            let _ = stream.write_all(&emsg).await;
            let _ = stream.flush().await;
        })
        .await;
    }

    /// Handle a client connection
    async fn handle_client(
        stream: TcpStream,
        addr: SocketAddr,
        state: Arc<ServerState>,
        config: Arc<ProxyConfig>,
        _shutdown_tx: broadcast::Sender<()>,
    ) -> Result<()> {
        tracing::debug!("New client connection from {}", addr);

        // Create session
        let session_id = Uuid::new_v4();
        let session = Arc::new(ClientSession {
            id: session_id,
            client_addr: addr,
            client_ip_str: addr.ip().to_string(),
            session_id_str: session_id.to_string(),
            current_node: RwLock::new(None),
            in_transaction: std::sync::atomic::AtomicBool::new(false),
            copy_in_progress: std::sync::atomic::AtomicBool::new(false),
            last_rfq_status: std::sync::atomic::AtomicU8::new(b'I'),
            last_response_error: std::sync::atomic::AtomicBool::new(false),
            tr_replay_tainted: std::sync::atomic::AtomicBool::new(false),
            backend_credential: RwLock::new(None),
            tx_state: RwLock::new(TransactionState::default()),
            variables: RwLock::new(HashMap::new()),
            created_at: chrono::Utc::now(),
            tr_mode: config.tr_mode,
            #[cfg(feature = "lag-routing")]
            last_write_at: RwLock::new(None),
            #[cfg(feature = "pool-modes")]
            pool_client_id: ClientId::new(),
            #[cfg(feature = "wasm-plugins")]
            plugin_identity: RwLock::new(None),
            #[cfg(feature = "edge-proxy")]
            edge_ineligible: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "edge-proxy")]
            pending_edge_copy_tables: std::sync::Mutex::new(None),
            #[cfg(feature = "rate-limiting")]
            rate_limit_key: std::sync::OnceLock::new(),
        });

        // Register the session, then attach an RAII guard that deregisters it on
        // ANY exit from here on — a normal return OR a panic unwind. Before this,
        // a panic anywhere in negotiation/startup/the query loop leaked the
        // ClientSession entry forever, inflating the active-session gauge and
        // stalling every graceful drain to its full timeout. The guard also
        // owns the rest of the per-connection teardown (metric + L1 cache) so it
        // too runs unconditionally.
        state.sessions.insert(session.id, session.clone());
        let mut _session_guard = SessionGuard {
            state: state.clone(),
            session_id: session.id,
            // Filled in below, once admission control has run: the guard owns
            // the slot so it is returned on every exit path, panics included.
            _client_slot: None,
        };

        // Negotiate client TLS (if the client sent SSLRequest). Produces a
        // ClientStream that is plaintext or TLS-wrapped; the rest of the
        // session is written against that single stream type. `pre` carries
        // a first startup/cancel message already read while peeking.
        //
        // Bound the pre-auth negotiation (first-message read + TLS handshake) in
        // time: a client that connects and then stalls must not pin this task
        // and its session-map slot indefinitely (slow-loris). ONE deadline
        // covers the handshake *and* the first startup message read below, so
        // the TLS path is bounded by the same budget as the plaintext one. The
        // query loop that follows is intentionally NOT under this deadline.
        let handshake_deadline = tokio::time::Instant::now() + state.limits.startup_timeout;
        let negotiated = match tokio::time::timeout_at(
            handshake_deadline,
            Self::negotiate_client_tls(stream, &state),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => {
                tracing::debug!(client = %addr, "pre-auth negotiation timed out; closing");
                Err(ProxyError::Connection(
                    "startup negotiation timeout".to_string(),
                ))
            }
        };
        let result = match negotiated {
            Ok((mut client_stream, pre)) => {
                // Resolve the first startup-phase message before anything else:
                // admission control ([limits] max_client_connections) must be
                // able to tell a real `Startup` from a `CancelRequest`, and on
                // the TLS path that message only arrives after the handshake, so
                // it is read here (inside the same pre-auth deadline) rather
                // than inside `handle_startup`. `buffer` carries any bytes read
                // past it into the query loop.
                let mut buffer = BytesMut::with_capacity(8192);
                let first = match pre {
                    Some(msg) => Ok(Some(msg)),
                    None => match tokio::time::timeout_at(
                        handshake_deadline,
                        Self::read_startup_message(&mut client_stream, &mut buffer),
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(_) => {
                            tracing::debug!(client = %addr, "startup message timed out; closing");
                            Err(ProxyError::Connection(
                                "startup negotiation timeout".to_string(),
                            ))
                        }
                    },
                };
                match first {
                    // Client closed before sending a complete startup message.
                    Ok(None) => Ok(()),
                    Ok(Some(msg)) => match Self::admit_client_slot(&state, &msg) {
                        Ok(slot) => {
                            _session_guard._client_slot = slot;
                            Self::client_loop(
                                &mut client_stream,
                                Some(msg),
                                buffer,
                                &session,
                                &state,
                                &config,
                            )
                            .await
                        }
                        Err(()) => {
                            tracing::warn!(
                                client = %addr,
                                "client connection cap reached; refusing connection"
                            );
                            Self::refuse_over_capacity(
                                &mut client_stream,
                                state.limits.client_write_timeout,
                            )
                            .await;
                            Ok(())
                        }
                    },
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        };

        // Session deregistration, the connections-closed metric, and the L1
        // cache reclaim are all handled by `_session_guard`'s Drop (which runs
        // here on the normal path and on an unwind), so nothing is required here.
        result
    }

    /// Deadline for the client's next message, or `None` when this session must
    /// not be reclaimed by the idle-session timeout right now.
    ///
    /// PostgreSQL's `idle_session_timeout` only applies to a session that is
    /// genuinely waiting for a NEW command, so the deadline is armed only at a
    /// true message boundary:
    ///
    /// * not while `buffer` still holds a partially received message — a client
    ///   trickling a large message is slow, not idle, and killing it would
    ///   truncate a legitimate statement;
    /// * not while the session is inside a `COPY FROM STDIN` — a COPY producer
    ///   that pauses longer than the timeout would otherwise be killed
    ///   mid-stream and the bulk load aborted.
    ///
    /// `None` is also returned when the timeout is disabled (the default), in
    /// which case the read below is unbounded exactly as it was before the key
    /// existed.
    fn client_idle_deadline(
        state: &ServerState,
        buffer: &BytesMut,
        session: &ClientSession,
    ) -> Option<tokio::time::Instant> {
        let idle = state.limits.client_idle_timeout?;
        if !buffer.is_empty() {
            return None;
        }
        if session
            .copy_in_progress
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return None;
        }
        Some(tokio::time::Instant::now() + idle)
    }

    /// Read the client's next message into `buffer`, optionally under an
    /// idle-session deadline.
    async fn read_client_bytes(
        stream: &mut ClientStream,
        buffer: &mut BytesMut,
        idle_deadline: Option<tokio::time::Instant>,
    ) -> Result<ClientRead> {
        let read = match idle_deadline {
            Some(deadline) => {
                match tokio::time::timeout_at(deadline, stream.read_buf(buffer)).await {
                    Ok(r) => r,
                    Err(_) => return Ok(ClientRead::IdleTimeout),
                }
            }
            None => stream.read_buf(buffer).await,
        };
        Ok(ClientRead::Bytes(read.map_err(|e| {
            ProxyError::Network(format!("Read error: {}", e))
        })?))
    }

    /// Wait for the client's next message.
    ///
    /// When `watch_node` names a cached backend connection, that socket is
    /// watched at the same time so unsolicited backend traffic (LISTEN/NOTIFY
    /// notifications, NoticeResponse, ParameterStatus, the delayed tail of a
    /// `Flush` response) is relayed to the client promptly instead of sitting
    /// unread until the next query, and a backend that dies while the session is
    /// idle is noticed at once — in which case its entry is removed from `conns`
    /// (the session survives; the next query redials) and the wait continues on
    /// the client alone.
    ///
    /// `idle_deadline` is the session's idle-session deadline; relaying async
    /// backend traffic to an idle client is not client activity, so the deadline
    /// is NOT re-armed while doing so.
    /// Length of the longest prefix of `buf` made up of WHOLE backend frames.
    ///
    /// A backend message is a 1-byte tag followed by a 4-byte big-endian length
    /// that counts itself, so one frame occupies `1 + len` bytes. Returns an
    /// error for a length below the 4-byte minimum: that is a malformed frame,
    /// and treating it as "incomplete" would stall reassembly until the session
    /// buffer cap instead of surfacing the fault (H-07).
    fn complete_frame_prefix(buf: &[u8], max_frame_bytes: usize) -> Result<usize> {
        let mut off = 0usize;
        while let Some(len) = backend_frame_len(&buf[off..], max_frame_bytes)? {
            let end = off + 1 + len;
            if end > buf.len() {
                break;
            }
            off = end;
        }
        Ok(off)
    }

    async fn read_next_client_message(
        stream: &mut ClientStream,
        buffer: &mut BytesMut,
        conns: &mut HashMap<String, BackendConn>,
        watch_node: Option<&str>,
        abuf: &mut BytesMut,
        idle_deadline: Option<tokio::time::Instant>,
        state: &Arc<ServerState>,
    ) -> Result<ClientRead> {
        let Some(node) = watch_node else {
            return Self::read_client_bytes(stream, buffer, idle_deadline).await;
        };
        // Built once, outside the watch loop: a `select!` evaluates even a
        // disabled branch's expression, so an inline `sleep_until` would
        // construct a timer on every relay iteration — and with no idle timeout
        // configured (the default) this future never registers one at all.
        let idle_wait = async move {
            match idle_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(idle_wait);
        loop {
            let mut backend_gone = false;
            let mut client_bytes: Option<usize> = None;
            // `abuf` is the caller's session-scoped reassembly buffer (Fix S4:
            // reused, never re-allocated, and never zero-filled — `read_buf`
            // appends into spare capacity). It is deliberately NOT cleared here:
            // one read can end mid-frame, and only whole frames may be forwarded,
            // so any trailing partial frame stays buffered until the rest of it
            // arrives.
            abuf.reserve(16 * 1024);
            {
                let bc = conns.get_mut(node).expect("watch_node is in conns");
                tokio::select! {
                    r = stream.read_buf(buffer) => {
                        client_bytes = Some(r.map_err(|e| {
                            ProxyError::Network(format!("Read error: {}", e))
                        })?);
                    }
                    _ = &mut idle_wait => return Ok(ClientRead::IdleTimeout),
                    r = bc.stream.read_buf(&mut *abuf) => match r {
                        Ok(0) => backend_gone = true,
                        Ok(_) => {
                            // Publish only COMPLETE frames. A truncated frame is
                            // unrecoverable: the client blocks waiting for the
                            // rest, and nothing may ever be injected after it —
                            // not an ErrorResponse, and certainly not the rows of
                            // a re-executed statement, which would be appended to
                            // whatever the partial frame turns out to be.
                            let whole = Self::complete_frame_prefix(abuf, state.limits.max_backend_frame_bytes)?;
                            if whole > 0 {
                                tokio::time::timeout(state.limits.client_write_timeout, stream.write_all(&abuf[..whole]))
                                    .await
                                    .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                                    .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                                let _ = abuf.split_to(whole);
                                state.metrics.bytes_sent.fetch_add(whole as u64, Ordering::Relaxed);
                            }
                            // A single asynchronous frame must not grow the
                            // session's memory without bound while it reassembles.
                            if abuf.len() > state.limits.max_pending_bytes {
                                return Err(ProxyError::Protocol(format!(
                                    "backend asynchronous frame exceeds {} bytes",
                                    state.limits.max_pending_bytes
                                )));
                            }
                        }
                        Err(e) => {
                            tracing::debug!(node = %node, error = %e, "backend read error while idle; dropping cached connection");
                            backend_gone = true;
                        }
                    },
                }
            }
            if backend_gone {
                // Drop the dead cached connection but keep the client session
                // alive — the next query redials. (Mid-transaction the next
                // forward fails and surfaces the error.) Any partially received
                // frame dies with it: it was never published, so the client
                // stream stays frame-aligned and a synthesized error or a
                // re-executed statement can still be written safely.
                abuf.clear();
                conns.remove(node);
                return Self::read_client_bytes(stream, buffer, idle_deadline).await;
            }
            if let Some(cn) = client_bytes {
                return Ok(ClientRead::Bytes(cn));
            }
            // Otherwise we relayed async backend bytes; keep watching.
        }
    }

    /// Main client processing loop with full PostgreSQL protocol handling
    async fn client_loop(
        stream: &mut ClientStream,
        pre: Option<StartupMessage>,
        mut buffer: BytesMut,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<()> {
        let codec = ProtocolCodec::new();

        // Handle startup phase. The session keeps a per-node cache of
        // authenticated backend connections (`conns`) instead of a single
        // stream: when read/write routing moves a session between primary
        // and standby it now reuses the already-authenticated connection to
        // each node rather than dropping the socket and paying a fresh TCP
        // connect + startup + SCRAM handshake on every switch (Batch C).
        // Connections are authenticated with the client's own credentials
        // (auth is pass-through). In Transaction/Statement pooling mode they
        // are returned to a shared, identity-keyed idle pool at each
        // transaction boundary (DISCARD ALL reset on release) and reused by the
        // next same-identity acquisition — see `release_to_pool_if_idle` /
        // `ensure_conn`. The first connection of a session is still
        // established through the authenticated startup path; drawing the
        // startup connection from the pool (to reduce *concurrent* backend
        // connections below the client count) additionally needs
        // proxy-terminated backend auth and is the documented next increment.
        let mut conns: HashMap<String, BackendConn> = HashMap::new();
        // Bound the startup/authentication exchange in time (the TLS path reads
        // the real startup packet here, after negotiation). A client that opens
        // the connection but never completes startup must not hold the task and
        // its session slot open indefinitely.
        let startup_result = match tokio::time::timeout(
            state.limits.startup_timeout,
            Self::handle_startup(stream, &mut buffer, pre, session, state, config),
        )
        .await
        {
            Ok(r) => r,
            Err(_) => Err(ProxyError::Connection("startup timeout".to_string())),
        };
        let mut current_node: Option<String> = match startup_result {
            Ok((Some(stream_conn), node_addr)) => {
                conns.insert(node_addr.clone(), BackendConn::new(stream_conn));
                Some(node_addr)
            }
            Ok((None, _)) => {
                // SSL rejected or cancel request, connection should close
                return Ok(());
            }
            Err(e) => {
                tracing::error!("Startup failed: {}", e);
                // Send error to client
                let err_msg =
                    Self::create_error_response("08006", &format!("Startup failed: {}", e));
                let _ = stream.write_all(&err_msg).await;
                return Err(e);
            }
        };

        // Main query loop.
        //
        // Two wire shapes are handled. Simple-query (`Query`) messages are
        // self-contained: route, forward, and stream the response back
        // frame-by-frame until ReadyForQuery. Extended-protocol messages
        // (`Parse`/`Bind`/`Describe`/`Execute`/`Close`) carry no response of
        // their own until the client sends `Sync` (or `Flush`), so they are
        // accumulated into `pending` and forwarded as one batch at that
        // boundary — this is what stops the per-message 30s backend-read
        // timeout that made every prepared-statement driver unusable. The
        // routing decision for an extended batch is taken from the SQL in its
        // first `Parse`; a batch with no `Parse` (a re-`Bind`/`Execute` of a
        // named prepared statement) stays on the connection the statement was
        // prepared on.
        let mut pending = BytesMut::new();
        let mut pending_route_sql: Option<String> = None;
        // Prepared-statement tracking (Batch F.4). `stmt_registry` is the
        // session's canonical record of every *named* `Parse` the client has
        // issued (name -> full Parse message bytes) so the proxy can re-prepare
        // a statement on any backend connection that is missing it. `batch_*`
        // accumulate, for the in-flight extended batch, which named statements
        // it defines (Parse), references (Bind/Describe-S), and closes
        // (Close-S) — resolved at the Sync/Flush boundary.
        let mut stmt_registry: HashMap<String, bytes::Bytes> = HashMap::new();
        // Running sum of the bytes held in `stmt_registry`, kept in step with
        // it so the aggregate-size cap is O(1) per Parse (see MAX_PREPARED_BYTES).
        let mut stmt_registry_bytes: usize = 0;
        let mut batch_defines: Vec<String> = Vec::new();
        let mut batch_refs: Vec<String> = Vec::new();
        let mut batch_closes: Vec<String> = Vec::new();
        // Edge invalidation metadata for extended-protocol statements,
        // memoized at Parse time (name -> Some(tables) when the statement is
        // DML, None when read-only) so steady-state Bind/Execute cycles of a
        // prepared write cost a map lookup, not a regex pass. The unnamed
        // statement gets its own slot; `edge_batch_bound_unnamed` records
        // whether the in-flight batch Bind-referenced it. Resolved into an
        // invalidation at the Sync boundary.
        #[cfg(feature = "edge-proxy")]
        let mut edge_stmt_meta: HashMap<String, Option<Vec<String>>> = HashMap::new();
        #[cfg(feature = "edge-proxy")]
        let mut edge_unnamed_meta: Option<Vec<String>> = None;
        #[cfg(feature = "edge-proxy")]
        let mut edge_batch_bound_unnamed = false;
        // Unnamed-`Parse` promotion (Batch H). `held_unnamed` parks an unnamed
        // Parse that is the FIRST message of a batch (so the batch stays the
        // clean Parse→Bind→…→Sync shape) — it is NOT appended to `pending`; the
        // decision to forward or skip it is taken at the batch boundary once the
        // target connection is known. Holds (full Parse message, signature).
        let promote_unnamed = config.optimize_unnamed_parse;
        let mut held_unnamed: Option<(bytes::Bytes, bytes::Bytes)> = None;
        // Scratch buffer for the backend-watch below, created once per
        // session rather than once per client-message wait (a stack array
        // here would force the whole `handle_client` future to carry 16 KiB
        // live across every `.await` in this loop). Cleared — not
        // re-allocated — before each backend read via the same `clear()` +
        // `read_buf`-into-spare-capacity idiom `stream_flush` uses below, so
        // steady state costs no allocation and no zero-fill.
        let mut abuf = BytesMut::with_capacity(16384);
        // In-session Transaction Replay state (`tr_mode`): tracked session
        // SETs, the lost-while-idle backend, the aborted-transaction emulation
        // and the open extended-protocol cycle. See `tr_handle_fault`.
        let mut tr = TrSession::new(session.tr_mode);
        loop {
            // Read the client's next message directly into the accumulation
            // buffer (no intermediate zeroed scratch, no extra copy). `read_buf`
            // appends into `buffer`'s spare capacity and advances its length.
            //
            // While waiting, ALSO watch the session's current backend connection
            // so unsolicited backend traffic — LISTEN/NOTIFY notifications,
            // NoticeResponse, ParameterStatus, and the delayed tail of a `Flush`
            // response — is relayed to the client promptly instead of sitting
            // unread until the next query, and a backend that dies while the
            // session is idle is noticed at once. At the top of the loop the
            // backend is always quiescent (every query response is fully drained
            // before we return here), so any bytes it produces are out-of-band
            // and are relayed verbatim. A mid-COPY backend is excluded — it is
            // legitimately awaiting CopyData, which the client drives.
            buffer.reserve(16384);
            // Idle-session deadline — armed only when this session is genuinely
            // waiting for a NEW command (see `client_idle_deadline`), i.e. never
            // mid-COPY and never with a partially received message pending.
            // `None` (the default, `client_idle_timeout_secs = 0`) leaves every
            // read unbounded exactly as before.
            let idle_deadline = Self::client_idle_deadline(state, &buffer, session);
            // Borrowed, not cloned (Fix S4): the watch target is always the
            // session's current node, so cloning the node key into a fresh
            // `String` on every client-message wait was pure hot-path overhead.
            let watch_node: Option<&str> = if session
                .copy_in_progress
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                None
            } else {
                current_node
                    .as_deref()
                    .filter(|node| conns.contains_key(*node))
            };
            let n: usize = match Self::read_next_client_message(
                stream,
                &mut buffer,
                &mut conns,
                watch_node,
                &mut abuf,
                idle_deadline,
                state,
            )
            .await?
            {
                ClientRead::Bytes(n) => n,
                ClientRead::IdleTimeout => {
                    // PostgreSQL's `idle_session_timeout` behaviour: tell the
                    // client why, then close. A session idle INSIDE an open
                    // transaction is terminated too — PostgreSQL splits that
                    // case out into the separate
                    // `idle_in_transaction_session_timeout` GUC, which this
                    // proxy does not implement. Leaving the loop (rather than
                    // returning here) keeps the normal teardown path, so under
                    // transaction/statement pooling the session's still-idle
                    // backend connections are parked for reuse as usual.
                    if !tr.ext_dispatched {
                        Self::terminate_idle_session(stream, state, session).await;
                    }
                    break;
                }
            };
            // `read_next_client_message` drops a watched backend connection that
            // died while the session was idle; the session survives and the next
            // query redials. `watch_node` is a reborrow of `current_node` (see
            // where it is built above), so a now-missing entry is always the
            // session's current node — no equality re-check is needed.
            let watched_backend_gone = watch_node.is_some_and(|node| !conns.contains_key(node));
            if watched_backend_gone {
                // F3: dropping the dead cached connection is enough only with
                // `tr_mode = none` outside a transaction (the next query simply
                // redials, as before). Otherwise the loss must be carried into
                // the next request as a not-delivered backend fault against
                // this node: a transaction that was open died with the socket
                // (it must NOT silently continue in autocommit on a fresh
                // connection), and session state (SETs) has to be restored on
                // the replacement connection. `tr_handle_fault` picks this up.
                //
                // Recorded here rather than inside `read_next_client_message`
                // (where O2/S4 moved the watch loop) because the helper is
                // session-agnostic — it takes no `&ClientSession` and owns no
                // `TrSession` — and `watched_backend_gone` already reconstructs
                // the same signal losslessly from `conns`.
                if session.tr_mode != TrMode::None
                    || tr.ext_dispatched
                    || session
                        .in_transaction
                        .load(std::sync::atomic::Ordering::Relaxed)
                {
                    tr.lost_backend = watch_node.map(|node| node.to_string());
                }
                current_node = None;
            }

            if n == 0 {
                // Client disconnected
                break;
            }

            state
                .metrics
                .bytes_received
                .fetch_add(n as u64, Ordering::Relaxed);

            // Bound a single in-flight message: refuse before the accumulation
            // buffer for one (possibly malicious) oversized frame can exhaust
            // memory. A legitimate client never needs a single >64 MiB message.
            if buffer.len() > state.limits.max_pending_bytes {
                let emsg =
                    Self::create_error_response("53400", "message exceeds per-session size limit");
                let _ = stream.write_all(&emsg).await;
                let _ = stream.write_all(&Self::create_ready_for_query(b'I')).await;
                tracing::warn!(
                    client = %session.client_addr,
                    bytes = buffer.len(),
                    "inbound message exceeds size cap; closing connection"
                );
                return Ok(());
            }

            // Process all complete messages in buffer
            while let Some(msg) = codec.decode_message(&mut buffer)? {
                match msg.msg_type {
                    MessageType::Terminate => return Ok(()),

                    // ---- Simple query protocol ----
                    MessageType::Query => {
                        // Anomaly detector — record every Query message before
                        // the plugin hook so a detection lands in the audit
                        // trail even if a plugin later blocks.
                        #[cfg(feature = "anomaly-detection")]
                        Self::record_anomaly_observation(&msg, state, session);

                        // Plugin pre-query hook — may rewrite the SQL, block,
                        // or return a cached response.
                        let (msg, action) = Self::apply_pre_query_hook(msg, state, session);

                        if let PreQueryAction::Block(reason) = &action {
                            tracing::info!(reason = %reason, "pre-query plugin blocked query");
                            Self::send_block_response(stream, reason, state).await?;
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        #[cfg(feature = "wasm-plugins")]
                        if let PreQueryAction::Cached(bytes) = &action {
                            match Self::synthesise_cached_response(bytes) {
                                Ok(reply) => {
                                    stream.write_all(&reply).await.map_err(|e| {
                                        ProxyError::Network(format!("Write error: {}", e))
                                    })?;
                                    state
                                        .metrics
                                        .bytes_sent
                                        .fetch_add(reply.len() as u64, Ordering::Relaxed);
                                    state
                                        .metrics
                                        .queries_processed
                                        .fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, "failed to synthesise cached response; falling back to backend");
                                }
                            }
                        }

                        // Traffic mirror: offer the (final, post-rewrite)
                        // statement to the secondary backend. Non-blocking —
                        // never delays the client path.
                        if let Some(ref mirror) = state.mirror {
                            if let Some(sql) = crate::protocol::query_text(&msg.payload) {
                                mirror.offer(sql, Self::is_write_query(sql));
                            }
                        }

                        // Aborted-transaction emulation after an in-session
                        // failover: the client's transaction died with the old
                        // backend, so until it ends the transaction every other
                        // statement is refused exactly as PostgreSQL would
                        // (25P02) — never run in autocommit on the new backend.
                        if tr.tx_aborted {
                            let sql = crate::protocol::query_text(&msg.payload).unwrap_or("");
                            let mut resp = if Self::tr_ends_transaction(sql) {
                                tr.tx_aborted = false;
                                Self::note_ready_for_query(session, b'I', false);
                                let mut r = Self::create_command_complete("ROLLBACK");
                                r.extend_from_slice(&Self::create_ready_for_query(b'I'));
                                r
                            } else {
                                Self::note_ready_for_query(session, b'E', true);
                                let mut r = Self::create_error_response(
                                    "25P02",
                                    "current transaction is aborted, commands ignored until end of transaction block (aborted by proxy failover)",
                                );
                                r.extend_from_slice(&Self::create_ready_for_query(b'E'));
                                r
                            };
                            stream.write_all(&resp).await.map_err(|e| {
                                ProxyError::Network(format!("Client write error: {}", e))
                            })?;
                            Self::tr_after_simple(&mut tr, &msg, session, state).await;
                            tr.ext_dispatched = false;
                            state
                                .metrics
                                .bytes_sent
                                .fetch_add(resp.len() as u64, Ordering::Relaxed);
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                            resp.clear();
                            continue;
                        }

                        #[cfg(feature = "wasm-plugins")]
                        let forward_start = std::time::Instant::now();
                        let mut fault: Option<BackendFault> = None;
                        let fr = match tr.lost_backend.take() {
                            // The session's backend died while idle: the request
                            // was certainly not delivered anywhere.
                            Some(lost) => {
                                fault = Some(BackendFault {
                                    node: lost,
                                    phase: FaultPhase::NotDelivered,
                                    progress: ResponseProgress::default(),
                                    kind: None,
                                    error: "backend connection closed while the session was idle"
                                        .to_string(),
                                });
                                Err(ProxyError::Connection(
                                    "backend connection lost".to_string(),
                                ))
                            }
                            None => {
                                Self::forward_simple_query(
                                    stream,
                                    &msg,
                                    &mut conns,
                                    current_node.as_deref(),
                                    session,
                                    state,
                                    config,
                                    &mut fault,
                                )
                                .await
                            }
                        };
                        #[cfg(feature = "wasm-plugins")]
                        Self::fire_post_query_hook(
                            &msg,
                            session,
                            state,
                            &fr,
                            forward_start.elapsed(),
                        );
                        let (used_node, sent) = match fr {
                            Ok(v) => v,
                            Err(e) => match fault.take() {
                                Some(f) => {
                                    match Self::tr_handle_fault(
                                        stream,
                                        &mut conns,
                                        &mut current_node,
                                        f,
                                        InFlight::Simple(&msg),
                                        &mut tr,
                                        &stmt_registry,
                                        session,
                                        state,
                                        config,
                                    )
                                    .await?
                                    {
                                        Some(v) => v,
                                        None => return Ok(()),
                                    }
                                }
                                None => {
                                    if matches!(e, ProxyError::NoHealthyNodes) {
                                        if tr.ext_dispatched {
                                            return Ok(());
                                        }
                                        Self::send_no_healthy_nodes(stream, session, true).await;
                                        return Ok(());
                                    }
                                    return Err(e);
                                }
                            },
                        };
                        if let Some(n) = used_node {
                            current_node = Some(n);
                        }
                        Self::tr_after_simple(&mut tr, &msg, session, state).await;
                        tr.ext_dispatched = false;
                        // Transaction/Statement pooling: park the connection
                        // back to the shared pool once the session is idle.
                        #[cfg(feature = "pool-modes")]
                        Self::release_to_pool_if_idle(
                            &mut conns,
                            current_node.as_deref(),
                            session,
                            state,
                            config,
                        )
                        .await;
                        state.metrics.bytes_sent.fetch_add(sent, Ordering::Relaxed);
                        state
                            .metrics
                            .queries_processed
                            .fetch_add(1, Ordering::Relaxed);
                    }

                    // ---- Extended query protocol: accumulate until Sync/Flush ----
                    MessageType::Parse
                    | MessageType::Bind
                    | MessageType::Describe
                    | MessageType::Execute
                    | MessageType::Close => {
                        // Whether this message is appended to `pending`. An
                        // unnamed Parse held aside for promotion is the lone
                        // exception (resolved at the batch boundary).
                        let mut add_to_pending = true;
                        match msg.msg_type {
                            MessageType::Parse => {
                                // Register named statements so they can be
                                // re-prepared on a different backend later, and
                                // borrow the query (2nd cstring) for routing.
                                let name = Self::parse_stmt_name(&msg.payload);
                                let unnamed = name.is_empty();
                                if !unnamed {
                                    let name = name.to_string();
                                    let existed = stmt_registry.contains_key(&name);
                                    // Cap distinct prepared statements per session so a
                                    // client issuing unbounded named `Parse`s can't grow
                                    // `stmt_registry` without limit.
                                    if !existed
                                        && stmt_registry.len()
                                            >= state.limits.max_prepared_statements
                                    {
                                        let emsg = Self::create_error_response(
                                            "54000",
                                            "too many prepared statements for this session",
                                        );
                                        let _ = stream.write_all(&emsg).await;
                                        let _ = stream
                                            .write_all(&Self::create_ready_for_query(b'I'))
                                            .await;
                                        tracing::warn!(
                                            client = %session.client_addr,
                                            limit = state.limits.max_prepared_statements,
                                            "prepared-statement cap exceeded; closing connection"
                                        );
                                        return Ok(());
                                    }
                                    let encoded = msg.encode().freeze();
                                    // Bound the AGGREGATE bytes retained, not just the
                                    // count: a (possibly re-Parsed) statement that would
                                    // push the session over the byte cap is refused.
                                    let old_len =
                                        stmt_registry.get(&name).map(|b| b.len()).unwrap_or(0);
                                    let projected =
                                        stmt_registry_bytes.saturating_sub(old_len) + encoded.len();
                                    if projected > state.limits.max_prepared_bytes {
                                        let emsg = Self::create_error_response(
                                            "54000",
                                            "prepared-statement memory limit exceeded for this session",
                                        );
                                        let _ = stream.write_all(&emsg).await;
                                        let _ = stream
                                            .write_all(&Self::create_ready_for_query(b'I'))
                                            .await;
                                        tracing::warn!(
                                            client = %session.client_addr,
                                            limit = state.limits.max_prepared_bytes,
                                            "prepared-statement byte cap exceeded; closing connection"
                                        );
                                        return Ok(());
                                    }
                                    stmt_registry.insert(name.clone(), encoded);
                                    stmt_registry_bytes = projected;
                                    batch_defines.push(name);
                                }
                                if pending_route_sql.is_none() {
                                    if let Some(end) = msg.payload.iter().position(|&b| b == 0) {
                                        if let Some(q) =
                                            crate::protocol::query_text(&msg.payload[end + 1..])
                                        {
                                            if !q.is_empty() {
                                                pending_route_sql = Some(q.to_string());
                                                #[cfg(feature = "anomaly-detection")]
                                                Self::record_anomaly_sql(q, state, session);
                                            }
                                        }
                                    }
                                }
                                // Edge invalidation metadata: classify every
                                // Parse'd statement ONCE (is it DML? which
                                // tables?) so Sync-time invalidation of
                                // re-executed prepared writes is a map hit.
                                // Extended-protocol statements can dirty
                                // session state too (JDBC-style SET) — the
                                // sticky edge-ineligibility flag must catch
                                // them, not just simple-protocol text.
                                #[cfg(feature = "edge-proxy")]
                                if config.edge.enabled {
                                    if let Some(end) = msg.payload.iter().position(|&b| b == 0) {
                                        if let Some(q) =
                                            crate::protocol::query_text(&msg.payload[end + 1..])
                                        {
                                            if !session
                                                .edge_ineligible
                                                .load(std::sync::atomic::Ordering::Relaxed)
                                                && Self::stmt_leaves_session_state(q)
                                            {
                                                session.edge_ineligible.store(
                                                    true,
                                                    std::sync::atomic::Ordering::Relaxed,
                                                );
                                            }
                                            let meta = if Self::is_edge_dml_sql(q) {
                                                Some(crate::edge::fingerprint::tables_only(q))
                                            } else if Self::is_edge_procedural_sql(q)
                                                || Self::is_edge_txn_end_sql(q)
                                            {
                                                // Opaque write (EXECUTE/CALL/DO) or a
                                                // COMMIT/END that makes in-transaction
                                                // writes visible — memoize as the
                                                // empty-set wildcard so the batch's
                                                // Sync hook full-flushes.
                                                Some(Vec::new())
                                            } else {
                                                None
                                            };
                                            if unnamed {
                                                edge_unnamed_meta = meta;
                                            } else {
                                                edge_stmt_meta.insert(
                                                    Self::parse_stmt_name(&msg.payload).to_string(),
                                                    meta,
                                                );
                                            }
                                        }
                                    }
                                }
                                // Promotion: park an unnamed Parse that opens a
                                // fresh batch. Its signature is the payload after
                                // the empty statement-name NUL (query + param
                                // types). Anything that breaks the clean shape
                                // (a second Parse, a non-empty `pending`) un-parks
                                // it back into `pending` to preserve wire order.
                                if promote_unnamed
                                    && unnamed
                                    && pending.is_empty()
                                    && held_unnamed.is_none()
                                {
                                    let sig = bytes::Bytes::copy_from_slice(&msg.payload[1..]);
                                    held_unnamed = Some((msg.encode().freeze(), sig));
                                    add_to_pending = false;
                                } else if let Some((held_msg, _)) = held_unnamed.take() {
                                    let mut combined =
                                        BytesMut::with_capacity(held_msg.len() + pending.len());
                                    combined.extend_from_slice(&held_msg);
                                    combined.extend_from_slice(&pending);
                                    pending = combined;
                                }
                            }
                            MessageType::Bind => {
                                let stmt_ref = Self::bind_stmt_ref(&msg.payload);
                                // Unnamed statement: not tracked in
                                // batch_refs, but the edge invalidation must
                                // still see its execution.
                                #[cfg(feature = "edge-proxy")]
                                if stmt_ref.is_none() {
                                    edge_batch_bound_unnamed = true;
                                }
                                if let Some(name) = stmt_ref {
                                    batch_refs.push(name.to_string());
                                }
                            }
                            MessageType::Describe => {
                                if let Some(name) = Self::stmt_kind_name(&msg.payload) {
                                    batch_refs.push(name.to_string());
                                }
                            }
                            MessageType::Close => {
                                if let Some(name) = Self::stmt_kind_name(&msg.payload) {
                                    batch_closes.push(name.to_string());
                                }
                            }
                            _ => {}
                        }
                        if add_to_pending {
                            msg.encode_into(&mut pending);
                        }
                    }

                    // ---- Extended batch boundary ----
                    MessageType::Sync | MessageType::Flush => {
                        let wait_ready = msg.msg_type == MessageType::Sync;
                        msg.encode_into(&mut pending);
                        let batch = pending.split().freeze();
                        // Re-prepare any named statement this batch references
                        // but does not itself define, in case the target
                        // connection (after a switch/redial) is missing it.
                        let reprepare: Vec<String> = batch_refs
                            .iter()
                            .filter(|r| !batch_defines.contains(r))
                            .cloned()
                            .collect();
                        // Aborted-transaction emulation (see the simple-query
                        // path): only a transaction end goes through to the new
                        // backend; anything else is refused with 25P02. A bare
                        // Sync just yields the failed-transaction ReadyForQuery.
                        let mut skip_forward = false;
                        if tr.tx_aborted {
                            let ends = pending_route_sql
                                .as_deref()
                                .map(Self::tr_ends_transaction)
                                .unwrap_or(false);
                            if ends {
                                tr.tx_aborted = false;
                            } else {
                                skip_forward = true;
                                let bare_sync = batch.len() == 5 && batch[0] == b'S';
                                let mut resp = Vec::new();
                                if !bare_sync {
                                    resp.extend_from_slice(&Self::create_error_response(
                                        "25P02",
                                        "current transaction is aborted, commands ignored until end of transaction block (aborted by proxy failover)",
                                    ));
                                }
                                if wait_ready {
                                    resp.extend_from_slice(&Self::create_ready_for_query(b'E'));
                                    Self::note_ready_for_query(session, b'E', !bare_sync);
                                }
                                if !resp.is_empty() {
                                    stream.write_all(&resp).await.map_err(|e| {
                                        ProxyError::Network(format!("Client write error: {}", e))
                                    })?;
                                    state
                                        .metrics
                                        .bytes_sent
                                        .fetch_add(resp.len() as u64, Ordering::Relaxed);
                                }
                                held_unnamed = None;
                                // Nothing was closed on the backend: forget the
                                // batch's Close requests without pruning.
                                batch_closes.clear();
                            }
                        }
                        let (used_node, sent) = if skip_forward {
                            (None, 0)
                        } else {
                            let mut fault: Option<BackendFault> = None;
                            let fr = match tr.lost_backend.take() {
                                Some(lost) => {
                                    fault = Some(BackendFault {
                                        node: lost,
                                        phase: FaultPhase::NotDelivered,
                                        progress: ResponseProgress::default(),
                                        kind: None,
                                        error:
                                            "backend connection closed while the session was idle"
                                                .to_string(),
                                    });
                                    Err(ProxyError::Connection(
                                        "backend connection lost".to_string(),
                                    ))
                                }
                                None => {
                                    Self::forward_extended_batch(
                                        stream,
                                        &batch,
                                        pending_route_sql.as_deref(),
                                        wait_ready,
                                        &mut conns,
                                        current_node.as_deref(),
                                        &stmt_registry,
                                        &reprepare,
                                        &batch_defines,
                                        held_unnamed.as_ref(),
                                        session,
                                        state,
                                        config,
                                        &mut fault,
                                    )
                                    .await
                                }
                            };
                            match fr {
                                Ok(v) => v,
                                Err(e) => match fault.take() {
                                    Some(f) => {
                                        match Self::tr_handle_fault(
                                            stream,
                                            &mut conns,
                                            &mut current_node,
                                            f,
                                            InFlight::Extended {
                                                batch: &batch,
                                                route_sql: pending_route_sql.as_deref(),
                                                wait_ready,
                                                reprepare: &reprepare,
                                                defines: &batch_defines,
                                                unnamed: held_unnamed.as_ref(),
                                            },
                                            &mut tr,
                                            &stmt_registry,
                                            session,
                                            state,
                                            config,
                                        )
                                        .await?
                                        {
                                            Some(v) => v,
                                            None => return Ok(()),
                                        }
                                    }
                                    None => {
                                        if matches!(e, ProxyError::NoHealthyNodes) {
                                            if tr.ext_dispatched {
                                                return Ok(());
                                            }
                                            Self::send_no_healthy_nodes(
                                                stream, session, wait_ready,
                                            )
                                            .await;
                                            return Ok(());
                                        }
                                        return Err(e);
                                    }
                                },
                            }
                        };
                        if let Some(n) = used_node {
                            current_node = Some(n);
                        }
                        if !skip_forward {
                            Self::tr_after_extended(
                                &mut tr,
                                &batch,
                                held_unnamed.as_ref(),
                                pending_route_sql.as_deref(),
                                wait_ready,
                                &batch_defines,
                                &batch_refs,
                                &stmt_registry,
                                session,
                                state,
                            )
                            .await;
                            tr.ext_dispatched = !wait_ready;
                        }
                        held_unnamed = None;
                        // A `Sync` is the extended-protocol transaction/statement
                        // boundary (it yields ReadyForQuery); a `Flush` is not, so
                        // only a Sync triggers a pool release.
                        #[cfg(feature = "pool-modes")]
                        if wait_ready {
                            Self::release_to_pool_if_idle(
                                &mut conns,
                                current_node.as_deref(),
                                session,
                                state,
                                config,
                            )
                            .await;
                        }
                        state.metrics.bytes_sent.fetch_add(sent, Ordering::Relaxed);
                        // Extended-protocol writes drove zero edge invalidation
                        // before this hook — prepared-statement drivers
                        // (JDBC/Npgsql/asyncpg) would leave every edge stale
                        // until TTL. Union the tables of the batch's referenced
                        // DML statements (memoized at Parse) and invalidate/
                        // broadcast exactly like the simple path. Errors inside
                        // the batch only over-invalidate — safe. A COPY ... FROM
                        // batch is deferred to its CopyDone drain (rows visible
                        // then). Runs BEFORE the batch_closes drain below, so an
                        // execute-then-Close batch still sees its statement's
                        // metadata; only Sync-terminated batches fire (a pure
                        // Flush pipeline accumulates refs until its Sync).
                        #[cfg(feature = "edge-proxy")]
                        if wait_ready && config.edge.enabled {
                            if let Some(tables) = Self::edge_extended_batch_tables(
                                &batch_refs,
                                edge_batch_bound_unnamed,
                                &edge_stmt_meta,
                                &edge_unnamed_meta,
                            ) {
                                if session
                                    .copy_in_progress
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                {
                                    *session
                                        .pending_edge_copy_tables
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner()) = Some(tables);
                                } else {
                                    Self::edge_invalidate_write(state, config, tables).await;
                                }
                            }
                            edge_batch_bound_unnamed = false;
                        }
                        // Closed statements are deallocated everywhere — forget
                        // their canonical Parse so they are never re-prepared.
                        #[cfg(not(feature = "edge-proxy"))]
                        for name in batch_closes.drain(..) {
                            if let Some(removed) = stmt_registry.remove(&name) {
                                stmt_registry_bytes =
                                    stmt_registry_bytes.saturating_sub(removed.len());
                            }
                        }
                        // Edge builds also prune per-statement invalidation metadata
                        // — but that metadata is consumed by the Sync-time hook ABOVE
                        // (which runs first) and must survive a Close seen at an
                        // earlier Flush of the same batch, and a name Closed then
                        // re-Parsed in this batch must keep its FRESH meta. So the
                        // close queue is drained only at the Sync (stmt_registry is
                        // still pruned every boundary — idempotent), and edge meta is
                        // removed there only for names not redefined this batch.
                        // Retaining meta for a genuinely-closed name is safe: a later
                        // re-Parse overwrites it, and a stale entry only ever
                        // over-invalidates.
                        #[cfg(feature = "edge-proxy")]
                        {
                            for name in &batch_closes {
                                if let Some(removed) = stmt_registry.remove(name) {
                                    stmt_registry_bytes =
                                        stmt_registry_bytes.saturating_sub(removed.len());
                                }
                            }
                            if wait_ready {
                                for name in Self::edge_meta_prunable(&batch_closes, &batch_defines)
                                {
                                    edge_stmt_meta.remove(name);
                                }
                                batch_closes.clear();
                            }
                        }
                        if wait_ready {
                            // Sync ends the extended cycle; reset routing so the
                            // next Parse can re-route. Flush leaves it intact so
                            // the rest of the in-flight sequence stays put.
                            pending_route_sql = None;
                            batch_defines.clear();
                            batch_refs.clear();
                            state
                                .metrics
                                .queries_processed
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    // ---- COPY sub-protocol (client -> backend) ----
                    MessageType::CopyData | MessageType::CopyDone | MessageType::CopyFail => {
                        let is_copy_end =
                            matches!(msg.msg_type, MessageType::CopyDone | MessageType::CopyFail);
                        let conn = current_node.as_ref().and_then(|n| conns.get_mut(n));
                        match conn {
                            Some(b) => {
                                if let Err(e) = b.stream.write_all(&msg.encode()).await {
                                    // The backend died mid-COPY: the data stream
                                    // cannot be recovered in any tr_mode — one
                                    // error, then close (never a bare drop).
                                    let node = current_node.clone().unwrap_or_default();
                                    let err = format!("Backend copy write error: {}", e);
                                    conns.remove(&node);
                                    Self::record_backend_failure(state, &node, &err);
                                    session
                                        .copy_in_progress
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    let in_tx = session
                                        .in_transaction
                                        .load(std::sync::atomic::Ordering::Relaxed);
                                    let _ = Self::tr_send_error(
                                        stream,
                                        "08006",
                                        &format!(
                                            "backend {} failed during COPY ({}); connection closed",
                                            node, err
                                        ),
                                        in_tx,
                                        true,
                                    )
                                    .await;
                                    return Ok(());
                                }
                                if is_copy_end {
                                    let node = current_node.clone().unwrap();
                                    let r = Self::stream_until_ready(
                                        stream,
                                        &mut b.stream,
                                        session,
                                        state,
                                    )
                                    .await;
                                    // Copy has drained back to ReadyForQuery — the
                                    // session is no longer mid-COPY, so pool release
                                    // is allowed again.
                                    session
                                        .copy_in_progress
                                        .store(false, std::sync::atomic::Ordering::Relaxed);
                                    match r {
                                        Ok(sent) => {
                                            Self::tr_after_copy_drain(&mut tr, session).await;
                                            state
                                                .metrics
                                                .bytes_sent
                                                .fetch_add(sent, Ordering::Relaxed);
                                            // COPY ... FROM rows became visible at
                                            // this drain: fire the deferred edge
                                            // invalidation now. CopyFail loaded
                                            // nothing — just clear the stash.
                                            #[cfg(feature = "edge-proxy")]
                                            if config.edge.enabled {
                                                let stashed = session
                                                    .pending_edge_copy_tables
                                                    .lock()
                                                    .unwrap_or_else(|e| e.into_inner())
                                                    .take();
                                                if let Some(tables) = stashed {
                                                    if matches!(msg.msg_type, MessageType::CopyDone)
                                                    {
                                                        Self::edge_invalidate_write(
                                                            state, config, tables,
                                                        )
                                                        .await;
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            conns.remove(&node);
                                            let err = e.to_string();
                                            if err.contains("Client") {
                                                return Err(e.error);
                                            }
                                            if e.progress.terminal || e.progress.raw {
                                                return Ok(());
                                            }
                                            // Backend fault mid-COPY drain: tell the
                                            // client (08006) and close — every tr_mode.
                                            Self::record_backend_failure(state, &node, &err);
                                            let in_tx = session
                                                .in_transaction
                                                .load(std::sync::atomic::Ordering::Relaxed);
                                            let _ = Self::tr_send_error(
                                                stream,
                                                "08006",
                                                &format!(
                                                    "backend {} failed during COPY ({}); connection closed",
                                                    node, err
                                                ),
                                                in_tx,
                                                true,
                                            )
                                            .await;
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            None => {
                                // The client is streaming COPY frames but the
                                // backend connection is gone (dropped/redialed).
                                // Silently discarding them hangs the client
                                // forever; instead tell it the copy failed and
                                // return it to a clean idle state.
                                session
                                    .copy_in_progress
                                    .store(false, std::sync::atomic::Ordering::Relaxed);
                                // The COPY aborted — nothing loaded — so discard any
                                // deferred invalidation stash, else it would later
                                // fire against an unrelated COPY's drain.
                                #[cfg(feature = "edge-proxy")]
                                {
                                    *session
                                        .pending_edge_copy_tables
                                        .lock()
                                        .unwrap_or_else(|e| e.into_inner()) = None;
                                }
                                if is_copy_end {
                                    let emsg = Self::create_error_response(
                                        "57000",
                                        "COPY aborted: backend connection lost",
                                    );
                                    let _ = stream.write_all(&emsg).await;
                                    let _ =
                                        stream.write_all(&Self::create_ready_for_query(b'I')).await;
                                }
                            }
                        }
                    }

                    // ---- Anything else: forward to current backend best-effort ----
                    _ => {
                        if let Some(ref node) = current_node {
                            if let Some(b) = conns.get_mut(node) {
                                let _ = b.stream.write_all(&msg.encode()).await;
                            }
                        }
                    }
                }
            }

            // Bound un-flushed extended-protocol accumulation: a client must
            // reach a Sync/Flush boundary before this many bytes pile up in
            // `pending` (otherwise a never-syncing client grows it unbounded).
            if pending.len() > state.limits.max_pending_bytes {
                let emsg = Self::create_error_response(
                    "53400",
                    "un-flushed extended-protocol buffer exceeds per-session limit",
                );
                let _ = stream.write_all(&emsg).await;
                let _ = stream.write_all(&Self::create_ready_for_query(b'I')).await;
                tracing::warn!(
                    client = %session.client_addr,
                    pending = pending.len(),
                    "pending extended-protocol buffer cap exceeded; closing connection"
                );
                return Ok(());
            }
        }

        // On disconnect, park this session's still-idle connections so a later
        // same-identity client can reuse them (cross-client pooling). Anything
        // mid-transaction is left to drop (closed → backend rolls back).
        #[cfg(feature = "pool-modes")]
        if state.backend_pool.is_some() {
            let nodes: Vec<String> = conns.keys().cloned().collect();
            for node in nodes {
                Self::release_to_pool_if_idle(
                    &mut conns,
                    Some(node.as_str()),
                    session,
                    state,
                    config,
                )
                .await;
            }
        }

        Ok(())
    }

    /// Read one startup-phase message (`Startup`, `SSLRequest` or
    /// `CancelRequest`) from a client stream, appending whatever it reads into
    /// `buffer` so any bytes that follow the message are preserved for the
    /// caller. `Ok(None)` = the client closed before a complete message
    /// arrived. Callers bound this in time (pre-auth `startup_timeout`).
    async fn read_startup_message<S: AsyncRead + Unpin>(
        stream: &mut S,
        buffer: &mut BytesMut,
    ) -> Result<Option<StartupMessage>> {
        let codec = ProtocolCodec::new();
        let mut read_buf = vec![0u8; 1024];
        loop {
            if let Some(msg) = codec.decode_startup(buffer)? {
                return Ok(Some(msg));
            }
            let n = stream
                .read(&mut read_buf)
                .await
                .map_err(|e| ProxyError::Network(format!("Startup read error: {}", e)))?;
            if n == 0 {
                return Ok(None);
            }
            buffer.extend_from_slice(&read_buf[..n]);
        }
    }

    /// Peek the first startup-phase message and negotiate client TLS.
    ///
    /// On `SSLRequest` the proxy answers `S` and runs a rustls server
    /// handshake when a TLS acceptor is configured, otherwise `N`
    /// (plaintext). A `Startup`/`CancelRequest` arriving first (no
    /// SSLRequest) is returned in `pre` so the caller doesn't re-read it.
    async fn negotiate_client_tls(
        mut tcp: TcpStream,
        state: &Arc<ServerState>,
    ) -> Result<(ClientStream, Option<StartupMessage>)> {
        let mut buffer = BytesMut::with_capacity(1024);
        let first = match Self::read_startup_message(&mut tcp, &mut buffer).await? {
            Some(msg) => msg,
            None => {
                return Err(ProxyError::Connection(
                    "client closed before startup".to_string(),
                ))
            }
        };

        match first {
            StartupMessage::SSLRequest => match state.tls_acceptor.as_ref() {
                Some(acceptor) => {
                    tcp.write_all(b"S")
                        .await
                        .map_err(|e| ProxyError::Network(format!("SSL accept write: {}", e)))?;
                    let tls = acceptor
                        .accept(tcp)
                        .await
                        .map_err(|e| ProxyError::Network(format!("TLS handshake failed: {}", e)))?;
                    if tls.get_ref().1.peer_certificates().is_some() {
                        tracing::debug!("client presented a certificate (mTLS)");
                    }
                    Ok((ClientStream::Tls(Box::new(tls)), None))
                }
                None => {
                    tcp.write_all(b"N")
                        .await
                        .map_err(|e| ProxyError::Network(format!("SSL reject write: {}", e)))?;
                    Ok((ClientStream::Plain(tcp), None))
                }
            },
            other => Ok((ClientStream::Plain(tcp), Some(other))),
        }
    }

    /// Handle PostgreSQL startup phase (authentication). TLS/SSLRequest is
    /// already handled upstream in `negotiate_client_tls`; `pre` carries the
    /// first startup/cancel message when it was read during negotiation.
    async fn handle_startup(
        client_stream: &mut ClientStream,
        buffer: &mut BytesMut,
        pre: Option<StartupMessage>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<TcpStream>, String)> {
        // Use the message already read during TLS negotiation, or read one
        // now (the TLS case, where the real startup follows the handshake).
        let startup_msg = match pre {
            Some(msg) => Some(msg),
            None => match Self::read_startup_message(client_stream, buffer).await? {
                Some(msg) => Some(msg),
                // Client closed before sending a complete startup message.
                None => return Ok((None, String::new())),
            },
        };

        match startup_msg {
            Some(StartupMessage::SSLRequest) => {
                // SSL is negotiated upstream; a second SSLRequest here is a
                // protocol error — reject defensively.
                client_stream
                    .write_all(b"N")
                    .await
                    .map_err(|e| ProxyError::Network(format!("SSL reject error: {}", e)))?;
                Err(ProxyError::Protocol(
                    "unexpected SSLRequest after startup".to_string(),
                ))
            }
            Some(StartupMessage::CancelRequest { pid, key }) => {
                // Forward the cancel to the backend that owns this key, then
                // close (the client opened this connection only to cancel).
                Self::forward_cancel_request(state, pid, key).await;
                Ok((None, String::new()))
            }
            Some(StartupMessage::Startup { params, .. }) => {
                Self::connect_and_authenticate(client_stream, &params, session, state, config).await
            }
            None => Err(ProxyError::Protocol(
                "Incomplete startup message".to_string(),
            )),
        }
    }

    /// Evaluate pg_hba-style admission rules in order. The first rule whose
    /// user, database, and address all match decides; if none match, admit.
    fn hba_admits(rules: &[HbaRule], ip: std::net::IpAddr, user: &str, database: &str) -> bool {
        for r in rules {
            let user_ok = r.user == "all" || r.user == user;
            let db_ok = r.database == "all" || r.database == database;
            if user_ok && db_ok && Self::hba_addr_matches(&r.address, ip) {
                return r.action == HbaAction::Allow;
            }
        }
        true
    }

    /// Match a client address against an hba `address` spec: "all", a bare
    /// IP, or a CIDR (`10.0.0.0/8`, `::1/128`).
    fn hba_addr_matches(spec: &str, ip: std::net::IpAddr) -> bool {
        use std::net::IpAddr;
        if spec == "all" {
            return true;
        }
        if let Some((net, bits)) = spec.split_once('/') {
            let bits: u32 = match bits.parse() {
                Ok(b) => b,
                Err(_) => return false,
            };
            match (net.parse::<IpAddr>(), ip) {
                (Ok(IpAddr::V4(n)), IpAddr::V4(i)) if bits <= 32 => {
                    let mask = if bits == 0 {
                        0
                    } else {
                        u32::MAX << (32 - bits)
                    };
                    (u32::from(n) & mask) == (u32::from(i) & mask)
                }
                (Ok(IpAddr::V6(n)), IpAddr::V6(i)) if bits <= 128 => {
                    let mask = if bits == 0 {
                        0
                    } else {
                        u128::MAX << (128 - bits)
                    };
                    (u128::from(n) & mask) == (u128::from(i) & mask)
                }
                _ => false,
            }
        } else {
            spec.parse::<IpAddr>().map(|s| s == ip).unwrap_or(false)
        }
    }

    /// Run a proxy-terminated SCRAM-SHA-256 server exchange against the
    /// client, validating its password with the configured `auth_file`. On
    /// success the client is authenticated by the proxy (no AuthenticationOk
    /// is sent here — the backend's is forwarded later). On any failure
    /// returns Err; the caller emits an ErrorResponse and closes.
    async fn proxy_scram_auth(
        client: &mut ClientStream,
        user: &str,
        state: &Arc<ServerState>,
    ) -> std::result::Result<(), String> {
        use crate::auth_scram::ScramServer;
        let auth_file = state.auth_file.as_ref().ok_or("scram not configured")?;

        // 1. AuthenticationSASL: advertise SCRAM-SHA-256.
        let mut sasl = BytesMut::new();
        sasl.put_i32(10); // SASL
        sasl.extend_from_slice(b"SCRAM-SHA-256\0");
        sasl.put_u8(0); // end of mechanism list
        Self::write_auth_frame(client, &sasl).await?;

        // 2. Read SASLInitialResponse ('p'): mechanism cstring + i32 len + data.
        let init = Self::read_password_message(client).await?;
        let mech_end = init
            .iter()
            .position(|&b| b == 0)
            .ok_or("malformed SASLInitialResponse (no mechanism)")?;
        if init.len() < mech_end + 5 {
            return Err("short SASLInitialResponse".into());
        }
        let client_first =
            std::str::from_utf8(&init[mech_end + 5..]).map_err(|_| "client-first not UTF-8")?;

        // 3. Look up the verifier (unknown user -> generic failure).
        let verifier = auth_file.get(user).ok_or("no such user")?.clone();

        // 4. server-first.
        let server_nonce = Self::random_nonce();
        let (server, server_first) = ScramServer::start(verifier, client_first, &server_nonce)?;

        // 5. AuthenticationSASLContinue.
        let mut cont = BytesMut::new();
        cont.put_i32(11);
        cont.extend_from_slice(server_first.as_bytes());
        Self::write_auth_frame(client, &cont).await?;

        // 6. Read SASLResponse ('p'): payload = client-final.
        let client_final_raw = Self::read_password_message(client).await?;
        let client_final =
            std::str::from_utf8(&client_final_raw).map_err(|_| "client-final not UTF-8")?;

        // 7. Verify -> server-final.
        let server_final = server.finish(client_final)?;

        // 8. AuthenticationSASLFinal (no AuthenticationOk — backend's follows).
        let mut fin = BytesMut::new();
        fin.put_i32(12);
        fin.extend_from_slice(server_final.as_bytes());
        Self::write_auth_frame(client, &fin).await?;
        Ok(())
    }

    /// Write an AuthenticationRequest ('R') frame with the given payload.
    async fn write_auth_frame(
        client: &mut ClientStream,
        payload: &[u8],
    ) -> std::result::Result<(), String> {
        let mut frame = BytesMut::with_capacity(payload.len() + 5);
        frame.put_u8(b'R');
        frame.put_u32((payload.len() + 4) as u32);
        frame.extend_from_slice(payload);
        client
            .write_all(&frame)
            .await
            .map_err(|e| format!("client write: {}", e))
    }

    /// Read one Password/SASL ('p') message from the client, returning its
    /// payload. Errors on EOF or any non-'p' frame.
    async fn read_password_message(
        client: &mut ClientStream,
    ) -> std::result::Result<BytesMut, String> {
        let codec = ProtocolCodec::new();
        let mut buffer = BytesMut::with_capacity(1024);
        let mut read_buf = vec![0u8; 1024];
        loop {
            if let Some(msg) = codec
                .decode_message(&mut buffer)
                .map_err(|e| format!("decode: {}", e))?
            {
                if msg.msg_type == MessageType::Password {
                    return Ok(msg.payload);
                }
                return Err(format!("expected SASL response, got {:?}", msg.msg_type));
            }
            let n = client
                .read(&mut read_buf)
                .await
                .map_err(|e| format!("client read: {}", e))?;
            if n == 0 {
                return Err("client closed during SASL".into());
            }
            buffer.extend_from_slice(&read_buf[..n]);
        }
    }

    /// A fresh random SCRAM server nonce (printable, no comma).
    fn random_nonce() -> String {
        use rand::Rng;
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rand::thread_rng();
        (0..24)
            .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
            .collect()
    }

    /// Connect to backend and handle authentication
    async fn connect_and_authenticate(
        client_stream: &mut ClientStream,
        params: &HashMap<String, String>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<(Option<TcpStream>, String)> {
        // pg_hba-style admission: reject disallowed (user, database, client
        // address) combinations before opening any backend connection.
        let user = params.get("user").map(String::as_str).unwrap_or("");
        let database = params.get("database").map(String::as_str).unwrap_or(user);
        if !Self::hba_admits(&config.hba, session.client_addr.ip(), user, database) {
            tracing::info!(%user, %database, client = %session.client_addr, "connection rejected by hba rule");
            let err = Self::create_error_response(
                "28000",
                "connection rejected by proxy admission rules",
            );
            let _ = client_stream.write_all(&err).await;
            return Ok((None, String::new()));
        }

        // Proxy-terminated SCRAM-SHA-256: when an auth_file is configured the
        // proxy authenticates the client itself (becoming the auth boundary)
        // instead of relaying credentials to the backend. On success it falls
        // through to the normal backend connect, whose AuthenticationOk +
        // session messages are forwarded to the already-authenticated client.
        if state.auth_file.is_some() {
            if let Err(e) = Self::proxy_scram_auth(client_stream, user, state).await {
                tracing::info!(%user, error = %e, "proxy SCRAM auth failed");
                let err =
                    Self::create_error_response("28P01", &format!("authentication failed: {}", e));
                let _ = client_stream.write_all(&err).await;
                return Ok((None, String::new()));
            }
            tracing::debug!(%user, "client authenticated by proxy SCRAM");
        }

        // Plugin Authenticate hook — may deny the connection outright or
        // attach a richer identity (roles, tenant_id, claims) onto the
        // session for downstream plugins to consume. Happens before any
        // backend connection is opened so denials cost nothing on the
        // backend side.
        Self::apply_authenticate_hook(params, session, state).await?;

        // Migration cutover: when active, redirect this connection to the
        // promoted target, substituting the target's credentials/database for
        // the client's so the cutover is transparent to the application.
        let cutover = state.cutover.load_full();
        let (node_addr, effective_params) = if let Some(t) = cutover.as_ref() {
            let mut p = params.clone();
            p.insert("user".to_string(), t.user.clone());
            if let Some(ref db) = t.database {
                p.insert("database".to_string(), db.clone());
            } else {
                p.remove("database");
            }
            tracing::debug!(target = %t.addr, "routing connection to cutover target");
            (t.addr.clone(), p)
        } else {
            (
                Self::select_node(session, state, config).await?,
                params.clone(),
            )
        };

        // Connect to backend. A failure here (the node is down at the moment a
        // new client connects) demotes the node in-band too — not just failures
        // on the forward path — so a dead backend is detected on the very next
        // connection instead of waiting for the periodic health checker.
        let mut backend = match tokio::time::timeout(
            config.pool.acquire_timeout(),
            TcpStream::connect(&node_addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                let msg = format!("Failed to connect to {}: {}", node_addr, e);
                Self::note_backend_failure(state, &node_addr, &msg);
                return Err(ProxyError::Connection(msg));
            }
            Err(_) => {
                let msg = format!("Connection timeout to {}", node_addr);
                Self::note_backend_failure(state, &node_addr, &msg);
                return Err(ProxyError::Connection(msg));
            }
        };
        let _ = backend.set_nodelay(true);

        // Build and send startup message to backend
        let params = &effective_params;
        let startup_bytes = Self::build_startup_message(params);
        backend
            .write_all(&startup_bytes)
            .await
            .map_err(|e| ProxyError::Network(format!("Backend startup write error: {}", e)))?;

        if let Some(af) = state.auth_file.as_ref() {
            // The proxy is the auth boundary: the client is already
            // authenticated, so the backend's challenges must NOT be relayed to
            // it. Authenticate the backend ourselves (SCRAM/MD5/cleartext with
            // the user's plaintext auth_file entry; trust needs nothing), then
            // hand the client a synthesized AuthenticationOk followed by the
            // backend's own ParameterStatus/BackendKeyData/ReadyForQuery.
            let credential = af.password(user).map(str::to_string);
            let post_auth = match Self::complete_backend_auth(
                &mut backend,
                state.limits.max_pending_bytes,
                user,
                credential.as_deref(),
            )
            .await
            {
                Ok(frames) => frames,
                Err(e) => {
                    Self::note_backend_failure(state, &node_addr, &e.to_string());
                    let err = Self::create_error_response(
                        "08006",
                        &format!("backend authentication failed: {}", e),
                    );
                    let _ = client_stream.write_all(&err).await;
                    return Err(e);
                }
            };
            *session.backend_credential.write().await = credential;
            // Register the cancel key from the BackendKeyData frame.
            let mut off = 0usize;
            while off + 5 <= post_auth.len() {
                let len = u32::from_be_bytes([
                    post_auth[off + 1],
                    post_auth[off + 2],
                    post_auth[off + 3],
                    post_auth[off + 4],
                ]) as usize;
                if len < 4 || off + 1 + len > post_auth.len() {
                    break;
                }
                if post_auth[off] == b'K' && len + 1 >= 13 {
                    let f = &post_auth[off..off + 1 + len];
                    let pid = u32::from_be_bytes([f[5], f[6], f[7], f[8]]);
                    let key = u32::from_be_bytes([f[9], f[10], f[11], f[12]]);
                    Self::register_cancel_key(state, pid, key, &node_addr);
                }
                off += 1 + len;
            }
            let mut to_client = Vec::with_capacity(9 + post_auth.len());
            to_client.extend_from_slice(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0]); // AuthenticationOk
            to_client.extend_from_slice(&post_auth);
            client_stream
                .write_all(&to_client)
                .await
                .map_err(|e| ProxyError::Network(format!("Client auth write error: {}", e)))?;
        } else {
            // Pass-through: forward authentication messages between client and
            // backend. Registers the backend's BackendKeyData so a later
            // CancelRequest can be routed back to this node.
            Self::proxy_authentication(client_stream, &mut backend, state, &node_addr).await?;
        }

        // Store session variables
        {
            let mut vars = session.variables.write().await;
            for (k, v) in params {
                vars.insert(k.clone(), v.clone());
            }
        }

        Ok((Some(backend), node_addr))
    }

    /// Build PostgreSQL startup message
    fn build_startup_message(params: &HashMap<String, String>) -> Vec<u8> {
        let mut payload = BytesMut::new();

        // Protocol version 3.0
        payload.put_u32(196608);

        // Parameters
        for (key, value) in params {
            payload.extend_from_slice(key.as_bytes());
            payload.put_u8(0);
            payload.extend_from_slice(value.as_bytes());
            payload.put_u8(0);
        }
        payload.put_u8(0); // Terminator

        // Build complete message with length prefix
        let mut msg = BytesMut::new();
        msg.put_u32((payload.len() + 4) as u32);
        msg.extend_from_slice(&payload);

        msg.to_vec()
    }

    // The operational limits/timeouts that were compiled-in `const`s here are
    // now tunable via the `[limits]` config section (`crate::config::LimitsToml`),
    // resolved once at startup into `ServerState::limits` (`ResolvedLimits`).
    // Defaults are byte-for-byte the prior constants; see that struct.

    /// Record the backend that owns a BackendKeyData (pid, secret) pair.
    fn register_cancel_key(state: &Arc<ServerState>, pid: u32, key: u32, node_addr: &str) {
        // FIFO-evict the oldest registrations when at capacity, rather than
        // dropping all of them. Evict a small batch so we don't churn the lock
        // on every insert once full.
        {
            let mut order = state.cancel_order.lock();
            while state.cancel_map.len() >= state.limits.max_cancel_keys {
                match order.pop_front() {
                    Some(old) => {
                        state.cancel_map.remove(&old);
                    }
                    None => {
                        // Order queue empty but map full (shouldn't happen) —
                        // fall back to a clear to stay bounded.
                        state.cancel_map.clear();
                        break;
                    }
                }
            }
            order.push_back((pid, key));
        }
        state.cancel_map.insert((pid, key), node_addr.to_string());
    }

    /// Forward a client CancelRequest to the backend that issued the
    /// matching BackendKeyData. Best-effort: unknown keys are ignored.
    async fn forward_cancel_request(state: &Arc<ServerState>, pid: u32, key: u32) {
        let Some(addr) = state.cancel_map.get(&(pid, key)).map(|e| e.clone()) else {
            tracing::debug!(pid, "cancel request for unknown key; ignoring");
            return;
        };
        // CancelRequest: int32 len(16) + int32 code(80877102) + pid + key.
        let mut msg = BytesMut::with_capacity(16);
        msg.put_u32(16);
        msg.put_u32(80877102);
        msg.put_u32(pid);
        msg.put_u32(key);
        match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&addr)).await {
            Ok(Ok(mut conn)) => {
                let _ = conn.set_nodelay(true);
                if let Err(e) = conn.write_all(&msg).await {
                    tracing::warn!(node = %addr, error = %e, "failed to forward CancelRequest");
                }
                // PG closes the connection after handling a CancelRequest.
            }
            other => {
                tracing::warn!(node = %addr, ?other, "could not connect to forward CancelRequest")
            }
        }
    }

    /// Proxy authentication messages between client and backend
    async fn proxy_authentication(
        client_stream: &mut ClientStream,
        backend_stream: &mut TcpStream,
        state: &Arc<ServerState>,
        node_addr: &str,
    ) -> Result<()> {
        // Bidirectional relay driven by readiness, not a fixed poll. The old
        // loop read the backend (untimed), forwarded, then polled the client
        // with a fixed 100ms window; a client that answered an auth challenge
        // more than 100ms after receiving it (WAN RTT, slow SCRAM client) missed
        // its window, and the loop then re-blocked on the untimed backend read
        // while the backend waited for the very response the proxy never read —
        // a deadlock until PostgreSQL's authentication_timeout killed it. Here
        // both directions are relayed as either side becomes readable, under one
        // overall deadline, so multi-round SCRAM completes regardless of client
        // latency.
        //
        // Backend-side frames are inspected by RAW tag (the wire decoder is
        // direction-agnostic — 'E' would decode to the client-side `Execute`,
        // not `ErrorResponse`), so a backend auth error is recognised here
        // rather than falling through to a misleading timeout.
        let mut backend_buffer = BytesMut::with_capacity(4096);
        let mut cbuf = vec![0u8; 4096];
        let mut bbuf = vec![0u8; 4096];
        let deadline = tokio::time::Instant::now() + state.limits.startup_timeout;

        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(ProxyError::Auth(
                        "authentication timed out".to_string(),
                    ));
                }
                // Backend -> client: relay every byte, then scan complete frames
                // for the auth terminal states.
                r = backend_stream.read(&mut bbuf) => {
                    let n = r.map_err(|e| {
                        ProxyError::Network(format!("Backend auth read error: {}", e))
                    })?;
                    if n == 0 {
                        return Err(ProxyError::Connection(
                            "Backend closed during auth".to_string(),
                        ));
                    }
                    client_stream
                        .write_all(&bbuf[..n])
                        .await
                        .map_err(|e| ProxyError::Network(format!("Client auth write error: {}", e)))?;
                    backend_buffer.extend_from_slice(&bbuf[..n]);

                    // Walk complete frames by raw tag.
                    loop {
                        if backend_buffer.len() < 5 {
                            break;
                        }
                        let len = u32::from_be_bytes([
                            backend_buffer[1],
                            backend_buffer[2],
                            backend_buffer[3],
                            backend_buffer[4],
                        ]) as usize;
                        if len < 4 {
                            break;
                        }
                        validate_backend_frame_len(len, state.limits.max_pending_bytes)?;
                        if backend_buffer.len() < len + 1 {
                            break;
                        }
                        let tag = backend_buffer[0];
                        let frame = backend_buffer.split_to(len + 1);
                        match tag {
                            // BackendKeyData: 5-byte header + pid(4) + key(4).
                            // Remember which backend owns this cancel key.
                            b'K' if frame.len() >= 13 => {
                                let pid = u32::from_be_bytes([
                                    frame[5], frame[6], frame[7], frame[8],
                                ]);
                                let key = u32::from_be_bytes([
                                    frame[9], frame[10], frame[11], frame[12],
                                ]);
                                Self::register_cancel_key(state, pid, key, node_addr);
                            }
                            // ReadyForQuery: authentication + startup complete.
                            b'Z' => return Ok(()),
                            // ErrorResponse: auth failed (already relayed to the
                            // client above); surface the failure to the caller.
                            b'E' => {
                                return Err(ProxyError::Auth("Authentication failed".to_string()));
                            }
                            _ => {}
                        }
                    }
                }
                // Client -> backend: relay the client's auth response(s)
                // whenever they arrive, with no artificial deadline of their own.
                r = client_stream.read(&mut cbuf) => {
                    let n = r.map_err(|e| {
                        ProxyError::Network(format!("Client auth read error: {}", e))
                    })?;
                    if n == 0 {
                        return Err(ProxyError::Connection(
                            "Client closed during auth".to_string(),
                        ));
                    }
                    backend_stream
                        .write_all(&cbuf[..n])
                        .await
                        .map_err(|e| {
                            ProxyError::Network(format!("Backend password write error: {}", e))
                        })?;
                }
            }
        }
    }

    /// Decide which node a request should be routed to, without doing any
    /// I/O. Reuses `current_node` when it is healthy and role-compatible
    /// (sticky session), otherwise selects a fresh primary/read node. The
    /// returned address is the key into the per-session connection cache.
    async fn choose_target_node(
        is_write: bool,
        forced_target: Option<String>,
        current_node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        // After a migration cutover, every request stays on the promoted
        // target — never route back to the former primary.
        if let Some(t) = state.cutover.load_full().as_ref() {
            return Ok(t.addr.clone());
        }

        // Read-your-writes: within the window after a write, a read is pinned to
        // the primary (overriding the reuse-of-a-standby path) so the client
        // observes its own writes despite replica lag.
        #[cfg(feature = "lag-routing")]
        if !is_write && forced_target.is_none() && config.lag_routing.enabled {
            let last_write = *session.last_write_at.read().await;
            if Self::ryw_pins_primary(last_write, config.lag_routing.ryw_window_ms) {
                tracing::debug!(target: "helios::routing", "read-your-writes: pinning read to primary");
                return Self::select_primary_with_timeout(session, state, config).await;
            }
        }

        let need_switch = if let Some(ref forced) = forced_target {
            let health = state.health.load_full();
            let reuse = current_node
                .map(|c| c == forced && health.get(c).map(|h| h.healthy).unwrap_or(false))
                .unwrap_or(false);
            !reuse
        } else if let Some(current) = current_node {
            let health = state.health.load_full();
            let current_healthy = health.get(current).map(|h| h.healthy).unwrap_or(false);
            if !current_healthy {
                true
            } else if is_write {
                let is_primary = config
                    .nodes
                    .iter()
                    .find(|n| n.address() == current)
                    .map(|n| n.role == NodeRole::Primary)
                    .unwrap_or(false);
                !is_primary
            } else {
                false
            }
        } else {
            true
        };

        if let Some(forced) = forced_target {
            // Resolve a node *name* to its address; an address is passed
            // through unchanged. This lets `/*helios:node=pg-standby*/` (and a
            // plugin `Node("name")`) target a node by its configured name
            // rather than requiring the raw host:port.
            let resolved = config
                .nodes
                .iter()
                .find(|n| n.name.as_deref() == Some(forced.as_str()) || n.address() == forced)
                .map(|n| n.address().to_string())
                .unwrap_or(forced);
            Ok(resolved)
        } else if need_switch {
            if is_write {
                Self::select_primary_with_timeout(session, state, config).await
            } else {
                Self::select_read_node(session, state, config).await
            }
        } else {
            Ok(current_node.unwrap().to_string())
        }
    }

    /// Ensure the per-session cache holds an authenticated backend connection
    /// to `target`, dialing + silently re-authenticating one (with the
    /// client's pass-through credentials) only if absent. The cached
    /// connection is then reused across read/write route switches.
    async fn ensure_conn(
        conns: &mut HashMap<String, BackendConn>,
        target: &str,
        session: &Arc<ClientSession>,
        config: &ProxyConfig,
        state: &Arc<ServerState>,
    ) -> Result<()> {
        if conns.contains_key(target) {
            return Ok(());
        }

        // Transaction/Statement pooling: lease a parked, identity-matched
        // connection before paying for a fresh TCP connect + startup + auth.
        // The parked connection was `DISCARD ALL`-reset on release, so it is
        // clean for this (same-identity) client.
        #[cfg(feature = "pool-modes")]
        if let Some(pool) = state.backend_pool.as_ref() {
            let key = Self::pool_key_for(target, session).await;
            if let Some(stream) = pool.checkout(&key) {
                tracing::info!(
                    target: "helios::pool",
                    node = %target,
                    "reused pooled backend connection"
                );
                conns.insert(target.to_string(), BackendConn::new(stream));
                return Ok(());
            }
        }

        let mut backend =
            tokio::time::timeout(config.pool.acquire_timeout(), TcpStream::connect(target))
                .await
                .map_err(|_| ProxyError::Connection(format!("Connection timeout to {}", target)))?
                .map_err(|e| {
                    ProxyError::Connection(format!("Failed to connect to {}: {}", target, e))
                })?;
        let _ = backend.set_nodelay(true);

        let params = session.variables.read().await.clone();
        let startup = Self::build_startup_message(&params);
        backend
            .write_all(&startup)
            .await
            .map_err(|e| ProxyError::Network(format!("Backend startup error: {}", e)))?;
        let user = params.get("user").map(String::as_str).unwrap_or("");
        let credential = session.backend_credential.read().await.clone();
        Self::complete_backend_auth(
            &mut backend,
            state.limits.max_pending_bytes,
            user,
            credential.as_deref(),
        )
        .await?;
        #[cfg(feature = "pool-modes")]
        if state.backend_pool.is_some() {
            tracing::debug!(target: "helios::pool", node = %target, "dialed fresh backend connection (pool miss)");
        }
        tracing::debug!(node = %target, "opened backend connection");
        conns.insert(target.to_string(), BackendConn::new(backend));
        Ok(())
    }

    /// Startup parameters that change how the backend interprets or renders
    /// values and are reset by `DISCARD ALL`/`RESET ALL` to the connection's
    /// *startup* values. Two clients of the same `(node,user,db)` but different
    /// values for any of these must NOT share a pooled connection, or the
    /// borrower silently inherits the lender's encoding/date/number formatting.
    /// This mirrors PgBouncer's `track_extra_parameters` intent.
    #[cfg(feature = "pool-modes")]
    const POOL_IDENTITY_PARAMS: &'static [&'static str] = &[
        "client_encoding",
        "DateStyle",
        "TimeZone",
        "IntervalStyle",
        "standard_conforming_strings",
        "options",
    ];

    /// Build the pool key for the current session's connection identity.
    /// Base identity is `(node, user, database)` — connections are reused only
    /// within an identity, so a borrower always matches the principal the parked
    /// connection was authenticated as. When any routing-relevant startup GUC
    /// (see `POOL_IDENTITY_PARAMS`) is set, a hash of those is folded in so the
    /// borrower also matches the lender's value-formatting settings. The common
    /// case (no custom GUCs) keeps the bare `(node,user,db)` key unchanged.
    #[cfg(feature = "pool-modes")]
    async fn pool_key_for(target: &str, session: &Arc<ClientSession>) -> String {
        let vars = session.variables.read().await;
        let user = vars.get("user").map(|s| s.as_str()).unwrap_or("");
        // PostgreSQL defaults the database to the role name when unset.
        let database = vars.get("database").map(|s| s.as_str()).unwrap_or(user);
        let base = crate::pool::pool_key(target, user, database);

        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        let mut any = false;
        for k in Self::POOL_IDENTITY_PARAMS {
            if let Some(v) = vars.get(*k) {
                any = true;
                k.hash(&mut h);
                v.hash(&mut h);
            }
        }
        if any {
            format!("{}\0{:016x}", base, h.finish())
        } else {
            base
        }
    }

    /// Reset a backend connection to a clean session state before parking it
    /// for reuse: runs the configured reset query (default `DISCARD ALL`,
    /// which deallocates prepared statements, drops temp tables, resets GUCs
    /// and advisory locks) and drains its response to `ReadyForQuery`. Returns
    /// `Err` if the connection is unhealthy OR the reset itself did not cleanly
    /// succeed — the caller then drops (closes) it instead of parking a
    /// poisoned connection.
    ///
    /// "Cleanly succeeded" means: no `ErrorResponse` frame in the reply AND the
    /// terminating `ReadyForQuery` reported idle (`'I'`, not in/failed
    /// transaction). The previous version returned `Ok` on the first
    /// `ReadyForQuery` regardless, so a failed `DISCARD ALL` (e.g. a copy-abort,
    /// or a custom `reset_query` that errors) would park a connection with its
    /// GUCs / temp tables / prepared statements intact — exactly what pooling
    /// must never do. Frames are walked by raw tag because the wire decoder is
    /// direction-agnostic (`'E'` decodes to the client-side `Execute`), so a
    /// backend `ErrorResponse` cannot be recognised via `msg_type`.
    #[cfg(feature = "pool-modes")]
    async fn reset_backend<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        stream: &mut S,
        reset_sql: &str,
        backend_write_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<()> {
        let msg = crate::protocol::QueryMessage {
            query: reset_sql.to_string(),
        }
        .encode();
        tokio::time::timeout(backend_write_timeout, stream.write_all(&msg.encode()))
            .await
            .map_err(|_| ProxyError::Network("reset write timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("reset write error: {}", e)))?;

        let mut buf = BytesMut::with_capacity(1024);
        let mut had_error = false;
        loop {
            // Walk complete frames by raw tag, tracking any ErrorResponse and
            // stopping at ReadyForQuery.
            let mut consumed = 0usize;
            let mut ready_status: Option<u8> = None;
            loop {
                let rem = &buf[consumed..];
                let Some(len) = backend_frame_len(rem, max_frame_bytes)? else {
                    break;
                };
                if rem.len() < len + 1 {
                    break;
                }
                let mtype = rem[0];
                let frame_total = len + 1;
                if mtype == b'E' {
                    had_error = true;
                }
                consumed += frame_total;
                if mtype == b'Z' {
                    ready_status = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                    break;
                }
            }
            let _ = buf.split_to(consumed);

            if let Some(status) = ready_status {
                if had_error || status != b'I' {
                    return Err(ProxyError::Connection(format!(
                        "reset query did not cleanly succeed (error={}, status={})",
                        had_error, status as char
                    )));
                }
                return Ok(());
            }

            buf.reserve(1024);
            let n = tokio::time::timeout(Duration::from_secs(5), stream.read_buf(&mut buf))
                .await
                .map_err(|_| ProxyError::Network("reset drain timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("reset drain read error: {}", e)))?;
            if n == 0 {
                return Err(ProxyError::Connection(
                    "backend closed during reset".to_string(),
                ));
            }
        }
    }

    /// Transaction/Statement pooling release point: when the session is at an
    /// idle boundary (`ReadyForQuery` reported not-in-transaction), reset the
    /// just-used connection and park it for reuse by the next same-identity
    /// client. A no-op in Session mode or when the feature is off. Never
    /// releases mid-transaction.
    #[cfg(feature = "pool-modes")]
    async fn release_to_pool_if_idle(
        conns: &mut HashMap<String, BackendConn>,
        node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) {
        let Some(pool) = state.backend_pool.as_ref() else {
            return;
        };
        let Some(node) = node else {
            return;
        };
        // Only release at a clean transaction boundary — never mid-transaction
        // and never mid-COPY (the backend is awaiting CopyData; resetting +
        // parking the socket now aborts the copy and hangs the client).
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
            || session
                .copy_in_progress
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        let Some(mut bc) = conns.remove(node) else {
            return;
        };

        // Conditional reset: a connection that provably touched no session
        // state — no dirtying simple statement, no named prepared statement, no
        // unnamed prepared statement — can be parked WITHOUT the `DISCARD ALL`
        // round-trip, removing a backend RTT from the critical path for clean
        // autocommit workloads. Any of these three signals forces the full
        // reset. Extended-protocol traffic always has `prepared`/`unnamed_sig`
        // set, so it is never clean-skipped (conservative by construction).
        let clean = !bc.dirty && bc.prepared.is_empty() && bc.unnamed_sig.is_none();
        if config.pool_mode.skip_clean_reset && clean {
            let key = Self::pool_key_for(node, session).await;
            if pool.checkin(&key, bc.stream) {
                pool.note_reset_skipped();
                tracing::debug!(target: "helios::pool", node = %node, "parked clean backend connection (reset skipped)");
            }
            return;
        }

        if Self::reset_backend(
            &mut bc.stream,
            &config.pool_mode.reset_query,
            state.limits.backend_write_timeout,
            state.limits.max_backend_frame_bytes,
        )
        .await
        .is_ok()
        {
            let key = Self::pool_key_for(node, session).await;
            if pool.checkin(&key, bc.stream) {
                tracing::debug!(target: "helios::pool", node = %node, "parked backend connection for reuse");
            }
        }
        // On reset failure the connection is dropped here (closed).
    }

    /// Forward a simple-query (`Query`) message and stream its response back
    /// to the client frame-by-frame, ending at ReadyForQuery. Picks (and, if
    /// needed, opens) the target node's connection from the per-session
    /// cache. Returns `(Some(node_used), bytes)` — `None` node means the
    /// request was short-circuited (plugin block) without touching a backend.
    ///
    /// On a backend fault the broken connection is dropped, the node is
    /// demoted, `fault` is filled with the failed node and whether the
    /// statement had already been delivered, and `Err` is returned so the
    /// caller can run the `tr_mode` recovery. A client-side error leaves
    /// `fault` untouched.
    #[allow(clippy::too_many_arguments)]
    async fn forward_simple_query(
        client: &mut ClientStream,
        msg: &Message,
        conns: &mut HashMap<String, BackendConn>,
        current_node: Option<&str>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        fault: &mut Option<BackendFault>,
    ) -> Result<(Option<String>, u64)> {
        // Rate-limit gate: deny before any backend selection.
        #[cfg(feature = "rate-limiting")]
        if let Some(mut resp) = Self::rate_limit_check(session, state, config).await {
            resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        // Lazily-memoized lexical classification: every cheap fact this
        // forward path may need about the statement (write? session-dirtying?
        // cacheable read? multi-statement?) is derived AT MOST ONCE here,
        // instead of each gate re-walking the same SQL string — and only if
        // the gate that needs it is actually enabled, so the default
        // configuration keeps paying for exactly one classification (the write
        // check below, which every route decision needs). Rebuilt further down
        // only if a hint strip / rewrite / tenant transform replaces the text.
        let mut facts = StmtFacts::of_query(msg);
        let default_is_write = facts.is_write();
        let plugin_override = Self::apply_route_hook(msg, state, session);

        // Block short-circuits before any backend selection.
        if let RouteOverride::Block(reason) = plugin_override {
            let mut response = Vec::with_capacity(64 + reason.len());
            response.extend_from_slice(&Self::create_error_response(
                "42000",
                &format!("Query blocked by route plugin: {}", reason),
            ));
            response.extend_from_slice(&Self::create_ready_for_query(b'I'));
            client
                .write_all(&response)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, response.len() as u64));
        }

        // SQL-comment routing hints (feature + `[routing_hints] enabled`)
        // refine the override, recompute the write flag on the stripped SQL,
        // and may rewrite the message to drop the hint comment.
        #[cfg(feature = "routing-hints")]
        let (route_override, default_is_write, stripped_msg) =
            Self::resolve_simple_route(msg, plugin_override, default_is_write, state);
        #[cfg(not(feature = "routing-hints"))]
        let (route_override, stripped_msg): (RouteOverride, Option<Message>) =
            (plugin_override, None);

        let (is_write, forced_target) = match route_override {
            RouteOverride::None => (default_is_write, None),
            RouteOverride::Primary => (true, None),
            RouteOverride::Standby => (false, None),
            RouteOverride::Node(name) => (default_is_write, Some(name)),
            RouteOverride::Block(_) => unreachable!("handled above"),
        };

        // Read-your-writes: stamp the session on a write so subsequent reads
        // pin to the primary for the configured window.
        #[cfg(feature = "lag-routing")]
        if is_write && config.lag_routing.enabled {
            *session.last_write_at.write().await = Some(std::time::Instant::now());
        }

        // Forward the stripped message when routing-hints rewrote it, else the
        // original (borrowed, no copy).
        let forward_msg = stripped_msg.as_ref().unwrap_or(msg);

        // Query rewriting: apply rules to the SQL; if any rule fired, forward a
        // rebuilt Query carrying the rewritten SQL (so caching + the backend
        // both see the rewritten form).
        #[cfg(feature = "query-rewriting")]
        let rewritten_msg: Option<Message> = state.rewriter.as_ref().and_then(|rw| {
            let sql = crate::protocol::query_text(&forward_msg.payload)?;
            match rw.rewrite(sql) {
                Ok(res) if res.was_rewritten() => {
                    tracing::debug!(target: "helios::rewrite", rules = ?res.rules_applied, "query rewritten");
                    Some(crate::protocol::QueryMessage { query: res.query().to_string() }.encode())
                }
                _ => None,
            }
        });
        #[cfg(feature = "query-rewriting")]
        if rewritten_msg.is_some() {
            // The recorded client text is not what executes — the enclosing
            // transaction must not be blindly replayed after a failover.
            session
                .tr_replay_tainted
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(feature = "query-rewriting")]
        let forward_msg = rewritten_msg.as_ref().unwrap_or(forward_msg);

        // Multi-tenancy: resolve the session's tenant and inject a row-level
        // tenant filter. Done BEFORE the cache lookup so each tenant's results
        // are cached under their own (filtered) SQL — no cross-tenant leakage.
        #[cfg(feature = "multi-tenancy")]
        let tenant_msg: Option<Message> = if let Some(tm) = state.tenant_manager.as_ref() {
            match crate::protocol::query_text(&forward_msg.payload) {
                Some(sql) => {
                    let ctx = Self::tenant_request_ctx(session).await;
                    match tm.identify_tenant(&ctx) {
                        Some(tenant) => {
                            let res = tm.transform_query(sql, &tenant);
                            if res.transformed {
                                tracing::debug!(target: "helios::tenant", tenant = %tenant.0, "tenant filter injected");
                                Some(crate::protocol::QueryMessage { query: res.query }.encode())
                            } else {
                                None
                            }
                        }
                        None => None,
                    }
                }
                None => None,
            }
        } else {
            None
        };
        #[cfg(feature = "multi-tenancy")]
        if tenant_msg.is_some() {
            session
                .tr_replay_tainted
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(feature = "multi-tenancy")]
        let forward_msg = tenant_msg.as_ref().unwrap_or(forward_msg);

        // `forward_msg` is now final. The hint strip / rewrite / tenant
        // transforms above may each have replaced the SQL, and facts only
        // describe the text they were derived from — so rebuild them (which
        // clears every memo cell) when any of them fired. An untouched query
        // (the common path) keeps the value built from the original message,
        // and rebuilding classifies nothing by itself. The block is gated on
        // the features that read facts below, so a build with none of them
        // carries no dead assignment.
        #[cfg(any(
            feature = "pool-modes",
            feature = "query-cache",
            feature = "edge-proxy"
        ))]
        {
            let sql_changed = stripped_msg.is_some();
            #[cfg(feature = "query-rewriting")]
            let sql_changed = sql_changed || rewritten_msg.is_some();
            #[cfg(feature = "multi-tenancy")]
            let sql_changed = sql_changed || tenant_msg.is_some();
            if sql_changed {
                facts = StmtFacts::of_query(forward_msg);
            }
        }

        // Edge cache: independent of the query-cache below — when both are
        // enabled the edge lookup runs first, so an edge hit returns before
        // the query-cache is consulted. On a miss the read's stamp/epoch is
        // taken BEFORE forwarding, so the store gate can reject the store if
        // an invalidation lands while the read is in flight.
        //
        // Session-state stickiness (F2): the shared cache cannot model
        // session-local execution context (SET search_path / SET ROLE / RLS
        // GUCs / temp objects). Any statement that leaves such state makes
        // the session permanently ineligible for edge lookup AND store —
        // conservative, but the alternative is cross-session wrong rows.
        #[cfg(feature = "edge-proxy")]
        if config.edge.enabled
            && !session
                .edge_ineligible
                .load(std::sync::atomic::Ordering::Relaxed)
            && facts.leaves_session_state()
        {
            session
                .edge_ineligible
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        #[cfg(feature = "edge-proxy")]
        let edge_ctx: Option<(crate::edge::CacheKey, Vec<String>, u64, u64)> = if is_write
            || !config.edge.enabled
            || session
                .edge_ineligible
                .load(std::sync::atomic::Ordering::Relaxed)
        {
            None
        } else {
            let sql = facts.sql();
            // Same gate as the query-cache: a plain deterministic SELECT, and
            // never mid-transaction (visibility would be wrong).
            if facts.is_cacheable_read()
                && !session
                    .in_transaction
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                // Tenant identity + result-affecting startup params
                // (TimeZone, options=-c..., extra_float_digits, ...) all
                // partition the key: identical SQL under a different
                // session environment must not share a slot.
                let (database, user, session_vars) = {
                    let vars = session.variables.read().await;
                    let database = vars
                        .get("database")
                        .cloned()
                        .unwrap_or_else(|| "default".to_string());
                    let user = vars.get("user").cloned().unwrap_or_default();
                    let mut rest: Vec<(String, String)> = vars
                        .iter()
                        .filter(|(k, _)| k.as_str() != "database" && k.as_str() != "user")
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    rest.sort();
                    (database, user, rest)
                };
                let fp = crate::edge::fingerprint::analyze(sql, &database, &user, &session_vars);
                // A read with no extractable tables could never be dropped by
                // a table-targeted invalidation — never cache it (coherence
                // rule).
                if fp.tables.is_empty() {
                    None
                } else {
                    let key = crate::edge::CacheKey {
                        fingerprint: fp.fingerprint,
                        params_hash: fp.params_hash,
                        database,
                        user,
                    };
                    if let Some(entry) = state.edge_cache.get(&key) {
                        tracing::debug!(target: "helios::edge", "edge cache hit");
                        client.write_all(&entry.response_bytes).await.map_err(|e| {
                            ProxyError::Network(format!("Client write error: {}", e))
                        })?;
                        return Ok((None, entry.response_bytes.len() as u64));
                    }
                    // Miss: stamp the read before it reaches a backend, in
                    // the INVALIDATION clock's domain. Home role: mint from
                    // the local counter (the same clock that versions
                    // writes). Edge role: writes are versioned by the HOME,
                    // so stamp with the last observed home version — any
                    // later home write mints a strictly greater version and
                    // always sweeps this entry; the store race is closed by
                    // the invalidation-epoch snapshot instead of the hwm.
                    let (read_version, inval_epoch) =
                        if config.edge.role == crate::edge::EdgeRole::Edge {
                            (
                                state.edge_cache.observed_home_version(),
                                state.edge_cache.invalidation_epoch(),
                            )
                        } else {
                            (state.edge_cache.next_version(), 0)
                        };
                    Some((key, fp.tables, read_version, inval_epoch))
                }
            } else {
                None
            }
        };

        // Query cache: on a cacheable read, a hit is served from cache with no
        // backend round-trip; on a miss we keep the context to store the result.
        // The lookup's hint parse + normalization are carried over to the
        // store below (`put_prepared`), so a miss+store normalizes the SQL
        // once instead of twice.
        #[cfg(feature = "query-cache")]
        let cache_ctx: Option<(crate::cache::CacheContext, crate::cache::QueryPrep)> = if is_write {
            None
        } else if let Some(qc) = state.query_cache.as_ref() {
            let sql = facts.sql();
            match Self::cacheable_read_ctx(session, facts.is_cacheable_read()).await {
                Some(ctx) => {
                    let (lookup, prep) = qc.get_with_prep(sql, &ctx).await;
                    if let crate::cache::CacheLookup::Hit { result, level } = lookup {
                        tracing::debug!(target: "helios::cache", level = %level, "cache hit");
                        client.write_all(&result.data).await.map_err(|e| {
                            ProxyError::Network(format!("Client write error: {}", e))
                        })?;
                        return Ok((None, result.data.len() as u64));
                    }
                    Some((ctx, prep))
                }
                None => None,
            }
        } else {
            None
        };

        // Schema/workload routing: pin an analytical (OLAP) read to the
        // configured analytics node, unless something already forced a target.
        #[cfg(feature = "schema-routing")]
        let forced_target = match state.schema_analyzer.as_ref() {
            Some(analyzer)
                if forced_target.is_none()
                    && !is_write
                    && !config.schema_routing.analytics_node.is_empty() =>
            {
                match crate::protocol::query_text(&forward_msg.payload) {
                    Some(sql) if analyzer.analyze(sql).is_analytics() => {
                        tracing::debug!(target: "helios::schema", "OLAP query routed to analytics node");
                        Some(config.schema_routing.analytics_node.clone())
                    }
                    _ => forced_target,
                }
            }
            _ => forced_target,
        };

        // Analytics: capture the forwarded SQL + start the latency timer.
        #[cfg(feature = "query-analytics")]
        let analytics_sql =
            crate::protocol::query_text(&forward_msg.payload).map(|s| s.to_string());
        #[cfg(feature = "query-analytics")]
        let started = std::time::Instant::now();

        let target = Self::choose_target_node(
            is_write,
            forced_target,
            current_node,
            session,
            state,
            config,
        )
        .await?;
        tracing::debug!(target: "helios::routing", node = %target, is_write, "routed simple query");

        // Circuit breaker: fast-fail when the chosen node's circuit is open.
        #[cfg(feature = "circuit-breaker")]
        if let Some(mut resp) = Self::circuit_fast_fail(state, &target) {
            resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        // A connect/auth failure trips the breaker (and is propagated as today).
        if let Err(e) = Self::ensure_conn(conns, &target, session, config, state).await {
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            return Err(e);
        }
        let backend = conns.get_mut(&target).expect("just ensured");

        // Conditional-reset bookkeeping: if this statement is not provably
        // session-neutral, mark the connection dirty so it is fully reset (not
        // clean-skipped) when parked. Still evaluated only when the
        // optimisation is enabled and the connection is not already dirty (one
        // O(len) scan at most, until the first dirtying statement) — the facts
        // memo just means the edge gate above, when it is also on, shares that
        // one scan instead of doing its own.
        #[cfg(feature = "pool-modes")]
        if config.pool_mode.skip_clean_reset && !backend.dirty && facts.leaves_session_state() {
            backend.dirty = true;
        }

        let backend_err = match tokio::time::timeout(
            state.limits.backend_write_timeout,
            backend.stream.write_all(&forward_msg.encode()),
        )
        .await
        {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(format!("Backend write error: {}", e)),
            Err(_) => Some("Backend write timeout".to_string()),
        };
        if let Some(msg) = backend_err {
            let e = ProxyError::Network(msg);
            conns.remove(&target);
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            return Err(e);
        }

        // Cacheable read miss (query-cache and/or edge cache): capture the
        // response frames ONCE and store them so a later identical read is
        // served from cache without a backend hit. Both caches share the one
        // captured buffer — never capture twice.
        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
        {
            #[cfg(feature = "query-cache")]
            let qc_wants = cache_ctx.is_some() && state.query_cache.is_some();
            #[cfg(not(feature = "query-cache"))]
            let qc_wants = false;
            #[cfg(feature = "edge-proxy")]
            let edge_wants = edge_ctx.is_some();
            #[cfg(not(feature = "edge-proxy"))]
            let edge_wants = false;
            if qc_wants || edge_wants {
                return match Self::stream_until_ready_capture(
                    client,
                    &mut backend.stream,
                    session,
                    state.limits.relay(),
                    config.cache.max_cacheable_response_bytes,
                    &state.metrics,
                )
                .await
                {
                    Ok((sent, captured, cacheable, rows)) => {
                        #[cfg(not(feature = "query-cache"))]
                        let _ = rows; // row count only feeds the query-cache
                        #[cfg(feature = "circuit-breaker")]
                        Self::circuit_record(state, &target, true, "");
                        // One zero-copy conversion; both caches then share the
                        // refcounted buffer (no per-miss memcpy, F20).
                        let body = bytes::Bytes::from(captured);
                        // Edge store. The race-checked insert variants re-verify
                        // the gate UNDER the map lock: a concurrent invalidation
                        // that completed between the read and this store must
                        // reject it (read-after-invalidate TOCTOU, F18). Home
                        // role gates on the version hwm; edge role on the
                        // invalidation-epoch snapshot taken before forwarding.
                        #[cfg(feature = "edge-proxy")]
                        if let Some((key, tables, read_version, inval_epoch)) = edge_ctx {
                            if cacheable && !body.is_empty() {
                                let entry = crate::edge::CacheEntry {
                                    version: read_version,
                                    response_bytes: body.clone(),
                                    tables,
                                    expires_at: std::time::Instant::now()
                                        + config.edge.default_ttl(),
                                };
                                let stored = if config.edge.role == crate::edge::EdgeRole::Edge {
                                    state.edge_cache.insert_if_epoch(key, entry, inval_epoch)
                                } else {
                                    state.edge_cache.insert_if_fresh(key, entry)
                                };
                                if !stored {
                                    tracing::debug!(
                                        target: "helios::edge",
                                        "edge store skipped — invalidation raced the read"
                                    );
                                }
                            }
                        }
                        #[cfg(feature = "query-cache")]
                        if let (Some((ctx, prep)), Some(qc)) =
                            (cache_ctx.as_ref(), state.query_cache.as_ref())
                        {
                            if cacheable && !body.is_empty() {
                                let sql =
                                    crate::protocol::query_text(&forward_msg.payload).unwrap_or("");
                                qc.put_prepared(
                                    sql,
                                    ctx,
                                    prep,
                                    body.clone(),
                                    rows,
                                    std::time::Duration::ZERO,
                                )
                                .await;
                            }
                        }
                        let _ = body;
                        #[cfg(feature = "query-analytics")]
                        if let Some(sql) = analytics_sql.as_deref() {
                            Self::record_analytics(
                                state,
                                session,
                                sql,
                                &target,
                                started.elapsed(),
                                None,
                            )
                            .await;
                        }
                        Ok((Some(target), sent))
                    }
                    Err(e) => {
                        conns.remove(&target);
                        Self::record_backend_failure(state, &target, &e.to_string());
                        BackendFault::set_response(fault, &target, &e);
                        Err(e.error)
                    }
                };
            }
        }

        match Self::stream_until_ready(client, &mut backend.stream, session, state).await {
            Ok(sent) => {
                #[cfg(feature = "circuit-breaker")]
                Self::circuit_record(state, &target, true, "");
                // Invalidate cached reads referencing tables this write touched.
                #[cfg(feature = "query-cache")]
                if is_write {
                    if let Some(qc) = state.query_cache.as_ref() {
                        let sql = crate::protocol::query_text(&forward_msg.payload).unwrap_or("");
                        qc.invalidate_query(sql).await;
                    }
                }
                // Edge cache: drop local entries for the touched tables and
                // (home role only) fan the invalidation out to every
                // registered edge over SSE. Also fires for SELECT-leading
                // multi-statement strings (the trailing write would otherwise
                // invalidate nothing); bare transaction-control statements
                // (BEGIN/START/SAVEPOINT/RELEASE/ROLLBACK) change no rows and
                // are exempted — but COMMIT and SET keep their conservative
                // full flush. A `COPY ... FROM` is deferred to its CopyDone
                // drain (rows become visible then).
                #[cfg(feature = "edge-proxy")]
                if config.edge.enabled {
                    let sql = facts.sql();
                    if Self::edge_write_needs_invalidation(
                        is_write,
                        sql,
                        facts.has_interior_semicolon(),
                    ) {
                        // `tables_only` skips the fingerprint/params-hash work
                        // (discarded here) — a bulk INSERT must not pay full-
                        // buffer regex rewrites on the forward path. An EMPTY
                        // table set invalidates everything at or below the
                        // version — an unparseable write must not leave stale
                        // entries behind (coherence rule).
                        let tables = crate::edge::fingerprint::tables_only(sql);
                        if session
                            .copy_in_progress
                            .load(std::sync::atomic::Ordering::Relaxed)
                        {
                            *session
                                .pending_edge_copy_tables
                                .lock()
                                .unwrap_or_else(|e| e.into_inner()) = Some(tables);
                        } else {
                            Self::edge_invalidate_write(state, config, tables).await;
                        }
                    }
                }
                // Transaction Replay: journal the write for failover/time-travel.
                #[cfg(feature = "ha-tr")]
                if is_write && config.tr_enabled {
                    if let Some(sql) = crate::protocol::query_text(&forward_msg.payload) {
                        Self::journal_write(state, session, sql).await;
                    }
                }
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(state, session, sql, &target, started.elapsed(), None)
                        .await;
                }
                Ok((Some(target), sent))
            }
            Err(e) => {
                // Drop the broken connection so the next use redials.
                conns.remove(&target);
                Self::record_backend_failure(state, &target, &e.to_string());
                BackendFault::set_response(fault, &target, &e);
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(
                        state,
                        session,
                        sql,
                        &target,
                        started.elapsed(),
                        Some(e.to_string()),
                    )
                    .await;
                }
                Err(e.error)
            }
        }
    }

    /// Forward an accumulated extended-protocol batch (Parse/Bind/Describe/
    /// Execute/Close terminated by Sync or Flush) and stream the response.
    /// Routing is taken from `route_sql` (the first Parse's SQL); when it is
    /// `None` (a re-Bind/Execute of a named prepared statement) the request
    /// stays on the connection the statement was prepared on — no switch.
    ///
    /// `reprepare` lists named statements this batch references but does not
    /// itself define; any that the chosen connection has not seen are
    /// re-prepared from `registry` (their original `Parse`) before the batch is
    /// sent, so a named statement survives a backend switch/redial (Batch F.4).
    /// `defines` are the named statements this batch's own `Parse`s create —
    /// recorded against the connection once it accepts the batch.
    ///
    /// `fault` is filled on a backend fault exactly like `forward_simple_query`.
    #[allow(clippy::too_many_arguments)]
    async fn forward_extended_batch(
        client: &mut ClientStream,
        batch: &[u8],
        route_sql: Option<&str>,
        wait_ready: bool,
        conns: &mut HashMap<String, BackendConn>,
        current_node: Option<&str>,
        registry: &HashMap<String, bytes::Bytes>,
        reprepare: &[String],
        defines: &[String],
        unnamed: Option<&(bytes::Bytes, bytes::Bytes)>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        fault: &mut Option<BackendFault>,
    ) -> Result<(Option<String>, u64)> {
        // Rate-limit gate. The terminating ReadyForQuery is only appended when
        // the batch carried a Sync (`wait_ready`); a Flush-terminated batch
        // expects an ErrorResponse with no ReadyForQuery.
        #[cfg(feature = "rate-limiting")]
        if let Some(mut resp) = Self::rate_limit_check(session, state, config).await {
            if wait_ready {
                resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            }
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        // Analytics: the routable SQL (first Parse) + latency timer.
        #[cfg(feature = "query-analytics")]
        let analytics_sql = route_sql.map(|s| s.to_string());
        #[cfg(feature = "query-analytics")]
        let started = std::time::Instant::now();

        let target = match route_sql {
            Some(sql) => {
                // Routing-hints, when active, can override the verb-based
                // target (and recompute the write flag on the stripped SQL).
                #[cfg(feature = "routing-hints")]
                let (is_write, forced) = Self::extended_hint_route(state, sql)
                    .unwrap_or_else(|| (Self::is_write_query(sql), None));
                #[cfg(not(feature = "routing-hints"))]
                let (is_write, forced): (bool, Option<String>) = (Self::is_write_query(sql), None);
                #[cfg(feature = "lag-routing")]
                if is_write && config.lag_routing.enabled {
                    *session.last_write_at.write().await = Some(std::time::Instant::now());
                }
                Self::choose_target_node(is_write, forced, current_node, session, state, config)
                    .await?
            }
            // No Parse in this batch: stay on the prepared-statement /
            // portal connection. Fall back to a read node only if the
            // session has no current connection yet.
            None => match current_node {
                Some(c) => c.to_string(),
                None => Self::select_read_node(session, state, config).await?,
            },
        };

        // Circuit breaker: fast-fail when the chosen node's circuit is open.
        #[cfg(feature = "circuit-breaker")]
        if let Some(mut resp) = Self::circuit_fast_fail(state, &target) {
            if wait_ready {
                resp.extend_from_slice(&Self::create_ready_for_query(b'I'));
            }
            client
                .write_all(&resp)
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
            return Ok((None, resp.len() as u64));
        }

        if let Err(e) = Self::ensure_conn(conns, &target, session, config, state).await {
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
            return Err(e);
        }
        let backend = conns.get_mut(&target).expect("just ensured");

        // Transparently re-prepare any referenced named statement this socket
        // is missing. Each is sent as its original `Parse` + `Flush`; the
        // resulting `ParseComplete` is consumed here so the client never sees
        // the extra round trip. A re-prepare failure recycles the connection.
        for name in reprepare {
            if backend.prepared.contains(name) {
                continue;
            }
            let Some(parse_bytes) = registry.get(name) else {
                continue; // unknown statement — let the batch surface the error
            };
            match Self::reprepare_statement(
                &mut backend.stream,
                parse_bytes,
                state.limits.reprepare_timeout,
                state.limits.max_backend_frame_bytes,
            )
            .await
            {
                Ok(()) => {
                    backend.prepared.insert(name.clone());
                }
                Err(e) => {
                    conns.remove(&target);
                    // A socket-level failure here is a backend fault (the batch
                    // was never sent); a backend *rejecting* the re-prepare is
                    // a protocol-level problem and is propagated as before.
                    if matches!(e, ProxyError::Network(_)) {
                        Self::record_backend_failure(state, &target, &e.to_string());
                        BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
                    }
                    return Err(e);
                }
            }
        }

        // Unnamed-`Parse` promotion: if the held unnamed Parse matches what this
        // connection's unnamed statement already holds, skip forwarding it and
        // synthesize its `ParseComplete` to the client; otherwise forward it
        // first (re-establishing the connection's unnamed statement) and record
        // its signature. A fresh/redialed connection has no signature, so the
        // Parse is always (re)forwarded there — correctness is preserved.
        let mut inject_parse_complete = false;
        let mut new_unnamed_sig: Option<bytes::Bytes> = None;
        if let Some((parse_msg, sig)) = unnamed {
            if backend.unnamed_sig.as_deref() == Some(&sig[..]) {
                inject_parse_complete = true;
            } else {
                if let Err(e) = tokio::time::timeout(
                    state.limits.backend_write_timeout,
                    backend.stream.write_all(parse_msg),
                )
                .await
                .map_err(|_| ProxyError::Network("Backend write timeout".to_string()))
                .and_then(|r| {
                    r.map_err(|e| ProxyError::Network(format!("Backend write error: {}", e)))
                }) {
                    conns.remove(&target);
                    Self::record_backend_failure(state, &target, &e.to_string());
                    BackendFault::set(fault, &target, FaultPhase::NotDelivered, &e);
                    return Err(e);
                }
                new_unnamed_sig = Some(sig.clone());
            }
        }

        if let Err((e, phase)) = Self::tr_write_batch(
            &mut backend.stream,
            batch,
            state.limits.backend_write_timeout,
        )
        .await
        {
            conns.remove(&target);
            Self::record_backend_failure(state, &target, &e.to_string());
            BackendFault::set(fault, &target, phase, &e);
            return Err(e);
        }

        // The client expects `ParseComplete` first; the backend won't send one
        // for a skipped Parse, so emit it here before relaying the response.
        let mut injected: u64 = 0;
        if inject_parse_complete {
            if let Err(e) = client
                .write_all(&[b'1', 0, 0, 0, 4])
                .await
                .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))
            {
                conns.remove(&target);
                return Err(e);
            }
            injected = 5;
        }

        let r = if wait_ready {
            Self::stream_until_ready(client, &mut backend.stream, session, state).await
        } else {
            Self::stream_flush(client, &mut backend.stream, session, state).await
        };
        match r {
            Ok(sent) => {
                #[cfg(feature = "circuit-breaker")]
                Self::circuit_record(state, &target, true, "");
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(state, session, sql, &target, started.elapsed(), None)
                        .await;
                }
                // The connection now holds these named statements.
                for name in defines {
                    backend.prepared.insert(name.clone());
                }
                // ...and the (re)forwarded unnamed statement.
                if let Some(sig) = new_unnamed_sig {
                    backend.unnamed_sig = Some(sig);
                }
                Ok((Some(target), sent + injected))
            }
            Err(mut e) => {
                conns.remove(&target);
                Self::record_backend_failure(state, &target, &e.to_string());
                e.progress.bytes += injected;
                BackendFault::set_response(fault, &target, &e);
                #[cfg(feature = "query-analytics")]
                if let Some(sql) = analytics_sql.as_deref() {
                    Self::record_analytics(
                        state,
                        session,
                        sql,
                        &target,
                        started.elapsed(),
                        Some(e.to_string()),
                    )
                    .await;
                }
                Err(e.error)
            }
        }
    }

    /// Re-issue one named `Parse` on a backend socket out-of-band: send the
    /// original `Parse` bytes followed by a `Flush`, then read and discard the
    /// single `ParseComplete` the backend emits. The statement persists on the
    /// connection (the implicit transaction is closed later by the real
    /// batch's `Sync`). An `ErrorResponse` means the re-prepare failed.
    async fn reprepare_statement<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        backend: &mut S,
        parse_bytes: &[u8],
        reprepare_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<()> {
        tokio::time::timeout(reprepare_timeout, backend.write_all(parse_bytes))
            .await
            .map_err(|_| ProxyError::Network("re-prepare write timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("re-prepare write error: {}", e)))?;
        // Flush: 'H' + length 4.
        tokio::time::timeout(reprepare_timeout, backend.write_all(&[b'H', 0, 0, 0, 4]))
            .await
            .map_err(|_| ProxyError::Network("re-prepare flush timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("re-prepare flush error: {}", e)))?;
        let mtype = tokio::time::timeout(
            reprepare_timeout,
            Self::read_one_frame_type(backend, max_frame_bytes),
        )
        .await
        .map_err(|_| ProxyError::Network("re-prepare read timeout".to_string()))??;
        match mtype {
            b'1' => Ok(()), // ParseComplete
            b'E' => Err(ProxyError::Protocol(
                "re-prepare rejected by backend".to_string(),
            )),
            other => Err(ProxyError::Protocol(format!(
                "unexpected re-prepare reply: {}",
                other as char
            ))),
        }
    }

    /// Read exactly one backend message frame (5-byte header + body) and return
    /// its type byte, discarding the body. Used to consume the `ParseComplete`
    /// produced by an out-of-band re-prepare.
    async fn read_one_frame_type<S: AsyncReadExt + Unpin>(
        backend: &mut S,
        max_frame_bytes: usize,
    ) -> Result<u8> {
        let mut header = [0u8; 5];
        backend
            .read_exact(&mut header)
            .await
            .map_err(|e| ProxyError::Network(format!("re-prepare read error: {}", e)))?;
        // H-07: validate before trusting the length, and never allocate the
        // advertised body size — discard it through a fixed scratch buffer.
        let len = backend_frame_len(&header, max_frame_bytes)?.expect("5 header bytes were read");
        let mut remaining = len - 4;
        let mut scratch = [0u8; 16 * 1024];
        while remaining > 0 {
            let n = remaining.min(scratch.len());
            backend
                .read_exact(&mut scratch[..n])
                .await
                .map_err(|e| ProxyError::Network(format!("re-prepare body read error: {}", e)))?;
            remaining -= n;
        }
        Ok(header[0])
    }

    /// Name a `Parse` defines: its first cstring. `""` is the unnamed
    /// statement, which is per-protocol transient and never tracked.
    fn parse_stmt_name(payload: &[u8]) -> &str {
        let end = payload.iter().position(|&b| b == 0).unwrap_or(0);
        std::str::from_utf8(&payload[..end]).unwrap_or("")
    }

    /// Prepared-statement name a `Bind` references: the *second* cstring
    /// (portal name first, then statement name). `None` for the unnamed
    /// statement.
    fn bind_stmt_ref(payload: &[u8]) -> Option<&str> {
        let portal_end = payload.iter().position(|&b| b == 0)?;
        let rest = &payload[portal_end + 1..];
        let stmt_end = rest.iter().position(|&b| b == 0)?;
        let name = std::str::from_utf8(&rest[..stmt_end]).ok()?;
        (!name.is_empty()).then_some(name)
    }

    /// Statement name a `Describe`/`Close` targets — only when it is
    /// statement-kind (`'S'`, not portal `'P'`). `None` otherwise.
    fn stmt_kind_name(payload: &[u8]) -> Option<&str> {
        if payload.first() != Some(&b'S') {
            return None;
        }
        let rest = &payload[1..];
        let end = rest.iter().position(|&b| b == 0)?;
        let name = std::str::from_utf8(&rest[..end]).ok()?;
        (!name.is_empty()).then_some(name)
    }

    /// Stream backend response frames to the client until ReadyForQuery (end
    /// of a Sync/simple-query response). Forwards bytes verbatim, coalescing
    /// all currently-complete frames into one write and keeping only a
    /// partial-frame tail buffered, so proxy memory stays O(frame) rather
    /// than O(result). Also yields on CopyInResponse/CopyBothResponse so the
    /// client can supply COPY data. Updates `tx_state` from the RFQ status.
    /// Returns bytes streamed to the client.
    async fn stream_until_ready(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> std::result::Result<u64, ResponseFailure> {
        let client_write_timeout = state.limits.client_write_timeout;
        let backend_read_timeout = state.limits.backend_read_timeout;
        let mut buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let mut had_error = false;
        let mut command_complete = false;

        let response = async {
            loop {
                // Walk complete frames in `buf`, stopping at a boundary frame.
                let mut consumed = 0usize;
                let mut ready_status: Option<u8> = None;
                let mut yield_for_copy = false;
                loop {
                    let rem = &buf[consumed..];
                    // H-07: a malformed header (len < 4, or above the budget) is
                    // an error NOW, not "wait for more bytes" — no byte count can
                    // make it valid and the accumulator must not chase it.
                    let Some(len) = backend_frame_len(rem, state.limits.max_backend_frame_bytes)?
                    else {
                        break;
                    };
                    if rem.len() < len + 1 {
                        break; // incomplete — need more bytes
                    }
                    let frame_total = len + 1;
                    let mtype = rem[0];
                    consumed += frame_total;
                    if mtype == b'E' {
                        had_error = true;
                    }
                    command_complete |= mtype == b'C';
                    if mtype == b'Z' {
                        // ReadyForQuery: payload is one status byte at rem[5].
                        ready_status = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                        break;
                    }
                    if mtype == b'G' || mtype == b'W' {
                        // CopyInResponse / CopyBothResponse: the backend now wants
                        // CopyData from the client — forward up to here and yield.
                        yield_for_copy = true;
                        break;
                    }
                }

                if consumed > 0 {
                    tokio::time::timeout(client_write_timeout, client.write_all(&buf[..consumed]))
                        .await
                        .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                        .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                    sent += consumed as u64;
                    let _ = buf.split_to(consumed);
                }

                if let Some(status) = ready_status {
                    Self::note_ready_for_query(session, status, had_error);
                    return Ok(sent);
                }
                if yield_for_copy {
                    // The backend now awaits CopyData from the client; the session
                    // is mid-COPY, not at a clean boundary. Mark it so pool release
                    // is suppressed until the COPY drains (cleared in the CopyDone
                    // path). Harmless in session mode (release is a no-op there).
                    session
                        .copy_in_progress
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return Ok(sent);
                }

                // Read straight into the frame accumulator — no zeroed scratch, no
                // copy. `read_buf` appends to `buf`'s spare capacity.
                buf.reserve(16384);
                let n = tokio::time::timeout(backend_read_timeout, backend.read_buf(&mut buf))
                    .await
                    .map_err(|_| ProxyError::Network("Backend read timeout".to_string()))?
                    .map_err(|e| ProxyError::Network(format!("Backend read error: {}", e)))?;
                if n == 0 {
                    return Err(ProxyError::Connection(
                        "Backend closed mid-response".to_string(),
                    ));
                }
            }
        }
        .await;
        response.map_err(|error| ResponseFailure {
            error,
            progress: ResponseProgress {
                bytes: sent,
                terminal: had_error || command_complete,
                raw: false,
            },
        })
    }

    /// Like `stream_until_ready` but also captures the full response bytes for
    /// caching (query-cache and/or edge cache — one shared capture).
    /// Returns `(bytes_sent, captured, cacheable, row_count)`.
    /// `cacheable` is false if the response carried an `ErrorResponse`, ended in
    /// a non-idle transaction status, yielded for COPY, or contained an
    /// asynchronous backend frame — NotificationResponse ('A'), NoticeResponse
    /// ('N'), or ParameterStatus ('S'). Async frames belong to THIS session's
    /// moment in time (a LISTENing session's notification, a GUC change);
    /// replaying them to every later hitter would leak the notification
    /// payload cross-session and desynchronize hitters' parameter state. The
    /// frames are still forwarded to the live requester — only the store is
    /// suppressed.
    ///
    /// `max_capture_bytes` ([cache] `max_cacheable_response_bytes`) bounds the
    /// capture buffer: the capture is a *transient* held per concurrent
    /// session ON TOP of the bytes already streamed out, so an unbounded one
    /// let a single `SELECT * FROM big_table` pin the whole result set in the
    /// proxy. The moment appending would cross the bound the buffer is dropped
    /// (memory freed immediately), nothing further is captured, and the
    /// response is reported non-cacheable. Forwarding to the client is
    /// untouched — the client still receives every byte, byte-for-byte.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    async fn stream_until_ready_capture(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        relay: RelayLimits,
        max_capture_bytes: usize,
        metrics: &ServerMetrics,
    ) -> std::result::Result<(u64, Vec<u8>, bool, usize), ResponseFailure> {
        let mut buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let mut captured: Vec<u8> = Vec::with_capacity(4096);
        let mut had_error = false;
        let mut command_complete = false;
        let mut saw_async = false;
        let mut row_count: usize = 0;
        // Set once the response outgrows `max_capture_bytes`; `captured` is
        // then empty and stays empty for the rest of the response.
        let mut oversize = false;

        let response = async {
            loop {
                let mut consumed = 0usize;
                let mut ready_status: Option<u8> = None;
                let mut yield_for_copy = false;
                loop {
                    let rem = &buf[consumed..];
                    let Some(len) = backend_frame_len(rem, relay.max_frame_bytes)? else {
                        break;
                    };
                    if rem.len() < len + 1 {
                        break;
                    }
                    let frame_total = len + 1;
                    let mtype = rem[0];
                    if mtype == b'E' {
                        had_error = true;
                    }
                    command_complete |= mtype == b'C';
                    // Async backend frames (backend 'S' is unambiguous here —
                    // PortalSuspended is lowercase 's').
                    if mtype == b'A' || mtype == b'N' || mtype == b'S' {
                        saw_async = true;
                    }
                    if mtype == b'C' {
                        // CommandComplete tag, e.g. "SELECT 5" — take the row count.
                        if let Some(tag) = rem.get(5..frame_total) {
                            if let Some(end) = tag.iter().position(|&b| b == 0) {
                                if let Ok(s) = std::str::from_utf8(&tag[..end]) {
                                    if let Some(n) =
                                        s.rsplit(' ').next().and_then(|x| x.parse::<usize>().ok())
                                    {
                                        row_count = n;
                                    }
                                }
                            }
                        }
                    }
                    consumed += frame_total;
                    if mtype == b'Z' {
                        ready_status = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                        break;
                    }
                    if mtype == b'G' || mtype == b'W' {
                        yield_for_copy = true;
                        break;
                    }
                }

                if consumed > 0 {
                    tokio::time::timeout(
                        relay.client_write_timeout,
                        client.write_all(&buf[..consumed]),
                    )
                    .await
                    .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                    .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                    if !oversize {
                        if captured.len().saturating_add(consumed) > max_capture_bytes {
                            oversize = true;
                            // Free the transient NOW (`clear` alone keeps the
                            // allocation alive for the rest of the response).
                            captured = Vec::new();
                            metrics
                                .cache_capture_oversize
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::debug!(
                                target: "helios::cache",
                                limit = max_capture_bytes,
                                "response exceeds cache.max_cacheable_response_bytes — \
                                 capture abandoned, response not cached"
                            );
                        } else {
                            captured.extend_from_slice(&buf[..consumed]);
                        }
                    }
                    sent += consumed as u64;
                    let _ = buf.split_to(consumed);
                }

                if let Some(status) = ready_status {
                    Self::note_ready_for_query(session, status, had_error);
                    let cacheable = !had_error && status == b'I' && !saw_async && !oversize;
                    return Ok((sent, captured, cacheable, row_count));
                }
                if yield_for_copy {
                    session
                        .copy_in_progress
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                    return Ok((sent, captured, false, row_count));
                }

                // Read straight into the frame accumulator — no zeroed scratch.
                buf.reserve(16384);
                let n =
                    tokio::time::timeout(relay.backend_read_timeout, backend.read_buf(&mut buf))
                        .await
                        .map_err(|_| ProxyError::Network("Backend read timeout".to_string()))?
                        .map_err(|e| ProxyError::Network(format!("Backend read error: {}", e)))?;
                if n == 0 {
                    return Err(ProxyError::Connection(
                        "Backend closed mid-response".to_string(),
                    ));
                }
            }
        }
        .await;
        response.map_err(|error| ResponseFailure {
            error,
            progress: ResponseProgress {
                bytes: sent,
                terminal: had_error || command_complete,
                raw: false,
            },
        })
    }

    /// Relay whatever the backend has *already* produced in response to a
    /// `Flush` (which, unlike `Sync`, yields no ReadyForQuery), then return
    /// immediately — without waiting.
    ///
    /// Any Flush output that has not landed in the socket yet is delivered by
    /// the main loop's backend watch (which relays the current backend's
    /// out-of-band bytes while waiting for the client), so there is no fixed
    /// post-Flush stall: the previous version blocked the session loop for up to
    /// 200 ms after the last backend byte before it would read the client's next
    /// message, adding that latency to every `Parse`/`Flush`-then-`Bind` prepare
    /// cycle. Here we drain what is instantly available and hand control back;
    /// the client's next frames are read at once. The eventual `Sync` drains the
    /// final ReadyForQuery via `stream_until_ready`.
    async fn stream_flush(
        client: &mut ClientStream,
        backend: &mut TcpStream,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) -> std::result::Result<u64, ResponseFailure> {
        let _ = session;
        // Reused across every read of this call via `try_read_buf`, which
        // writes straight into the buffer's spare capacity from the read
        // syscall — unlike `vec![0u8; 16384]`, nothing here is
        // zero-initialized before use.
        let mut read_buf = BytesMut::with_capacity(16384);
        let mut sent: u64 = 0;
        let response = async {
            loop {
                read_buf.clear();
                match backend.try_read_buf(&mut read_buf) {
                    Ok(0) => {
                        return Err(ProxyError::Connection(
                            "Backend closed mid-flush".to_string(),
                        ))
                    }
                    Ok(n) => {
                        tokio::time::timeout(
                            state.limits.client_write_timeout,
                            client.write_all(&read_buf[..n]),
                        )
                        .await
                        .map_err(|_| ProxyError::Network("Client write timeout".to_string()))?
                        .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
                        sent += n as u64;
                    }
                    // Nothing more instantly available — the backend watch delivers
                    // any remaining Flush output as it arrives.
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(sent),
                    Err(e) => {
                        return Err(ProxyError::Network(format!("Backend read error: {}", e)))
                    }
                }
            }
        }
        .await;
        response.map_err(|error| ResponseFailure {
            error,
            progress: ResponseProgress {
                bytes: sent,
                terminal: false,
                raw: sent > 0,
            },
        })
    }

    /// Check if SQL query is a write operation
    fn is_write_query(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let trimmed = sql.trim();

        // Write operations
        if starts_with_ci(trimmed, "INSERT")
            || starts_with_ci(trimmed, "UPDATE")
            || starts_with_ci(trimmed, "DELETE")
            || starts_with_ci(trimmed, "CREATE")
            || starts_with_ci(trimmed, "DROP")
            || starts_with_ci(trimmed, "ALTER")
            || starts_with_ci(trimmed, "TRUNCATE")
            || starts_with_ci(trimmed, "GRANT")
            || starts_with_ci(trimmed, "REVOKE")
            || starts_with_ci(trimmed, "VACUUM")
            || starts_with_ci(trimmed, "REINDEX")
            || starts_with_ci(trimmed, "CLUSTER")
        {
            return true;
        }

        // Transaction control goes to current node
        if starts_with_ci(trimmed, "BEGIN")
            || starts_with_ci(trimmed, "START")
            || starts_with_ci(trimmed, "COMMIT")
            || starts_with_ci(trimmed, "ROLLBACK")
            || starts_with_ci(trimmed, "SAVEPOINT")
            || starts_with_ci(trimmed, "RELEASE")
        {
            return true;
        }

        // SET commands go to primary to maintain session state
        if starts_with_ci(trimmed, "SET") && !starts_with_ci(trimmed, "SET TRANSACTION READ ONLY") {
            return true;
        }

        false
    }

    /// Should this successfully-executed simple-query string trigger an edge
    /// invalidation?
    ///
    /// - Bare transaction control (BEGIN/START/SAVEPOINT/RELEASE/ROLLBACK)
    ///   changes no rows: exempt, or every ORM transaction would full-flush
    ///   the whole fleet twice per request. COMMIT is deliberately NOT
    ///   exempt (its flush closes the invalidate-at-statement vs
    ///   commit-visibility window for in-transaction writes), nor is SET
    ///   (the flush is the interim mitigation for GUC-sensitive results).
    /// - A multi-statement string (interior `;`) may hide a trailing write
    ///   behind a read-classified lead — always invalidate (the fingerprint
    ///   extracts tables from every sub-statement; over-invalidation only).
    /// - `COPY ... FROM` loads rows but is not classified `is_write` (it
    ///   must not be re-routed); catch it here for invalidation.
    ///
    /// `multi_stmt` is `StmtFacts::has_interior_semicolon` for the same SQL —
    /// passed in rather than re-scanned (`stmt_has_interior_semicolon` is the
    /// single definition of that fact).
    #[cfg(feature = "edge-proxy")]
    fn edge_write_needs_invalidation(is_write: bool, sql: &str, multi_stmt: bool) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if !multi_stmt
            && (starts_with_ci(core, "BEGIN")
                || starts_with_ci(core, "START")
                || starts_with_ci(core, "SAVEPOINT")
                || starts_with_ci(core, "RELEASE")
                || starts_with_ci(core, "ROLLBACK"))
        {
            return false;
        }
        is_write
            || multi_stmt
            || Self::is_edge_copy_write_sql(core)
            || Self::is_edge_procedural_sql(core)
            || Self::is_edge_txn_end_sql(core)
    }

    /// Does the simple-query string carry more than one statement? A `;`
    /// before the optional trailing one means a leading-keyword check cannot
    /// vouch for what follows. (A `;` inside a string literal also trips this —
    /// conservative, and only ever costs an extra invalidation/reset.)
    #[cfg(feature = "edge-proxy")]
    fn stmt_has_interior_semicolon(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        core.contains(';')
    }

    /// `COPY ... FROM ...` (STDIN or file) loads rows. Word-boundary FROM so
    /// `COPY t TO ...` stays a read; a `COPY (SELECT ... FROM t) TO ...`
    /// false-positive only over-invalidates — safe.
    #[cfg(feature = "edge-proxy")]
    fn is_edge_copy_write_sql(sql: &str) -> bool {
        crate::protocol::starts_with_ci(sql.trim_start(), "COPY")
            && Self::contains_word_ci(sql, "from")
    }

    /// Which of a batch's Closed statement names may have their edge
    /// invalidation metadata pruned at a Sync boundary. A name Closed and then
    /// re-Parsed in the SAME batch must keep its FRESH metadata (dropping it
    /// would silently disable invalidation for the live re-prepared statement —
    /// the Npgsql statement-replacement pattern), so it is excluded. Pruning is
    /// additionally gated on the Sync by the caller, so a Close seen at an
    /// earlier Flush keeps its metadata alive for the terminating Sync's
    /// invalidation hook. Regression guard for the G1 finding.
    #[cfg(feature = "edge-proxy")]
    fn edge_meta_prunable<'a>(closes: &'a [String], defines: &[String]) -> Vec<&'a str> {
        closes
            .iter()
            .filter(|n| !defines.contains(n))
            .map(String::as_str)
            .collect()
    }

    /// SQL-level statements whose written table set cannot be attributed
    /// statically — `EXECUTE` (runs a prepared plan), `CALL` (a procedure),
    /// `DO` (an anonymous block, often dynamic SQL). Treated as an
    /// invalidate-everything wildcard write. Rare on the wire (no mainstream
    /// driver emits them for data changes), so the full flush is cheap in
    /// practice and over-invalidation is always safe.
    #[cfg(feature = "edge-proxy")]
    fn is_edge_procedural_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        starts_with_ci(t, "EXECUTE") || starts_with_ci(t, "CALL") || starts_with_ci(t, "DO")
    }

    /// Transaction-ending statements: `COMMIT`/`END` and their variants
    /// (`COMMIT WORK`/`AND CHAIN`/`PREPARED`, `END TRANSACTION`). They make a
    /// transaction's in-flight writes visible, so — like the simple-path
    /// COMMIT — they trigger the conservative wildcard flush that closes the
    /// invalidate-at-statement vs commit-visibility window. `BEGIN`/`START`/
    /// `SAVEPOINT`/`RELEASE`/`ROLLBACK` are excluded (they make nothing newly
    /// visible). `END` is a COMMIT synonym here; a rare non-transaction
    /// statement that happens to start with `END` only over-invalidates.
    #[cfg(feature = "edge-proxy")]
    fn is_edge_txn_end_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        starts_with_ci(t, "COMMIT") || starts_with_ci(t, "END")
    }

    /// Extended-protocol statement classifier for edge invalidation: does
    /// this Parse'd SQL modify table data when executed? Deliberately NOT
    /// `is_write_query` — that routing classifier counts BEGIN/COMMIT/SET as
    /// writes, and their empty table set would full-flush the fleet on every
    /// transaction commit. WITH-prefixed statements are checked for
    /// data-modifying CTE verbs by word (false positives over-invalidate
    /// only).
    #[cfg(feature = "edge-proxy")]
    fn is_edge_dml_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        if starts_with_ci(t, "INSERT")
            || starts_with_ci(t, "UPDATE")
            || starts_with_ci(t, "DELETE")
            || starts_with_ci(t, "MERGE")
            || starts_with_ci(t, "CREATE")
            || starts_with_ci(t, "DROP")
            || starts_with_ci(t, "ALTER")
            || starts_with_ci(t, "TRUNCATE")
            || starts_with_ci(t, "GRANT")
            || starts_with_ci(t, "REVOKE")
        {
            return true;
        }
        if starts_with_ci(t, "COPY") {
            return Self::contains_word_ci(t, "from");
        }
        if starts_with_ci(t, "WITH") {
            return Self::contains_word_ci(t, "insert")
                || Self::contains_word_ci(t, "update")
                || Self::contains_word_ci(t, "delete")
                || Self::contains_word_ci(t, "merge");
        }
        false
    }

    /// Union the invalidation table set for an extended-protocol batch from
    /// the per-statement metadata memoized at Parse time. Returns `None`
    /// when the batch references no DML statement; `Some(vec![])` (the
    /// invalidate-everything wildcard) when any referenced DML has an
    /// unattributable table set.
    #[cfg(feature = "edge-proxy")]
    fn edge_extended_batch_tables(
        refs: &[String],
        bound_unnamed: bool,
        named_meta: &HashMap<String, Option<Vec<String>>>,
        unnamed_meta: &Option<Vec<String>>,
    ) -> Option<Vec<String>> {
        let mut any_dml = false;
        let mut wipe_all = false;
        let mut union: Vec<String> = Vec::new();
        {
            let mut consider = |meta: &Option<Vec<String>>| {
                if let Some(tables) = meta {
                    any_dml = true;
                    if tables.is_empty() {
                        wipe_all = true;
                    } else {
                        for t in tables {
                            if !union.contains(t) {
                                union.push(t.clone());
                            }
                        }
                    }
                }
            };
            for name in refs {
                if let Some(meta) = named_meta.get(name) {
                    consider(meta);
                }
            }
            if bound_unnamed {
                consider(unnamed_meta);
            }
        }
        if !any_dml {
            None
        } else if wipe_all {
            Some(Vec::new())
        } else {
            Some(union)
        }
    }

    /// Version-stamp a completed write, drop matching local entries, and
    /// (home role) fan the invalidation out over SSE. An edge never
    /// broadcasts — the home versions writes, so the edge sweeps in the
    /// observed-home domain and lets the home's own event follow.
    #[cfg(feature = "edge-proxy")]
    async fn edge_invalidate_write(
        state: &Arc<ServerState>,
        config: &ProxyConfig,
        tables: Vec<String>,
    ) {
        if config.edge.role == crate::edge::EdgeRole::Home {
            let version = state.edge_cache.next_version();
            let dropped = state.edge_cache.invalidate(version, &tables);
            let (notified, pruned) = state
                .edge_registry
                .broadcast(crate::edge::InvalidationEvent {
                    up_to_version: version,
                    tables,
                    committed_at: chrono::Utc::now().to_rfc3339(),
                    epoch: state.edge_cache.epoch(),
                })
                .await;
            tracing::debug!(
                target: "helios::edge",
                version,
                dropped,
                notified,
                pruned,
                "write invalidation broadcast to edges"
            );
        } else {
            // Edge-local sweep in the observed-home domain: every locally
            // cached entry is stamped at or below the observed version, so
            // this drops all entries for the touched tables. It also bumps
            // the invalidation epoch, rejecting in-flight read stores that
            // raced this write.
            let version = state.edge_cache.observed_home_version();
            let dropped = state.edge_cache.invalidate(version, &tables);
            tracing::debug!(
                target: "helios::edge",
                version,
                dropped,
                "write invalidated local edge cache (edge role — home broadcasts)"
            );
        }
    }

    /// Conservative classifier for the conditional-reset optimisation: could
    /// this forwarded simple-query SQL leave *session-level* state on the
    /// backend connection that `DISCARD ALL` would need to clear before another
    /// client reuses it (a `SET`/GUC, temp table, prepared statement, cursor
    /// WITH HOLD, `LISTEN`, advisory lock, session authorization, …)?
    ///
    /// Biased hard toward `true`. A false negative (calling a dirtying
    /// statement clean) would leak state to the next borrower — a correctness
    /// and security bug — so only statements *provably* session-neutral return
    /// `false`; everything ambiguous returns `true` (forcing the full reset,
    /// which is merely slower, never unsafe).
    ///
    /// Known, documented limitation: a `SELECT` that calls a user-defined
    /// function which internally runs `set_config(..., is_local => false)` or
    /// takes an advisory lock via an aliased path is NOT detectable from the
    /// SQL text. The direct forms (`set_config`, `pg_advisory*`, `nextval`,
    /// `setval`) ARE caught. This is why `skip_clean_reset` is opt-in and
    /// intended for autocommit/simple-protocol workloads.
    ///
    /// Also reused by the edge cache as its sticky session-eligibility
    /// gate: a session that leaves session state (GUCs, SET ROLE, temp
    /// objects) no longer matches the shared cache's key model and is
    /// permanently excluded from edge lookup/store.
    #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
    fn stmt_leaves_session_state(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim();
        if t.is_empty() {
            return false;
        }
        // Multiple statements in one simple-query string: a leading-keyword
        // check cannot vouch for what follows a `;`, so treat any non-trailing
        // `;` as dirtying. A `;` inside a string literal also trips this —
        // safe, merely an unnecessary reset.
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.contains(';') {
            return true;
        }
        // The statement's leading keyword must be one that provably leaves no
        // session state. CREATE / SET / PREPARE / DECLARE / LISTEN / DISCARD /
        // RESET / GRANT / ALTER / LOCK / COPY / … are all absent here, so they
        // fall through to `true` (dirtying).
        let neutral_lead = starts_with_ci(core, "SELECT")
            || starts_with_ci(core, "INSERT")
            || starts_with_ci(core, "UPDATE")
            || starts_with_ci(core, "DELETE")
            || starts_with_ci(core, "WITH")
            || starts_with_ci(core, "VALUES")
            || starts_with_ci(core, "TABLE")
            || starts_with_ci(core, "SHOW")
            || starts_with_ci(core, "EXPLAIN")
            || starts_with_ci(core, "FETCH")
            || starts_with_ci(core, "BEGIN")
            || starts_with_ci(core, "START")
            || starts_with_ci(core, "COMMIT")
            || starts_with_ci(core, "END")
            || starts_with_ci(core, "ROLLBACK")
            || starts_with_ci(core, "ABORT")
            || starts_with_ci(core, "SAVEPOINT")
            || starts_with_ci(core, "RELEASE");
        if !neutral_lead {
            return true;
        }
        // A neutral-lead statement can still create session state:
        //  * `SELECT ... INTO [TEMP] t` (and the `WITH … SELECT … INTO` form)
        //    creates a table. The `INTO` keyword is matched as a whole word (so
        //    a column name like `into_total` does not trip it) and ONLY for
        //    SELECT/WITH leads — `INSERT INTO`, `UPDATE`, `DELETE` use `INTO`
        //    (or not) as ordinary syntax and leave no session state.
        //  * `set_config()` sets a GUC; `pg_advisory*` takes a session lock;
        //    `nextval`/`setval` touch the per-session sequence cache.
        if (starts_with_ci(core, "SELECT") || starts_with_ci(core, "WITH"))
            && Self::contains_word_ci(core, "into")
        {
            return true;
        }
        // Same one-pass-lowercase trick as `is_cacheable_read_sql`: rather
        // than 4 separate case-insensitive windowed scans over `core`,
        // lowercase it once and use plain `str::contains`.
        const DIRTY_TOKENS: [&str; 4] = ["set_config", "advisory", "nextval", "setval"];
        let lower = core.to_ascii_lowercase();
        DIRTY_TOKENS.iter().any(|tok| lower.contains(tok))
    }

    /// Case-insensitive whole-word (ASCII identifier-boundary) search — a match
    /// requires the token to be bounded by a non-`[A-Za-z0-9_]` char (or the
    /// string edge) on both sides, so a real SQL keyword like `INTO` is caught
    /// regardless of surrounding whitespace while an identifier substring
    /// (`into_total`) is not. Always compiled: the in-session TR statement
    /// classifier (`tr_classify`) uses it on every build.
    fn contains_word_ci(haystack: &str, word: &str) -> bool {
        let hb = haystack.as_bytes();
        let wb = word.as_bytes();
        if wb.is_empty() || hb.len() < wb.len() {
            return false;
        }
        let is_ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
        let mut i = 0;
        while i + wb.len() <= hb.len() {
            if hb[i..i + wb.len()].eq_ignore_ascii_case(wb) {
                let before_ok = i == 0 || !is_ident(hb[i - 1]);
                let after = i + wb.len();
                let after_ok = after == hb.len() || !is_ident(hb[after]);
                if before_ok && after_ok {
                    return true;
                }
            }
            i += 1;
        }
        false
    }

    /// Derive the rate-limit bucket key for a session per the configured
    /// keying dimension, memoizing it on the session.
    ///
    /// Every input is fixed for the life of a connection: `key_by` comes from
    /// the config snapshot this connection was accepted under, the client
    /// address never changes, and `user`/`database` are set once from the
    /// startup packet. So the key (and its rendered metrics string) is built
    /// once and every later query borrows it — no `LimiterKey` allocation, no
    /// `variables` read lock, and no `format!` on the per-query gate.
    ///
    /// The one case that is *not* memoized is a key derived from a startup
    /// parameter that is not present yet: that would freeze a placeholder for
    /// the rest of the session, so it is recomputed (exactly as before) until
    /// the parameter appears.
    #[cfg(feature = "rate-limiting")]
    async fn rate_limit_key<'a>(
        session: &'a Arc<ClientSession>,
        config: &ProxyConfig,
    ) -> std::borrow::Cow<'a, crate::rate_limit::CachedLimiterKey> {
        use crate::config::RateLimitKeyBy;
        use crate::rate_limit::{CachedLimiterKey, LimiterKey};
        use std::borrow::Cow;

        if let Some(cached) = session.rate_limit_key.get() {
            return Cow::Borrowed(cached);
        }

        // `stable` is false only when the key had to fall back to a default
        // because the startup parameter it keys on is not populated yet.
        let (key, stable) = match config.rate_limit.key_by {
            RateLimitKeyBy::Global => (LimiterKey::Global, true),
            RateLimitKeyBy::ClientIp => (LimiterKey::ClientIp(session.client_addr.ip()), true),
            RateLimitKeyBy::Database => {
                let vars = session.variables.read().await;
                match vars.get("database") {
                    Some(db) => (LimiterKey::Database(db.clone()), true),
                    None => (LimiterKey::Database(String::new()), false),
                }
            }
            RateLimitKeyBy::User => {
                let vars = session.variables.read().await;
                match vars.get("user") {
                    Some(user) => (LimiterKey::User(user.clone()), true),
                    None => (LimiterKey::User(String::new()), false),
                }
            }
        };

        let resolved = CachedLimiterKey::new(key);
        if !stable {
            return Cow::Owned(resolved);
        }

        // A concurrent racer may win the init; either way the stored value is
        // the same key, so borrow whatever landed.
        Cow::Borrowed(session.rate_limit_key.get_or_init(|| resolved))
    }

    /// Check rate limits before a query is forwarded. Returns `Some(bytes)` —
    /// a PG `ErrorResponse` WITHOUT a trailing `ReadyForQuery` (the caller
    /// appends one as the protocol requires) — when the query is denied; `None`
    /// when it may proceed. A throttle/queue verdict is honored by sleeping for
    /// the engine-supplied delay (real backpressure, capped) and then allowing.
    #[cfg(feature = "rate-limiting")]
    async fn rate_limit_check(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Option<Vec<u8>> {
        use crate::rate_limit::RateLimitResult;
        let limiter = state.rate_limiter.as_ref()?;
        let key = Self::rate_limit_key(session, config).await;
        let key = key.as_ref();
        match limiter.check_cached(key, 1) {
            RateLimitResult::Allowed => None,
            RateLimitResult::Warned(msg) => {
                tracing::warn!(key = %key, reason = %msg, "rate limit warning");
                None
            }
            RateLimitResult::Throttled(d) | RateLimitResult::Queued(d) => {
                // Cap the backpressure sleep so a misconfiguration can't pin a
                // connection task indefinitely.
                tokio::time::sleep(d.min(Duration::from_secs(5))).await;
                None
            }
            RateLimitResult::Denied(exc) => {
                tracing::info!(key = %key, "rate limit exceeded");
                let msg = format!(
                    "rate limit exceeded: {} (retry after {}ms)",
                    exc.message,
                    exc.retry_after.as_millis()
                );
                Some(Self::create_error_response("53400", &msg))
            }
        }
    }

    /// In-band failure feedback. When a query fails against a backend, demote
    /// that node's health *immediately* — a copy-on-write update of the shared
    /// health snapshot, the same structure the periodic health checker
    /// maintains — so routing stops sending work to a dead node within one
    /// query instead of waiting up to a full health-check interval (the
    /// ~`check_interval` blind window). The periodic checker restores the node
    /// on its next successful probe, so this only ever *accelerates* detection.
    ///
    /// True when `err` is evidence the backend itself is unhealthy — and so
    /// should demote it in-band (and trip its circuit breaker) — as opposed to a
    /// client-side problem or a merely slow but healthy query.
    ///
    /// Excluded (return `false`, no penalty):
    /// * `Client …` — a failed or timed-out client write is the client's fault.
    /// * `Backend read timeout` — a backend that emits no bytes within the
    ///   streaming read window is indistinguishable from a legitimately slow but
    ///   healthy query (large sort/aggregate, lock wait, bulk DML). Demoting the
    ///   whole node — cluster-wide, for every session, bypassing the configured
    ///   `failure_threshold` — over one slow query is a false positive; a
    ///   genuinely unresponsive-but-connected backend is still caught by the
    ///   periodic protocol-level health probe.
    ///
    /// Still faults (return `true`): a backend read/write *error* (reset, EOF,
    /// broken pipe), a backend *write* timeout (the backend is not draining its
    /// socket), and any connect-time failure.
    fn is_backend_fault(err: &str) -> bool {
        !err.contains("Client") && !err.contains("Backend read timeout")
    }

    /// Errors that do not demote a backend are filtered via `is_backend_fault`:
    /// a client disconnecting mid-query, or one merely-slow query, must never
    /// take a healthy backend out of rotation for every session.
    fn note_backend_failure(state: &Arc<ServerState>, addr: &str, err: &str) {
        if !Self::is_backend_fault(err) {
            return;
        }
        // Serialize the read-modify-write of the shared health snapshot. ArcSwap
        // makes only the final pointer swap atomic; without this lock two
        // concurrent writers — in-band demotions for different nodes, or an
        // in-band demotion racing the periodic checker's full-map rebuild — can
        // each load the same snapshot and clobber the other's update (a lost
        // update that resurrects a demoted node, or evicts a recovered one,
        // until the next probe). The lock serializes writers only; every routing
        // read stays lock-free on the ArcSwap.
        let _writers = state.health_write.lock();
        let snapshot = state.health.load_full();
        // Only act (and pay the clone) when the node is currently marked
        // healthy — avoids churning the snapshot on an already-down node.
        if snapshot.get(addr).map(|h| h.healthy).unwrap_or(false) {
            let mut next = (*snapshot).clone();
            if let Some(nh) = next.get_mut(addr) {
                nh.healthy = false;
                nh.failure_count = nh.failure_count.saturating_add(1);
                nh.last_error = Some(format!("in-band failure: {}", err));
                tracing::warn!(
                    node = %addr,
                    error = %err,
                    "in-band failure — node marked unhealthy for fast failover"
                );
            }
            state.health.store(Arc::new(next));
        }
    }

    /// Record a backend forward failure: demote the node's health in-band AND
    /// (when the feature is on) trip its circuit breaker — the single place the
    /// data path reports "this backend just failed". Both signals consult the
    /// same `is_backend_fault` classifier, so they can never drift apart: a
    /// client-side error or a slow-query read timeout penalizes neither.
    fn record_backend_failure(state: &Arc<ServerState>, node: &str, err: &str) {
        Self::note_backend_failure(state, node, err);
        #[cfg(feature = "circuit-breaker")]
        if Self::is_backend_fault(err) {
            Self::circuit_record(state, node, false, err);
        }
    }

    /// True when `node`'s circuit is open (avoid it / fast-fail). A half-open
    /// circuit returns false so a probe query is admitted.
    #[cfg(feature = "circuit-breaker")]
    fn circuit_is_open(state: &Arc<ServerState>, node: &str) -> bool {
        state
            .circuit_breaker
            .as_ref()
            .map(|cb| {
                cb.get_breaker(node).get_state() == crate::circuit_breaker::CircuitState::Open
            })
            .unwrap_or(false)
    }

    /// Record the outcome of a forward to `node` on its circuit breaker.
    #[cfg(feature = "circuit-breaker")]
    fn circuit_record(state: &Arc<ServerState>, node: &str, success: bool, err: &str) {
        if let Some(cb) = state.circuit_breaker.as_ref() {
            let breaker = cb.get_breaker(node);
            if success {
                breaker.record_success();
            } else {
                breaker.record_failure(err);
            }
        }
    }

    /// If `node`'s circuit is open, build the fast-fail `ErrorResponse` (without
    /// a trailing `ReadyForQuery` — the caller appends one). `None` when the
    /// circuit is closed or half-open and the request may proceed.
    #[cfg(feature = "circuit-breaker")]
    fn circuit_fast_fail(state: &Arc<ServerState>, node: &str) -> Option<Vec<u8>> {
        if Self::circuit_is_open(state, node) {
            tracing::info!(node = %node, "circuit open — fast-failing");
            Some(Self::create_error_response(
                "08006",
                &format!("circuit open for node {node}: backend temporarily unavailable"),
            ))
        } else {
            None
        }
    }

    /// Read-your-writes decision: should reads be pinned to the primary given
    /// the session's last write and the configured window? Pure for testing.
    #[cfg(feature = "lag-routing")]
    fn ryw_pins_primary(last_write: Option<std::time::Instant>, window_ms: u64) -> bool {
        window_ms > 0
            && last_write
                .map(|t| t.elapsed() < Duration::from_millis(window_ms))
                .unwrap_or(false)
    }

    /// Lag-exclusion decision: should a standby be dropped from read routing
    /// given its measured lag and the configured ceiling? `max=0` disables
    /// exclusion; unknown lag (None) never excludes. Pure for testing.
    #[cfg(feature = "lag-routing")]
    fn lag_excludes_standby(lag_bytes: Option<u64>, max_lag_bytes: u64) -> bool {
        max_lag_bytes > 0 && lag_bytes.map(|l| l > max_lag_bytes).unwrap_or(false)
    }

    /// Pure predicate: is `sql` a plain, deterministic, SINGLE-statement
    /// SELECT safe to cache? (Not WITH/locking/volatile/multi-statement/
    /// SELECT INTO.) Transaction state is checked separately. Shared by the
    /// query-cache and edge-cache read gates.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    fn is_cacheable_read_sql(sql: &str) -> bool {
        use crate::protocol::starts_with_ci;
        let t = sql.trim_start();
        if !starts_with_ci(t, "SELECT") {
            return false;
        }
        // Multi-statement simple-query strings (`SELECT ...; UPDATE ...`)
        // must never be cached: replaying the capture would fabricate the
        // trailing statements' results while executing nothing. One
        // trailing `;` is fine; a `;` inside a string literal only costs
        // cacheability — safe.
        let core = t.trim_end();
        let core = core.strip_suffix(';').map(str::trim_end).unwrap_or(core);
        if core.contains(';') {
            return false;
        }
        // `SELECT ... INTO t` CREATES a table (CREATE TABLE AS synonym);
        // replaying it from cache would silently skip the DDL. Word-boundary
        // match, so newline/tab-delimited INTO is caught while a column
        // named `into_x` is not (a literal containing the word merely skips
        // caching).
        if Self::contains_word_ci(core, "into") {
            return false;
        }
        // The remaining checks are all "does `t` contain one of these
        // needles, case-insensitively" — instead of the 13 separate
        // windowed case-insensitive scans (FOR UPDATE, FOR SHARE, 11
        // VOLATILE tokens) that used to each walk `t` byte-by-byte,
        // lowercase `t` ONCE into a scratch buffer and do plain `str::contains`
        // checks against it (Two-Way substring search — no SIMD, but a single
        // linear scan replacing 13 windowed case-insensitive ones is still a
        // clear win). ASCII-only lowercasing (`to_ascii_lowercase`) leaves any
        // non-ASCII bytes untouched, matching `contains_ci`'s byte-for-byte
        // semantics for non-ASCII input.
        let lower = t.to_ascii_lowercase();
        if lower.contains("for update") || lower.contains("for share") {
            return false;
        }
        // Non-deterministic or side-effectful reads must not be reused
        // (set_config emits ParameterStatus and mutates GUCs — replaying it
        // would suppress the side effect).
        const VOLATILE: [&str; 11] = [
            "now(",
            "current_timestamp",
            "current_date",
            "current_time",
            "clock_timestamp",
            "statement_timestamp",
            "random(",
            "nextval(",
            "uuid_generate",
            "gen_random_uuid",
            "set_config(",
        ];
        !VOLATILE.iter().any(|v| lower.contains(v))
    }

    /// Decide whether a read query is safe to serve from / store in the cache,
    /// and build its `CacheContext`. Returns `None` for anything not a plain,
    /// deterministic, non-transactional SELECT.
    ///
    /// `is_cacheable_read` is `StmtFacts::is_cacheable_read` for the statement
    /// (i.e. `is_cacheable_read_sql`, already computed once for this message).
    #[cfg(feature = "query-cache")]
    async fn cacheable_read_ctx(
        session: &Arc<ClientSession>,
        is_cacheable_read: bool,
    ) -> Option<crate::cache::CacheContext> {
        if !is_cacheable_read {
            return None;
        }
        // Never cache mid-transaction (visibility would be wrong).
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return None;
        }
        let (user, database) = {
            let vars = session.variables.read().await;
            (
                vars.get("user").cloned(),
                vars.get("database")
                    .cloned()
                    .unwrap_or_else(|| "default".to_string()),
            )
        };
        Some(crate::cache::CacheContext {
            database,
            user,
            branch: None,
            connection_id: Some(session.id.as_u64_pair().0),
        })
    }

    /// Build a multi-tenancy `RequestContext` from the session's startup
    /// parameters (user, database, application_name, ...) so the configured
    /// identifier can resolve the tenant.
    #[cfg(feature = "multi-tenancy")]
    async fn tenant_request_ctx(
        session: &Arc<ClientSession>,
    ) -> crate::multi_tenancy::RequestContext {
        let vars = session.variables.read().await;
        crate::multi_tenancy::RequestContext {
            headers: vars.clone(),
            username: vars.get("user").cloned(),
            database: vars.get("database").cloned(),
            auth_token: None,
            sql_context: HashMap::new(),
            client_ip: Some(session.client_addr.ip().to_string()),
            connection_id: Some(session.id.as_u64_pair().0),
        }
    }

    /// Journal a successful write statement (Transaction Replay). Each write is
    /// recorded as its own auto-commit transaction so the time-travel/failover
    /// replay engine can re-apply it onto a promoted primary or a staging
    /// target. Best-effort: journal errors never fail the client query.
    #[cfg(feature = "ha-tr")]
    async fn journal_write(state: &Arc<ServerState>, session: &Arc<ClientSession>, sql: &str) {
        // One lock acquisition and one cheap id draw per write: `begin_and_log`
        // is the fused begin+log, and the auto-commit id comes from the
        // per-process counter instead of the OS RNG (see
        // `transaction_journal::next_auto_commit_tx_id`). Explicit
        // transactions still use begin_transaction + log_statement.
        let tx_id = crate::transaction_journal::next_auto_commit_tx_id();
        let _ = state
            .transaction_journal
            .begin_and_log(
                tx_id,
                session.id,
                crate::NodeId::new(),
                0,
                sql.to_string(),
                Vec::new(),
                None,
                None,
                0,
            )
            .await;
    }

    /// Hand a forwarded query to the analytics engine. This only builds the
    /// `QueryExecution` and pushes it onto the engine's bounded queue — the
    /// fingerprinting, metrics, slow-query log and pattern detection all run
    /// on the single background consumer task (see
    /// `QueryAnalytics::start_consumer`), so the connection task never pays
    /// for them. A full queue drops the sample rather than stalling the relay.
    /// No-op when analytics is disabled.
    #[cfg(feature = "query-analytics")]
    async fn record_analytics(
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
        sql: &str,
        node: &str,
        duration: Duration,
        error: Option<String>,
    ) {
        let Some(analytics) = state.analytics.as_ref() else {
            return;
        };
        let (user, database) = {
            let vars = session.variables.read().await;
            (
                vars.get("user").cloned().unwrap_or_default(),
                vars.get("database").cloned().unwrap_or_default(),
            )
        };
        let mut exec = crate::analytics::QueryExecution::new(sql, duration);
        exec.user = user;
        exec.database = database;
        // Both pre-rendered at session creation — see `ClientSession`.
        exec.client_ip = session.client_ip_str.clone();
        exec.node = node.to_string();
        exec.session_id = Some(session.session_id_str.clone());
        exec.error = error;
        analytics.record(exec);
    }

    /// Select primary node with write timeout during failover
    async fn select_primary_with_timeout(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        let timeout = config.write_timeout();
        let start = std::time::Instant::now();
        // Poll for the promoted primary fairly tightly so writes resume
        // quickly after a failover (was 500ms — a needless recovery floor).
        let check_interval = Duration::from_millis(100);

        loop {
            // Try to find a healthy primary. Every enabled primary is
            // considered in config order (not just the first): a config that
            // lists a promoted/secondary primary must be able to fail over to
            // it once the first is demoted in-band — `select_node_for_startup`
            // already applies the same rule for new sessions.
            let health = state.health.load_full();
            let primary = config.nodes.iter().find(|n| {
                n.role == NodeRole::Primary
                    && n.enabled
                    && health.get(n.address()).map(|h| h.healthy).unwrap_or(false)
            });

            if let Some(primary_node) = primary {
                // Update session's current node
                let node_addr = primary_node.address().to_string();
                let mut current = session.current_node.write().await;
                *current = Some(node_addr.clone());
                return Ok(node_addr);
            }
            drop(health);

            // Check if timeout exceeded
            if start.elapsed() >= timeout {
                state.metrics.failovers.fetch_add(1, Ordering::Relaxed);
                return Err(ProxyError::NoHealthyNodes);
            }

            tracing::warn!(
                "Primary unavailable, waiting for failover... ({:.1}s elapsed, {:.1}s timeout)",
                start.elapsed().as_secs_f64(),
                timeout.as_secs_f64()
            );

            // Wait before retry
            tokio::time::sleep(check_interval).await;
        }
    }

    /// Select node for read operations with load balancing
    async fn select_read_node(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        // If in transaction, stick to current node
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            if let Some(node) = session.current_node.read().await.clone() {
                return Ok(node);
            }
        }

        // Get healthy nodes (prefer standbys for reads)
        let health = state.health.load_full();
        let healthy_standbys: Vec<&NodeConfig> = config
            .nodes
            .iter()
            .filter(|n| {
                let base = n.enabled
                    && (n.role == NodeRole::Standby || n.role == NodeRole::ReadReplica)
                    && health.get(n.address()).map(|h| h.healthy).unwrap_or(false);
                // Drop a standby whose circuit is open so reads avoid it.
                #[cfg(feature = "circuit-breaker")]
                let base = base && !Self::circuit_is_open(state, n.address());
                // Drop a standby lagging beyond the configured byte threshold.
                #[cfg(feature = "lag-routing")]
                let base = base
                    && !Self::lag_excludes_standby(
                        health
                            .get(n.address())
                            .and_then(|h| h.replication_lag_bytes),
                        config.lag_routing.max_lag_bytes,
                    );
                base
            })
            .collect();

        if !healthy_standbys.is_empty() {
            // Round-robin across healthy standbys
            let ticket = state.lb_state.rr_counter.fetch_add(1, Ordering::Relaxed);
            let index = ticket as usize % healthy_standbys.len();
            let node_addr = healthy_standbys[index].address().to_string();

            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // Fall back to primary if no healthy standbys
        Self::select_node(session, state, config).await
    }

    /// Complete backend authentication by reading until ReadyForQuery
    /// This is used when switching backends - we don't forward auth to client.
    /// `max_frame_len` bounds a single declared backend frame length (reuses
    /// `state.limits.max_pending_bytes`; see [`validate_backend_frame_len`]).
    /// Drive a freshly dialed backend connection (startup already sent) to
    /// `ReadyForQuery`, answering its authentication challenges with
    /// `credential` when it has any: SCRAM-SHA-256 (the RFC-5802 client in
    /// `backend::auth`), MD5 and cleartext password. Without a credential
    /// only a non-challenging (trust) backend can be completed — a challenge
    /// then fails fast with `ProxyError::Auth` instead of timing out.
    ///
    /// Returns the post-authentication frames the backend sent (ParameterStatus,
    /// BackendKeyData, ReadyForQuery — every frame except the `R` auth
    /// requests) so a caller that is the client's auth boundary can forward
    /// them after its own synthesized `AuthenticationOk`.
    async fn complete_backend_auth<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        backend: &mut S,
        max_frame_len: usize,
        user: &str,
        credential: Option<&str>,
    ) -> Result<Vec<u8>> {
        let mut buffer = BytesMut::with_capacity(4096);
        let mut forward: Vec<u8> = Vec::with_capacity(512);
        let mut scram: Option<crate::backend::auth::Scram> = None;
        let timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(ProxyError::Auth(
                    "Backend authentication timeout".to_string(),
                ));
            }

            buffer.reserve(4096);
            let n = tokio::time::timeout(Duration::from_secs(5), backend.read_buf(&mut buffer))
                .await
                .map_err(|_| ProxyError::Auth("Read timeout during backend auth".to_string()))?
                .map_err(|e| ProxyError::Network(format!("Backend auth read error: {}", e)))?;

            if n == 0 {
                return Err(ProxyError::Connection(
                    "Backend closed during auth".to_string(),
                ));
            }

            // Walk complete frames by raw tag. The wire decoder is
            // direction-agnostic ('E' decodes to the client-side `Execute`), so
            // a backend ErrorResponse must be detected by its raw tag rather
            // than by `msg_type` — the previous version matched
            // `MessageType::ErrorResponse`, which never fired, so a failed
            // backend auth surfaced as a misleading timeout.
            loop {
                if buffer.len() < 5 {
                    break;
                }
                let len = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]) as usize;
                if len < 4 {
                    break;
                }
                validate_backend_frame_len(len, max_frame_len)?;
                if buffer.len() < len + 1 {
                    break;
                }
                let tag = buffer[0];
                let frame = buffer.split_to(len + 1);
                match tag {
                    // ReadyForQuery: authentication complete.
                    b'Z' => {
                        forward.extend_from_slice(&frame);
                        return Ok(forward);
                    }
                    // ErrorResponse: parse its message for a clear error.
                    b'E' => {
                        let payload = BytesMut::from(&frame[5..]);
                        let err = ErrorResponse::parse(payload)
                            .map(|e| e.message().unwrap_or("Unknown error").to_string())
                            .unwrap_or_else(|_| "authentication failed".to_string());
                        return Err(ProxyError::Auth(err));
                    }
                    // Authentication request: answer the challenge.
                    b'R' if frame.len() >= 9 => {
                        let kind = u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]);
                        let body = &frame[9..];
                        let reply: Option<Vec<u8>> = match kind {
                            0 => None, // AuthenticationOk
                            3 | 5 | 10 | 11 | 12 => {
                                let Some(password) = credential else {
                                    return Err(ProxyError::Auth(format!(
                                        "backend requires authentication (type {}) for user '{}' but the proxy holds no credential: pass-through auth cannot open a fresh backend connection — configure [auth] mode = \"scram\" with a plaintext auth_file entry, or a trust backend",
                                        kind, user
                                    )));
                                };
                                match kind {
                                    // CleartextPassword
                                    3 => {
                                        let mut p = password.as_bytes().to_vec();
                                        p.push(0);
                                        Some(p)
                                    }
                                    // MD5Password: 4-byte salt.
                                    5 => {
                                        if body.len() < 4 {
                                            return Err(ProxyError::Auth(
                                                "malformed MD5 challenge".to_string(),
                                            ));
                                        }
                                        let salt = [body[0], body[1], body[2], body[3]];
                                        Some(crate::backend::auth::md5_password_response(
                                            user, password, &salt,
                                        ))
                                    }
                                    // SASL: mechanism list (cstrings, empty terminator).
                                    10 => {
                                        let offered =
                                            body.split(|&b| b == 0).any(|m| m == b"SCRAM-SHA-256");
                                        if !offered {
                                            return Err(ProxyError::Auth(
                                                "backend offers no SCRAM-SHA-256 SASL mechanism"
                                                    .to_string(),
                                            ));
                                        }
                                        let (client, first) =
                                            crate::backend::auth::Scram::client_first(
                                                Self::random_nonce(),
                                            );
                                        scram = Some(client);
                                        Some(first.0)
                                    }
                                    // SASLContinue: server-first.
                                    11 => {
                                        let Some(client) = scram.as_mut() else {
                                            return Err(ProxyError::Auth(
                                                "SASLContinue before SASL start".to_string(),
                                            ));
                                        };
                                        Some(
                                            client
                                                .client_final(body, password)
                                                .map_err(|e| {
                                                    ProxyError::Auth(format!("SCRAM: {}", e))
                                                })?
                                                .0,
                                        )
                                    }
                                    // SASLFinal: verify the server signature.
                                    _ => {
                                        let Some(client) = scram.as_ref() else {
                                            return Err(ProxyError::Auth(
                                                "SASLFinal before SASL start".to_string(),
                                            ));
                                        };
                                        client.verify_server(body).map_err(|e| {
                                            ProxyError::Auth(format!("SCRAM: {}", e))
                                        })?;
                                        None
                                    }
                                }
                            }
                            other => {
                                return Err(ProxyError::Auth(format!(
                                    "unsupported backend authentication method {}",
                                    other
                                )));
                            }
                        };
                        if let Some(payload) = reply {
                            // PasswordMessage: 'p' + int32 len + payload.
                            let mut msg = Vec::with_capacity(payload.len() + 5);
                            msg.push(b'p');
                            msg.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
                            msg.extend_from_slice(&payload);
                            backend.write_all(&msg).await.map_err(|e| {
                                ProxyError::Network(format!("Backend auth write error: {}", e))
                            })?;
                        }
                    }
                    _ => forward.extend_from_slice(&frame),
                }
            }
        }
    }

    /// Create PostgreSQL error response message
    fn create_error_response(code: &str, message: &str) -> Vec<u8> {
        Self::create_severity_response("ERROR", code, message)
    }

    /// Create a PostgreSQL `ErrorResponse` with `FATAL` severity — the severity
    /// PostgreSQL itself uses for a connection it is about to close (e.g.
    /// `53300 too_many_connections`, `57P05 idle_session_timeout`). Drivers
    /// (pgx, npgsql, JDBC) key on `FATAL` to mark the connection dead; with
    /// `ERROR` they try to keep using it and then hit a bare EOF.
    fn create_fatal_response(code: &str, message: &str) -> Vec<u8> {
        Self::create_severity_response("FATAL", code, message)
    }

    fn create_severity_response(severity: &str, code: &str, message: &str) -> Vec<u8> {
        let mut fields = HashMap::new();
        fields.insert('S', severity.to_string());
        fields.insert('V', severity.to_string());
        fields.insert('C', code.to_string());
        fields.insert('M', message.to_string());

        let err = ErrorResponse { fields };
        err.encode().encode().to_vec()
    }

    /// Create a `ReadyForQuery` frame with the given transaction-status byte
    /// (`b'I'` = idle, `b'T'` = in transaction, `b'E'` = failed transaction).
    fn create_ready_for_query(status: u8) -> Vec<u8> {
        let mut payload = BytesMut::with_capacity(1);
        payload.put_u8(status);
        Message::new(MessageType::ReadyForQuery, payload)
            .encode()
            .to_vec()
    }

    /// Synthesise a full PostgreSQL simple-query response from a cached
    /// payload produced by a plugin's `PreQueryResult::Cached`.
    ///
    /// # Payload format
    ///
    /// The plugin is expected to serialise a JSON document of the form:
    ///
    /// ```json
    /// {
    ///   "columns": [
    ///     {"name": "id",    "oid": 23},
    ///     {"name": "email", "oid": 25}
    ///   ],
    ///   "rows": [
    ///     ["1", "alice@example.com"],
    ///     ["2", null]
    ///   ]
    /// }
    /// ```
    ///
    /// `oid` is the PostgreSQL type OID (`23` = int4, `25` = text,
    /// `20` = int8, `16` = bool, `1184` = timestamptz, etc.). Row values
    /// are strings in text format; `null` encodes a SQL NULL. The type
    /// OID is advisory — pgwire clients accept `25` (text) universally
    /// and cast as needed.
    ///
    /// # Returned bytes
    ///
    /// One concatenated PostgreSQL wire response:
    ///
    /// ```text
    /// RowDescription (T) + DataRow (D) × N + CommandComplete (C: "SELECT N")
    ///                    + ReadyForQuery (Z: idle)
    /// ```
    ///
    /// Returns an error on malformed JSON; the caller falls back to
    /// backend forwarding.
    #[cfg(feature = "wasm-plugins")]
    fn synthesise_cached_response(bytes: &[u8]) -> Result<Vec<u8>> {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct CachedPayload {
            columns: Vec<ColumnDef>,
            rows: Vec<Vec<Option<String>>>,
        }

        #[derive(Deserialize)]
        struct ColumnDef {
            name: String,
            #[serde(default = "default_text_oid")]
            oid: u32,
        }

        fn default_text_oid() -> u32 {
            25 // text
        }

        let payload: CachedPayload = serde_json::from_slice(bytes)
            .map_err(|e| ProxyError::Protocol(format!("invalid cached payload JSON: {}", e)))?;

        if payload.columns.is_empty() {
            return Err(ProxyError::Protocol(
                "cached payload must declare at least one column".to_string(),
            ));
        }

        let mut reply = Vec::new();

        // RowDescription (tag 'T')
        let mut rd = BytesMut::new();
        rd.put_u16(payload.columns.len() as u16);
        for col in &payload.columns {
            rd.extend_from_slice(col.name.as_bytes());
            rd.put_u8(0); // cstring terminator
            rd.put_i32(0); // tableOID (unknown)
            rd.put_i16(0); // columnNumber (unknown)
            rd.put_u32(col.oid);
            rd.put_i16(-1); // typeLen (unspecified)
            rd.put_i32(-1); // typeMod (unspecified)
            rd.put_i16(0); // format code: text
        }
        reply.extend_from_slice(&Message::new(MessageType::RowDescription, rd).encode());

        // DataRow (tag 'D') per row
        let column_count = payload.columns.len();
        for row in &payload.rows {
            if row.len() != column_count {
                return Err(ProxyError::Protocol(format!(
                    "cached row has {} values but {} columns are declared",
                    row.len(),
                    column_count
                )));
            }
            let mut dr = BytesMut::new();
            dr.put_u16(row.len() as u16);
            for value in row {
                match value {
                    Some(s) => {
                        dr.put_i32(s.len() as i32);
                        dr.extend_from_slice(s.as_bytes());
                    }
                    None => {
                        dr.put_i32(-1); // NULL sentinel
                    }
                }
            }
            reply.extend_from_slice(&Message::new(MessageType::DataRow, dr).encode());
        }

        // CommandComplete (tag 'C')
        let tag = format!("SELECT {}", payload.rows.len());
        let mut cc = BytesMut::new();
        cc.extend_from_slice(tag.as_bytes());
        cc.put_u8(0);
        reply.extend_from_slice(&Message::new(MessageType::CommandComplete, cc).encode());

        // ReadyForQuery (tag 'Z', status 'I' idle)
        reply.extend_from_slice(&Self::create_ready_for_query(b'I'));

        Ok(reply)
    }

    /// Run the pre-query plugin hook on a client message.
    ///
    /// When the `wasm-plugins` feature is off, or the plugin manager has no
    /// loaded plugins, this is a zero-cost passthrough that returns the
    /// message untouched with `PreQueryAction::Forward`.
    ///
    /// Only simple-query (`MessageType::Query`) messages are inspected today.
    /// Extended-protocol messages (`Parse`/`Bind`/`Execute`) are passed
    /// through unchanged — a future task wires them in.
    fn apply_pre_query_hook(
        msg: Message,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) -> (Message, PreQueryAction) {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return (msg, PreQueryAction::Forward),
            };

            if msg.msg_type != MessageType::Query {
                return (msg, PreQueryAction::Forward);
            }

            // Zero plugins registered for this hook — skip the payload
            // clone, SQL parse, and context construction entirely.
            if !pm.has_hook(HookType::PreQuery) {
                return (msg, PreQueryAction::Forward);
            }

            let query_msg = match QueryMessage::parse(msg.payload.clone()) {
                Ok(q) => q,
                Err(_) => return (msg, PreQueryAction::Forward),
            };

            let ctx = Self::build_query_context(&query_msg.query, session);

            match pm.execute_pre_query(&ctx) {
                PreQueryResult::Continue => (msg, PreQueryAction::Forward),
                PreQueryResult::Block(reason) => (msg, PreQueryAction::Block(reason)),
                PreQueryResult::Rewrite(new_sql) => {
                    let rewritten = QueryMessage { query: new_sql }.encode();
                    (rewritten, PreQueryAction::Forward)
                }
                PreQueryResult::Cached(bytes) => (msg, PreQueryAction::Cached(bytes)),
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            let _ = (state, session);
            (msg, PreQueryAction::Forward)
        }
    }

    /// Feed the anomaly detector a per-query observation. Cheap —
    /// only the SQL-injection scan and the novel-fingerprint check
    /// are non-trivial, both well under a microsecond on
    /// representative queries. Returns nothing; detections land in
    /// the detector's ring buffer and are surfaced via /api/anomalies.
    #[cfg(feature = "anomaly-detection")]
    fn record_anomaly_observation(
        msg: &Message,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) {
        if msg.msg_type != MessageType::Query {
            return;
        }
        // Borrow the SQL straight out of the payload — the message is
        // forwarded verbatim, so no deep copy of the frame is needed.
        if let Some(query) = crate::protocol::query_text(&msg.payload) {
            Self::record_anomaly_sql(query, state, session);
        }
    }

    /// Feed one SQL statement to the anomaly detector. Shared by the
    /// simple-query path and the extended-protocol `Parse` path so
    /// prepared-statement traffic is observed too.
    #[cfg(feature = "anomaly-detection")]
    fn record_anomaly_sql(query: &str, state: &Arc<ServerState>, session: &Arc<ClientSession>) {
        // Tenant identifier is the most-specific known per-session
        // attribute the proxy can attribute traffic to. Multi-tenancy
        // sets `tenant_id` in `variables`; otherwise we fall back to
        // the client address. session.variables is a tokio RwLock but this
        // is a sync helper — try_read avoids an await; on contention we
        // fall back to the client IP, still a valid per-source identifier.
        let tenant = match session.variables.try_read() {
            Ok(vars) => vars
                .get("tenant_id")
                .or_else(|| vars.get("user"))
                .cloned()
                .unwrap_or_else(|| session.client_addr.ip().to_string()),
            Err(_) => session.client_addr.ip().to_string(),
        };
        // Both the fingerprint and the SQL are *lent* to the detector:
        // the fingerprint from a buffer sized for this call, the SQL
        // straight from the wire frame. The detector copies only on
        // the rare paths that retain something (first-seen
        // fingerprint, emitted event excerpt). The fingerprint buffer
        // is allocated per call rather than cached in a thread-local,
        // so nothing is retained between queries — a client sending
        // one very large statement does not leave its buffer
        // permanently resident on the worker thread.
        let mut fingerprint = String::with_capacity(query.len());
        anomaly_fingerprint_into(query, &mut fingerprint);
        let obs = crate::anomaly::QueryObservation {
            tenant,
            fingerprint: std::borrow::Cow::Borrowed(fingerprint.as_str()),
            sql: std::borrow::Cow::Borrowed(query),
            timestamp: std::time::Instant::now(),
        };
        for ev in state.anomaly_detector.record_query(&obs) {
            tracing::warn!(anomaly = ?ev, "anomaly detected");
        }
    }

    /// Send the client a `Block`-outcome response: an error frame plus
    /// `ReadyForQuery` so the client's state machine returns to idle and
    /// the next query can be accepted.
    async fn send_block_response(
        stream: &mut ClientStream,
        reason: &str,
        state: &Arc<ServerState>,
    ) -> Result<()> {
        let err =
            Self::create_error_response("42000", &format!("Query blocked by plugin: {}", reason));
        stream
            .write_all(&err)
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
        let rfq = Self::create_ready_for_query(b'I');
        stream
            .write_all(&rfq)
            .await
            .map_err(|e| ProxyError::Network(format!("Write error: {}", e)))?;
        state
            .metrics
            .bytes_sent
            .fetch_add((err.len() + rfq.len()) as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Build a `QueryContext` for the plugin hook. Populated fields: `query`
    /// (verbatim), `is_read_only` (derived from SQL verb), and `hook_context`
    /// with the session id as `client_id`. `normalized` and `tables` are
    /// left as cheap stand-ins until the analytics normaliser is wired in
    /// (T0-d, unified context).
    #[cfg(feature = "wasm-plugins")]
    fn build_query_context(query: &str, session: &Arc<ClientSession>) -> QueryContext {
        let is_read_only = !Self::is_write_query(query);
        let hook_context = HookContext {
            client_id: Some(session.id.to_string()),
            ..HookContext::default()
        };
        QueryContext {
            query: query.to_string(),
            normalized: query.to_string(),
            tables: Vec::new(),
            is_read_only,
            hook_context,
        }
    }

    /// Run the Authenticate plugin hook at startup. Called from
    /// `connect_and_authenticate` before any backend connection.
    ///
    /// Behaviour by `AuthResult`:
    /// * `Defer` — no plugin opinion; proceed with the default
    ///   PostgreSQL auth flow unchanged.
    /// * `Success(identity)` — store the identity on the session so
    ///   downstream plugins (masking, residency) can gate on roles /
    ///   tenant_id / claims. PostgreSQL backend auth still runs
    ///   normally afterwards (the plugin does not replace PG auth in
    ///   this iteration; that's a follow-up).
    /// * `Denied(reason)` — surfaces as `ProxyError::Auth`, which the
    ///   caller already handles by writing an ErrorResponse to the
    ///   client and closing the connection.
    ///
    /// The `AuthRequest` populated here carries username, database,
    /// and client IP from the PostgreSQL startup parameters. Password
    /// is deliberately `None` — PG protocol sends the password in
    /// response to the backend's challenge, not at startup, so
    /// password-aware plugin auth is a separate future task.
    async fn apply_authenticate_hook(
        _params: &HashMap<String, String>,
        _session: &Arc<ClientSession>,
        _state: &Arc<ServerState>,
    ) -> Result<()> {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match _state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return Ok(()),
            };

            let request = PluginAuthRequest {
                headers: HashMap::new(),
                username: _params.get("user").cloned(),
                password: None,
                client_ip: _session.client_addr.ip().to_string(),
                database: _params.get("database").cloned(),
            };

            match pm.execute_authenticate(&request) {
                AuthResult::Defer => Ok(()),
                AuthResult::Success(identity) => {
                    tracing::debug!(
                        user = %identity.username,
                        roles = ?identity.roles,
                        "plugin authenticated user"
                    );
                    *_session.plugin_identity.write().await = Some(identity);
                    Ok(())
                }
                AuthResult::Denied(reason) => {
                    tracing::info!(
                        reason = %reason,
                        client = %_session.client_addr,
                        user = ?_params.get("user"),
                        "plugin denied authentication"
                    );
                    Err(ProxyError::Auth(format!(
                        "authentication denied by plugin: {}",
                        reason
                    )))
                }
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            Ok(())
        }
    }

    /// Run the Route plugin hook on a message. Only simple-query messages
    /// are inspected; other message types always return `None`.
    fn apply_route_hook(
        msg: &Message,
        state: &Arc<ServerState>,
        session: &Arc<ClientSession>,
    ) -> RouteOverride {
        #[cfg(feature = "wasm-plugins")]
        {
            let pm = match state.plugin_manager.as_ref() {
                Some(pm) => pm,
                None => return RouteOverride::None,
            };
            if msg.msg_type != MessageType::Query {
                return RouteOverride::None;
            }
            // Zero plugins registered for this hook — skip the payload
            // clone, SQL parse, and context construction entirely.
            if !pm.has_hook(HookType::Route) {
                return RouteOverride::None;
            }
            let query_msg = match QueryMessage::parse(msg.payload.clone()) {
                Ok(q) => q,
                Err(_) => return RouteOverride::None,
            };
            let ctx = Self::build_query_context(&query_msg.query, session);
            match pm.execute_route(&ctx) {
                RouteResult::Default => RouteOverride::None,
                RouteResult::Primary => RouteOverride::Primary,
                RouteResult::Standby => RouteOverride::Standby,
                RouteResult::Node(name) => RouteOverride::Node(name),
                RouteResult::Block(reason) => RouteOverride::Block(reason),
                RouteResult::Branch(name) => {
                    tracing::warn!(
                        branch = %name,
                        "Route hook returned Branch but branch routing is not yet wired — using default"
                    );
                    RouteOverride::None
                }
            }
        }
        #[cfg(not(feature = "wasm-plugins"))]
        {
            let _ = (msg, state, session);
            RouteOverride::None
        }
    }

    /// Map parsed SQL-comment hints to a `RouteOverride`. Precedence:
    /// `node=` > `route=` > `consistency=strong`. Read-tier route targets
    /// (standby/sync/semisync/async/local) all map to the read path; `any`
    /// and `vector` impose no constraint. `lag=` / `consistency=bounded`
    /// freshness enforcement arrives with the lag-routing feature.
    #[cfg(feature = "routing-hints")]
    fn hint_to_override(hints: &crate::routing::ParsedHints) -> RouteOverride {
        use crate::routing::{ConsistencyLevel, RouteTarget};
        if let Some(node) = &hints.node {
            return RouteOverride::Node(node.clone());
        }
        if let Some(route) = hints.route {
            return match route {
                RouteTarget::Primary => RouteOverride::Primary,
                RouteTarget::Standby
                | RouteTarget::Sync
                | RouteTarget::SemiSync
                | RouteTarget::Async
                | RouteTarget::Local => RouteOverride::Standby,
                RouteTarget::Any | RouteTarget::Vector => RouteOverride::None,
            };
        }
        if hints.consistency == Some(ConsistencyLevel::Strong) {
            return RouteOverride::Primary;
        }
        RouteOverride::None
    }

    /// Resolve the effective routing for a simple `Query` when the
    /// routing-hints feature is active. Returns `(override, is_write,
    /// forward_msg)`: the write flag is recomputed on the hint-stripped SQL so
    /// a leading hint comment never masks the verb, and `forward_msg` is a
    /// rebuilt `Query` (hint removed) when stripping is on. An explicit
    /// positional hint wins over a plugin route override; a plugin `Block` is
    /// handled by the caller before this runs.
    #[cfg(feature = "routing-hints")]
    fn resolve_simple_route(
        msg: &Message,
        plugin_override: RouteOverride,
        default_is_write: bool,
        state: &Arc<ServerState>,
    ) -> (RouteOverride, bool, Option<Message>) {
        let parser = match state.hint_parser.as_ref() {
            Some(p) => p,
            None => return (plugin_override, default_is_write, None),
        };
        let sql = match crate::protocol::query_text(&msg.payload) {
            Some(s) => s,
            None => return (plugin_override, default_is_write, None),
        };
        let hints = parser.parse(sql);
        if hints.is_empty() {
            return (plugin_override, default_is_write, None);
        }
        let stripped = parser.strip(sql);
        let is_write = Self::is_write_query(&stripped);
        let effective = match Self::hint_to_override(&hints) {
            RouteOverride::None => plugin_override,
            hint_override => hint_override,
        };
        let forward = if parser.strip_hints {
            Some(crate::protocol::QueryMessage { query: stripped }.encode())
        } else {
            None
        };
        (effective, is_write, forward)
    }

    /// Resolve hint-driven routing for an extended-protocol batch from the
    /// first Parse's SQL. `Some((is_write, forced_node))` when hints are
    /// present (write flag computed on the stripped SQL), else `None` so the
    /// caller uses verb-based defaults. The hint comment is left in the
    /// forwarded `Parse` (a no-op SQL comment); rewriting the batch buffer is
    /// unnecessary for correctness.
    #[cfg(feature = "routing-hints")]
    fn extended_hint_route(state: &Arc<ServerState>, sql: &str) -> Option<(bool, Option<String>)> {
        let parser = state.hint_parser.as_ref()?;
        let hints = parser.parse(sql);
        if hints.is_empty() {
            return None;
        }
        let stripped = parser.strip(sql);
        let is_write = Self::is_write_query(&stripped);
        match Self::hint_to_override(&hints) {
            RouteOverride::Primary => Some((true, None)),
            RouteOverride::Standby => Some((false, None)),
            RouteOverride::Node(n) => Some((is_write, Some(n))),
            _ => Some((is_write, None)),
        }
    }

    /// Fire post-query hooks after a message has been forwarded (or failed
    /// to forward). Best-effort; errors from individual plugins are logged
    /// by the plugin manager and never surface here.
    #[cfg(feature = "wasm-plugins")]
    fn fire_post_query_hook(
        msg: &Message,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        result: &Result<(Option<String>, u64)>,
        elapsed: Duration,
    ) {
        let pm = match state.plugin_manager.as_ref() {
            Some(pm) => pm,
            None => return,
        };
        if msg.msg_type != MessageType::Query {
            return;
        }
        // Zero plugins registered for this hook — skip the payload
        // clone, SQL parse, and context construction entirely.
        if !pm.has_hook(HookType::PostQuery) {
            return;
        }
        let query_msg = match QueryMessage::parse(msg.payload.clone()) {
            Ok(q) => q,
            Err(_) => return,
        };
        let ctx = Self::build_query_context(&query_msg.query, session);
        let outcome = match result {
            Ok((node, bytes)) => PostQueryOutcome {
                success: true,
                target_node: node.clone(),
                elapsed_us: elapsed.as_micros() as u64,
                response_bytes: *bytes,
                error: None,
            },
            Err(e) => PostQueryOutcome {
                success: false,
                target_node: None,
                elapsed_us: elapsed.as_micros() as u64,
                response_bytes: 0,
                error: Some(e.to_string()),
            },
        };
        pm.execute_post_query(&ctx, &outcome);
    }

    /// Select a backend node for the request
    /// Select a backend node for initial connection
    /// Prefers primary but falls back to standbys for read connections
    async fn select_node(
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        // If in a transaction, stick to the current node
        if session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            if let Some(node) = session.current_node.read().await.clone() {
                return Ok(node);
            }
        }

        // Get healthy nodes
        let health = state.health.load_full();
        let healthy_nodes: Vec<&NodeConfig> = config
            .nodes
            .iter()
            .filter(|n| n.enabled && health.get(n.address()).map(|h| h.healthy).unwrap_or(false))
            .collect();

        if healthy_nodes.is_empty() {
            return Err(ProxyError::NoHealthyNodes);
        }

        // Try to find healthy primary first
        if let Some(primary) = healthy_nodes.iter().find(|n| n.role == NodeRole::Primary) {
            let node_addr = primary.address().to_string();
            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // Fall back to standby if primary is unavailable
        // (Initial connection will work, writes will use write timeout to wait for primary)
        if let Some(standby) = healthy_nodes.iter().find(|n| n.role == NodeRole::Standby) {
            tracing::warn!("Primary unavailable, connecting to standby for initial session");
            let node_addr = standby.address().to_string();
            let mut current = session.current_node.write().await;
            *current = Some(node_addr.clone());
            return Ok(node_addr);
        }

        // No nodes available
        Err(ProxyError::NoHealthyNodes)
    }

    /// Spawn health checker background task
    fn spawn_health_checker(&self) -> tokio::task::JoinHandle<()> {
        let state = self.state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Clamp to a 1s floor: `tokio::time::interval` panics on a zero
            // period, which would silently kill this task (it would then never
            // probe, so the proxy keeps routing to dead backends). `validate()`
            // already rejects 0 in a file config; this defends a
            // programmatically-built or reloaded config too.
            let interval_secs = state.live_config.load().health.check_interval_secs.max(1);
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Read the live config each tick so a SIGHUP that
                        // adds/removes nodes is checked on the next sweep.
                        let config = state.live_config.load_full();
                        // Run the sweep in a child task so an unexpected panic in
                        // check_all_nodes surfaces as a JoinError and is logged —
                        // the health loop keeps running instead of dying silently
                        // and freezing health at its last snapshot forever.
                        let st = state.clone();
                        if let Err(e) = tokio::spawn(async move {
                            Self::check_all_nodes(&st, &config).await;
                        })
                        .await
                        {
                            tracing::error!(error = %e, "health-check sweep panicked; health loop continuing");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        })
    }

    /// Check health of all nodes.
    ///
    /// Probes run concurrently (one slow/unreachable node no longer delays
    /// detection on the others — lowers the failover-detection latency
    /// floor), then a single new health snapshot is published via ArcSwap so
    /// readers on the query path never block.
    async fn check_all_nodes(state: &Arc<ServerState>, config: &ProxyConfig) {
        // Probe every node in parallel (owned address + timeout so each
        // probe is 'static and runs on its own task).
        let timeout = Duration::from_secs(config.health.check_timeout_secs);
        let mut set = tokio::task::JoinSet::new();
        for node in &config.nodes {
            let addr = node.address().to_string();
            set.spawn(async move {
                let r = Self::check_node_addr(&addr, timeout).await;
                (addr, r)
            });
        }
        let mut results = Vec::with_capacity(config.nodes.len());
        while let Some(joined) = set.join_next().await {
            if let Ok(pair) = joined {
                results.push(pair);
            }
        }

        // Clone-and-modify the current snapshot, then atomically swap it in.
        // Hold the write lock so a concurrent in-band demotion landing in this
        // load→store window (or a SIGHUP reconcile) cannot clobber, or be
        // clobbered by, this full-map rebuild. All node probing above already
        // completed; no await is held under the guard.
        let _writers = state.health_write.lock();
        let mut next = (*state.health.load_full()).clone();
        for (addr, result) in results {
            if let Some(node_health) = next.get_mut(&addr) {
                match result {
                    Ok(latency) => {
                        node_health.healthy = true;
                        node_health.failure_count = 0;
                        node_health.latency_ms = latency;
                        node_health.last_error = None;
                    }
                    Err(e) => {
                        node_health.failure_count += 1;
                        node_health.last_error = Some(e.to_string());
                        if node_health.failure_count >= config.health.failure_threshold {
                            node_health.healthy = false;
                            tracing::warn!(
                                "Node {} marked unhealthy after {} failures",
                                addr,
                                node_health.failure_count
                            );
                        }
                    }
                }
                node_health.last_check = chrono::Utc::now();
            }
        }
        state.health.store(Arc::new(next));
    }

    /// Check health of a single node with a protocol-level liveness probe.
    ///
    /// A bare TCP connect is not enough: a wedged backend (postmaster stuck,
    /// out of backend slots, mid-crash-recovery) still *accepts* the socket but
    /// never processes the wire protocol, so a connect-only probe reports it
    /// healthy. Instead we connect, send a PostgreSQL `SSLRequest`, and require
    /// the postmaster to answer (`S`/`N`) within the timeout. The SSLRequest is
    /// auth-free and not logged, so it costs the backend essentially nothing,
    /// yet it proves the server is actually servicing the protocol. Returns the
    /// round-trip latency in milliseconds.
    async fn check_node_addr(addr: &str, timeout: Duration) -> Result<f64> {
        // length(8) + SSLRequest code 80877103 (0x04D2162F).
        const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F];
        let start = std::time::Instant::now();
        let mut stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| ProxyError::HealthCheck(format!("Timeout connecting to {}", addr)))?
            .map_err(|e| {
                ProxyError::HealthCheck(format!("Failed to connect to {}: {}", addr, e))
            })?;

        let probe = async {
            stream.write_all(&SSL_REQUEST).await?;
            let mut resp = [0u8; 1];
            stream.read_exact(&mut resp).await?;
            Ok::<u8, std::io::Error>(resp[0])
        };
        // Budget whatever time is left after the connect for the handshake.
        let remaining = timeout
            .saturating_sub(start.elapsed())
            .max(Duration::from_millis(1));
        let byte = tokio::time::timeout(remaining, probe)
            .await
            .map_err(|_| {
                ProxyError::HealthCheck(format!("{} did not answer protocol probe in time", addr))
            })?
            .map_err(|e| {
                ProxyError::HealthCheck(format!("{} protocol probe error: {}", addr, e))
            })?;
        // 'S' (TLS available) or 'N' (not) both prove the postmaster is live and
        // talking the protocol; anything else means a non-PostgreSQL listener.
        if byte != b'S' && byte != b'N' {
            return Err(ProxyError::HealthCheck(format!(
                "{} sent unexpected probe reply {:#x}",
                addr, byte
            )));
        }
        let latency = start.elapsed().as_secs_f64() * 1000.0;
        Ok(latency)
    }

    /// Spawn pool manager background task
    fn spawn_pool_manager(&self) -> tokio::task::JoinHandle<()> {
        // Only referenced by the pool-modes eviction/cleanup arms below.
        #[cfg(feature = "pool-modes")]
        let state = self.state.clone();
        // Resolved from [limits] at startup; captured before the move so the
        // reaper cadence is configurable without a recompile.
        let reap_interval = self.state.limits.pool_reap_interval;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(reap_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Evict idle connections from pool-modes manager
                        #[cfg(feature = "pool-modes")]
                        if let Some(ref pool_manager) = state.pool_manager {
                            pool_manager.evict_idle().await;
                            tracing::trace!("Pool-modes idle eviction completed");
                        }
                        // Reap data-path idle backend connections older than the
                        // configured idle timeout, so a connection the backend
                        // would close on its own idle timeout is never handed out
                        // stale and idle FDs are returned to the OS.
                        #[cfg(feature = "pool-modes")]
                        if let Some(ref backend_pool) = state.backend_pool {
                            let ttl = std::time::Duration::from_secs(
                                state.live_config.load().pool_mode.idle_timeout_secs,
                            );
                            // idle_timeout_secs = 0 means "no idle TTL" (the
                            // PgBouncer convention). Skip reaping entirely rather
                            // than reaping every parked connection each cycle
                            // (elapsed() < ZERO is always false → retain drops
                            // all), which would defeat connection reuse.
                            let n = if ttl.is_zero() {
                                0
                            } else {
                                backend_pool.reap_idle(ttl)
                            };
                            if n > 0 {
                                tracing::debug!(
                                    target: "helios::pool",
                                    reaped = n,
                                    idle_remaining = backend_pool.idle_count(),
                                    "reaped idle backend connections (TTL)"
                                );
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        // Cleanup on shutdown
                        #[cfg(feature = "pool-modes")]
                        if let Some(ref pool_manager) = state.pool_manager {
                            pool_manager.close_all().await;
                            tracing::info!("Pool-modes manager closed all connections");
                        }
                        break;
                    }
                }
            }
        })
    }

    /// Spawn the edge-registry maintenance task: a periodic GC sweep that
    /// prunes edges not seen within the liveness window. Healthy subscribers
    /// are continually refreshed by their SSE heartbeat writes (registry
    /// `touch`), so this is a backstop that reaps wedged or dead peers whose
    /// heartbeats stopped succeeding — a pruned edge simply re-registers on
    /// its next reconnect.
    #[cfg(feature = "edge-proxy")]
    fn spawn_edge_maintenance(&self) -> tokio::task::JoinHandle<()> {
        let state = self.state.clone();
        // validate() rejects 0 when edge is enabled; .max(1) keeps the
        // interval panic-proof regardless of how the config was built.
        let gc_secs = self.config.edge.subscribe_gc_secs.max(1);
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(gc_secs));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let pruned = state.edge_registry.prune_stale();
                        if pruned > 0 {
                            tracing::debug!(
                                target: "helios::edge",
                                pruned,
                                remaining = state.edge_registry.count(),
                                "edge registry GC pruned stale edges"
                            );
                        }
                    }
                    _ = shutdown_rx.recv() => break,
                }
            }
        })
    }

    /// Shutdown the server
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Get pool mode statistics (if pool-modes feature enabled)
    #[cfg(feature = "pool-modes")]
    pub async fn pool_mode_stats(&self) -> Option<PoolModeStatsSnapshot> {
        if let Some(ref pool_manager) = self.state.pool_manager {
            let stats = pool_manager.get_stats().await;
            let metrics = pool_manager.metrics().snapshot();
            let default_mode = pool_manager.default_mode();

            // Calculate average lease duration across all modes
            let avg_lease_duration_ms = metrics
                .mode_stats
                .get(&default_mode)
                .map(|s| s.avg_lease_duration_ms as u64)
                .unwrap_or(0);

            Some(PoolModeStatsSnapshot {
                mode: format!("{:?}", default_mode),
                total_connections: stats.total_connections,
                active_leases: stats.active_connections,
                idle_connections: stats.idle_connections,
                node_count: stats.node_count,
                acquires: metrics.acquires,
                releases: metrics.releases,
                acquire_failures: metrics.acquire_failures,
                acquire_timeouts: metrics.acquire_timeouts,
                transactions_completed: metrics.transactions_completed,
                statements_executed: metrics.statements_executed,
                avg_lease_duration_ms,
            })
        } else {
            None
        }
    }

    /// Add a node to the pool manager (if pool-modes feature enabled)
    #[cfg(feature = "pool-modes")]
    pub async fn add_node_to_pool(&self, node: &NodeConfig) {
        if let Some(ref pool_manager) = self.state.pool_manager {
            let endpoint = NodeEndpoint::new(&node.host, node.port)
                .with_role(match node.role {
                    NodeRole::Primary => crate::NodeRole::Primary,
                    NodeRole::Standby => crate::NodeRole::Standby,
                    NodeRole::ReadReplica => crate::NodeRole::ReadReplica,
                })
                .with_weight(node.weight);
            pool_manager.add_node(&endpoint).await;
            tracing::info!("Added node {} to pool manager", node.address());
        }
    }

    /// Get server metrics
    pub fn metrics(&self) -> ServerMetricsSnapshot {
        ServerMetricsSnapshot {
            connections_accepted: self
                .state
                .metrics
                .connections_accepted
                .load(Ordering::Relaxed),
            connections_rejected: self
                .state
                .metrics
                .connections_rejected
                .load(Ordering::Relaxed),
            connections_closed: self
                .state
                .metrics
                .connections_closed
                .load(Ordering::Relaxed),
            queries_processed: self.state.metrics.queries_processed.load(Ordering::Relaxed),
            bytes_received: self.state.metrics.bytes_received.load(Ordering::Relaxed),
            bytes_sent: self.state.metrics.bytes_sent.load(Ordering::Relaxed),
            failovers: self.state.metrics.failovers.load(Ordering::Relaxed),
            cache_capture_oversize: self
                .state
                .metrics
                .cache_capture_oversize
                .load(Ordering::Relaxed),
            tr: self.state.metrics.tr.snapshot(),
        }
    }
}

// ---------------------------------------------------------------------------
// In-session Transaction Replay (`tr_mode`)
//
// Transparent failover of a LIVE client session when its backend connection
// fails: the forward path reports the fault (`BackendFault`), a pure decision
// table (`tr_decide`) picks an action from the configured `TrMode`, and a thin
// async orchestrator (`tr_handle_fault`) executes it — reconnecting to a
// healthy primary, restoring session state, replaying the recorded explicit
// transaction and/or re-executing the interrupted request, or returning ONE
// well-formed `ErrorResponse` instead of dropping the socket.
//
// Hard rules encoded in the table: a write whose outcome is unknown is never
// re-executed except as part of `transaction` mode's replay of an UNCOMMITTED
// transaction (the original copy died uncommitted with the old backend), and
// a COMMIT whose outcome is unknown is never retried.
// ---------------------------------------------------------------------------

/// Where a backend fault struck relative to the in-flight request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultPhase {
    /// Dispatch has not begun (e.g. connect failure or an idle socket loss),
    /// or a single Query frame was not completely delivered. Never use this
    /// for a failed extended-batch write: complete prefix frames may have run.
    NotDelivered,
    /// Dispatch was attempted, or the response was lost before ReadyForQuery.
    /// The backend may have processed the whole request or complete prefix
    /// frames of an extended batch; write_all failure does not establish zero delivery.
    OutcomeUnknown,
}

/// What the client has already seen from the interrupted response.
#[derive(Debug, Clone, Copy, Default)]
struct ResponseProgress {
    bytes: u64,
    /// A CommandComplete or ErrorResponse was already published. Neither may be
    /// followed by a synthesized frame: a second ErrorResponse has no legal
    /// place, and an ErrorResponse after a CommandComplete would report failure
    /// for a statement the backend actually finished — a client that then
    /// retries an INSERT double-applies it. The socket closes instead, which is
    /// the one report a client cannot misread as "the statement did not run".
    terminal: bool,
    /// Flush forwarding may have ended inside a frame.
    raw: bool,
}

#[derive(Debug)]
struct ResponseFailure {
    error: ProxyError,
    progress: ResponseProgress,
}

impl std::fmt::Display for ResponseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

/// A backend fault reported by the forward path to the session loop.
#[derive(Debug, Clone)]
struct BackendFault {
    /// Address of the backend that failed.
    node: String,
    phase: FaultPhase,
    progress: ResponseProgress,
    /// Populated by recovery after classifying the whole interrupted request.
    kind: Option<StmtKind>,
    /// Human-readable cause (used in the client-visible error message).
    error: String,
}

impl BackendFault {
    /// Record a fault into `slot` — unless the error is client-side (the client
    /// went away; there is nothing to recover for and the caller propagates).
    fn set(slot: &mut Option<BackendFault>, node: &str, phase: FaultPhase, err: &ProxyError) {
        let error = err.to_string();
        if error.contains("Client") {
            return;
        }
        *slot = Some(BackendFault {
            node: node.to_string(),
            phase,
            progress: ResponseProgress::default(),
            kind: None,
            error,
        });
    }

    fn set_response(slot: &mut Option<BackendFault>, node: &str, failure: &ResponseFailure) {
        Self::set(slot, node, FaultPhase::OutcomeUnknown, &failure.error);
        if let Some(fault) = slot {
            fault.progress = failure.progress;
        }
    }
}

/// Coarse classification of the statement a fault interrupted (and of every
/// statement recorded inside an explicit transaction).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StmtKind {
    /// Side-effect-free read: `SELECT`/read-only `WITH`/`VALUES`/`TABLE`/`SHOW`/
    /// `COPY ... TO`/plain `EXPLAIN`.
    Read,
    /// Modifies data or schema (or may: data-modifying CTE, `COPY ... FROM`,
    /// `CALL`/`DO`/`EXECUTE`, `SELECT ... INTO`, a multi-statement string).
    Write,
    /// Makes a transaction durable: `COMMIT`/`END`/`PREPARE TRANSACTION`/
    /// `COMMIT PREPARED` (or a multi-statement string that may contain one).
    Commit,
    /// Idempotent session/transaction control with no data effect: `BEGIN`/
    /// `START`, `SAVEPOINT`, `RELEASE`, `ROLLBACK [TO]`, `ABORT`, `SET`/`RESET`,
    /// `DISCARD`, the empty query.
    Control,
    /// Anything else (`LISTEN`/`NOTIFY`, `LOCK`, cursors, `EXPLAIN ANALYZE`,
    /// `SELECT nextval(...)`, unknown verbs): conservatively treated like a
    /// write for re-execution purposes.
    Other,
}

/// What the proxy does about a backend fault (see `tr_decide`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrAction {
    /// A response may be complete or mid-frame: close without appending bytes.
    CloseIncompleteResponse,
    /// Send `ErrorResponse(sqlstate)` + `ReadyForQuery`, then close the client.
    CloseWithError(&'static str),
    /// Re-home the session on a healthy primary, send ONE
    /// `ErrorResponse(sqlstate)` + `ReadyForQuery` for the in-flight request
    /// (aborting the client-visible transaction if inside one) and keep
    /// serving the session.
    ErrorAndContinue(&'static str),
    /// Re-home the session and transparently re-execute the in-flight request.
    Reexecute,
    /// Re-home the session, replay the recorded explicit transaction from its
    /// `BEGIN` (responses discarded), then re-execute the in-flight request
    /// inside it.
    ReplayThenReexecute,
}

/// The request a fault interrupted, borrowed so it can be re-executed.
enum InFlight<'a> {
    Simple(&'a Message),
    Extended {
        batch: &'a [u8],
        route_sql: Option<&'a str>,
        wait_ready: bool,
        reprepare: &'a [String],
        defines: &'a [String],
        unnamed: Option<&'a (bytes::Bytes, bytes::Bytes)>,
    },
}

/// Why a transaction replay failed.
enum ReplayFailure {
    /// The replacement backend's socket failed mid-replay.
    Backend(ProxyError),
    /// A replayed statement was rejected (or re-prepare rejected).
    Statement(String),
}

/// Flush-terminated extended-protocol frames of the cycle in progress,
/// accumulated until its `Sync` completes and the cycle is recorded as ONE
/// replay entry (a Flush yields no `ReadyForQuery`, so a replay must send the
/// whole cycle before draining once).
struct TrExtCycle {
    frames: BytesMut,
    unnamed_parse: Option<bytes::Bytes>,
    defines: Vec<String>,
    refs: Vec<String>,
    route_sql: Option<String>,
}

/// Per-session, loop-local state for in-session TR (no locks: only the
/// session's own task touches it).
struct TrSession {
    /// Record statements of explicit transactions (`select`/`transaction`).
    record_tx: bool,
    /// Track session `SET`/`RESET` for restore (`tr_mode != none`).
    track_gucs: bool,
    /// Session-level `SET`/`RESET` statements to replay on a new backend, in
    /// order, bounded by `[limits] tr_max_session_set_statements`.
    gucs: Vec<String>,
    /// Tracking stopped at the cap — the restore is incomplete.
    guc_cap_hit: bool,
    /// `SET`s issued inside the current explicit transaction: promoted into
    /// `gucs` only if the transaction COMMITs (a rolled-back `SET` is undone).
    pending_tx_gucs: Vec<String>,
    /// The session's backend socket died while idle: the next request is a
    /// not-delivered fault against this node.
    lost_backend: Option<String>,
    /// The client-visible transaction was aborted by a failover: every
    /// statement except a transaction end is answered with 25P02 until the
    /// client issues ROLLBACK/COMMIT (which ends it as a ROLLBACK), so no
    /// statement of the dead transaction leaks into autocommit on the new
    /// backend.
    tx_aborted: bool,
    /// `ReadyForQuery` status before the statement being recorded.
    prev_status: u8,
    ext_cycle: Option<TrExtCycle>,
    /// An open Flush cycle exceeded its recording budget before any RFQ.
    ext_cycle_dropped: bool,
    /// A Flush has dispatched part of the current cycle, regardless of TR mode.
    ext_dispatched: bool,
}

impl TrSession {
    fn new(mode: TrMode) -> Self {
        Self {
            record_tx: matches!(mode, TrMode::Select | TrMode::Transaction),
            track_gucs: mode != TrMode::None,
            gucs: Vec::new(),
            guc_cap_hit: false,
            pending_tx_gucs: Vec::new(),
            lost_backend: None,
            tx_aborted: false,
            prev_status: b'I',
            ext_cycle: None,
            ext_cycle_dropped: false,
            ext_dispatched: false,
        }
    }
}

impl ProxyServer {
    /// Unlike a single Query frame, a partially written extended batch may
    /// contain complete Execute/Sync frames. `write_all` and its timeout do not
    /// report a trustworthy zero-delivery guarantee (including buffered TLS).
    /// Conservatively preserve uncertainty even if the first write fails.
    async fn tr_write_batch<W: tokio::io::AsyncWrite + Unpin>(
        stream: &mut W,
        batch: &[u8],
        write_timeout: Duration,
    ) -> std::result::Result<(), (ProxyError, FaultPhase)> {
        let message = match tokio::time::timeout(write_timeout, stream.write_all(batch)).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => format!("Backend write error: {}", e),
            Err(_) => "Backend write timeout".to_string(),
        };
        Err((ProxyError::Network(message), FaultPhase::OutcomeUnknown))
    }

    /// Record the `ReadyForQuery` that closed a response: the hot-path
    /// `in_transaction` flag plus the raw status byte and whether the response
    /// carried an `ErrorResponse` (both read by the in-session TR bookkeeping).
    fn note_ready_for_query(session: &ClientSession, status: u8, had_error: bool) {
        let st = TransactionStatus::from_byte(status);
        session.in_transaction.store(
            st != TransactionStatus::Idle,
            std::sync::atomic::Ordering::Relaxed,
        );
        session
            .last_rfq_status
            .store(st.to_byte(), std::sync::atomic::Ordering::Relaxed);
        session
            .last_response_error
            .store(had_error, std::sync::atomic::Ordering::Relaxed);
    }

    /// The pure in-session TR decision table. `in_tx` is the client-visible
    /// transaction state BEFORE the interrupted statement; `tx_has_writes` /
    /// `tx_replayable` describe the recorded transaction (irrelevant when
    /// `!in_tx`); `kind` classifies the interrupted statement.
    fn tr_decide(
        mode: TrMode,
        phase: FaultPhase,
        in_tx: bool,
        tx_has_writes: bool,
        tx_replayable: bool,
        kind: StmtKind,
    ) -> TrAction {
        use TrAction::*;
        if mode == TrMode::None {
            return CloseWithError("57P01");
        }
        // Safe to run again even if the backend already ran it once.
        let idempotent = matches!(kind, StmtKind::Read | StmtKind::Control);
        // Can the recorded transaction be reproduced on the new backend in
        // this mode? `select` only ever replays READ-ONLY transactions (no
        // effect can be doubled); `transaction` replays any uncommitted one.
        let can_replay = tx_replayable
            && match mode {
                TrMode::Select => !tx_has_writes,
                TrMode::Transaction => true,
                TrMode::None | TrMode::Session => false,
            };
        match phase {
            FaultPhase::NotDelivered => {
                if !in_tx {
                    // Never ran, no transaction context lost: just run it.
                    return Reexecute;
                }
                // The transaction died with the old backend; the statement
                // itself never ran.
                if can_replay {
                    ReplayThenReexecute
                } else {
                    ErrorAndContinue("57P01")
                }
            }
            FaultPhase::OutcomeUnknown => {
                if mode == TrMode::Session {
                    return ErrorAndContinue("08007");
                }
                if !in_tx {
                    // Autocommit: a write may have been applied — never redo it.
                    return if idempotent {
                        Reexecute
                    } else {
                        ErrorAndContinue("08007")
                    };
                }
                // Inside an explicit transaction the old copy died UNCOMMITTED,
                // so nothing was applied — unless the in-flight statement was
                // the COMMIT itself, which may have landed.
                if kind == StmtKind::Commit {
                    return ErrorAndContinue("08007");
                }
                if can_replay {
                    ReplayThenReexecute
                } else {
                    ErrorAndContinue("08007")
                }
            }
        }
    }

    /// Never concatenate a replayed response to bytes already seen by the
    /// client. Complete row frames can be followed by one error; terminal
    /// responses and raw Flush fragments must end with a socket close. The
    /// `raw`/`terminal` tests are deliberately independent of the byte count:
    /// both already imply bytes were written, and predicating the close on that
    /// would leave the invariant resting on a coincidence of two other
    /// functions rather than on the flags themselves.
    fn tr_response_action(
        action: TrAction,
        progress: ResponseProgress,
        prior_flush: bool,
    ) -> TrAction {
        if prior_flush || progress.raw || progress.terminal {
            return TrAction::CloseIncompleteResponse;
        }
        if progress.bytes > 0
            && matches!(action, TrAction::Reexecute | TrAction::ReplayThenReexecute)
        {
            return TrAction::ErrorAndContinue("08007");
        }
        action
    }

    /// Case-insensitive keyword prefix with an identifier boundary after it
    /// (`SET x` matches `SET`, `SETTINGS` does not).
    fn starts_with_word_ci(s: &str, word: &str) -> bool {
        crate::protocol::starts_with_ci(s, word)
            && s.as_bytes()
                .get(word.len())
                .map(|&c| !(c.is_ascii_alphanumeric() || c == b'_'))
                .unwrap_or(true)
    }

    /// Classify one client statement for in-session TR (see `StmtKind`).
    /// Conservative by construction: anything not positively recognised as a
    /// read or as idempotent control is treated as a possible write.
    /// Whether `sql` durably commits the transaction it ends, so SETs issued
    /// inside that transaction survive. Distinct from `StmtKind::Commit`, which
    /// answers the wider replay-safety question.
    fn tr_commits_session_state(sql: &str) -> bool {
        crate::replay_sql::boundaries(sql)
            .map(|b| b.may_commit && !b.ends_tx)
            .unwrap_or(false)
    }

    /// TR-03: can this read-shaped statement be re-executed on an unknown
    /// outcome? Only if every syntactic function call is a PostgreSQL built-in
    /// known to be side-effect-free (or operator-listed), no quoted identifier
    /// is called (its case is opaque to the policy), and the statement does not
    /// select INTO or consume a sequence. Anything else is `Other`: it may have
    /// run once already, so the proxy will not run it again.
    fn tr_read_eligible(core: &str, policy: &TrReadPolicy) -> bool {
        if Self::contains_word_ci(core, "into") {
            return false;
        }
        let mut ok = true;
        let scanned = crate::replay_sql::words(core, |w| {
            if w.call && (w.quoted || !policy.allows_call(w.text)) {
                ok = false;
                return false;
            }
            true
        });
        scanned.is_ok() && ok
    }

    fn tr_classify(sql: &str, policy: &TrReadPolicy) -> StmtKind {
        let Ok(boundaries) = crate::replay_sql::boundaries(sql) else {
            // The session's lexical rules or malformed input make commit
            // boundaries uncertain. Never replay a possibly durable outcome.
            return StmtKind::Commit;
        };
        if boundaries.may_commit {
            return StmtKind::Commit;
        }
        // Preserve the existing read/control eligibility restrictions. The
        // lexical guard strengthens commit detection; it must not silently
        // broaden the read subset before the volatility work in TR-03.
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.is_empty() {
            return StmtKind::Control;
        }
        if core.contains(';') {
            // Multi-statement string: opaque. If it may end a transaction it
            // must never be retried on an unknown outcome.
            return if Self::contains_word_ci(core, "commit")
                || Self::contains_word_ci(core, "end")
                || Self::contains_word_ci(core, "prepare")
            {
                StmtKind::Commit
            } else {
                StmtKind::Write
            };
        }
        let kw = |w: &str| Self::starts_with_word_ci(core, w);
        if kw("COMMIT") || kw("END") || kw("PREPARE TRANSACTION") {
            return StmtKind::Commit;
        }
        if kw("BEGIN")
            || kw("START")
            || kw("SAVEPOINT")
            || kw("RELEASE")
            || kw("ROLLBACK")
            || kw("ABORT")
            || kw("SET")
            || kw("RESET")
            || kw("DISCARD")
        {
            return StmtKind::Control;
        }
        if kw("SELECT") || kw("VALUES") || kw("TABLE") {
            return if Self::tr_read_eligible(core, policy) {
                StmtKind::Read
            } else {
                StmtKind::Other
            };
        }
        if kw("SHOW") {
            return StmtKind::Read;
        }
        if kw("WITH") {
            return if Self::contains_word_ci(core, "insert")
                || Self::contains_word_ci(core, "update")
                || Self::contains_word_ci(core, "delete")
                || Self::contains_word_ci(core, "merge")
            {
                StmtKind::Write
            } else if Self::tr_read_eligible(core, policy) {
                StmtKind::Read
            } else {
                StmtKind::Other
            };
        }
        if kw("COPY") {
            return if Self::contains_word_ci(core, "from") {
                StmtKind::Write
            } else {
                StmtKind::Read
            };
        }
        if kw("EXPLAIN") {
            return if Self::contains_word_ci(core, "analyze")
                || Self::contains_word_ci(core, "analyse")
            {
                StmtKind::Other
            } else {
                StmtKind::Read
            };
        }
        if kw("INSERT")
            || kw("UPDATE")
            || kw("DELETE")
            || kw("MERGE")
            || kw("CREATE")
            || kw("DROP")
            || kw("ALTER")
            || kw("TRUNCATE")
            || kw("GRANT")
            || kw("REVOKE")
            || kw("VACUUM")
            || kw("REINDEX")
            || kw("CLUSTER")
            || kw("CALL")
            || kw("DO")
            || kw("EXECUTE")
            || kw("REFRESH")
            || kw("IMPORT")
            || kw("COMMENT")
            || kw("SECURITY")
            || kw("ANALYZE")
        {
            return StmtKind::Write;
        }
        StmtKind::Other
    }

    /// A single-statement transaction end the aborted-transaction emulation
    /// accepts: `ROLLBACK`/`ABORT`/`COMMIT`/`END` (optionally `WORK`/
    /// `TRANSACTION`) — not `ROLLBACK TO`, not `* PREPARED`, not `AND CHAIN`.
    fn tr_ends_transaction(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.contains(';') {
            return false;
        }
        let mut words = core.split_ascii_whitespace();
        let Some(first) = words.next() else {
            return false;
        };
        let first_ok = ["ROLLBACK", "ABORT", "COMMIT", "END"]
            .iter()
            .any(|w| first.eq_ignore_ascii_case(w));
        if !first_ok {
            return false;
        }
        match words.next() {
            None => true,
            Some(w) => {
                (w.eq_ignore_ascii_case("WORK") || w.eq_ignore_ascii_case("TRANSACTION"))
                    && words.next().is_none()
            }
        }
    }

    /// Is this simple-protocol statement a session-level GUC change worth
    /// replaying onto a replacement backend? `SET LOCAL` / `SET TRANSACTION` /
    /// `SET CONSTRAINTS` are transaction-scoped and excluded.
    fn tr_is_session_set(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        if core.contains(';') {
            return false;
        }
        if Self::starts_with_word_ci(core, "RESET") {
            return true;
        }
        if !Self::starts_with_word_ci(core, "SET") {
            return false;
        }
        let rest = core[3..].trim_start();
        !(Self::starts_with_word_ci(rest, "LOCAL")
            || Self::starts_with_word_ci(rest, "TRANSACTION")
            || Self::starts_with_word_ci(rest, "CONSTRAINTS"))
    }

    /// `RESET ALL` / `DISCARD ALL` wipe every tracked session GUC.
    fn tr_resets_all(sql: &str) -> bool {
        let t = sql.trim();
        let core = t.strip_suffix(';').unwrap_or(t).trim_end();
        (Self::starts_with_word_ci(core, "RESET") || Self::starts_with_word_ci(core, "DISCARD"))
            && Self::contains_word_ci(core, "all")
    }

    /// SQL text of an encoded `Parse` message (5-byte header, name cstring,
    /// query cstring).
    fn parse_msg_sql(parse_bytes: &[u8]) -> Option<&str> {
        let body = parse_bytes.get(5..)?;
        let name_end = body.iter().position(|&b| b == 0)?;
        crate::protocol::query_text(&body[name_end + 1..])
    }

    /// Classify every Execute in wire order, preserving Bind-time statement
    /// identity (replacing a Parse does not change an already bound portal).
    /// Borrow names from the bounded batch; never decode or rewrite Bind values.
    /// References outside this batch are deliberately opaque: the routing
    /// registry does not retain the acknowledged portal/statement generations.
    /// Such a reference may be a COMMIT and cannot authorize recovery.
    fn tr_extended_kind(
        batch: &[u8],
        unnamed: Option<&[u8]>,
        max_bindings: usize,
        policy: &TrReadPolicy,
    ) -> StmtKind {
        Self::tr_extended_cycle_kind(&[batch], unnamed, max_bindings, policy)
    }

    fn tr_extended_cycle_kind(
        batches: &[&[u8]],
        unnamed: Option<&[u8]>,
        max_bindings: usize,
        policy: &TrReadPolicy,
    ) -> StmtKind {
        let statement_kind = |sql: &str| -> StmtKind {
            let kind = ProxyServer::tr_classify(sql, policy);
            // ROLLBACK can expose subsequent Executes to autocommit. Reads and
            // writes need no second lexical pass; only possible controls do.
            if matches!(kind, StmtKind::Control | StmtKind::Other) {
                if let Ok(boundaries) = crate::replay_sql::boundaries(sql) {
                    if ProxyServer::starts_with_word_ci(boundaries.head, "ROLLBACK")
                        || ProxyServer::starts_with_word_ci(boundaries.head, "ABORT")
                    {
                        return StmtKind::Commit;
                    }
                }
            }
            kind
        };
        fn cstring<'a>(body: &mut &'a [u8]) -> Option<&'a [u8]> {
            let end = memchr::memchr(0, body)?;
            let value = &body[..end];
            *body = &body[end + 1..];
            Some(value)
        }
        let mut statements = HashMap::<&[u8], StmtKind>::new();
        let mut portals = HashMap::<&[u8], StmtKind>::new();
        // The common unnamed Parse/Bind/Execute shape needs no map allocation.
        let mut unnamed_statement = None;
        let mut unnamed_portal = None;
        if let Some(parse) = unnamed {
            let Some(sql) = Self::parse_msg_sql(parse) else {
                return StmtKind::Commit;
            };
            unnamed_statement = Some(statement_kind(sql));
        }
        let mut kind = StmtKind::Control;
        for batch in batches {
            let mut rest = *batch;
            while !rest.is_empty() {
                let Some(header) = rest.get(..5) else {
                    return StmtKind::Commit;
                };
                let len = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
                if len < 4 {
                    return StmtKind::Commit;
                }
                let Some(frame_len) = len.checked_add(1) else {
                    return StmtKind::Commit;
                };
                let Some(mut body) = rest.get(5..frame_len) else {
                    return StmtKind::Commit;
                };
                match header[0] {
                    b'P' => {
                        let Some(name) = cstring(&mut body) else {
                            return StmtKind::Commit;
                        };
                        let Some(sql) =
                            cstring(&mut body).and_then(|b| std::str::from_utf8(b).ok())
                        else {
                            return StmtKind::Commit;
                        };
                        if name.is_empty() {
                            unnamed_statement = Some(statement_kind(sql));
                        } else {
                            if statements.len() >= max_bindings && !statements.contains_key(name) {
                                return StmtKind::Commit;
                            }
                            statements.insert(name, statement_kind(sql));
                        }
                    }
                    b'B' => {
                        let Some(portal) = cstring(&mut body) else {
                            return StmtKind::Commit;
                        };
                        let Some(name) = cstring(&mut body) else {
                            return StmtKind::Commit;
                        };
                        let bound_kind = if name.is_empty() {
                            unnamed_statement
                        } else {
                            statements.get(name).copied()
                        }
                        .unwrap_or(StmtKind::Commit);
                        if portal.is_empty() {
                            unnamed_portal = Some(bound_kind);
                        } else {
                            if portals.len() >= max_bindings && !portals.contains_key(portal) {
                                return StmtKind::Commit;
                            }
                            portals.insert(portal, bound_kind);
                        }
                    }
                    b'E' => {
                        let Some(portal) = cstring(&mut body) else {
                            return StmtKind::Commit;
                        };
                        let k = if portal.is_empty() {
                            unnamed_portal
                        } else {
                            portals.get(portal).copied()
                        }
                        .unwrap_or(StmtKind::Commit);
                        kind = match (kind, k) {
                            (StmtKind::Commit, _) | (_, StmtKind::Commit) => {
                                return StmtKind::Commit
                            }
                            (StmtKind::Write | StmtKind::Other, _)
                            | (_, StmtKind::Write | StmtKind::Other) => StmtKind::Write,
                            (StmtKind::Read, _) | (_, StmtKind::Read) => StmtKind::Read,
                            _ => StmtKind::Control,
                        };
                    }
                    b'C' => {
                        let Some((&target, mut name)) = body.split_first() else {
                            return StmtKind::Commit;
                        };
                        let Some(name) = cstring(&mut name) else {
                            return StmtKind::Commit;
                        };
                        match target {
                            b'S' if name.is_empty() => unnamed_statement = None,
                            b'P' if name.is_empty() => unnamed_portal = None,
                            b'S' => {
                                statements.remove(name);
                            }
                            b'P' => {
                                portals.remove(name);
                            }
                            _ => return StmtKind::Commit,
                        }
                    }
                    b'D' | b'H' | b'S' => {}
                    _ => return StmtKind::Commit,
                }
                rest = &rest[frame_len..];
            }
        }
        kind
    }

    /// Representative SQL for logging/bookkeeping only. Never use routing SQL
    /// as a recovery safety decision; a batch can execute several statements.
    fn tr_extended_sql<'a>(
        route_sql: Option<&'a str>,
        refs: &[String],
        registry: &'a HashMap<String, bytes::Bytes>,
    ) -> Option<&'a str> {
        route_sql.or_else(|| {
            refs.iter()
                .find_map(|n| registry.get(n).and_then(|b| Self::parse_msg_sql(b)))
        })
    }

    /// Append one tracked session `SET`, honouring the cap.
    fn tr_push_guc(tr: &mut TrSession, sql: &str, state: &Arc<ServerState>) {
        if tr.guc_cap_hit {
            return;
        }
        if tr.gucs.len() >= state.limits.tr_max_session_set_statements {
            tr.guc_cap_hit = true;
            state
                .metrics
                .tr
                .session_set_cap_exceeded
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                limit = state.limits.tr_max_session_set_statements,
                "in-session TR: session SET tracking cap reached; failover restore will be incomplete"
            );
            return;
        }
        tr.gucs.push(sql.to_string());
    }

    /// Per-response in-session TR bookkeeping, run after every fully relayed
    /// simple-query or extended-protocol response. `sql` is the client's
    /// statement text (extended: the batch's resolved SQL); `entry` builds
    /// the replay record and is only invoked while inside an explicit
    /// transaction in `select`/`transaction` mode. `extended_kind` supplies
    /// the whole cycle's safety classification; None denotes simple protocol
    /// and enables GUC tracking (extended SETs are not tracked).
    ///
    /// Hot-path cost outside a transaction: a few prefix compares, no
    /// allocation, no lock.
    async fn tr_after_response(
        tr: &mut TrSession,
        sql: Option<&str>,
        extended_kind: Option<StmtKind>,
        entry: impl FnOnce() -> StatementLog,
        entry_bytes: usize,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) {
        if !tr.record_tx && !tr.track_gucs {
            return;
        }
        // A COPY that yielded for client data has no ReadyForQuery yet; the
        // cycle is finished (and the status refreshed) by the CopyDone drain.
        if session
            .copy_in_progress
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            if tr.record_tx {
                let mut ts = session.tx_state.write().await;
                if tr.prev_status != b'I' || ts.in_transaction {
                    // A COPY inside the transaction: its data is not recorded.
                    ts.non_replayable = true;
                    ts.statements = Vec::new();
                    ts.replay_bytes = 0;
                }
            }
            return;
        }
        let status = session
            .last_rfq_status
            .load(std::sync::atomic::Ordering::Relaxed);
        let had_error = session
            .last_response_error
            .load(std::sync::atomic::Ordering::Relaxed);
        let prev_status = tr.prev_status;
        tr.prev_status = status;
        let now_in_tx = status != b'I';
        let was_in_tx = prev_status != b'I';
        let sql = sql.unwrap_or("");
        // Classify lazily: only needed inside a transaction or for a SET.
        let simple = extended_kind.is_none();
        let mut kind_cache = extended_kind;
        let kind = |kind_cache: &mut Option<StmtKind>| -> StmtKind {
            *kind_cache.get_or_insert_with(|| Self::tr_classify(sql, &state.tr_read_policy))
        };

        // --- session GUC tracking (simple protocol only) ---
        if tr.track_gucs && simple {
            let is_set_like = crate::protocol::starts_with_ci(sql.trim_start(), "SET")
                || crate::protocol::starts_with_ci(sql.trim_start(), "RESET")
                || crate::protocol::starts_with_ci(sql.trim_start(), "DISCARD");
            if is_set_like && !had_error {
                if Self::tr_resets_all(sql) {
                    tr.gucs.clear();
                    tr.pending_tx_gucs.clear();
                    tr.guc_cap_hit = false;
                } else if Self::tr_is_session_set(sql) {
                    if now_in_tx {
                        tr.pending_tx_gucs.push(sql.to_string());
                    } else {
                        Self::tr_push_guc(tr, sql, state);
                    }
                }
            }
            if was_in_tx && !now_in_tx {
                // Transaction ended: SETs made inside it persist only if it
                // committed (a failed transaction's COMMIT is a ROLLBACK).
                // `StmtKind::Commit` is the replay-safety classification: it is
                // deliberately wide and also covers `ROLLBACK; <DML>`, where the
                // trailing DML commits in autocommit but the transaction's own
                // session state was DISCARDED. Promoting that transaction's SETs
                // would restore settings the database never kept, so the durable
                // commit is confirmed against the statement itself.
                let committed = prev_status == b'T'
                    && !had_error
                    && kind(&mut kind_cache) == StmtKind::Commit
                    && Self::tr_commits_session_state(sql);
                if committed {
                    let pending = std::mem::take(&mut tr.pending_tx_gucs);
                    for s in pending {
                        Self::tr_push_guc(tr, &s, state);
                    }
                } else {
                    tr.pending_tx_gucs.clear();
                }
            }
        }

        // --- explicit-transaction statement recording ---
        if !now_in_tx {
            session.tr_replay_tainted.store(false, Ordering::Relaxed);
        }
        if !tr.record_tx || (!now_in_tx && !was_in_tx) {
            return;
        }
        let mut ts = session.tx_state.write().await;
        if !now_in_tx {
            // Transaction over (COMMIT/ROLLBACK/implicit): release the record.
            *ts = TransactionState::default();
            return;
        }
        if !was_in_tx {
            // Idle -> InTx: this statement opened the transaction.
            *ts = TransactionState::default();
            ts.in_transaction = true;
            ts.tx_id = Some(Uuid::new_v4());
            ts.read_only = true;
        }
        let k = kind(&mut kind_cache);
        if !matches!(k, StmtKind::Read | StmtKind::Control) {
            ts.has_writes = true;
            ts.read_only = false;
        }
        let tainted = session
            .tr_replay_tainted
            .swap(false, std::sync::atomic::Ordering::Relaxed);
        if status == b'E' || tainted || k == StmtKind::Commit {
            // A failed transaction can only be rolled back; a transformed
            // statement's recorded text is not what executed.
            ts.non_replayable = true;
        }
        if ts.non_replayable {
            if !ts.statements.is_empty() {
                ts.statements = Vec::new();
                ts.replay_bytes = 0;
            }
            return;
        }
        if ts.statements.len() >= state.limits.tr_max_replay_statements
            || ts.replay_bytes.saturating_add(entry_bytes) > state.limits.tr_max_replay_bytes
        {
            ts.non_replayable = true;
            ts.statements = Vec::new();
            ts.replay_bytes = 0;
            state
                .metrics
                .tr
                .replay_cap_exceeded
                .fetch_add(1, Ordering::Relaxed);
            tracing::debug!(
                target: "helios::tr",
                "transaction exceeded [limits] tr_max_replay_statements/bytes; marked non-replayable"
            );
            return;
        }
        ts.statements.push(entry());
        ts.replay_bytes += entry_bytes;
    }

    /// Bookkeeping after a simple-query response.
    async fn tr_after_simple(
        tr: &mut TrSession,
        msg: &Message,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) {
        let incomplete_cycle = tr.ext_cycle.take().is_some();
        if std::mem::take(&mut tr.ext_cycle_dropped) || incomplete_cycle {
            // A simple Query can follow a Flush without Sync. The current
            // entry cannot represent that earlier extended prefix, so never
            // replay a transaction from this incomplete history.
            session.tr_replay_tainted.store(true, Ordering::Relaxed);
        }
        let sql = crate::protocol::query_text(&msg.payload);
        let bytes = sql.map(|s| s.len()).unwrap_or(0);
        Self::tr_after_response(
            tr,
            sql,
            None,
            || StatementLog {
                sql: sql.unwrap_or("").to_string(),
                params: Vec::new(),
                result_checksum: None,
                executed_at: chrono::Utc::now(),
                extended: None,
            },
            bytes,
            session,
            state,
        )
        .await;
    }

    /// Bookkeeping after an extended-protocol batch. A Flush-terminated batch
    /// is accumulated into the open cycle; the Sync-terminated batch closes
    /// the cycle and records it as one replay entry.
    #[allow(clippy::too_many_arguments)]
    async fn tr_after_extended(
        tr: &mut TrSession,
        batch: &bytes::Bytes,
        unnamed: Option<&(bytes::Bytes, bytes::Bytes)>,
        route_sql: Option<&str>,
        wait_ready: bool,
        defines: &[String],
        refs: &[String],
        registry: &HashMap<String, bytes::Bytes>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
    ) {
        if !tr.record_tx {
            return;
        }
        // A Flush may open BEGIN while the last RFQ still says Idle. Retain
        // that bounded prefix until Sync establishes the transaction state.
        let in_tx_context = tr.prev_status != b'I'
            || session
                .in_transaction
                .load(std::sync::atomic::Ordering::Relaxed);
        if !in_tx_context && wait_ready {
            tr.ext_cycle = None;
            if std::mem::take(&mut tr.ext_cycle_dropped) {
                *session.tx_state.write().await = TransactionState::default();
            }
            return;
        }
        if !wait_ready {
            // A client can issue arbitrarily many Flushes without a Sync.
            // Apply the replay budget before retaining each chunk, rather
            // than only when the completed cycle becomes a history entry.
            let mut ts = session.tx_state.write().await;
            let retained = tr.ext_cycle.as_ref().map_or(0, |c| {
                c.frames
                    .len()
                    .saturating_add(c.unnamed_parse.as_ref().map_or(0, |p| p.len()))
            });
            let projected = ts
                .replay_bytes
                .saturating_add(retained)
                .saturating_add(batch.len())
                .saturating_add(unnamed.map_or(0, |(p, _)| p.len()));
            if !ts.non_replayable
                && (projected > state.limits.tr_max_replay_bytes
                    || ts.statements.len() >= state.limits.tr_max_replay_statements)
            {
                ts.non_replayable = true;
                ts.statements = Vec::new();
                ts.replay_bytes = 0;
                state
                    .metrics
                    .tr
                    .replay_cap_exceeded
                    .fetch_add(1, Ordering::Relaxed);
            }
            if ts.non_replayable {
                tr.ext_cycle = None;
                tr.ext_cycle_dropped = true;
                return;
            }
            drop(ts);
            let first_chunk = tr.ext_cycle.is_none();
            let cycle = tr.ext_cycle.get_or_insert_with(|| TrExtCycle {
                frames: BytesMut::new(),
                unnamed_parse: None,
                defines: Vec::new(),
                refs: Vec::new(),
                route_sql: None,
            });
            if let Some((p, _)) = unnamed {
                if first_chunk {
                    cycle.unnamed_parse = Some(p.clone());
                } else {
                    // A later optimized Parse replaces the unnamed statement
                    // at this point, after earlier Binds/Executes and Flushes.
                    cycle.frames.extend_from_slice(p);
                }
            }
            cycle.frames.extend_from_slice(batch);
            cycle.defines.extend(defines.iter().cloned());
            cycle.refs.extend(refs.iter().cloned());
            if cycle.route_sql.is_none() {
                cycle.route_sql = route_sql.map(|s| s.to_string());
            }
            return;
        }
        let cycle = tr.ext_cycle.take();
        let dropped = std::mem::take(&mut tr.ext_cycle_dropped);
        let cycle_route: Option<String> = cycle.as_ref().and_then(|c| c.route_sql.clone());
        let resolved = Self::tr_extended_sql(route_sql.or(cycle_route.as_deref()), refs, registry);
        // The first held Parse remains a prefix; every later held Parse is
        // retained at its actual position between chunks.
        let unnamed_len = cycle
            .as_ref()
            .and_then(|c| c.unnamed_parse.as_ref().map(|p| p.len()))
            .unwrap_or(0)
            .saturating_add(unnamed.map_or(0, |(p, _)| p.len()));
        let entry_bytes = batch
            .len()
            .saturating_add(cycle.as_ref().map_or(0, |c| c.frames.len()))
            .saturating_add(unnamed_len);
        Self::tr_after_response(
            tr,
            resolved,
            Some(match &cycle {
                _ if dropped => StmtKind::Commit,
                Some(c) => Self::tr_extended_cycle_kind(
                    &[
                        &c.frames,
                        unnamed.map_or(&[][..], |(p, _)| p.as_ref()),
                        batch,
                    ],
                    c.unnamed_parse.as_deref(),
                    state.limits.max_prepared_statements,
                    &state.tr_read_policy,
                ),
                None => Self::tr_extended_kind(
                    batch,
                    unnamed.map(|(p, _)| p.as_ref()),
                    state.limits.max_prepared_statements,
                    &state.tr_read_policy,
                ),
            }),
            || {
                let (frames, unnamed_parse, mut all_defines, mut all_refs) = match cycle {
                    Some(mut c) => {
                        if let Some((p, _)) = unnamed {
                            c.frames.extend_from_slice(p);
                        }
                        c.frames.extend_from_slice(batch);
                        (c.frames.freeze(), c.unnamed_parse, c.defines, c.refs)
                    }
                    None => (
                        batch.clone(),
                        unnamed.map(|(p, _)| p.clone()),
                        Vec::new(),
                        Vec::new(),
                    ),
                };
                all_defines.extend(defines.iter().cloned());
                all_refs.extend(refs.iter().cloned());
                StatementLog {
                    sql: resolved.unwrap_or("").to_string(),
                    params: Vec::new(),
                    result_checksum: None,
                    executed_at: chrono::Utc::now(),
                    extended: Some(ExtendedBatchLog {
                        frames,
                        unnamed_parse,
                        defines: all_defines,
                        refs: all_refs,
                    }),
                }
            },
            entry_bytes,
            session,
            state,
        )
        .await;
    }

    /// Finish the bookkeeping for a COPY cycle once its CopyDone/CopyFail
    /// drained to `ReadyForQuery`: refresh the status baseline and, if the
    /// COPY ran inside an explicit transaction, mark it non-replayable (its
    /// data stream is not recorded).
    async fn tr_after_copy_drain(tr: &mut TrSession, session: &Arc<ClientSession>) {
        let status = session
            .last_rfq_status
            .load(std::sync::atomic::Ordering::Relaxed);
        tr.prev_status = status;
        if !tr.record_tx {
            return;
        }
        let mut ts = session.tx_state.write().await;
        if status == b'I' {
            *ts = TransactionState::default();
        } else {
            ts.in_transaction = true;
            ts.non_replayable = true;
            ts.has_writes = true;
            ts.read_only = false;
            ts.statements = Vec::new();
            ts.replay_bytes = 0;
        }
    }

    /// Build `CommandComplete(tag)`.
    fn create_command_complete(tag: &str) -> Vec<u8> {
        crate::protocol::CommandComplete {
            tag: tag.to_string(),
        }
        .encode()
        .encode()
        .to_vec()
    }

    /// Write `ErrorResponse(code, message)` (+ `ReadyForQuery` when
    /// `with_ready`, status `E` inside a transaction else `I`) to the client.
    /// Returns bytes written.
    async fn tr_send_error(
        client: &mut ClientStream,
        code: &str,
        message: &str,
        in_tx: bool,
        with_ready: bool,
    ) -> Result<u64> {
        let mut resp = Self::create_error_response(code, message);
        if with_ready {
            resp.extend_from_slice(&Self::create_ready_for_query(if in_tx {
                b'E'
            } else {
                b'I'
            }));
        }
        client
            .write_all(&resp)
            .await
            .map_err(|e| ProxyError::Network(format!("Client write error: {}", e)))?;
        Ok(resp.len() as u64)
    }

    /// Read and discard backend frames up to and including one `ReadyForQuery`.
    /// Returns `(status byte, saw ErrorResponse)`. Every read is bounded by
    /// `read_timeout`. A COPY-in request from the backend cannot be satisfied
    /// here and is an error.
    async fn drain_until_ready<S: AsyncReadExt + Unpin>(
        backend: &mut S,
        read_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<(u8, bool)> {
        let mut buf = BytesMut::with_capacity(4096);
        let mut had_error = false;
        loop {
            let mut consumed = 0usize;
            let mut ready: Option<u8> = None;
            loop {
                let rem = &buf[consumed..];
                let Some(len) = backend_frame_len(rem, max_frame_bytes)? else {
                    break;
                };
                if rem.len() < len + 1 {
                    break;
                }
                let mtype = rem[0];
                let frame_total = len + 1;
                consumed += frame_total;
                match mtype {
                    b'E' => had_error = true,
                    b'G' | b'W' => {
                        return Err(ProxyError::Protocol(
                            "backend requested COPY data during replay".to_string(),
                        ))
                    }
                    b'Z' => {
                        ready = Some(if frame_total >= 6 { rem[5] } else { b'I' });
                        break;
                    }
                    _ => {}
                }
            }
            let _ = buf.split_to(consumed);
            if let Some(status) = ready {
                return Ok((status, had_error));
            }
            buf.reserve(4096);
            let n = tokio::time::timeout(read_timeout, backend.read_buf(&mut buf))
                .await
                .map_err(|_| ProxyError::Network("replay drain read timeout".to_string()))?
                .map_err(|e| ProxyError::Network(format!("replay drain read error: {}", e)))?;
            if n == 0 {
                return Err(ProxyError::Connection(
                    "backend closed during replay".to_string(),
                ));
            }
        }
    }

    /// Run one simple-query statement on a backend socket and discard its
    /// response. `Err(Protocol)` if the backend rejected it; `Err(Network|
    /// Connection)` on a socket failure.
    async fn tr_run_discard<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        backend: &mut S,
        sql: &str,
        write_timeout: Duration,
        read_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<u8> {
        let msg = crate::protocol::QueryMessage {
            query: sql.to_string(),
        }
        .encode()
        .encode();
        tokio::time::timeout(write_timeout, backend.write_all(&msg))
            .await
            .map_err(|_| ProxyError::Network("replay write timeout".to_string()))?
            .map_err(|e| ProxyError::Network(format!("replay write error: {}", e)))?;
        let (status, had_error) =
            Self::drain_until_ready(backend, read_timeout, max_frame_bytes).await?;
        if had_error {
            return Err(ProxyError::Protocol(format!(
                "backend rejected replayed statement: {}",
                Self::tr_short_sql(sql)
            )));
        }
        Ok(status)
    }

    /// Statement text abbreviated for log/error messages.
    fn tr_short_sql(sql: &str) -> String {
        let t = sql.trim();
        if t.len() <= 80 {
            t.to_string()
        } else {
            let mut end = 80;
            while !t.is_char_boundary(end) {
                end -= 1;
            }
            format!("{}...", &t[..end])
        }
    }

    /// Restore session-level state on a freshly dialed replacement
    /// connection: replay the tracked `SET`/`RESET` statements in order.
    /// Returns how many were replayed. `Err(Protocol)` if the backend rejected
    /// one (the session cannot be faithfully restored); `Err(Network|Connection)`
    /// on a socket failure.
    async fn tr_restore_session_state<S: AsyncReadExt + AsyncWriteExt + Unpin>(
        backend: &mut S,
        gucs: &[String],
        write_timeout: Duration,
        read_timeout: Duration,
        max_frame_bytes: usize,
    ) -> Result<usize> {
        for sql in gucs {
            Self::tr_run_discard(backend, sql, write_timeout, read_timeout, max_frame_bytes)
                .await?;
        }
        Ok(gucs.len())
    }

    /// Re-home the session: wait for a healthy primary (bounded by
    /// `write_timeout_secs`), dial it (startup params re-sent by
    /// `ensure_conn`), restore the tracked session GUCs, and make it the
    /// session's current node. A node that fails to connect or whose socket
    /// dies during restore is demoted and the wait continues until the
    /// deadline; a backend that *rejects* a restore statement aborts at once.
    async fn tr_acquire_replacement(
        conns: &mut HashMap<String, BackendConn>,
        tr: &TrSession,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<String> {
        let deadline = std::time::Instant::now() + config.write_timeout();
        loop {
            let node = Self::select_primary_with_timeout(session, state, config).await?;
            let err = match Self::ensure_conn(conns, &node, session, config, state).await {
                Ok(()) => {
                    let bc = conns.get_mut(&node).expect("just ensured");
                    match Self::tr_restore_session_state(
                        &mut bc.stream,
                        &tr.gucs,
                        state.limits.backend_write_timeout,
                        state.limits.backend_read_timeout,
                        state.limits.max_backend_frame_bytes,
                    )
                    .await
                    {
                        Ok(n) => {
                            #[cfg(feature = "pool-modes")]
                            if n > 0 {
                                bc.dirty = true;
                            }
                            *session.current_node.write().await = Some(node.clone());
                            state.metrics.tr.failovers.fetch_add(1, Ordering::Relaxed);
                            tracing::info!(
                                target: "helios::tr",
                                node = %node,
                                restored_sets = n,
                                incomplete = tr.guc_cap_hit,
                                "in-session failover: session re-homed"
                            );
                            return Ok(node);
                        }
                        Err(e @ ProxyError::Protocol(_)) => {
                            return Err(e);
                        }
                        Err(e) => {
                            conns.remove(&node);
                            e
                        }
                    }
                }
                // A backend that challenges for a credential the proxy does
                // not hold (pass-through mode) or rejects it is healthy — do
                // not demote it, and do not wait: fail the recovery now.
                Err(e @ ProxyError::Auth(_)) => return Err(e),
                Err(e) => e,
            };
            Self::record_backend_failure(state, &node, &err.to_string());
            if std::time::Instant::now() >= deadline {
                return Err(err);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Replay a recorded explicit transaction on `node`'s connection,
    /// discarding every response. Named statements a cycle references are
    /// re-prepared from the registry unless an earlier replayed cycle (or the
    /// connection) already holds them. Leaves the connection inside the
    /// replayed (uncommitted) transaction on success.
    async fn tr_replay_transaction(
        conns: &mut HashMap<String, BackendConn>,
        node: &str,
        entries: &[StatementLog],
        registry: &HashMap<String, bytes::Bytes>,
        state: &Arc<ServerState>,
    ) -> std::result::Result<(), ReplayFailure> {
        let bc = conns.get_mut(node).ok_or_else(|| {
            ReplayFailure::Backend(ProxyError::Connection("no connection".into()))
        })?;
        let wt = state.limits.backend_write_timeout;
        let rt = state.limits.backend_read_timeout;
        let mf = state.limits.max_backend_frame_bytes;
        let total = entries.len();
        for (i, st) in entries.iter().enumerate() {
            let status = match &st.extended {
                None => Self::tr_run_discard(&mut bc.stream, &st.sql, wt, rt, mf)
                    .await
                    .map_err(|e| match e {
                        ProxyError::Protocol(_) => ReplayFailure::Statement(format!(
                            "statement {}/{} rejected: {}",
                            i + 1,
                            total,
                            Self::tr_short_sql(&st.sql)
                        )),
                        other => ReplayFailure::Backend(other),
                    })?,
                Some(ext) => {
                    for name in &ext.refs {
                        if bc.prepared.contains(name) || ext.defines.contains(name) {
                            continue;
                        }
                        let Some(parse_bytes) = registry.get(name) else {
                            continue;
                        };
                        Self::reprepare_statement(
                            &mut bc.stream,
                            parse_bytes,
                            state.limits.reprepare_timeout,
                            state.limits.max_backend_frame_bytes,
                        )
                        .await
                        .map_err(|e| match e {
                            ProxyError::Protocol(_) => ReplayFailure::Statement(format!(
                                "re-prepare of statement '{}' rejected",
                                name
                            )),
                            other => ReplayFailure::Backend(other),
                        })?;
                        bc.prepared.insert(name.clone());
                    }
                    let mut wire = Vec::with_capacity(
                        ext.frames.len() + ext.unnamed_parse.as_ref().map(|p| p.len()).unwrap_or(0),
                    );
                    if let Some(p) = &ext.unnamed_parse {
                        wire.extend_from_slice(p);
                    }
                    wire.extend_from_slice(&ext.frames);
                    tokio::time::timeout(wt, bc.stream.write_all(&wire))
                        .await
                        .map_err(|_| {
                            ReplayFailure::Backend(ProxyError::Network(
                                "replay write timeout".to_string(),
                            ))
                        })?
                        .map_err(|e| {
                            ReplayFailure::Backend(ProxyError::Network(format!(
                                "replay write error: {}",
                                e
                            )))
                        })?;
                    let (status, had_error) = Self::drain_until_ready(&mut bc.stream, rt, mf)
                        .await
                        .map_err(ReplayFailure::Backend)?;
                    if had_error {
                        return Err(ReplayFailure::Statement(format!(
                            "extended batch {}/{} rejected: {}",
                            i + 1,
                            total,
                            Self::tr_short_sql(&st.sql)
                        )));
                    }
                    for d in &ext.defines {
                        bc.prepared.insert(d.clone());
                    }
                    // The cycle may have (re)defined the unnamed statement.
                    bc.unnamed_sig = None;
                    status
                }
            };
            if status == b'E' {
                return Err(ReplayFailure::Statement(format!(
                    "statement {}/{} left the transaction in the failed state",
                    i + 1,
                    total
                )));
            }
        }
        Ok(())
    }

    /// Drop the session's recorded transaction (it died with the backend).
    async fn tr_clear_tx(session: &Arc<ClientSession>) {
        *session.tx_state.write().await = TransactionState::default();
    }

    /// Routing found no healthy node within `write_timeout_secs`: tell the
    /// client (08006) instead of dropping the socket. The caller closes.
    async fn send_no_healthy_nodes(
        client: &mut ClientStream,
        session: &Arc<ClientSession>,
        with_ready: bool,
    ) {
        let in_tx = session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed);
        let _ = Self::tr_send_error(
            client,
            "08006",
            "no healthy backend node available within write_timeout; connection closed",
            in_tx,
            with_ready,
        )
        .await;
    }

    /// Execute the in-session TR recovery for a backend fault. Returns
    /// `Ok(Some(forward_result))` when the session continues (the request was
    /// re-executed or answered with an error), `Ok(None)` when the client
    /// connection must be closed, `Err` only when the CLIENT socket failed.
    #[allow(clippy::too_many_arguments)]
    async fn tr_handle_fault(
        client: &mut ClientStream,
        conns: &mut HashMap<String, BackendConn>,
        current_node: &mut Option<String>,
        mut fault: BackendFault,
        inflight: InFlight<'_>,
        tr: &mut TrSession,
        registry: &HashMap<String, bytes::Bytes>,
        session: &Arc<ClientSession>,
        state: &Arc<ServerState>,
        config: &ProxyConfig,
    ) -> Result<Option<(Option<String>, u64)>> {
        let mode = session.tr_mode;
        let in_tx = session
            .in_transaction
            .load(std::sync::atomic::Ordering::Relaxed);
        let copying = session
            .copy_in_progress
            .load(std::sync::atomic::Ordering::Relaxed);
        let (kind, wait_ready) = match &inflight {
            InFlight::Simple(msg) => (
                crate::protocol::query_text(&msg.payload)
                    .map(|sql| Self::tr_classify(sql, &state.tr_read_policy))
                    .unwrap_or(StmtKind::Commit),
                true,
            ),
            InFlight::Extended {
                batch,
                wait_ready,
                unnamed,
                ..
            } => (
                Self::tr_extended_kind(
                    batch,
                    unnamed.map(|(p, _)| p.as_ref()),
                    state.limits.max_prepared_statements,
                    &state.tr_read_policy,
                ),
                *wait_ready,
            ),
        };
        fault.kind = Some(kind);
        let (tx_has_writes, tx_replayable) = {
            let ts = session.tx_state.read().await;
            (
                ts.has_writes,
                !ts.non_replayable && !ts.statements.is_empty(),
            )
        };
        let prior_flush = tr.ext_dispatched;
        if prior_flush {
            // The current chunk may be unsent, but earlier Executes in this
            // cycle may already have run (or even committed).
            fault.phase = FaultPhase::OutcomeUnknown;
        }
        let mut action = if copying {
            // The COPY data stream is unrecoverable in every mode.
            TrAction::CloseWithError("08006")
        } else {
            Self::tr_decide(mode, fault.phase, in_tx, tx_has_writes, tx_replayable, kind)
        };
        // A backend read timeout does not establish that execution stopped.
        if !Self::is_backend_fault(&fault.error)
            && matches!(action, TrAction::Reexecute | TrAction::ReplayThenReexecute)
        {
            action = TrAction::ErrorAndContinue(match fault.phase {
                FaultPhase::NotDelivered => "57P01",
                FaultPhase::OutcomeUnknown => "08007",
            });
        }
        action = Self::tr_response_action(action, fault.progress, prior_flush);
        state
            .metrics
            .bytes_sent
            .fetch_add(fault.progress.bytes, Ordering::Relaxed);
        tr.ext_cycle = None;
        tr.ext_cycle_dropped = false;
        tr.ext_dispatched = false;
        tracing::warn!(
            target: "helios::tr",
            node = %fault.node,
            error = %fault.error,
            phase = ?fault.phase,
            mode = ?mode,
            in_tx,
            kind = ?kind,
            action = ?action,
            "backend fault on a live session"
        );
        let phase_desc = match fault.phase {
            FaultPhase::NotDelivered => "before the statement was delivered",
            FaultPhase::OutcomeUnknown => "while the statement was in flight (outcome unknown)",
        };

        match action {
            TrAction::CloseIncompleteResponse => {
                Self::tr_clear_tx(session).await;
                Ok(None)
            }
            TrAction::CloseWithError(code) => {
                let message = if copying {
                    format!(
                        "backend {} failed during COPY ({}); connection closed",
                        fault.node, fault.error
                    )
                } else {
                    format!(
                        "backend {} failed {} ({}); closing connection (tr_mode = {:?})",
                        fault.node, phase_desc, fault.error, mode
                    )
                };
                let _ = Self::tr_send_error(client, code, &message, in_tx, wait_ready).await;
                Self::tr_clear_tx(session).await;
                Ok(None)
            }
            TrAction::ErrorAndContinue(code) => {
                let node =
                    match Self::tr_acquire_replacement(conns, tr, session, state, config).await {
                        Ok(n) => n,
                        Err(e) => {
                            return Self::tr_fail_no_replacement(
                                client, &fault, &e, in_tx, wait_ready, session,
                            )
                            .await;
                        }
                    };
                let message = format!(
                    "backend {} failed {} ({}){}",
                    fault.node,
                    phase_desc,
                    fault.error,
                    if code == "08007" {
                        "; outcome unknown — verify the database outcome before retrying"
                    } else if in_tx {
                        "; the transaction was aborted — ROLLBACK and retry"
                    } else {
                        ""
                    }
                );
                if code == "08007" {
                    state
                        .metrics
                        .tr
                        .unknown_outcome_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                let sent = Self::tr_send_error(client, code, &message, in_tx, wait_ready).await?;
                Self::tr_enter_aborted(tr, in_tx, session).await;
                *current_node = Some(node.clone());
                Ok(Some((Some(node), sent)))
            }
            TrAction::Reexecute | TrAction::ReplayThenReexecute => {
                let node =
                    match Self::tr_acquire_replacement(conns, tr, session, state, config).await {
                        Ok(n) => n,
                        Err(e) => {
                            return Self::tr_fail_no_replacement(
                                client, &fault, &e, in_tx, wait_ready, session,
                            )
                            .await;
                        }
                    };
                if action == TrAction::ReplayThenReexecute {
                    let entries = session.tx_state.read().await.statements.clone();
                    match Self::tr_replay_transaction(conns, &node, &entries, registry, state).await
                    {
                        Ok(()) => {
                            state
                                .metrics
                                .tr
                                .transactions_replayed
                                .fetch_add(1, Ordering::Relaxed);
                            tracing::info!(
                                target: "helios::tr",
                                node = %node,
                                statements = entries.len(),
                                "transaction replayed on new backend"
                            );
                        }
                        Err(failure) => {
                            state
                                .metrics
                                .tr
                                .replay_failures
                                .fetch_add(1, Ordering::Relaxed);
                            let detail = match failure {
                                ReplayFailure::Statement(d) => {
                                    // Leave the new backend clean.
                                    if let Some(bc) = conns.get_mut(&node) {
                                        let _ = Self::tr_run_discard(
                                            &mut bc.stream,
                                            "ROLLBACK",
                                            state.limits.backend_write_timeout,
                                            state.limits.backend_read_timeout,
                                            state.limits.max_backend_frame_bytes,
                                        )
                                        .await;
                                    }
                                    d
                                }
                                ReplayFailure::Backend(e) => {
                                    conns.remove(&node);
                                    Self::record_backend_failure(state, &node, &e.to_string());
                                    format!("replacement backend {} failed: {}", node, e)
                                }
                            };
                            let message =
                                format!("transaction replay failed after failover: {}", detail);
                            let sent =
                                Self::tr_send_error(client, "40001", &message, in_tx, wait_ready)
                                    .await?;
                            Self::tr_enter_aborted(tr, in_tx, session).await;
                            let cur = conns.contains_key(&node).then(|| node.clone());
                            *current_node = cur.clone();
                            return Ok(Some((cur, sent)));
                        }
                    }
                }
                state
                    .metrics
                    .tr
                    .statements_reexecuted
                    .fetch_add(1, Ordering::Relaxed);
                *current_node = Some(node.clone());
                let mut second: Option<BackendFault> = None;
                let r = match inflight {
                    InFlight::Simple(msg) => {
                        Self::forward_simple_query(
                            client,
                            msg,
                            conns,
                            Some(node.as_str()),
                            session,
                            state,
                            config,
                            &mut second,
                        )
                        .await
                    }
                    InFlight::Extended {
                        batch,
                        route_sql,
                        wait_ready,
                        reprepare,
                        defines,
                        unnamed,
                    } => {
                        Self::forward_extended_batch(
                            client,
                            batch,
                            route_sql,
                            wait_ready,
                            conns,
                            Some(node.as_str()),
                            registry,
                            reprepare,
                            defines,
                            unnamed,
                            session,
                            state,
                            config,
                            &mut second,
                        )
                        .await
                    }
                };
                match r {
                    Ok(v) => Ok(Some(v)),
                    Err(e) => {
                        let Some(f2) = second else {
                            // Client-side failure: nothing left to do.
                            return Err(e);
                        };
                        state
                            .metrics
                            .bytes_sent
                            .fetch_add(f2.progress.bytes, Ordering::Relaxed);
                        if Self::tr_response_action(
                            TrAction::ErrorAndContinue("08006"),
                            f2.progress,
                            false,
                        ) == TrAction::CloseIncompleteResponse
                        {
                            Self::tr_clear_tx(session).await;
                            return Ok(None);
                        }
                        // The replacement failed too: give this request up
                        // with one error rather than cascading recoveries.
                        tracing::warn!(
                            target: "helios::tr",
                            node = %f2.node,
                            error = %f2.error,
                            "replacement backend failed during re-execution"
                        );
                        let message = format!(
                            "backend {} failed during failover re-execution ({}); verify the database outcome before retrying",
                            f2.node, f2.error,
                        );
                        let code = if f2.phase == FaultPhase::OutcomeUnknown {
                            "08007"
                        } else {
                            "08006"
                        };
                        let sent =
                            Self::tr_send_error(client, code, &message, in_tx, wait_ready).await?;
                        Self::tr_enter_aborted(tr, in_tx, session).await;
                        *current_node = None;
                        Ok(Some((None, sent)))
                    }
                }
            }
        }
    }

    /// No healthy primary within `write_timeout_secs` (or the session state
    /// could not be restored): tell the client and close.
    async fn tr_fail_no_replacement(
        client: &mut ClientStream,
        fault: &BackendFault,
        err: &ProxyError,
        in_tx: bool,
        wait_ready: bool,
        session: &Arc<ClientSession>,
    ) -> Result<Option<(Option<String>, u64)>> {
        let message = match err {
            ProxyError::Protocol(_) => format!(
                "backend {} failed ({}); session state could not be restored on the new primary: {}",
                fault.node, fault.error, err
            ),
            ProxyError::Auth(_) => format!(
                "backend {} failed ({}); the proxy could not authenticate to the replacement primary: {}",
                fault.node, fault.error, err
            ),
            _ => format!(
                "backend {} failed ({}); no healthy primary became available within write_timeout: {}",
                fault.node, fault.error, err
            ),
        };
        // An autocommit INSERT whose response was lost may already be durable,
        // exactly like a lost COMMIT: the client must be told to verify rather
        // than shown a bare connection failure it would reasonably retry. Only
        // reads and pure control statements can claim nothing was decided.
        let uncertain = fault.phase == FaultPhase::OutcomeUnknown
            && !matches!(fault.kind, Some(StmtKind::Read) | Some(StmtKind::Control));
        let (code, message) = if uncertain {
            (
                "08007",
                format!("{message}; verify the database outcome before retrying"),
            )
        } else {
            ("08006", message)
        };
        let _ = Self::tr_send_error(client, code, &message, in_tx, wait_ready).await;
        Self::tr_clear_tx(session).await;
        Ok(None)
    }

    /// After an error was returned for the in-flight request: the recorded
    /// transaction is gone; if the client believes it is inside one, enter
    /// the aborted-transaction emulation until it ends it.
    async fn tr_enter_aborted(tr: &mut TrSession, in_tx: bool, session: &Arc<ClientSession>) {
        Self::tr_clear_tx(session).await;
        tr.pending_tx_gucs.clear();
        // The response the client just saw ended with ErrorResponse + RFQ.
        Self::note_ready_for_query(session, if in_tx { b'E' } else { b'I' }, true);
        if in_tx {
            tr.tx_aborted = true;
        }
    }
}

/// Metrics snapshot for external consumption
#[derive(Debug, Clone)]
pub struct ServerMetricsSnapshot {
    pub connections_accepted: u64,
    /// Connections refused at accept time by the `[limits]
    /// max_client_connections` cap. Disjoint from `connections_accepted`.
    pub connections_rejected: u64,
    pub connections_closed: u64,
    pub queries_processed: u64,
    pub bytes_received: u64,
    pub bytes_sent: u64,
    pub failovers: u64,
    /// Cacheable reads whose response outgrew
    /// `[cache] max_cacheable_response_bytes` and were therefore not cached.
    pub cache_capture_oversize: u64,
    /// In-session Transaction Replay (`tr_mode`) counters.
    pub tr: TrMetricsSnapshot,
}

/// In-session Transaction Replay counters (see `TrMode`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TrMetricsSnapshot {
    /// Sessions re-homed onto a replacement backend after a backend fault.
    pub failovers: u64,
    /// In-flight statements transparently re-executed on the new backend.
    pub statements_reexecuted: u64,
    /// Explicit transactions successfully replayed on the new backend.
    pub transactions_replayed: u64,
    /// Transaction replays that failed (client received SQLSTATE 40001).
    pub replay_failures: u64,
    /// SQLSTATE 08007 `transaction_resolution_unknown` errors returned.
    pub unknown_outcome_errors: u64,
    /// Transactions marked non-replayable by `[limits] tr_max_replay_*`.
    pub replay_cap_exceeded: u64,
    /// Sessions whose `SET` tracking hit `[limits] tr_max_session_set_statements`.
    pub session_set_cap_exceeded: u64,
}

/// Pool mode statistics snapshot (when pool-modes feature is enabled)
#[cfg(feature = "pool-modes")]
#[derive(Debug, Clone)]
pub struct PoolModeStatsSnapshot {
    /// Current pooling mode
    pub mode: String,
    /// Total connections across all pools
    pub total_connections: usize,
    /// Active (leased) connections
    pub active_leases: usize,
    /// Idle connections
    pub idle_connections: usize,
    /// Number of nodes in the pool
    pub node_count: usize,
    /// Total connection acquires
    pub acquires: u64,
    /// Total connection releases
    pub releases: u64,
    /// Failed acquire attempts
    pub acquire_failures: u64,
    /// Acquire timeouts
    pub acquire_timeouts: u64,
    /// Completed transactions (Transaction mode)
    pub transactions_completed: u64,
    /// Total statements executed
    pub statements_executed: u64,
    /// Average lease duration in milliseconds
    pub avg_lease_duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(feature = "wasm-plugins"))]
    use crate::protocol::QueryMessage;

    fn test_config() -> ProxyConfig {
        let mut config = ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(),
            ..Default::default()
        };
        config.add_node("127.0.0.1:5432", "primary").unwrap();
        config
    }

    // ---- single-pass statement facts ----
    mod stmt_facts {
        use super::super::stmt_fact_classifications;
        use super::{ProxyServer, StmtFacts};

        /// Statement table spanning every classifier branch the facts
        /// memoize: reads, CTEs, all DML verbs, DDL, COPY in both
        /// directions, session-state verbs, transaction control, PREPARE /
        /// EXECUTE / LISTEN, multi-statement strings, leading and hint
        /// comments, volatile and locking reads, `SELECT ... INTO`, and case
        /// / whitespace variants.
        const CASES: [&str; 48] = [
            "SELECT 1",
            "select v from t",
            "  \t\n SELECT v FROM t  ",
            "SELECT v FROM t;",
            "SELECT v FROM t ; ",
            "SELECT v FROM t WHERE into_total > 1",
            "SELECT * INTO tmp FROM t",
            "select now()",
            "SELECT random()",
            "select nextval('s')",
            "SELECT set_config('x','y',false)",
            "SELECT pg_advisory_lock(1)",
            "SELECT v FROM t FOR UPDATE",
            "SELECT v FROM t FOR SHARE",
            "WITH c AS (SELECT 1) SELECT * FROM c",
            "WITH c AS (SELECT 1) SELECT * INTO tmp FROM c",
            "VALUES (1),(2)",
            "TABLE t",
            "SHOW search_path",
            "EXPLAIN SELECT 1",
            "FETCH ALL FROM cur",
            "INSERT INTO t VALUES (1)",
            "insert into t (a) values (1)",
            "UPDATE t SET v = 1",
            "DELETE FROM t WHERE id = 1",
            "CREATE TABLE t (id int)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN c int",
            "TRUNCATE t",
            "GRANT SELECT ON t TO r",
            "REVOKE SELECT ON t FROM r",
            "VACUUM ANALYZE t",
            "REINDEX TABLE t",
            "CLUSTER t",
            "COPY t FROM STDIN",
            "COPY t TO STDOUT",
            "SET search_path TO tenant_b",
            "set TimeZone = 'UTC'",
            "SET TRANSACTION READ ONLY",
            "RESET ALL",
            "DISCARD ALL",
            "PREPARE p AS SELECT 1",
            "EXECUTE p(1)",
            "LISTEN chan",
            "BEGIN",
            "COMMIT",
            "ROLLBACK TO SAVEPOINT sp1;",
            "SELECT v FROM t; UPDATE t SET v = 1",
        ];

        /// Extra strings that are not plain statements: empty, whitespace,
        /// leading block/line comments and a routing-hint comment. These
        /// exercise the "leading comment masks the verb" branches.
        const ODD_CASES: [&str; 7] = [
            "",
            "   ",
            "/* leading */ SELECT 1",
            "-- leading\nSELECT 1",
            "/*helios:route=primary*/ SELECT 1",
            "/*helios:route=primary*/ UPDATE t SET v = 1",
            "BEGIN; UPDATE t SET v = 1; COMMIT",
        ];

        /// Every getter must agree with the legacy classifier it memoizes —
        /// for every statement shape. This is the contract that makes
        /// threading the facts through the forward path a pure optimisation
        /// rather than a behaviour change.
        #[test]
        fn facts_agree_with_legacy_classifiers() {
            assert!(CASES.len() + ODD_CASES.len() >= 40);
            for sql in CASES.iter().chain(ODD_CASES.iter()).copied() {
                let mut f = StmtFacts::new(sql);
                assert_eq!(
                    f.is_write(),
                    ProxyServer::is_write_query(sql),
                    "is_write mismatch for {sql:?}"
                );
                #[cfg(feature = "edge-proxy")]
                assert_eq!(
                    f.has_interior_semicolon(),
                    ProxyServer::stmt_has_interior_semicolon(sql),
                    "has_interior_semicolon mismatch for {sql:?}"
                );
                #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
                assert_eq!(
                    f.leaves_session_state(),
                    ProxyServer::stmt_leaves_session_state(sql),
                    "leaves_session_state mismatch for {sql:?}"
                );
                #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
                assert_eq!(
                    f.is_cacheable_read(),
                    ProxyServer::is_cacheable_read_sql(sql),
                    "is_cacheable_read mismatch for {sql:?}"
                );
            }
        }

        /// LAZINESS CONTRACT (the reason the memo is `Option`-celled rather
        /// than computed up front): building the facts — the one thing the
        /// forward path does for EVERY simple query — classifies nothing at
        /// all. In the stock configuration `skip_clean_reset`, the query cache
        /// and the edge proxy are all off, so no gate ever asks and the
        /// statement is never scanned by any of these classifiers.
        #[test]
        fn building_facts_classifies_nothing() {
            let before = stmt_fact_classifications();

            let facts = StmtFacts::new("SELECT a, b FROM t WHERE id = 1");
            let msg = crate::protocol::QueryMessage {
                query: "INSERT INTO t VALUES (1)".to_string(),
            }
            .encode();
            let from_msg = StmtFacts::of_query(&msg);
            // Reading the borrowed text is not a classification either.
            assert_eq!(facts.sql, "SELECT a, b FROM t WHERE id = 1");
            assert_eq!(from_msg.sql, "INSERT INTO t VALUES (1)");

            assert_eq!(
                stmt_fact_classifications(),
                before,
                "constructing StmtFacts must not run any classifier"
            );
        }

        /// …and once a gate does ask, the answer is computed exactly once no
        /// matter how many gates (or how many calls) consult it.
        #[test]
        fn each_fact_is_classified_at_most_once() {
            let sql = "SELECT v FROM t";
            let mut f = StmtFacts::new(sql);

            let before = stmt_fact_classifications();
            let first = f.is_write();
            let second = f.is_write();
            let third = f.is_write();
            assert_eq!(first, second);
            assert_eq!(first, third);
            assert_eq!(
                stmt_fact_classifications() - before,
                1,
                "is_write must be classified once, then memoized"
            );

            #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
            {
                let before = stmt_fact_classifications();
                let _ = f.leaves_session_state();
                let _ = f.leaves_session_state();
                assert_eq!(stmt_fact_classifications() - before, 1);
            }
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            {
                let before = stmt_fact_classifications();
                let _ = f.is_cacheable_read();
                let _ = f.is_cacheable_read();
                assert_eq!(stmt_fact_classifications() - before, 1);
            }
            #[cfg(feature = "edge-proxy")]
            {
                let before = stmt_fact_classifications();
                let _ = f.has_interior_semicolon();
                let _ = f.has_interior_semicolon();
                assert_eq!(stmt_fact_classifications() - before, 1);
            }
        }

        /// The forward path rebuilds the facts when a routing-hint strip, a
        /// rewrite rule or the tenant transform replaced the SQL. The rebuild
        /// must describe the NEW text — i.e. clear the memo — otherwise the
        /// cache / edge / pool gates would consult facts derived from the
        /// pre-rewrite string. The hint-strip case is discriminating: a
        /// leading `helios:` comment masks the SELECT lead, so the fact flips.
        #[test]
        fn rebuilding_on_changed_sql_clears_the_memo() {
            let hinted = "/*helios:route=primary*/ SELECT v FROM t";
            let stripped = "SELECT v FROM t";

            let mut before = StmtFacts::new(hinted);
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            assert!(
                !before.is_cacheable_read(),
                "leading comment masks the SELECT lead"
            );
            #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
            assert!(before.leaves_session_state(), "…and the neutral lead too");
            let _ = before.is_write();

            // Rebuilt exactly as `forward_simple_query` does it, on the final
            // message: a fresh value, so nothing memoized from `hinted`
            // survives.
            let msg = crate::protocol::QueryMessage {
                query: stripped.to_string(),
            }
            .encode();
            let mut after = StmtFacts::of_query(&msg);
            assert_eq!(after.sql, stripped);
            let count_before = stmt_fact_classifications();
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            assert!(
                after.is_cacheable_read(),
                "the stripped SELECT is cacheable"
            );
            #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
            assert!(!after.leaves_session_state());
            assert!(!after.is_write());
            assert!(
                stmt_fact_classifications() > count_before,
                "the rebuilt facts must re-classify, not reuse the old answers"
            );
        }

        /// The multi-statement fact is exactly the interior-`;` rule the edge
        /// invalidation gate used to re-derive inline.
        #[cfg(feature = "edge-proxy")]
        #[test]
        fn interior_semicolon_only_counts_non_trailing() {
            for (sql, want) in [
                ("SELECT 1", false),
                ("SELECT 1;", false),
                ("SELECT 1 ; ", false),
                ("SELECT 1; SELECT 2", true),
                ("BEGIN; UPDATE t SET v = 1; COMMIT", true),
                ("", false),
            ] {
                assert_eq!(
                    ProxyServer::stmt_has_interior_semicolon(sql),
                    want,
                    "{sql:?}"
                );
                assert_eq!(
                    StmtFacts::new(sql).has_interior_semicolon(),
                    want,
                    "{sql:?}"
                );
            }
        }

        /// `of_query` borrows the SQL carried by a `Query` message, and a
        /// payload that is not a valid query cstring falls back to the empty
        /// statement — for which every classifier answers `false`, the same
        /// fallback each individual call site used before the facts existed.
        #[test]
        fn of_query_reads_the_message_payload() {
            let msg = crate::protocol::QueryMessage {
                query: "UPDATE t SET v = 1".to_string(),
            }
            .encode();
            let mut f = StmtFacts::of_query(&msg);
            assert_eq!(f.sql, "UPDATE t SET v = 1");
            assert!(f.is_write());

            let empty = crate::protocol::Message::new(
                crate::protocol::MessageType::Query,
                bytes::BytesMut::new(),
            );
            let mut f = StmtFacts::of_query(&empty);
            assert_eq!(f.sql, "");
            assert!(!f.is_write());
            #[cfg(any(feature = "pool-modes", feature = "edge-proxy"))]
            assert!(!f.leaves_session_state());
            #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
            assert!(!f.is_cacheable_read());
            #[cfg(feature = "edge-proxy")]
            assert!(!f.has_interior_semicolon());
        }
    }

    /// A connected loopback `TcpStream` pair, shared by the relay tests
    /// below that need a real socket (so `try_read_buf`/`WouldBlock`
    /// behaves as it does in production, unlike an in-memory duplex pipe).
    async fn pair() -> (TcpStream, TcpStream) {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let (accepted, connected) = tokio::join!(l.accept(), TcpStream::connect(addr));
        (accepted.unwrap().0, connected.unwrap())
    }

    #[test]
    fn test_server_creation() {
        let config = test_config();
        let server = ProxyServer::new(config);
        assert!(server.is_ok());
    }

    #[test]
    fn is_backend_fault_excludes_client_and_slow_query_errors() {
        // Real backend faults — these must demote the node in-band.
        assert!(ProxyServer::is_backend_fault(
            "Backend read error: connection reset"
        ));
        assert!(ProxyServer::is_backend_fault(
            "Backend write error: broken pipe"
        ));
        assert!(ProxyServer::is_backend_fault("Backend write timeout"));
        assert!(ProxyServer::is_backend_fault(
            "Failed to connect to 127.0.0.1:5432: Connection refused"
        ));
        // Not backend faults — a client-side problem, or a merely slow but
        // healthy query, must NEVER take a backend out of rotation cluster-wide.
        assert!(!ProxyServer::is_backend_fault("Backend read timeout"));
        assert!(!ProxyServer::is_backend_fault("Client write timeout"));
        assert!(!ProxyServer::is_backend_fault(
            "Client write error: broken pipe"
        ));
        // A backend READ timeout is exempt, but a backend read ERROR is a fault.
        assert!(!ProxyServer::is_backend_fault("Backend read timeout"));
        assert!(ProxyServer::is_backend_fault(
            "Backend read error: timed out"
        ));
    }

    #[test]
    fn test_hba_addr_matches() {
        use std::net::IpAddr;
        let v4 = |s: &str| s.parse::<IpAddr>().unwrap();
        // "all" matches everything
        assert!(ProxyServer::hba_addr_matches("all", v4("203.0.113.7")));
        // CIDR membership
        assert!(ProxyServer::hba_addr_matches("10.0.0.0/8", v4("10.1.2.3")));
        assert!(!ProxyServer::hba_addr_matches("10.0.0.0/8", v4("11.1.2.3")));
        assert!(ProxyServer::hba_addr_matches(
            "127.0.0.1/32",
            v4("127.0.0.1")
        ));
        assert!(!ProxyServer::hba_addr_matches(
            "127.0.0.1/32",
            v4("127.0.0.2")
        ));
        // bare IP exact match
        assert!(ProxyServer::hba_addr_matches(
            "192.168.1.1",
            v4("192.168.1.1")
        ));
        assert!(!ProxyServer::hba_addr_matches(
            "192.168.1.1",
            v4("192.168.1.2")
        ));
        // IPv6 CIDR + /0 catch-all
        assert!(ProxyServer::hba_addr_matches("::1/128", v4("::1")));
        assert!(ProxyServer::hba_addr_matches("0.0.0.0/0", v4("8.8.8.8")));
    }

    #[test]
    fn test_hba_admits() {
        use crate::config::{HbaAction, HbaRule};
        use std::net::IpAddr;
        let ip: IpAddr = "10.0.0.5".parse().unwrap();
        // No rules -> admit all
        assert!(ProxyServer::hba_admits(&[], ip, "bench", "benchdb"));
        // Reject a specific user, allow others (default admit)
        let rules = vec![HbaRule {
            action: HbaAction::Reject,
            user: "bench".into(),
            database: "all".into(),
            address: "all".into(),
        }];
        assert!(!ProxyServer::hba_admits(&rules, ip, "bench", "benchdb"));
        assert!(ProxyServer::hba_admits(&rules, ip, "alice", "benchdb"));
        // First match wins: allow bench from 10/8, reject everything else
        let rules = vec![
            HbaRule {
                action: HbaAction::Allow,
                user: "bench".into(),
                database: "all".into(),
                address: "10.0.0.0/8".into(),
            },
            HbaRule {
                action: HbaAction::Reject,
                user: "all".into(),
                database: "all".into(),
                address: "all".into(),
            },
        ];
        assert!(ProxyServer::hba_admits(&rules, ip, "bench", "benchdb"));
        assert!(!ProxyServer::hba_admits(
            &rules,
            "192.168.0.1".parse().unwrap(),
            "bench",
            "benchdb"
        ));
        assert!(!ProxyServer::hba_admits(&rules, ip, "alice", "benchdb"));
    }

    #[test]
    fn test_initial_metrics() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let metrics = server.metrics();
        assert_eq!(metrics.connections_accepted, 0);
        assert_eq!(metrics.queries_processed, 0);
    }

    #[tokio::test]
    async fn test_session_creation() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();

        assert!(server.state.sessions.is_empty());
    }

    #[tokio::test]
    async fn test_node_health_initialization() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();

        let health = server.state.health.load_full();
        assert!(!health.is_empty());

        for node_health in health.values() {
            assert!(node_health.healthy);
            assert_eq!(node_health.failure_count, 0);
        }
    }

    /// Build a minimal `ClientSession` for plugin-hook unit tests.
    fn make_test_session() -> Arc<ClientSession> {
        let id = Uuid::new_v4();
        let client_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        Arc::new(ClientSession {
            id,
            client_addr,
            client_ip_str: client_addr.ip().to_string(),
            session_id_str: id.to_string(),
            current_node: RwLock::new(None),
            in_transaction: std::sync::atomic::AtomicBool::new(false),
            copy_in_progress: std::sync::atomic::AtomicBool::new(false),
            last_rfq_status: std::sync::atomic::AtomicU8::new(b'I'),
            last_response_error: std::sync::atomic::AtomicBool::new(false),
            tr_replay_tainted: std::sync::atomic::AtomicBool::new(false),
            backend_credential: RwLock::new(None),
            tx_state: RwLock::new(TransactionState::default()),
            variables: RwLock::new(HashMap::new()),
            created_at: chrono::Utc::now(),
            tr_mode: crate::config::TrMode::default(),
            #[cfg(feature = "lag-routing")]
            last_write_at: RwLock::new(None),
            #[cfg(feature = "pool-modes")]
            pool_client_id: crate::pool::lease::ClientId::default(),
            #[cfg(feature = "wasm-plugins")]
            plugin_identity: RwLock::new(None),
            #[cfg(feature = "edge-proxy")]
            edge_ineligible: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "edge-proxy")]
            pending_edge_copy_tables: std::sync::Mutex::new(None),
            #[cfg(feature = "rate-limiting")]
            rate_limit_key: std::sync::OnceLock::new(),
        })
    }

    /// The per-query analytics path reads pre-rendered client-IP and session-id
    /// strings off the session instead of formatting an `IpAddr`/`Uuid` on every
    /// query. They must match what the old per-query formatting produced.
    #[test]
    fn test_session_caches_client_ip_and_id_strings() {
        let session = make_test_session();
        assert_eq!(session.client_ip_str, session.client_addr.ip().to_string());
        assert_eq!(session.session_id_str, session.id.to_string());
        assert!(!session.session_id_str.is_empty());
    }

    /// With no plugin manager attached, `apply_route_hook` must be a
    /// zero-cost `None` return so the default SQL-verb routing applies.
    /// Verifies the feature-gated early-return path.
    #[tokio::test]
    async fn test_apply_route_hook_no_plugin_manager_returns_none() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        let msg = QueryMessage {
            query: "SELECT * FROM users".to_string(),
        }
        .encode();

        let decision = ProxyServer::apply_route_hook(&msg, &server.state, &session);
        assert!(matches!(decision, RouteOverride::None));
    }

    /// Same invariant for the pre-query hook: without a plugin manager,
    /// `apply_pre_query_hook` must return the message unchanged with
    /// `PreQueryAction::Forward`.
    #[tokio::test]
    async fn test_apply_pre_query_hook_no_plugin_manager_forwards() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        let original = QueryMessage {
            query: "SELECT 1".to_string(),
        }
        .encode();
        let original_bytes = original.encode().to_vec();

        let (msg_out, action) =
            ProxyServer::apply_pre_query_hook(original, &server.state, &session);

        assert!(matches!(action, PreQueryAction::Forward));
        // The message must survive the hook byte-for-byte when no plugins run.
        assert_eq!(msg_out.encode().to_vec(), original_bytes);
    }

    /// Non-Query message types (e.g., extended-protocol Parse/Execute) must
    /// bypass the Route hook entirely regardless of plugin state, because
    /// we haven't wired SQL extraction for those variants yet.
    #[tokio::test]
    async fn test_apply_route_hook_skips_non_query_messages() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        let sync_msg = Message::empty(MessageType::Sync);
        let decision = ProxyServer::apply_route_hook(&sync_msg, &server.state, &session);
        assert!(matches!(decision, RouteOverride::None));
    }

    /// By default, `[plugins].enabled = false`, so `init_plugin_manager`
    /// short-circuits without touching the filesystem or wasmtime and
    /// returns `None`. The proxy starts normally whether or not a plugin
    /// directory exists on the host.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_init_plugin_manager_disabled_by_default_returns_none() {
        let config = test_config();
        assert!(!config.plugins.enabled);
        let pm = ProxyServer::init_plugin_manager(&config.plugins);
        assert!(pm.is_none());
    }

    /// Plugins enabled but pointing at a directory that doesn't exist
    /// must still initialise the manager (so new plugins can be hot-
    /// loaded later) and log a warning — it must NOT fail startup.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_init_plugin_manager_missing_dir_logs_warning() {
        let mut config = test_config();
        config.plugins.enabled = true;
        config.plugins.plugin_dir = "/definitely/not/a/real/path".to_string();

        // Manager is created; no panic; Some(pm) returned even with empty dir.
        let pm = ProxyServer::init_plugin_manager(&config.plugins);
        assert!(pm.is_some());
    }

    /// With no plugin manager attached, `apply_authenticate_hook` is a
    /// zero-cost `Ok(())` that leaves session identity unset — the
    /// default PG auth flow applies.
    #[tokio::test]
    async fn test_apply_authenticate_hook_no_plugin_manager_defers() {
        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        let mut params = HashMap::new();
        params.insert("user".to_string(), "alice".to_string());
        params.insert("database".to_string(), "app".to_string());

        let result = ProxyServer::apply_authenticate_hook(&params, &session, &server.state).await;
        assert!(result.is_ok());

        // No plugin → no identity stored.
        #[cfg(feature = "wasm-plugins")]
        {
            let ident = session.plugin_identity.read().await;
            assert!(ident.is_none());
        }
    }

    /// Cached-response synthesis round-trip: a well-formed plugin
    /// payload must produce concatenated wire frames in the order
    /// `T D D C Z`. We inspect the raw tag bytes directly because
    /// `MessageType::from_tag` conflates server→client DataRow (`'D'`)
    /// with client→server Describe (same byte) — a known quirk of the
    /// shared `MessageType` enum that the real proxy side-steps by
    /// knowing the direction at the call site.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_synthesise_cached_response_roundtrip() {
        let payload = br#"{
            "columns": [
                {"name": "id",    "oid": 23},
                {"name": "email", "oid": 25}
            ],
            "rows": [
                ["1", "alice@example.com"],
                ["2", null]
            ]
        }"#;
        let reply = ProxyServer::synthesise_cached_response(payload).expect("synthesis");

        // Walk the concatenation frame-by-frame via length prefixes.
        // Each PG message: tag(1) + length(4, big-endian, includes self) + payload.
        let mut tags = Vec::new();
        let mut i = 0;
        while i < reply.len() {
            let tag = reply[i];
            let len = u32::from_be_bytes([reply[i + 1], reply[i + 2], reply[i + 3], reply[i + 4]])
                as usize;
            tags.push(tag);
            i += 1 + len;
        }
        assert_eq!(i, reply.len(), "no trailing bytes");
        assert_eq!(tags, vec![b'T', b'D', b'D', b'C', b'Z'], "wire frame order");

        // Spot-check the final ReadyForQuery payload is 'I' (idle).
        assert_eq!(*reply.last().unwrap(), b'I');
    }

    /// Row width mismatch between columns and row data is rejected so
    /// the plugin author can't produce ambiguous wire frames.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_synthesise_cached_response_rejects_row_width_mismatch() {
        let payload = br#"{
            "columns": [{"name": "id", "oid": 23}, {"name": "name", "oid": 25}],
            "rows": [["1", "alice", "extra"]]
        }"#;
        let result = ProxyServer::synthesise_cached_response(payload);
        assert!(matches!(result, Err(ProxyError::Protocol(_))));
    }

    /// Empty payload (no columns) is rejected — a RowDescription with
    /// zero columns is technically valid PG but useless and likely a
    /// plugin bug.
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_synthesise_cached_response_rejects_empty_columns() {
        let payload = br#"{ "columns": [], "rows": [] }"#;
        let result = ProxyServer::synthesise_cached_response(payload);
        assert!(matches!(result, Err(ProxyError::Protocol(_))));
    }

    /// Malformed JSON must return a Protocol error, not panic. The
    /// caller treats this as "fall back to backend."
    #[cfg(feature = "wasm-plugins")]
    #[test]
    fn test_synthesise_cached_response_rejects_bad_json() {
        let payload = b"not json at all";
        let result = ProxyServer::synthesise_cached_response(payload);
        assert!(matches!(result, Err(ProxyError::Protocol(_))));
    }

    /// Denied by plugin surfaces as `ProxyError::Auth` so the existing
    /// error-response path in `handle_client` writes an ErrorResponse
    /// and closes the connection. Here we prove the error variant
    /// when the plugin manager is present but denies. We build a
    /// PluginManager with no plugins loaded — so it defers — and
    /// verify the Ok path. (Denial path requires an actual
    /// auth-plugin `.wasm`; covered by the plugin unit tests in
    /// `plugins::tests`.)
    #[cfg(feature = "wasm-plugins")]
    #[tokio::test]
    async fn test_apply_authenticate_hook_with_manager_no_plugins_defers() {
        use crate::plugins::{PluginManager, PluginRuntimeConfig};

        let config = test_config();
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        // Synthesise a state with a real PluginManager but zero
        // registered plugins — every hook must defer.
        let pm = Arc::new(PluginManager::new(PluginRuntimeConfig::default()).unwrap());
        #[cfg(feature = "edge-proxy")]
        let edge_defaults = crate::edge::EdgeConfig::default();
        let augmented_state = Arc::new(ServerState {
            limits: ResolvedLimits::default(),
            client_slots: None,
            sessions: DashMap::new(),
            health: ArcSwap::from_pointee(HashMap::new()),
            health_write: parking_lot::Mutex::new(()),
            live_config: ArcSwap::from_pointee(ProxyConfig::default()),
            metrics: ServerMetrics::default(),
            cancel_map: Arc::new(DashMap::new()),
            cancel_order: Arc::new(parking_lot::Mutex::new(std::collections::VecDeque::new())),
            tls_acceptor: None,
            auth_file: None,
            mirror: None,
            cutover: Arc::new(ArcSwap::from_pointee(None)),
            lb_state: LoadBalancerState {
                rr_counter: AtomicU64::new(0),
            },
            #[cfg(feature = "routing-hints")]
            hint_parser: None,
            #[cfg(feature = "rate-limiting")]
            rate_limiter: None,
            #[cfg(feature = "circuit-breaker")]
            circuit_breaker: None,
            #[cfg(feature = "query-analytics")]
            analytics: None,
            #[cfg(feature = "query-cache")]
            query_cache: None,
            #[cfg(feature = "query-rewriting")]
            rewriter: None,
            #[cfg(feature = "multi-tenancy")]
            tenant_manager: None,
            #[cfg(feature = "schema-routing")]
            schema_analyzer: None,
            #[cfg(feature = "pool-modes")]
            pool_manager: None,
            #[cfg(feature = "pool-modes")]
            backend_pool: None,
            plugin_manager: Some(pm),
            #[cfg(feature = "ha-tr")]
            transaction_journal: Arc::new(crate::transaction_journal::TransactionJournal::new()),
            tr_read_policy: Arc::new(TrReadPolicy::default()),
            #[cfg(feature = "anomaly-detection")]
            anomaly_detector: Arc::new(crate::anomaly::AnomalyDetector::new(
                ProxyConfig::default().anomaly.to_anomaly_config(),
            )),
            #[cfg(feature = "edge-proxy")]
            edge_cache: Arc::new(crate::edge::EdgeCache::new(
                edge_defaults.max_entries.max(1),
            )),
            #[cfg(feature = "edge-proxy")]
            edge_registry: Arc::new(crate::edge::EdgeRegistry::new(
                edge_defaults.max_edges,
                std::time::Duration::from_secs(edge_defaults.liveness_window_secs),
            )),
        });

        let mut params = HashMap::new();
        params.insert("user".to_string(), "alice".to_string());

        let result =
            ProxyServer::apply_authenticate_hook(&params, &session, &augmented_state).await;
        assert!(result.is_ok());
        let ident = session.plugin_identity.read().await;
        assert!(ident.is_none());
        // Unused bindings for the sync-state build path.
        let _ = server;
    }

    // ---- Batch F.4: prepared-statement tracking across backend switches ----

    fn cstr(s: &str) -> Vec<u8> {
        let mut v = s.as_bytes().to_vec();
        v.push(0);
        v
    }

    #[test]
    fn parse_stmt_name_extracts_named_and_unnamed() {
        // Parse payload = stmt-name cstring + query cstring + int16 nparams.
        let mut named = cstr("ps1");
        named.extend_from_slice(&cstr("SELECT 1"));
        named.extend_from_slice(&[0, 0]);
        assert_eq!(ProxyServer::parse_stmt_name(&named), "ps1");

        let mut unnamed = cstr("");
        unnamed.extend_from_slice(&cstr("SELECT 1"));
        unnamed.extend_from_slice(&[0, 0]);
        assert_eq!(ProxyServer::parse_stmt_name(&unnamed), "");
    }

    #[test]
    fn bind_stmt_ref_reads_second_cstring() {
        // Bind payload = portal cstring + statement cstring + ...
        let mut named = cstr("portal_a");
        named.extend_from_slice(&cstr("ps1"));
        named.extend_from_slice(&[0, 0]); // 0 param-format codes, 0 params
        assert_eq!(ProxyServer::bind_stmt_ref(&named), Some("ps1"));

        // Unnamed statement (empty second cstring) is not tracked.
        let mut unnamed = cstr("");
        unnamed.extend_from_slice(&cstr(""));
        assert_eq!(ProxyServer::bind_stmt_ref(&unnamed), None);
    }

    #[test]
    fn stmt_kind_name_only_matches_statement_kind() {
        // Describe/Close 'S' (statement) carries a trackable name.
        let mut stmt = vec![b'S'];
        stmt.extend_from_slice(&cstr("ps1"));
        assert_eq!(ProxyServer::stmt_kind_name(&stmt), Some("ps1"));

        // 'P' (portal) is not a statement reference.
        let mut portal = vec![b'P'];
        portal.extend_from_slice(&cstr("portal_a"));
        assert_eq!(ProxyServer::stmt_kind_name(&portal), None);

        // Statement-kind but unnamed -> nothing to track.
        let mut empty = vec![b'S'];
        empty.extend_from_slice(&cstr(""));
        assert_eq!(ProxyServer::stmt_kind_name(&empty), None);
    }

    #[tokio::test]
    async fn read_one_frame_type_consumes_full_frame() {
        // ParseComplete '1' with empty body, followed by a second frame to
        // prove only the first frame is consumed.
        let (mut a, mut b) = tokio::io::duplex(64);
        // frame 1: '1' + len(4) + no body; frame 2: 'Z' + len(5) + 'I'.
        let bytes = [b'1', 0, 0, 0, 4, b'Z', 0, 0, 0, 5, b'I'];
        b.write_all(&bytes).await.unwrap();
        let t = ProxyServer::read_one_frame_type(&mut a, usize::MAX)
            .await
            .unwrap();
        assert_eq!(t, b'1');
        // The next frame's type byte is still readable -> we stopped cleanly.
        let t2 = ProxyServer::read_one_frame_type(&mut a, usize::MAX)
            .await
            .unwrap();
        assert_eq!(t2, b'Z');
    }

    #[tokio::test]
    async fn reprepare_statement_accepts_parse_complete_and_rejects_error() {
        // Backend answers ParseComplete -> Ok.
        let (mut client, mut backend) = tokio::io::duplex(64);
        backend.write_all(&[b'1', 0, 0, 0, 4]).await.unwrap();
        let parse = {
            let mut p = vec![b'P', 0, 0, 0, 0];
            p.extend_from_slice(&cstr("ps1"));
            p.extend_from_slice(&cstr("SELECT 1"));
            p.extend_from_slice(&[0, 0]);
            p
        };
        assert!(ProxyServer::reprepare_statement(
            &mut client,
            &parse,
            Duration::from_secs(15),
            usize::MAX
        )
        .await
        .is_ok());

        // Backend answers ErrorResponse -> Err.
        let (mut client2, mut backend2) = tokio::io::duplex(64);
        backend2.write_all(&[b'E', 0, 0, 0, 4]).await.unwrap();
        assert!(ProxyServer::reprepare_statement(
            &mut client2,
            &parse,
            Duration::from_secs(15),
            usize::MAX
        )
        .await
        .is_err());
    }

    // ---- routing-hints: SQL-comment hint → RouteOverride mapping ----

    #[cfg(feature = "routing-hints")]
    mod routing_hints {
        use super::*;
        use crate::routing::HintParser;

        fn over(sql: &str) -> RouteOverride {
            let hints = HintParser::new().parse(sql);
            ProxyServer::hint_to_override(&hints)
        }

        #[test]
        fn route_primary_maps_to_primary() {
            assert!(matches!(
                over("/*helios:route=primary*/ SELECT 1"),
                RouteOverride::Primary
            ));
        }

        #[test]
        fn read_tier_targets_map_to_standby() {
            for t in ["standby", "sync", "semisync", "async", "local"] {
                assert!(
                    matches!(
                        over(&format!("/*helios:route={t}*/ SELECT 1")),
                        RouteOverride::Standby
                    ),
                    "route={t} should map to Standby"
                );
            }
        }

        #[test]
        fn any_and_vector_impose_no_constraint() {
            assert!(matches!(
                over("/*helios:route=any*/ SELECT 1"),
                RouteOverride::None
            ));
            assert!(matches!(
                over("/*helios:route=vector*/ SELECT 1"),
                RouteOverride::None
            ));
        }

        #[test]
        fn node_hint_maps_to_node_and_wins_over_route() {
            // node= beats route= (precedence).
            match over("/*helios:node=pg-standby,route=primary*/ SELECT 1") {
                RouteOverride::Node(n) => assert_eq!(n, "pg-standby"),
                other => panic!("expected Node, got {other:?}"),
            }
        }

        #[test]
        fn consistency_strong_forces_primary() {
            assert!(matches!(
                over("/*helios:consistency=strong*/ SELECT 1"),
                RouteOverride::Primary
            ));
        }

        #[test]
        fn no_hint_yields_none() {
            assert!(matches!(over("SELECT 1"), RouteOverride::None));
        }

        // The core correctness fix: a leading hint comment must NOT hide the
        // verb from write-detection. Raw classification misfires; classifying
        // on the stripped SQL is correct.
        #[test]
        fn write_verb_classified_after_strip() {
            let parser = HintParser::new();
            let raw = "/*helios:route=primary*/ INSERT INTO t VALUES (1)";
            // Raw (unstripped) wrongly looks like a read because it starts
            // with the comment.
            assert!(!ProxyServer::is_write_query(raw));
            // Stripped is correctly a write.
            assert!(ProxyServer::is_write_query(&parser.strip(raw)));
        }

        #[test]
        fn strip_removes_hint_comment() {
            let parser = HintParser::new();
            assert_eq!(
                parser.strip("/*helios:route=standby*/ SELECT 42"),
                "SELECT 42"
            );
        }
    }

    // ---- rate-limiting: the burst-then-deny contract the gate relies on ----

    #[cfg(feature = "rate-limiting")]
    mod rate_limiting {
        use crate::rate_limit::{LimiterKey, RateLimitConfig, RateLimitResult, RateLimiter};

        #[test]
        fn burst_allows_then_denies() {
            // Mirror the wiring's config conversion: tiny bucket, reject on
            // exceed (the engine default).
            let cfg = RateLimitConfig {
                enabled: true,
                default_qps: 1,
                default_burst: 2,
                ..Default::default()
            };
            let limiter = RateLimiter::new(cfg);
            let key = LimiterKey::User("u".to_string());

            // The first `burst` checks are admitted.
            assert!(matches!(limiter.check(&key, 1), RateLimitResult::Allowed));
            assert!(matches!(limiter.check(&key, 1), RateLimitResult::Allowed));

            // Rapid over-burst checks must produce at least one hard denial.
            let mut denied = false;
            for _ in 0..5 {
                if matches!(limiter.check(&key, 1), RateLimitResult::Denied(_)) {
                    denied = true;
                }
            }
            assert!(denied, "over-burst checks must yield a Denied verdict");
        }

        /// The per-session bucket key is resolved once and then reused: two
        /// gate invocations must hand back the *same* memoized value, not a
        /// freshly built one (the whole point of the cache — no key alloc, no
        /// `variables` read lock, no metrics `format!` per query).
        #[tokio::test]
        async fn session_key_is_memoized_after_startup_params() {
            use crate::config::RateLimitKeyBy;

            let mut cfg = super::test_config();
            cfg.rate_limit.key_by = RateLimitKeyBy::User;

            let session = super::make_test_session();
            {
                let mut vars = session.variables.write().await;
                vars.insert("user".into(), "alice".into());
            }

            let first = super::ProxyServer::rate_limit_key(&session, &cfg).await;
            assert_eq!(first.as_ref().to_string(), "user:alice");
            drop(first);

            assert!(
                session.rate_limit_key.get().is_some(),
                "a resolvable key must be cached on the session"
            );

            // The second call must borrow the memoized value rather than
            // rebuild one.
            let second = super::ProxyServer::rate_limit_key(&session, &cfg).await;
            assert!(
                matches!(second, std::borrow::Cow::Borrowed(_)),
                "key was rebuilt instead of reused"
            );
            assert_eq!(second.as_ref().to_string(), "user:alice");
        }

        /// Before the startup parameters land the key must NOT be memoized —
        /// otherwise a placeholder (`user:`) would be frozen for the whole
        /// session. The pre-startup verdict is byte-identical to the old
        /// recompute-every-time behavior.
        #[tokio::test]
        async fn key_is_not_memoized_before_startup_params() {
            use crate::config::RateLimitKeyBy;

            let mut cfg = super::test_config();
            cfg.rate_limit.key_by = RateLimitKeyBy::Database;

            let session = super::make_test_session();

            let early = super::ProxyServer::rate_limit_key(&session, &cfg).await;
            assert_eq!(early.as_ref().to_string(), "db:");
            assert!(
                matches!(early, std::borrow::Cow::Owned(_)),
                "a placeholder key must not be served from the cache"
            );
            drop(early);
            assert!(
                session.rate_limit_key.get().is_none(),
                "a placeholder key must never be cached"
            );

            {
                let mut vars = session.variables.write().await;
                vars.insert("database".into(), "shop".into());
            }

            let later = super::ProxyServer::rate_limit_key(&session, &cfg).await;
            assert_eq!(later.as_ref().to_string(), "db:shop");
            drop(later);
            assert!(session.rate_limit_key.get().is_some());
        }

        /// Keying dimensions that do not read session variables are cached on
        /// the very first call, and render exactly as before.
        #[tokio::test]
        async fn variable_free_keys_are_cached_immediately() {
            use crate::config::RateLimitKeyBy;

            for (key_by, expected) in [
                (RateLimitKeyBy::Global, "global"),
                (RateLimitKeyBy::ClientIp, "ip:127.0.0.1"),
            ] {
                let mut cfg = super::test_config();
                cfg.rate_limit.key_by = key_by;

                let session = super::make_test_session();
                let key = super::ProxyServer::rate_limit_key(&session, &cfg).await;
                assert_eq!(key.as_ref().to_string(), expected);
                drop(key);
                assert!(session.rate_limit_key.get().is_some());
            }
        }

        #[test]
        fn distinct_keys_have_independent_buckets() {
            let cfg = RateLimitConfig {
                enabled: true,
                default_qps: 1,
                default_burst: 1,
                ..Default::default()
            };
            let limiter = RateLimiter::new(cfg);
            // Each user gets its own bucket: both first checks are admitted.
            assert!(matches!(
                limiter.check(&LimiterKey::User("a".to_string()), 1),
                RateLimitResult::Allowed
            ));
            assert!(matches!(
                limiter.check(&LimiterKey::User("b".to_string()), 1),
                RateLimitResult::Allowed
            ));
        }
    }

    // ---- circuit-breaker: open-after-threshold contract the gate relies on ----

    #[cfg(feature = "circuit-breaker")]
    mod circuit_breaker {
        use crate::circuit_breaker::{
            CircuitBreakerConfig, CircuitBreakerManager, CircuitState, ManagerConfig,
        };
        use std::time::Duration;

        fn mgr(threshold: u32) -> CircuitBreakerManager {
            let cfg = CircuitBreakerConfig {
                failure_threshold: threshold,
                cooldown: Duration::from_secs(10),
                ..Default::default()
            };
            CircuitBreakerManager::new(ManagerConfig::new(cfg))
        }

        #[test]
        fn opens_after_threshold_failures() {
            let m = mgr(3);
            let b = m.get_breaker("n1");
            assert_eq!(b.get_state(), CircuitState::Closed);
            b.record_failure("boom");
            b.record_failure("boom");
            // Under threshold: still serving.
            assert_eq!(b.get_state(), CircuitState::Closed);
            // Threshold reached: tripped open.
            b.record_failure("boom");
            assert_eq!(b.get_state(), CircuitState::Open);
        }

        #[test]
        fn healthy_node_stays_closed() {
            let m = mgr(3);
            let b = m.get_breaker("n2");
            b.record_success();
            b.record_success();
            assert_eq!(b.get_state(), CircuitState::Closed);
        }
    }

    // ---- query-analytics: record + literal-collapsing normalizer ----

    #[cfg(feature = "query-analytics")]
    mod query_analytics {
        use crate::analytics::{AnalyticsConfig, OrderBy, QueryAnalytics, QueryExecution};
        use std::time::Duration;

        #[test]
        fn records_and_collapses_literals() {
            let a = QueryAnalytics::new(AnalyticsConfig::default());
            for n in [1, 2, 3] {
                a.record(QueryExecution::new(
                    format!("select {n}"),
                    Duration::from_millis(1),
                ));
            }
            let top = a.top_queries(OrderBy::Calls, 10);
            assert!(!top.is_empty(), "no fingerprints recorded");
            // The three literal variants collapse to one fingerprint (3 calls).
            assert!(
                top.iter().any(|s| s.calls >= 3),
                "literals did not collapse: {:?}",
                top.iter()
                    .map(|s| (s.normalized.clone(), s.calls))
                    .collect::<Vec<_>>()
            );
        }
    }

    // ---- lag-routing: read-your-writes window + lag-exclusion decisions ----

    #[cfg(feature = "lag-routing")]
    mod lag_routing {
        use super::ProxyServer;

        #[test]
        fn ryw_pins_recent_write() {
            // A write "now" falls inside a 1s window -> pin to primary.
            assert!(ProxyServer::ryw_pins_primary(
                Some(std::time::Instant::now()),
                1000
            ));
        }

        #[test]
        fn ryw_releases_old_write() {
            let old = std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(10))
                .unwrap();
            assert!(!ProxyServer::ryw_pins_primary(Some(old), 1000));
        }

        #[test]
        fn ryw_no_write_or_disabled() {
            assert!(!ProxyServer::ryw_pins_primary(None, 1000));
            // window=0 disables read-your-writes entirely.
            assert!(!ProxyServer::ryw_pins_primary(
                Some(std::time::Instant::now()),
                0
            ));
        }

        #[test]
        fn lag_exclusion_thresholds() {
            // max=0 disables exclusion.
            assert!(!ProxyServer::lag_excludes_standby(Some(999_999), 0));
            // unknown lag never excludes.
            assert!(!ProxyServer::lag_excludes_standby(None, 1000));
            // within ceiling stays in rotation.
            assert!(!ProxyServer::lag_excludes_standby(Some(500), 1000));
            // beyond ceiling is dropped.
            assert!(ProxyServer::lag_excludes_standby(Some(2000), 1000));
        }
    }

    // ---- query-cache: which read SQL is safe to cache ----

    #[cfg(feature = "query-cache")]
    mod query_cache {
        use super::ProxyServer;

        #[test]
        fn plain_selects_are_cacheable() {
            assert!(ProxyServer::is_cacheable_read_sql("select v from t"));
            assert!(ProxyServer::is_cacheable_read_sql(
                "  SELECT a, b FROM users WHERE id = 5"
            ));
        }

        #[test]
        fn writes_and_non_selects_are_not_cacheable() {
            assert!(!ProxyServer::is_cacheable_read_sql(
                "insert into t values (1)"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql("update t set v = 1"));
            assert!(!ProxyServer::is_cacheable_read_sql("show search_path"));
        }

        #[test]
        fn locking_and_volatile_selects_are_not_cacheable() {
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select * from t for update"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql("select now()"));
            assert!(!ProxyServer::is_cacheable_read_sql("select random()"));
            assert!(!ProxyServer::is_cacheable_read_sql("select nextval('s')"));
            // set_config mutates GUCs + emits ParameterStatus — replaying
            // from cache would suppress the side effect.
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select set_config('timezone', 'UTC', false) from t"
            ));
        }

        #[test]
        fn multi_statement_strings_are_not_cacheable() {
            // Replaying `SELECT ...; UPDATE ...` would fabricate the
            // UPDATE's CommandComplete while executing nothing.
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select v from t; update t set v = 1"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql("select 1; select 2"));
            // A single trailing semicolon stays cacheable.
            assert!(ProxyServer::is_cacheable_read_sql("select v from t;"));
            assert!(ProxyServer::is_cacheable_read_sql("SELECT v FROM t ; "));
        }

        #[test]
        fn literal_semicolon_or_into_over_rejects_by_design() {
            // G7 (accepted, safe-direction): the multi-statement and SELECT INTO
            // guards scan the RAW text, so a ';' or the word "into" inside a
            // string literal disqualifies an otherwise-cacheable SELECT. This
            // over-rejection is DELIBERATE and hit-rate-only — the raw scan is
            // the sole defense against multi-statement replay fabrication (a
            // literal-stripping pre-pass would misjudge `'x\'; UPDATE ...'` under
            // standard_conforming_strings and reopen that hole), and a real
            // SELECT INTO is protocol-indistinguishable from a plain SELECT.
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select v from t where url = 'a;b=c'"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select v from t where body like '%go into space%'"
            ));
            // A real SELECT INTO (it creates a table) must never be cached.
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select * into snapshot from t"
            ));
        }

        #[test]
        fn select_into_is_not_cacheable() {
            // SELECT ... INTO creates a table (CREATE TABLE AS synonym);
            // a cache replay would silently skip the DDL.
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select * into report_tmp from src"
            ));
            // Word-boundary: newline/tab-delimited INTO is caught too.
            assert!(!ProxyServer::is_cacheable_read_sql(
                "SELECT *\nINTO report_tmp\nFROM src"
            ));
            // ...but an identifier merely containing "into" is not.
            assert!(ProxyServer::is_cacheable_read_sql(
                "select into_total from t"
            ));
        }

        /// Regression for the single-lowercase-pass rewrite of the FOR
        /// UPDATE/FOR SHARE + VOLATILE-token checks: every needle must still
        /// be matched case-insensitively regardless of how the caller casts
        /// the keyword, exactly as the old per-needle `contains_ci` scan did.
        #[test]
        fn locking_and_volatile_checks_stay_case_insensitive() {
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select * from t FOR UPDATE"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select * from t For Update"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select * from t for share"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select * from t FOR SHARE"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql("SELECT NOW()"));
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select CURRENT_TIMESTAMP"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql(
                "select GEN_RANDOM_UUID()"
            ));
            assert!(!ProxyServer::is_cacheable_read_sql(
                "SELECT Set_Config('timezone', 'UTC', false)"
            ));
        }

        /// Non-ASCII bytes in the SQL text must not panic the lowercasing
        /// buffer (`to_ascii_lowercase` only touches ASCII bytes, so UTF-8
        /// validity is preserved) and must not be case-folded — same
        /// byte-for-byte semantics as the old `contains_ci`.
        #[test]
        fn non_ascii_sql_is_handled_safely() {
            assert!(ProxyServer::is_cacheable_read_sql(
                "select name from café where city = 'Zürich'"
            ));
            // A non-ASCII volatile-token lookalike must not be flagged —
            // "NÓW(" is not "now(" under byte-for-byte comparison.
            assert!(ProxyServer::is_cacheable_read_sql("select NÓW() from t"));
        }
    }

    // ---- edge-proxy: write-invalidation classifiers ----

    #[cfg(feature = "edge-proxy")]
    mod edge_proxy {
        use super::ProxyServer;
        use std::collections::HashMap;

        /// `edge_write_needs_invalidation` with the multi-statement fact
        /// derived from `sql` — exactly what the forward path passes from
        /// `StmtFacts::has_interior_semicolon`.
        fn ewni(is_write: bool, sql: &str) -> bool {
            ProxyServer::edge_write_needs_invalidation(
                is_write,
                sql,
                ProxyServer::stmt_has_interior_semicolon(sql),
            )
        }

        #[test]
        fn bare_txn_control_is_exempt_from_invalidation() {
            // F10: BEGIN/START/SAVEPOINT/RELEASE/ROLLBACK change no rows —
            // an ORM's txn-per-request must not full-flush the fleet twice
            // per request.
            for sql in [
                "BEGIN",
                "begin;",
                "START TRANSACTION",
                "SAVEPOINT sp1",
                "RELEASE SAVEPOINT sp1",
                "ROLLBACK",
                "ROLLBACK TO SAVEPOINT sp1;",
            ] {
                assert!(!ewni(true, sql), "{sql:?} must be exempt");
            }
            // COMMIT keeps its conservative flush (closes the in-txn
            // write visibility window), and SET stays (GUC mitigation).
            assert!(ewni(true, "COMMIT"));
            assert!(ewni(true, "SET search_path TO tenant_b"));
        }

        #[test]
        fn compound_txn_strings_still_invalidate() {
            // A multi-statement string may hide a write behind a
            // txn-control or SELECT lead — never exempt it.
            assert!(ewni(true, "BEGIN; UPDATE t SET v = 1; COMMIT"));
            // SELECT-leading batch with a trailing write classifies
            // is_write=false, but the interior `;` forces invalidation.
            assert!(ewni(false, "SELECT v FROM t; UPDATE t SET v = 1"));
            // A plain single SELECT does not invalidate.
            assert!(!ewni(false, "SELECT v FROM t"));
        }

        #[test]
        fn copy_from_counts_as_write() {
            assert!(ewni(false, "COPY t FROM STDIN"));
            assert!(ProxyServer::is_edge_copy_write_sql("copy t from stdin"));
            // COPY TO exports rows — a read.
            assert!(!ewni(false, "COPY t TO STDOUT"));
        }

        #[test]
        fn procedural_and_txn_end_trigger_invalidation() {
            // G3: SQL-level EXECUTE/CALL/DO are opaque writes — the simple path
            // must invalidate and the classifier flags them (the extended path
            // memoizes them as the empty-set wildcard via the same predicate).
            for sql in [
                "EXECUTE ins(1)",
                "execute ins(1)",
                "CALL do_write()",
                "DO $$ BEGIN PERFORM 1; END $$",
            ] {
                assert!(
                    ProxyServer::is_edge_procedural_sql(sql),
                    "{sql:?} is procedural"
                );
                assert!(
                    ewni(false, sql),
                    "{sql:?} must invalidate on the simple path"
                );
            }
            for sql in [
                "INSERT INTO t VALUES (1)",
                "SELECT 1 FROM t",
                "UPDATE t SET v=1",
            ] {
                assert!(
                    !ProxyServer::is_edge_procedural_sql(sql),
                    "{sql:?} is not procedural"
                );
            }

            // G2: COMMIT and its END synonym both trigger the wildcard flush
            // (closing the commit-visibility window); the openers do not.
            for sql in [
                "COMMIT",
                "commit work",
                "COMMIT PREPARED 'x'",
                "END",
                "END TRANSACTION",
                "end;",
            ] {
                assert!(ProxyServer::is_edge_txn_end_sql(sql), "{sql:?} ends a txn");
                assert!(
                    ewni(false, sql),
                    "{sql:?} must invalidate (commit-visibility window)"
                );
            }
            for sql in ["BEGIN", "START TRANSACTION", "SAVEPOINT s1", "ROLLBACK"] {
                assert!(
                    !ProxyServer::is_edge_txn_end_sql(sql),
                    "{sql:?} is not a txn-end"
                );
                assert!(
                    !ewni(false, sql),
                    "{sql:?} must stay exempt on the simple path"
                );
            }
        }

        #[test]
        fn edge_meta_prunable_protects_reparsed_names() {
            // G1: at a Sync, a Closed name NOT re-Parsed this batch is prunable;
            // a name Closed then re-Parsed (Npgsql statement replacement) must be
            // RETAINED so its fresh DML metadata survives to invalidate.
            let closes = vec!["S1".to_string(), "S2".to_string()];
            let none: Vec<String> = vec![];
            assert_eq!(
                ProxyServer::edge_meta_prunable(&closes, &none),
                vec!["S1", "S2"]
            );
            // S1 re-Parsed in the same batch → excluded from pruning.
            assert_eq!(
                ProxyServer::edge_meta_prunable(&closes, &["S1".to_string()]),
                vec!["S2"]
            );
            // Every closed name re-Parsed → nothing pruned (all meta kept fresh).
            assert!(ProxyServer::edge_meta_prunable(&closes, &closes).is_empty());
        }

        #[test]
        fn edge_dml_classifier_covers_dml_not_txn_control() {
            for sql in [
                "INSERT INTO t VALUES (1)",
                "update t set v = 1",
                "DELETE FROM t WHERE id = 1",
                "MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
                "TRUNCATE t",
                "ALTER TABLE t ADD COLUMN c int",
                "COPY t FROM STDIN",
                "WITH del AS (DELETE FROM t RETURNING id) SELECT * FROM del",
            ] {
                assert!(ProxyServer::is_edge_dml_sql(sql), "{sql:?} is DML");
            }
            // Txn control / reads / plain CTE reads: NOT invalidation
            // triggers (BEGIN/COMMIT here would full-flush per txn).
            for sql in [
                "BEGIN",
                "COMMIT",
                "ROLLBACK",
                "SET search_path TO x",
                "SELECT v FROM t",
                "WITH x AS (SELECT 1) SELECT * FROM x",
                "COPY t TO STDOUT",
            ] {
                assert!(!ProxyServer::is_edge_dml_sql(sql), "{sql:?} is not DML");
            }
        }

        #[test]
        fn extended_batch_tables_union_and_wildcard() {
            let mut named: HashMap<String, Option<Vec<String>>> = HashMap::new();
            named.insert("ins".into(), Some(vec!["orders".into()]));
            named.insert("upd".into(), Some(vec!["users".into()]));
            named.insert("sel".into(), None);
            named.insert("weird".into(), Some(vec![])); // unattributable DML
            let unnamed: Option<Vec<String>> = Some(vec!["events".into()]);

            // Read-only batch: no invalidation.
            assert_eq!(
                ProxyServer::edge_extended_batch_tables(
                    &["sel".to_string()],
                    false,
                    &named,
                    &unnamed
                ),
                None
            );
            // DML batch: union of referenced statements' tables.
            let t = ProxyServer::edge_extended_batch_tables(
                &["ins".to_string(), "upd".to_string(), "ins".to_string()],
                false,
                &named,
                &unnamed,
            )
            .expect("dml");
            assert_eq!(t, vec!["orders".to_string(), "users".to_string()]);
            // Unnamed execution folds in.
            let t = ProxyServer::edge_extended_batch_tables(&[], true, &named, &unnamed)
                .expect("unnamed dml");
            assert_eq!(t, vec!["events".to_string()]);
            // Any unattributable DML → wildcard (invalidate everything).
            let t = ProxyServer::edge_extended_batch_tables(
                &["ins".to_string(), "weird".to_string()],
                false,
                &named,
                &unnamed,
            )
            .expect("dml");
            assert!(t.is_empty(), "wildcard invalidation");
            // Unknown names (never Parse'd — backend errors anyway) are
            // treated as non-DML.
            assert_eq!(
                ProxyServer::edge_extended_batch_tables(
                    &["ghost".to_string()],
                    false,
                    &named,
                    &None
                ),
                None
            );
        }
    }

    /// F12: async backend frames (NotificationResponse 'A', NoticeResponse
    /// 'N', ParameterStatus 'S') captured inside a response window must
    /// suppress the store (`cacheable = false`) while still being forwarded
    /// byte-for-byte — a cached LISTEN/NOTIFY payload would replay to every
    /// later hitter cross-session.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    #[tokio::test]
    async fn capture_excludes_async_frames_from_cacheable() {
        use crate::client_tls::ClientStream;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt as _;

        fn frame(mtype: u8, body: &[u8]) -> Vec<u8> {
            let mut v = vec![mtype];
            v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
            v.extend_from_slice(body);
            v
        }

        let clean: Vec<u8> = [
            frame(b'T', b"rowdesc"),
            frame(b'D', b"row"),
            frame(b'C', b"SELECT 1\0"),
            frame(b'Z', b"I"),
        ]
        .concat();
        let with_notify: Vec<u8> = [
            frame(b'T', b"rowdesc"),
            frame(b'A', b"\x00\x00\x30\x39chan\0payload\0"),
            frame(b'D', b"row"),
            frame(b'C', b"SELECT 1\0"),
            frame(b'Z', b"I"),
        ]
        .concat();
        let with_param_status: Vec<u8> = [
            frame(b'D', b"row"),
            frame(b'C', b"SELECT 1\0"),
            frame(b'S', b"TimeZone\0UTC\0"),
            frame(b'Z', b"I"),
        ]
        .concat();

        for (bytes, want_cacheable) in [
            (clean, true),
            (with_notify, false),
            (with_param_status, false),
        ] {
            let (mut backend, mut backend_peer) = pair().await;
            let (client_raw, mut client_peer) = pair().await;
            let mut client = ClientStream::Plain(client_raw);
            let session = make_test_session();

            backend_peer.write_all(&bytes).await.unwrap();
            backend_peer.flush().await.unwrap();

            let metrics = ServerMetrics::default();
            let (sent, captured, cacheable, _rows) = ProxyServer::stream_until_ready_capture(
                &mut client,
                &mut backend,
                &session,
                RelayLimits {
                    client_write_timeout: Duration::from_secs(60),
                    backend_read_timeout: Duration::from_secs(30),
                    max_frame_bytes: usize::MAX,
                },
                usize::MAX,
                &metrics,
            )
            .await
            .expect("capture ok");
            assert_eq!(cacheable, want_cacheable, "cacheable flag");
            assert_eq!(sent as usize, bytes.len());
            assert_eq!(captured, bytes, "capture is byte-exact");
            assert_eq!(
                metrics
                    .cache_capture_oversize
                    .load(std::sync::atomic::Ordering::Relaxed),
                0,
                "no cap was hit"
            );

            // Every frame — async ones included — was forwarded to the
            // live client.
            let mut got = vec![0u8; bytes.len()];
            client_peer.read_exact(&mut got).await.unwrap();
            assert_eq!(got, bytes, "forwarding must not be filtered");
        }
    }

    /// O1: the capture buffer is a per-session transient held ON TOP of the
    /// bytes already streamed to the client, so it must be bounded. A response
    /// larger than `[cache] max_cacheable_response_bytes` must (a) reach the
    /// client byte-for-byte, (b) come back non-cacheable with an EMPTY capture
    /// (the allocation freed, not merely ignored), and (c) bump
    /// `cache_capture_oversize`. A response under the cap is unchanged.
    #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
    #[tokio::test]
    async fn capture_stops_and_frees_buffer_past_byte_cap() {
        use crate::client_tls::ClientStream;
        use std::sync::atomic::Ordering as AtomicOrdering;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt as _;

        fn frame(mtype: u8, body: &[u8]) -> Vec<u8> {
            let mut v = vec![mtype];
            v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
            v.extend_from_slice(body);
            v
        }

        // ~128 KiB of DataRows — far beyond the 1 KiB cap under test, and
        // beyond a socket buffer, so writer/reader must run concurrently.
        fn big_response(rows: usize) -> Vec<u8> {
            let row = vec![b'x'; 1024];
            let mut v = frame(b'T', b"rowdesc");
            for _ in 0..rows {
                v.extend_from_slice(&frame(b'D', &row));
            }
            v.extend_from_slice(&frame(b'C', format!("SELECT {}\0", rows).as_bytes()));
            v.extend_from_slice(&frame(b'Z', b"I"));
            v
        }

        const CAP: usize = 1024;

        for (bytes, want_cacheable, want_oversize) in [
            // Comfortably under the cap → today's behaviour, byte-for-byte.
            (big_response(0), true, 0u64),
            // Over the cap → forwarded in full, but never cached.
            (big_response(128), false, 1u64),
        ] {
            assert!(
                (bytes.len() > CAP) == (want_oversize == 1),
                "test fixture must straddle the cap"
            );

            let (mut backend, mut backend_peer) = pair().await;
            let (client_raw, mut client_peer) = pair().await;
            let mut client = ClientStream::Plain(client_raw);
            let session = make_test_session();
            let metrics = ServerMetrics::default();

            // Both peers must run concurrently with the relay: the response
            // exceeds the socket buffers in both directions.
            let to_write = bytes.clone();
            let writer = tokio::spawn(async move {
                backend_peer.write_all(&to_write).await.unwrap();
                backend_peer.flush().await.unwrap();
                backend_peer
            });
            let want_len = bytes.len();
            let reader = tokio::spawn(async move {
                let mut got = vec![0u8; want_len];
                client_peer.read_exact(&mut got).await.unwrap();
                got
            });

            let (sent, captured, cacheable, rows) = ProxyServer::stream_until_ready_capture(
                &mut client,
                &mut backend,
                &session,
                RelayLimits {
                    client_write_timeout: Duration::from_secs(60),
                    backend_read_timeout: Duration::from_secs(30),
                    max_frame_bytes: usize::MAX,
                },
                CAP,
                &metrics,
            )
            .await
            .expect("capture ok");

            let _ = writer.await.unwrap();
            let got = reader.await.unwrap();

            // (a) The client stream is untouched by the cap.
            assert_eq!(got, bytes, "client bytes must be byte-exact");
            assert_eq!(sent as usize, bytes.len(), "sent count");
            // Row count is parsed from CommandComplete either way.
            assert_eq!(rows, if want_oversize == 1 { 128 } else { 0 });
            // (b) Cacheability + capture buffer.
            assert_eq!(cacheable, want_cacheable, "cacheable flag");
            if want_oversize == 1 {
                assert!(captured.is_empty(), "oversize capture must be dropped");
                assert_eq!(captured.capacity(), 0, "allocation must be freed, not kept");
            } else {
                assert_eq!(captured, bytes, "under-cap capture is byte-exact");
            }
            // (c) Operator-visible counter.
            assert_eq!(
                metrics.cache_capture_oversize.load(AtomicOrdering::Relaxed),
                want_oversize,
                "cache_capture_oversize"
            );
        }
    }

    /// `stream_flush` (Fix S4) relays whatever the backend has already
    /// produced byte-for-byte, then returns as soon as the socket goes
    /// `WouldBlock` — it must never block waiting for more. The read buffer
    /// is a reused `BytesMut` (no per-call `vec![0u8; 16384]` zeroing); this
    /// sends more than one 16 KiB buffer's worth of data so the loop must
    /// `clear()` and refill the same buffer across multiple `try_read_buf`
    /// calls without dropping or duplicating bytes.
    #[tokio::test]
    async fn stream_flush_relays_available_bytes_then_returns_without_blocking() {
        use crate::client_tls::ClientStream;
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt as _;

        let (mut backend, mut backend_peer) = pair().await;
        let (client_raw, mut client_peer) = pair().await;
        let mut client = ClientStream::Plain(client_raw);
        let session = make_test_session();
        let server = ProxyServer::new(test_config()).unwrap();
        let state = server.state.clone();

        // Larger than one 16 KiB read, so a correct implementation must
        // loop `try_read_buf` (clearing and refilling the same buffer)
        // rather than stopping after the first chunk.
        let bytes: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();

        // Both sides of the relay run concurrently with the `stream_flush`
        // calls below, so the test does not depend on 40 KB fitting in the
        // kernel socket buffers: on a host with small `tcp_wmem`/`tcp_rmem`
        // an inline `write_all` (or an inline read only after the flush)
        // would deadlock rather than fail.
        let feed_bytes = bytes.clone();
        // Return the peer so it stays OPEN until the relay has drained
        // everything: dropping it here would deliver EOF right after the
        // payload, which `stream_flush` correctly reports as a closed backend.
        let feed = tokio::spawn(async move {
            backend_peer.write_all(&feed_bytes).await.unwrap();
            backend_peer.flush().await.unwrap();
            backend_peer
        });
        let bytes_len = bytes.len();
        let drain = tokio::spawn(async move {
            let mut got = vec![0u8; bytes_len];
            client_peer.read_exact(&mut got).await.unwrap();
            got
        });

        backend.readable().await.unwrap();
        // `stream_flush` is non-blocking (`try_read_buf`), so a single call
        // may see only part of the payload if the kernel hasn't finished
        // delivering it yet on a loaded host: loop calls (each `clear()`ing
        // and refilling the same reused buffer, per the doc comment above)
        // until every byte is relayed, bounded by an overall timeout so a
        // real regression fails fast instead of hanging.
        let mut sent: u64 = 0;
        tokio::time::timeout(Duration::from_secs(5), async {
            while (sent as usize) < bytes.len() {
                let n = ProxyServer::stream_flush(&mut client, &mut backend, &session, &state)
                    .await
                    .expect("flush ok");
                sent += n;
                if n == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        })
        .await
        .expect("all bytes must be relayed within the timeout");
        assert_eq!(sent as usize, bytes.len(), "all available bytes relayed");

        let got = tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .expect("client_peer must receive all relayed bytes within the timeout")
            .unwrap();
        assert_eq!(got, bytes, "forwarded bytes must be byte-exact");
        tokio::time::timeout(Duration::from_secs(5), feed)
            .await
            .expect("backend writer must finish within the timeout")
            .unwrap();

        // Nothing left to read: a second call must return immediately with
        // 0 rather than block — that is the whole point of Flush semantics
        // (no ReadyForQuery to wait for, unlike `stream_until_ready`).
        let sent2 = tokio::time::timeout(
            Duration::from_secs(2),
            ProxyServer::stream_flush(&mut client, &mut backend, &session, &state),
        )
        .await
        .expect("stream_flush must not block once the backend has nothing more to say")
        .expect("flush ok");
        assert_eq!(sent2, 0, "no more data means nothing sent");
    }

    // ---- query-rewriting: the rules-engine rewrite contract ----

    #[cfg(feature = "query-rewriting")]
    mod query_rewriting {
        use crate::rewriter::{
            QueryPattern, QueryRewriter, RewriteRule, RewriterConfig, Transformation,
        };

        fn rw_with_table_replace() -> QueryRewriter {
            let rw = QueryRewriter::new(RewriterConfig {
                enabled: true,
                ..Default::default()
            });
            rw.add_rule(
                RewriteRule::build("t")
                    .pattern(QueryPattern::Table("a".to_string()))
                    .transform(Transformation::ReplaceTable {
                        from: "a".to_string(),
                        to: "b".to_string(),
                    })
                    .build(),
            );
            rw
        }

        #[test]
        fn matching_query_is_rewritten() {
            let res = rw_with_table_replace().rewrite("select * from a").unwrap();
            assert!(res.was_rewritten(), "rule did not fire");
            assert!(res.query().contains('b'), "rewritten: {}", res.query());
            assert!(
                !res.query().contains("from a"),
                "still references a: {}",
                res.query()
            );
        }

        #[test]
        fn unmatched_query_is_unchanged() {
            let res = rw_with_table_replace()
                .rewrite("select * from other")
                .unwrap();
            assert!(!res.was_rewritten());
            assert_eq!(res.query(), "select * from other");
        }
    }

    // ---- multi-tenancy: row-filter injection per tenant ----

    #[cfg(feature = "multi-tenancy")]
    mod multi_tenancy {
        use crate::multi_tenancy::{
            IdentificationMethod, IsolationStrategy, MultiTenancyConfig, TenantConfig, TenantId,
            TenantManager, TenantManagerBuilder, TenantQueryTransformer,
        };

        fn manager() -> TenantManager {
            let transformer = TenantQueryTransformer::new().register_tables(&["t"], "tid");
            let tm = TenantManagerBuilder::new()
                .config(MultiTenancyConfig {
                    enabled: true,
                    identification: IdentificationMethod::Header {
                        header_name: "application_name".to_string(),
                    },
                    ..Default::default()
                })
                .query_transformer(transformer)
                .build();
            tm.register_tenant(TenantConfig::new(
                TenantId::new("acme"),
                IsolationStrategy::row("public", "tid"),
            ));
            tm
        }

        #[test]
        fn tenant_table_gets_filter() {
            let res = manager().transform_query("select * from t", &TenantId::new("acme"));
            assert!(res.transformed, "expected a tenant filter to be injected");
            let q = res.query.to_lowercase();
            assert!(
                q.contains("tid") && q.contains("acme"),
                "filter missing: {}",
                res.query
            );
        }

        #[test]
        fn non_tenant_table_passes_through() {
            let res = manager().transform_query("select * from other", &TenantId::new("acme"));
            assert!(!res.transformed);
        }
    }

    // ---- ha-tr: the journal records statements the replay engine reads ----

    #[cfg(feature = "ha-tr")]
    mod ha_tr {
        use crate::transaction_journal::TransactionJournal;
        use crate::NodeId;

        #[tokio::test]
        async fn journal_records_and_windows_a_statement() {
            let j = TransactionJournal::new();
            let from = chrono::Utc::now() - chrono::Duration::seconds(60);
            let tx = uuid::Uuid::new_v4();
            j.begin_transaction(tx, uuid::Uuid::new_v4(), NodeId::new(), 0)
                .await
                .unwrap();
            j.log_statement(
                tx,
                "insert into t values (1)".to_string(),
                Vec::new(),
                None,
                None,
                0,
            )
            .await
            .unwrap();
            let to = chrono::Utc::now() + chrono::Duration::seconds(60);
            let entries = j.entries_in_window(from, to).await;
            assert_eq!(entries.len(), 1, "journaled statement should be in window");
            assert!(entries[0].1.statement.contains("insert"));
        }

        /// The auto-commit write path (`journal_write`) records one
        /// single-statement transaction per write via the fused
        /// `begin_and_log`, with a distinct transaction id per write.
        #[tokio::test]
        async fn journal_write_records_one_auto_commit_tx_per_write() {
            use super::{make_test_session, test_config};
            use crate::server::ProxyServer;

            let server = ProxyServer::new(test_config()).unwrap();
            let session = make_test_session();
            let from = chrono::Utc::now() - chrono::Duration::seconds(60);

            ProxyServer::journal_write(&server.state, &session, "insert into t values (1)").await;
            ProxyServer::journal_write(&server.state, &session, "update t set a = 2").await;

            let to = chrono::Utc::now() + chrono::Duration::seconds(60);
            let entries = server
                .state
                .transaction_journal
                .entries_in_window(from, to)
                .await;
            assert_eq!(entries.len(), 2, "one journal entry per write");
            assert_ne!(
                entries[0].0, entries[1].0,
                "each write gets its own auto-commit transaction id"
            );
            assert_eq!(
                server.state.transaction_journal.active_count().await,
                2,
                "each write is its own uncommitted journal"
            );
            for (_, e) in &entries {
                assert_eq!(e.sequence, 1, "auto-commit journals hold one statement");
            }
        }
    }

    // ---- schema-routing: OLAP vs OLTP workload classification ----

    #[cfg(feature = "schema-routing")]
    mod schema_routing {
        use crate::schema_routing::{QueryAnalyzer, SchemaRegistry};
        use std::sync::Arc;

        fn analyzer() -> QueryAnalyzer {
            QueryAnalyzer::new(Arc::new(SchemaRegistry::new()))
        }

        #[test]
        fn aggregation_group_by_is_analytics() {
            let a = analyzer();
            assert!(a
                .analyze("select count(*) from orders group by region")
                .is_analytics());
        }

        #[test]
        fn simple_point_query_is_not_analytics() {
            let a = analyzer();
            assert!(!a
                .analyze("select * from orders where id = 1")
                .is_analytics());
        }
    }

    /// The session RAII guard must deregister the session and bump the
    /// connections-closed metric when dropped normally.
    #[tokio::test]
    async fn session_guard_deregisters_on_drop() {
        let server = ProxyServer::new(test_config()).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        state.sessions.insert(session.id, session.clone());
        assert_eq!(state.sessions.len(), 1);
        let before = state.metrics.connections_closed.load(Ordering::Relaxed);
        {
            let _g = SessionGuard {
                state: state.clone(),
                session_id: session.id,
                _client_slot: None,
            };
        }
        assert!(state.sessions.is_empty(), "guard must deregister on drop");
        assert_eq!(
            state.metrics.connections_closed.load(Ordering::Relaxed),
            before + 1
        );
    }

    /// The critical property: the guard must deregister even when the connection
    /// task unwinds via a panic (the leak this replaces).
    #[tokio::test]
    async fn session_guard_deregisters_on_panic() {
        let server = ProxyServer::new(test_config()).unwrap();
        let state = server.state.clone();
        let session = make_test_session();
        state.sessions.insert(session.id, session.clone());
        let sid = session.id;
        let st = state.clone();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = SessionGuard {
                state: st,
                session_id: sid,
                _client_slot: None,
            };
            panic!("simulated connection-task panic");
        }));
        assert!(r.is_err(), "closure must have panicked");
        assert!(
            state.sessions.is_empty(),
            "guard must deregister on a panic unwind"
        );
    }

    // ---- [limits] client-connection cap + idle-session timeout ----

    /// With no `[limits] max_client_connections` (the default 0) the permit
    /// pool is absent entirely, so the accept path is byte-for-byte the
    /// historical unbounded one.
    #[test]
    fn client_slots_absent_when_cap_is_zero() {
        let mut config = test_config();
        config.limits.max_client_connections = 0;
        let server = ProxyServer::new(config).unwrap();
        assert!(server.state.client_slots.is_none());
        assert_eq!(server.state.limits.max_client_connections, 0);
    }

    /// A configured cap sizes the permit pool exactly, an exhausted pool is the
    /// accept loop's refuse-and-close path, and the permit parked in the
    /// `SessionGuard` is returned when the session ends.
    #[test]
    fn client_slot_cap_exhausts_and_guard_returns_the_permit() {
        let mut config = test_config();
        config.limits.max_client_connections = 1;
        let server = ProxyServer::new(config).unwrap();
        let state = server.state.clone();
        let sem = state
            .client_slots
            .clone()
            .expect("a non-zero cap must size the permit pool");
        assert_eq!(sem.available_permits(), 1);

        // First connection takes the only slot (exactly what the accept loop does).
        let permit = Arc::clone(&sem)
            .try_acquire_owned()
            .expect("first connection gets the slot");
        // Second connection finds none -> the accept loop refuses it.
        assert!(
            Arc::clone(&sem).try_acquire_owned().is_err(),
            "cap of 1 must refuse the second concurrent connection"
        );

        // The permit lives in the session guard, so it is returned on drop.
        let session = make_test_session();
        state.sessions.insert(session.id, session.clone());
        {
            let _g = SessionGuard {
                state: state.clone(),
                session_id: session.id,
                _client_slot: Some(permit),
            };
            assert_eq!(sem.available_permits(), 0, "slot held for the live session");
        }
        assert_eq!(
            sem.available_permits(),
            1,
            "the slot must be released when the session ends"
        );
    }

    /// The slot must also come back when the connection task unwinds — the
    /// reason the permit is owned by the RAII guard rather than released at the
    /// end of `handle_client`.
    #[test]
    fn client_slot_returned_on_panic_unwind() {
        let mut config = test_config();
        config.limits.max_client_connections = 1;
        let server = ProxyServer::new(config).unwrap();
        let state = server.state.clone();
        let sem = state.client_slots.clone().expect("cap configured");
        let permit = Arc::clone(&sem).try_acquire_owned().unwrap();
        let session = make_test_session();
        state.sessions.insert(session.id, session.clone());
        let sid = session.id;
        let st = state.clone();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _g = SessionGuard {
                state: st,
                session_id: sid,
                _client_slot: Some(permit),
            };
            panic!("simulated connection-task panic");
        }));
        assert!(r.is_err(), "closure must have panicked");
        assert_eq!(
            sem.available_permits(),
            1,
            "the slot must be released on a panic unwind"
        );
    }

    /// The refusal frame a capped-out proxy sends must be a real PostgreSQL
    /// ErrorResponse carrying SQLSTATE 53300 (too_many_connections) at FATAL
    /// severity (which is what marks the connection dead for pgx/npgsql/JDBC),
    /// so a driver reports the condition instead of a bare connection reset.
    #[test]
    fn over_capacity_error_encodes_sqlstate_53300() {
        let bytes = ProxyServer::over_capacity_error_bytes();
        assert_eq!(bytes[0], b'E', "must be an ErrorResponse frame");
        assert!(
            bytes.windows(7).any(|w| w == b"C53300\0"),
            "SQLSTATE field must be 53300"
        );
        assert!(
            bytes.windows(7).any(|w| w == b"SFATAL\0"),
            "severity must be FATAL, as PostgreSQL sends for 53300"
        );
        assert!(String::from_utf8_lossy(&bytes).contains("sorry, too many clients already"));
    }

    /// End-to-end over a real socket: the refusal is written and the connection
    /// is then closed (the client sees the error, then EOF).
    #[tokio::test]
    async fn refuse_over_capacity_writes_error_then_closes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let srv = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            ProxyServer::refuse_over_capacity(
                &mut ClientStream::Plain(sock),
                Duration::from_secs(5),
            )
            .await;
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut buf = Vec::new();
        // Returns only at EOF, which proves the proxy closed the socket.
        client.read_to_end(&mut buf).await.unwrap();
        srv.await.unwrap();
        assert_eq!(buf[0], b'E');
        assert!(buf.windows(7).any(|w| w == b"C53300\0"));
        assert!(String::from_utf8_lossy(&buf).contains("sorry, too many clients already"));
    }

    /// `client_idle_timeout_secs = 0` (the default) must leave the query loop's
    /// client read unbounded exactly as before — no deadline is armed.
    #[test]
    fn idle_timeout_disabled_at_zero() {
        let mut config = test_config();
        config.limits.client_idle_timeout_secs = 0;
        let server = ProxyServer::new(config).unwrap();
        assert!(
            server.state.limits.client_idle_timeout.is_none(),
            "0 must disable the idle-session timeout"
        );
        // And the default config resolves the same way.
        assert!(ResolvedLimits::default().client_idle_timeout.is_none());
    }

    /// A non-zero `client_idle_timeout_secs` resolves to the deadline the query
    /// loop arms once per idle wait.
    #[test]
    fn idle_timeout_armed_when_configured() {
        let mut config = test_config();
        config.limits.client_idle_timeout_secs = 45;
        let server = ProxyServer::new(config).unwrap();
        assert_eq!(
            server.state.limits.client_idle_timeout,
            Some(Duration::from_secs(45))
        );
    }

    /// The frame sent to a session killed by the idle timeout must carry
    /// SQLSTATE 57P05 (idle_session_timeout) with PostgreSQL's wording.
    #[test]
    fn idle_session_timeout_error_encodes_sqlstate_57p05() {
        let bytes = ProxyServer::idle_session_timeout_error_bytes();
        assert_eq!(bytes[0], b'E', "must be an ErrorResponse frame");
        assert!(
            bytes.windows(7).any(|w| w == b"C57P05\0"),
            "SQLSTATE field must be 57P05"
        );
        assert!(
            bytes.windows(7).any(|w| w == b"SFATAL\0"),
            "severity must be FATAL, as PostgreSQL sends for 57P05"
        );
        assert!(String::from_utf8_lossy(&bytes)
            .contains("terminating connection due to idle-session timeout"));
    }

    // ---- wiring: admission control, the idle deadline, and the read path ----

    /// Startup-message bytes for a plain (non-TLS) client: len + protocol
    /// version 3.0 + a `user` parameter.
    fn startup_bytes(user: &str) -> Vec<u8> {
        let mut params = Vec::new();
        params.extend_from_slice(b"user\0");
        params.extend_from_slice(user.as_bytes());
        params.extend_from_slice(b"\0\0");
        let mut out = Vec::new();
        out.extend_from_slice(&((8 + params.len()) as u32).to_be_bytes());
        out.extend_from_slice(&196608u32.to_be_bytes()); // 3.0
        out.extend_from_slice(&params);
        out
    }

    /// CancelRequest bytes: len(16) + code 80877102 + pid + key.
    fn cancel_bytes(pid: u32, key: u32) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&16u32.to_be_bytes());
        out.extend_from_slice(&80877102u32.to_be_bytes());
        out.extend_from_slice(&pid.to_be_bytes());
        out.extend_from_slice(&key.to_be_bytes());
        out
    }

    fn startup_msg() -> StartupMessage {
        StartupMessage::Startup {
            protocol_version: 196608,
            params: HashMap::new(),
        }
    }

    /// Admission control: a real Startup takes a slot; when none is free it is
    /// refused and `connections_rejected` counts it.
    #[test]
    fn admission_takes_a_slot_and_counts_a_refusal() {
        let mut config = test_config();
        config.limits.max_client_connections = 1;
        let server = ProxyServer::new(config).unwrap();
        let state = server.state.clone();

        let slot = ProxyServer::admit_client_slot(&state, &startup_msg())
            .expect("the first connection is admitted");
        assert!(slot.is_some(), "a configured cap must hand out a slot");
        assert_eq!(
            state.metrics.connections_rejected.load(Ordering::Relaxed),
            0
        );

        // Cap saturated: the next Startup is refused and counted.
        assert!(ProxyServer::admit_client_slot(&state, &startup_msg()).is_err());
        assert_eq!(
            state.metrics.connections_rejected.load(Ordering::Relaxed),
            1,
            "a refusal must increment connections_rejected"
        );

        // Releasing the slot re-admits.
        drop(slot);
        assert!(ProxyServer::admit_client_slot(&state, &startup_msg()).is_ok());
    }

    /// A CancelRequest must be admitted even with the cap saturated — it is a
    /// throwaway connection that never becomes a session, and refusing it would
    /// make query cancellation impossible exactly when it is needed. It must
    /// also never consume a slot, nor count as a rejection.
    #[test]
    fn cancel_request_is_admitted_while_the_cap_is_saturated() {
        let mut config = test_config();
        config.limits.max_client_connections = 1;
        let server = ProxyServer::new(config).unwrap();
        let state = server.state.clone();
        let sem = state.client_slots.clone().expect("cap configured");
        let _held = Arc::clone(&sem).try_acquire_owned().unwrap();
        assert_eq!(sem.available_permits(), 0, "cap is saturated");

        let slot = ProxyServer::admit_client_slot(
            &state,
            &StartupMessage::CancelRequest { pid: 1, key: 2 },
        )
        .expect("a cancel request must never be refused");
        assert!(slot.is_none(), "a cancel request must not consume a slot");
        assert_eq!(
            state.metrics.connections_rejected.load(Ordering::Relaxed),
            0,
            "a cancel request is not a rejection"
        );
    }

    /// Over the wire through `handle_client`: with the cap saturated a client
    /// that sends a real Startup gets the 53300 FATAL frame then EOF, and the
    /// rejection is counted — while a client that sends a CancelRequest is
    /// served (no error frame) and never touches the cap.
    #[tokio::test]
    async fn handle_client_refuses_startup_but_serves_cancel_when_saturated() {
        let mut config = test_config();
        config.limits.max_client_connections = 1;
        let server = ProxyServer::new(config.clone()).unwrap();
        let state = server.state.clone();
        let sem = state.client_slots.clone().expect("cap configured");
        let _held = Arc::clone(&sem).try_acquire_owned().unwrap();
        let (shutdown_tx, _rx) = broadcast::channel(1);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // --- a real Startup while saturated: refused with 53300 ---
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (sock, peer) = listener.accept().await.unwrap();
        let h = tokio::spawn(ProxyServer::handle_client(
            sock,
            peer,
            state.clone(),
            Arc::new(config.clone()),
            shutdown_tx.clone(),
        ));
        client.write_all(&startup_bytes("alice")).await.unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        h.await.unwrap().unwrap();
        assert_eq!(buf[0], b'E', "refused client must get an ErrorResponse");
        assert!(
            buf.windows(7).any(|w| w == b"C53300\0"),
            "refusal must carry SQLSTATE 53300"
        );
        assert_eq!(
            state.metrics.connections_rejected.load(Ordering::Relaxed),
            1
        );
        assert!(state.sessions.is_empty(), "no session may be left behind");

        // --- a CancelRequest while still saturated: served, not refused ---
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (sock, peer) = listener.accept().await.unwrap();
        let h = tokio::spawn(ProxyServer::handle_client(
            sock,
            peer,
            state.clone(),
            Arc::new(config.clone()),
            shutdown_tx.clone(),
        ));
        client.write_all(&cancel_bytes(42, 43)).await.unwrap();
        let mut buf = Vec::new();
        client.read_to_end(&mut buf).await.unwrap();
        h.await.unwrap().unwrap();
        assert!(
            buf.is_empty(),
            "a cancel request must not be answered with an error frame, got {:?}",
            String::from_utf8_lossy(&buf)
        );
        assert_eq!(
            state.metrics.connections_rejected.load(Ordering::Relaxed),
            1,
            "a cancel request must not count as a rejection"
        );
        assert_eq!(sem.available_permits(), 0, "the cap is still saturated");
    }

    /// The idle deadline is armed ONLY when the session is genuinely waiting
    /// for a new command: never mid-COPY (a paused COPY FROM STDIN producer
    /// must not be killed and the bulk load aborted) and never with a partially
    /// received message in the buffer (a client trickling a large statement is
    /// slow, not idle).
    #[test]
    fn idle_deadline_armed_only_at_a_message_boundary() {
        let mut config = test_config();
        config.limits.client_idle_timeout_secs = 30;
        let server = ProxyServer::new(config).unwrap();
        let state = server.state.clone();
        let session = make_test_session();

        let empty = BytesMut::new();
        assert!(
            ProxyServer::client_idle_deadline(&state, &empty, &session).is_some(),
            "an idle session at a message boundary is armed"
        );

        // Half a message received: not idle, still arriving.
        let mut partial = BytesMut::new();
        partial.extend_from_slice(b"Q\0\0\0");
        assert!(
            ProxyServer::client_idle_deadline(&state, &partial, &session).is_none(),
            "a partially received message must not arm the idle timeout"
        );

        // COPY FROM STDIN in progress: the client may legitimately pause.
        session
            .copy_in_progress
            .store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(
            ProxyServer::client_idle_deadline(&state, &empty, &session).is_none(),
            "a COPY FROM STDIN must not be killed by the idle timeout"
        );
        session
            .copy_in_progress
            .store(false, std::sync::atomic::Ordering::Relaxed);

        // Disabled (the default) arms nothing at all.
        let server = ProxyServer::new(test_config()).unwrap();
        assert!(
            ProxyServer::client_idle_deadline(&server.state, &empty, &session).is_none(),
            "client_idle_timeout_secs = 0 must never arm a deadline"
        );
    }

    /// The idle deadline actually fires on the plain client read, and does not
    /// fire when the client speaks in time.
    #[tokio::test]
    async fn read_client_bytes_times_out_when_idle() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (sock, _) = listener.accept().await.unwrap();
        let mut stream = ClientStream::Plain(sock);
        let mut buffer = BytesMut::with_capacity(64);

        let deadline = tokio::time::Instant::now() + Duration::from_millis(60);
        let outcome = ProxyServer::read_client_bytes(&mut stream, &mut buffer, Some(deadline))
            .await
            .unwrap();
        assert_eq!(outcome, ClientRead::IdleTimeout);
        assert!(buffer.is_empty());

        // A client that speaks before the deadline is not timed out.
        client.write_all(b"hello").await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let outcome = ProxyServer::read_client_bytes(&mut stream, &mut buffer, Some(deadline))
            .await
            .unwrap();
        assert_eq!(outcome, ClientRead::Bytes(5));
        assert_eq!(&buffer[..], b"hello");
    }

    /// …and it fires the same way while the session's cached backend connection
    /// is being watched for unsolicited traffic (the `select!` arm), which is
    /// the state an idle pooled session actually sits in.
    #[tokio::test]
    async fn read_next_client_message_times_out_while_watching_the_backend() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _client = TcpStream::connect(addr).await.unwrap();
        let (sock, _) = listener.accept().await.unwrap();
        let mut stream = ClientStream::Plain(sock);

        // A quiet "backend" socket for the session to watch.
        let blistener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let baddr = blistener.local_addr().unwrap();
        let backend = TcpStream::connect(baddr).await.unwrap();
        let _backend_peer = blistener.accept().await.unwrap();
        let mut conns: HashMap<String, BackendConn> = HashMap::new();
        conns.insert("node-a".to_string(), BackendConn::new(backend));

        let server = ProxyServer::new(test_config()).unwrap();
        let mut buffer = BytesMut::with_capacity(64);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(60);
        let outcome = ProxyServer::read_next_client_message(
            &mut stream,
            &mut buffer,
            &mut conns,
            Some("node-a"),
            &mut BytesMut::with_capacity(16384),
            Some(deadline),
            &server.state,
        )
        .await
        .unwrap();
        assert_eq!(outcome, ClientRead::IdleTimeout);
        assert!(
            conns.contains_key("node-a"),
            "a live backend must not be dropped by the idle timeout"
        );
    }

    /// Backend frames are only forwarded to the client once they are COMPLETE.
    /// A truncated asynchronous frame (a large `NotificationResponse` split
    /// across reads, say) must stay buffered: a client that receives half a
    /// frame blocks forever waiting for the rest, and recovery can no longer
    /// inject anything after it — not an ErrorResponse, and not the rows of a
    /// re-executed statement, which would be appended inside the partial frame.
    #[tokio::test]
    async fn watch_relay_withholds_partial_backend_frames() {
        use tokio::io::AsyncReadExt as _;
        use tokio::io::AsyncWriteExt as _;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client_peer = TcpStream::connect(addr).await.unwrap();
        let (sock, _) = listener.accept().await.unwrap();
        let mut stream = ClientStream::Plain(sock);

        let blistener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let baddr = blistener.local_addr().unwrap();
        let backend = TcpStream::connect(baddr).await.unwrap();
        let (mut backend_peer, _) = blistener.accept().await.unwrap();
        let mut conns: HashMap<String, BackendConn> = HashMap::new();
        conns.insert("node-a".to_string(), BackendConn::new(backend));

        let server = ProxyServer::new(test_config()).unwrap();
        let mut buffer = BytesMut::with_capacity(64);
        // One `NotificationResponse`, delivered in two pieces.
        let payload = b"\x00\x00\x27\x0fchan\0hello\0".to_vec();
        let mut whole = vec![b'A'];
        whole.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
        whole.extend_from_slice(&payload);
        let split = whole.len() - 3;
        backend_peer.write_all(&whole[..split]).await.unwrap();
        backend_peer.flush().await.unwrap();

        // The relay sees the head of the frame and must publish nothing.
        let mut abuf = BytesMut::with_capacity(16384);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(120);
        let outcome = ProxyServer::read_next_client_message(
            &mut stream,
            &mut buffer,
            &mut conns,
            Some("node-a"),
            &mut abuf,
            Some(deadline),
            &server.state,
        )
        .await
        .unwrap();
        assert_eq!(outcome, ClientRead::IdleTimeout);
        assert_eq!(
            abuf.len(),
            split,
            "the partial frame must be retained for reassembly"
        );
        let mut peek = [0u8; 64];
        let seen =
            tokio::time::timeout(Duration::from_millis(50), client_peer.read(&mut peek)).await;
        assert!(
            seen.is_err(),
            "no bytes of an incomplete frame may reach the client"
        );

        // The remainder completes the frame: now the whole thing is published.
        backend_peer.write_all(&whole[split..]).await.unwrap();
        backend_peer.flush().await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(120);
        let outcome = ProxyServer::read_next_client_message(
            &mut stream,
            &mut buffer,
            &mut conns,
            Some("node-a"),
            &mut abuf,
            Some(deadline),
            &server.state,
        )
        .await
        .unwrap();
        assert_eq!(outcome, ClientRead::IdleTimeout);
        assert!(abuf.is_empty(), "a fully forwarded frame leaves no tail");
        let mut got = vec![0u8; whole.len()];
        tokio::time::timeout(Duration::from_millis(200), client_peer.read_exact(&mut got))
            .await
            .expect("the completed frame must be delivered")
            .unwrap();
        assert_eq!(got, whole, "the frame must arrive byte-exact");
    }

    /// The TR-03 allowlists are binary-searched, so they must stay sorted and
    /// lowercase, and no side-effecting name may creep in.
    #[test]
    fn tr_pure_builtins_are_sorted_and_exclude_side_effects() {
        for list in [TR_PURE_BUILTINS, TR_CALL_KEYWORDS] {
            assert!(list.windows(2).all(|w| w[0] < w[1]), "sorted, unique");
            assert!(list.iter().all(|n| *n == n.to_ascii_lowercase()));
        }
        for bad in [
            "nextval",
            "setval",
            "currval",
            "lastval",
            "pg_notify",
            "set_config",
            "pg_advisory_lock",
            "pg_try_advisory_lock",
            "txid_current",
            "setseed",
            "lo_import",
            "pg_terminate_backend",
            "dblink",
            "pg_reload_conf",
        ] {
            assert!(
                TR_PURE_BUILTINS.binary_search(&bad).is_err(),
                "{bad} must not be pure"
            );
        }
    }

    /// H-07: every relay validates a backend frame header the moment it is
    /// readable. A length below the 4-byte minimum or above the configured
    /// budget is an error immediately — never "wait for more bytes", and never
    /// an accumulator growing toward the advertised size.
    #[test]
    fn backend_frame_len_rejects_malformed_and_oversize_headers() {
        // Incomplete header: undecided, not an error.
        assert!(backend_frame_len(&[b'D', 0, 0], 1024).unwrap().is_none());
        assert!(backend_frame_len(&[], 1024).unwrap().is_none());
        // Smallest legal frame (ReadyForQuery, len 5) passes a budget of 5.
        assert_eq!(
            backend_frame_len(&[b'Z', 0, 0, 0, 5, b'I'], 5).unwrap(),
            Some(5)
        );
        // Below the self-counting minimum: malformed, fail closed now.
        for len in [0u32, 1, 2, 3] {
            let mut h = vec![b'D'];
            h.extend_from_slice(&len.to_be_bytes());
            assert!(backend_frame_len(&h, 1024).is_err(), "len {len}");
        }
        // Above the budget: refused before any accumulation.
        let mut big = vec![b'D'];
        big.extend_from_slice(&1025u32.to_be_bytes());
        assert!(backend_frame_len(&big, 1024).is_err());
        assert_eq!(backend_frame_len(&big, 1025).unwrap(), Some(1025));
        // usize::MAX budget must not overflow the `len + 1` frame arithmetic.
        let mut max = vec![b'D'];
        max.extend_from_slice(&u32::MAX.to_be_bytes());
        assert_eq!(
            backend_frame_len(&max, usize::MAX).unwrap(),
            Some(u32::MAX as usize)
        );
    }

    /// A streaming relay hits a malformed header and fails immediately with a
    /// protocol error, instead of waiting out the read timeout for bytes that
    /// can never complete the frame.
    #[tokio::test]
    async fn stream_until_ready_fails_fast_on_malformed_backend_frame() {
        use tokio::io::AsyncWriteExt as _;
        let (mut client_side, _client_peer) = tokio::io::duplex(4096);
        let _ = &mut client_side;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client_sock = TcpStream::connect(addr).await.unwrap();
        let (sock, _) = listener.accept().await.unwrap();
        let mut client = ClientStream::Plain(sock);
        let _keep = client_sock;

        let blistener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let baddr = blistener.local_addr().unwrap();
        let mut backend = TcpStream::connect(baddr).await.unwrap();
        let (mut backend_peer, _) = blistener.accept().await.unwrap();

        let mut config = test_config();
        config.limits.backend_read_timeout_secs = 30; // would be the old stall
        let server = ProxyServer::new(config).unwrap();
        let session = make_test_session();

        // Tag 'D' with a declared length of 2: can never be a valid frame.
        backend_peer.write_all(&[b'D', 0, 0, 0, 2]).await.unwrap();
        backend_peer.flush().await.unwrap();
        let started = std::time::Instant::now();
        let r = tokio::time::timeout(
            Duration::from_secs(5),
            ProxyServer::stream_until_ready(&mut client, &mut backend, &session, &server.state),
        )
        .await
        .expect("must not wait out the 30 s read timeout");
        assert!(
            matches!(r, Err(ref f) if matches!(f.error, ProxyError::Protocol(_))),
            "malformed frame must be a protocol error: {r:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    /// The out-of-band re-prepare reader refuses an oversize declared body
    /// without allocating it: an advertised 4 GiB frame used to be a
    /// `vec![0u8; len]` sized by the backend.
    #[tokio::test]
    async fn read_one_frame_type_refuses_oversize_without_allocating() {
        use tokio::io::AsyncWriteExt as _;
        let (mut a, mut b) = tokio::io::duplex(64);
        let mut hdr = vec![b'1'];
        hdr.extend_from_slice(&u32::MAX.to_be_bytes());
        b.write_all(&hdr).await.unwrap();
        let r = ProxyServer::read_one_frame_type(&mut a, 1024 * 1024).await;
        assert!(matches!(r, Err(ProxyError::Protocol(_))), "{r:?}");
        // Within budget, a body larger than the scratch buffer is discarded
        // in chunks and the type byte comes back.
        let (mut a2, mut b2) = tokio::io::duplex(64 * 1024);
        let body = vec![7u8; 40_000];
        let mut frame = vec![b'1'];
        frame.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        frame.extend_from_slice(&[b'Z', 0, 0, 0, 5, b'I']);
        tokio::spawn(async move { b2.write_all(&frame).await.unwrap() });
        assert_eq!(
            ProxyServer::read_one_frame_type(&mut a2, 1024 * 1024)
                .await
                .unwrap(),
            b'1'
        );
        assert_eq!(
            ProxyServer::read_one_frame_type(&mut a2, 1024 * 1024)
                .await
                .unwrap(),
            b'Z'
        );
    }

    /// The frame scanner returns only whole frames and rejects a length below
    /// the 4-byte minimum instead of stalling reassembly on it.
    #[test]
    fn complete_frame_prefix_stops_at_frame_boundaries() {
        fn f(tag: u8, body: &[u8]) -> Vec<u8> {
            let mut v = vec![tag];
            v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
            v.extend_from_slice(body);
            v
        }
        let a = f(b'A', b"one");
        let b = f(b'N', b"two");
        let both = [a.clone(), b.clone()].concat();
        assert_eq!(
            ProxyServer::complete_frame_prefix(&both, usize::MAX).unwrap(),
            both.len()
        );
        assert_eq!(
            ProxyServer::complete_frame_prefix(&a, usize::MAX).unwrap(),
            a.len()
        );
        // A trailing partial frame is excluded, whole ones ahead of it are not.
        let partial = [both.clone(), a[..3].to_vec()].concat();
        assert_eq!(
            ProxyServer::complete_frame_prefix(&partial, usize::MAX).unwrap(),
            both.len()
        );
        // Fewer than the 5 header bytes: nothing is complete.
        assert_eq!(
            ProxyServer::complete_frame_prefix(&a[..4], usize::MAX).unwrap(),
            0
        );
        assert_eq!(
            ProxyServer::complete_frame_prefix(&[], usize::MAX).unwrap(),
            0
        );
        // Length below the self-counting minimum is malformed, not incomplete.
        assert!(ProxyServer::complete_frame_prefix(&[b'A', 0, 0, 0, 3], usize::MAX).is_err());
        // A huge declared length is merely incomplete; the caller's buffer cap
        // is what bounds it.
        assert_eq!(
            ProxyServer::complete_frame_prefix(&[b'A', 0xff, 0xff, 0xff, 0xff, 1, 2], usize::MAX)
                .unwrap(),
            0
        );
    }

    /// The conditional-reset classifier must call every session-state-creating
    /// statement DIRTY (so it is reset before reuse) and only provably neutral
    /// statements CLEAN. A false "clean" would leak state across clients, so the
    /// dirty cases here are the security-critical half of the test.
    #[cfg(feature = "pool-modes")]
    #[test]
    fn stmt_classifier_is_conservative() {
        let clean = ProxyServer::stmt_leaves_session_state;
        // ---- Provably clean (reset may be skipped) ----
        assert!(!clean(
            "SELECT abalance FROM pgbench_accounts WHERE aid = 12345"
        ));
        assert!(!clean("SELECT 1"));
        assert!(!clean("SELECT 1;")); // single trailing ';'
        assert!(!clean("  select now()  ")); // read of a volatile fn: no session state
        assert!(!clean("INSERT INTO t VALUES (1)")); // INTO is INSERT syntax, not SELECT INTO
        assert!(!clean("UPDATE t SET c = 1 WHERE id = 2")); // "SET" is UPDATE syntax, not a GUC
        assert!(!clean("DELETE FROM t WHERE id = 3"));
        assert!(!clean("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(!clean("SELECT into_total FROM ledger")); // column named into_total, not INTO kw
        assert!(!clean("BEGIN"));
        assert!(!clean("COMMIT"));
        assert!(!clean("SELECT current_setting('work_mem')")); // reading a GUC is fine

        // ---- Must be DIRTY (reset required) ----
        assert!(clean("SET work_mem = '1GB'"), "SET GUC");
        assert!(clean("set search_path to public"), "lowercase SET");
        assert!(clean("CREATE TEMP TABLE t(x int)"), "temp table");
        assert!(clean("CREATE TEMPORARY TABLE t(x int)"), "temp table");
        assert!(clean("SELECT * INTO TEMP t FROM src"), "SELECT INTO temp");
        assert!(clean("select a into t from s"), "SELECT INTO lowercase");
        assert!(clean("PREPARE p AS SELECT 1"), "prepared statement");
        assert!(clean("DEALLOCATE p"), "deallocate");
        assert!(
            clean("DECLARE c CURSOR WITH HOLD FOR SELECT 1"),
            "held cursor"
        );
        assert!(clean("LISTEN my_channel"), "listen");
        assert!(clean("SELECT pg_advisory_lock(42)"), "advisory lock");
        assert!(clean("SELECT pg_try_advisory_lock(1)"), "try advisory lock");
        assert!(
            clean("SELECT set_config('work_mem','1GB',false)"),
            "set_config fn"
        );
        assert!(clean("SELECT nextval('s')"), "sequence cache");
        assert!(clean("SET ROLE admin"), "set role");
        assert!(clean("SET SESSION AUTHORIZATION bob"), "session auth");
        assert!(clean("DISCARD ALL"), "explicit discard");
        assert!(clean("RESET ALL"), "reset");
        // Multi-statement: a neutral lead cannot vouch for what follows a ';'.
        assert!(clean("SELECT 1; SET work_mem='1GB'"), "hidden SET after ;");
        assert!(
            clean("SELECT 1; CREATE TEMP TABLE t(x int)"),
            "hidden temp after ;"
        );
        // ';' inside a literal → conservatively dirty (safe over-reset).
        assert!(clean("SELECT 'a;b'"), "semicolon in literal");
        // Non-neutral leads.
        assert!(clean("COPY t FROM STDIN"), "copy");
        assert!(clean("GRANT SELECT ON t TO bob"), "grant");
        assert!(clean("ALTER TABLE t ADD COLUMN c int"), "ddl");
    }

    /// Regression for the single-lowercase-pass rewrite of the
    /// `DIRTY_TOKENS` scan: `set_config`/`advisory`/`nextval`/`setval` must
    /// still be matched case-insensitively no matter how the caller casts
    /// them, exactly as the old per-token `contains_ci` scan did. Also
    /// checks the lowercasing buffer doesn't panic or fold non-ASCII bytes.
    #[cfg(feature = "pool-modes")]
    #[test]
    fn stmt_classifier_dirty_tokens_stay_case_insensitive() {
        // `dirty(sql) == true` means `stmt_leaves_session_state` reports the
        // statement as session-state-creating (reset required before reuse).
        let dirty = ProxyServer::stmt_leaves_session_state;
        assert!(dirty("SELECT PG_ADVISORY_LOCK(1)"), "uppercase advisory");
        assert!(dirty("select Pg_Advisory_Unlock(1)"), "mixed-case advisory");
        assert!(dirty("SELECT NEXTVAL('s')"), "uppercase nextval");
        assert!(dirty("SELECT SETVAL('s', 1)"), "uppercase setval");
        assert!(
            dirty("SELECT Set_Config('work_mem','1GB',false)"),
            "mixed-case set_config"
        );
        // Non-ASCII bytes must not panic the lowercasing buffer, and must
        // not be folded into a false match.
        assert!(!dirty("SELECT name FROM café"), "plain non-ASCII select");
    }

    /// `reset_backend` must only report success when the reset query cleanly
    /// completed — no ErrorResponse and an idle ReadyForQuery. A poisoned reset
    /// (error, or a non-idle transaction status) must return `Err` so the caller
    /// drops the connection instead of parking it dirty (Group 2, 2.0.b).
    #[cfg(feature = "pool-modes")]
    #[tokio::test]
    async fn reset_backend_rejects_error_and_nonidle() {
        use tokio::io::AsyncWriteExt as _;
        fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
            let mut v = vec![tag];
            v.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
            v.extend_from_slice(body);
            v
        }
        let rfq = |st: u8| frame(b'Z', &[st]);
        let cc = frame(b'C', b"DISCARD ALL\0");
        let err = frame(b'E', b"SERROR\0C25P02\0Mreset failed\0\0");

        // Clean: CommandComplete + ReadyForQuery('I') -> Ok.
        let (mut client, mut server) = tokio::io::duplex(4096);
        let mut resp = cc.clone();
        resp.extend_from_slice(&rfq(b'I'));
        server.write_all(&resp).await.unwrap();
        assert!(
            ProxyServer::reset_backend(
                &mut client,
                "DISCARD ALL",
                Duration::from_secs(30),
                usize::MAX
            )
            .await
            .is_ok(),
            "clean reset must succeed"
        );

        // ErrorResponse before RFQ -> Err (connection is poisoned).
        let (mut client, mut server) = tokio::io::duplex(4096);
        let mut resp = err.clone();
        resp.extend_from_slice(&rfq(b'I'));
        server.write_all(&resp).await.unwrap();
        assert!(
            ProxyServer::reset_backend(
                &mut client,
                "DISCARD ALL",
                Duration::from_secs(30),
                usize::MAX
            )
            .await
            .is_err(),
            "reset that errored must be rejected"
        );

        // Non-idle status ('T') -> Err (still in a transaction).
        let (mut client, mut server) = tokio::io::duplex(4096);
        let mut resp = cc.clone();
        resp.extend_from_slice(&rfq(b'T'));
        server.write_all(&resp).await.unwrap();
        assert!(
            ProxyServer::reset_backend(
                &mut client,
                "DISCARD ALL",
                Duration::from_secs(30),
                usize::MAX
            )
            .await
            .is_err(),
            "reset leaving a non-idle txn must be rejected"
        );
    }

    /// The pool identity key stays the bare `(node,user,db)` triple when no
    /// routing-relevant startup GUC is set (backward-compatible with existing
    /// pooling), but diverges when a client sets a different `client_encoding` /
    /// `DateStyle` / etc., so such clients never share a connection (Group 2,
    /// 2.0.c).
    #[cfg(feature = "pool-modes")]
    #[tokio::test]
    async fn pool_key_folds_startup_params() {
        let base = make_test_session();
        {
            let mut v = base.variables.write().await;
            v.insert("user".into(), "u".into());
            v.insert("database".into(), "d".into());
        }
        let k_plain = ProxyServer::pool_key_for("n:5432", &base).await;
        assert_eq!(k_plain, crate::pool::pool_key("n:5432", "u", "d"));

        // Same identity but a distinct client_encoding must produce a
        // different key (no cross-encoding sharing).
        let utf8 = make_test_session();
        let latin1 = make_test_session();
        for (s, enc) in [(&utf8, "UTF8"), (&latin1, "LATIN1")] {
            let mut v = s.variables.write().await;
            v.insert("user".into(), "u".into());
            v.insert("database".into(), "d".into());
            v.insert("client_encoding".into(), enc.into());
        }
        let k_utf8 = ProxyServer::pool_key_for("n:5432", &utf8).await;
        let k_latin1 = ProxyServer::pool_key_for("n:5432", &latin1).await;
        assert_ne!(k_utf8, k_latin1, "different client_encoding must not share");
        assert_ne!(k_utf8, k_plain, "GUC-bearing key must differ from bare key");
    }

    /// A declared backend frame length within the cap is accepted — this
    /// must keep passing for every legitimate frame the auth-phase scanners
    /// see today (S5-backend-auth-frame-cap).
    #[test]
    fn test_validate_backend_frame_len_within_cap_ok() {
        assert!(validate_backend_frame_len(4, 1024).is_ok());
        assert!(validate_backend_frame_len(1024, 1024).is_ok());
    }

    /// A hostile/compromised backend declaring a length far past the
    /// configured cap (e.g. len=0xFFFFFFFF, which would otherwise grow the
    /// scanner's accumulation buffer toward 4 GiB) must be rejected instead
    /// of silently accepted. This is the regression case for
    /// S5-backend-auth-frame-cap: on the old code (no comparison against any
    /// cap at all) this assertion fails because there was no length check to
    /// call.
    #[test]
    fn test_validate_backend_frame_len_exceeds_cap_rejected() {
        let err = validate_backend_frame_len(0xFFFF_FFFF, 1024)
            .expect_err("oversized backend frame length must be rejected");
        assert!(matches!(err, ProxyError::Protocol(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("4294967295"),
            "message should name the offending length: {msg}"
        );
        assert!(
            msg.contains("1024"),
            "message should name the configured max: {msg}"
        );
    }

    /// Boundary: exactly at the cap is still allowed (only strictly-greater
    /// is rejected), matching the `len > max_message_size` boundary
    /// `ProtocolCodec` already uses on the decoded path.
    #[test]
    fn test_validate_backend_frame_len_boundary() {
        assert!(validate_backend_frame_len(1024, 1024).is_ok());
        assert!(validate_backend_frame_len(1025, 1024).is_err());
    }

    /// Verbatim copy of the pre-optimisation `anomaly_fingerprint`,
    /// which built a fresh `String` per call. The reusable-buffer
    /// rewrite must produce byte-identical fingerprints.
    #[cfg(feature = "anomaly-detection")]
    fn legacy_anomaly_fingerprint(sql: &str) -> String {
        let mut out = String::with_capacity(sql.len());
        let mut in_single = false;
        let mut prev_space = false;
        let mut chars = sql.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '\'' {
                in_single = !in_single;
                if in_single {
                    out.push('?');
                    while let Some(&n) = chars.peek() {
                        chars.next();
                        if n == '\'' {
                            in_single = false;
                            break;
                        }
                    }
                    prev_space = false;
                    continue;
                }
            }
            if c.is_ascii_digit() {
                if !out.ends_with('?') {
                    out.push('?');
                }
                while matches!(chars.peek(), Some(c) if c.is_ascii_digit() || *c == '.') {
                    chars.next();
                }
                prev_space = false;
                continue;
            }
            if c.is_ascii_whitespace() {
                if !prev_space && !out.is_empty() {
                    out.push(' ');
                    prev_space = true;
                }
                continue;
            }
            out.push(c.to_ascii_lowercase());
            prev_space = false;
        }
        out.trim_end().to_string()
    }

    #[cfg(feature = "anomaly-detection")]
    const FINGERPRINT_CORPUS: &[&str] = &[
        "",
        "   ",
        "SELECT 1",
        "SELECT * FROM users WHERE id = 1",
        "SELECT * FROM users WHERE id = 99",
        "select   *\n from\tusers  where name = 'bob'   ",
        "INSERT INTO t VALUES (1, 2.5, 'a''b', NULL)",
        "SELECT '' FROM t",
        "SELECT 'unterminated FROM t",
        "UPDATE t SET x = 3.14159 WHERE y = 'Ünïcode'",
        "SELECT * FROM «таблица» WHERE имя = 'ЗНАЧЕНИЕ'",
        "SELECT * FROM t WHERE n = 1 OR 1=1 -- 💥",
        "SELECT 1;",
        "\n\n\t",
    ];

    #[cfg(feature = "anomaly-detection")]
    #[test]
    fn anomaly_fingerprint_matches_legacy_implementation() {
        for sql in FINGERPRINT_CORPUS {
            assert_eq!(
                anomaly_fingerprint(sql),
                legacy_anomaly_fingerprint(sql),
                "fingerprint diverged for {:?}",
                sql
            );
        }
    }

    #[cfg(feature = "anomaly-detection")]
    #[test]
    fn anomaly_fingerprint_into_reuses_buffer_without_residue() {
        let mut buf = String::new();
        // A reused buffer must yield exactly what a fresh one does,
        // in any order — no leftovers from the previous statement.
        for sql in FINGERPRINT_CORPUS {
            anomaly_fingerprint_into(sql, &mut buf);
            assert_eq!(buf, legacy_anomaly_fingerprint(sql), "for {:?}", sql);
        }
        anomaly_fingerprint_into("SELECT a_very_long_identifier FROM some_table", &mut buf);
        let grown = buf.capacity();
        anomaly_fingerprint_into("SELECT 1", &mut buf);
        assert_eq!(buf, "select ?");
        assert_eq!(
            buf.capacity(),
            grown,
            "capacity should be reused, not reset"
        );
    }

    /// The fingerprint normalises literals, so queries differing only
    /// in their literal values collapse to one shape — the property
    /// the novel-query detector depends on.
    #[cfg(feature = "anomaly-detection")]
    #[test]
    fn anomaly_fingerprint_collapses_literals() {
        let mut buf = String::new();
        anomaly_fingerprint_into("SELECT * FROM users WHERE id = 1", &mut buf);
        let a = buf.clone();
        anomaly_fingerprint_into("select * from USERS where id = 99", &mut buf);
        assert_eq!(a, buf);
        assert_eq!(a, "select * from users where id = ?");
    }

    // ---- In-session Transaction Replay (tr_mode) ----

    mod tr_in_session {
        use super::*;
        use crate::config::TrMode;
        use crate::protocol::QueryMessage;

        const MODES: [TrMode; 4] = [
            TrMode::None,
            TrMode::Session,
            TrMode::Select,
            TrMode::Transaction,
        ];
        const PHASES: [FaultPhase; 2] = [FaultPhase::NotDelivered, FaultPhase::OutcomeUnknown];
        const KINDS: [StmtKind; 5] = [
            StmtKind::Read,
            StmtKind::Write,
            StmtKind::Commit,
            StmtKind::Control,
            StmtKind::Other,
        ];

        /// Every cell of the decision table must satisfy the hard rules.
        #[test]
        fn tr_decide_exhaustive_invariants() {
            use TrAction::*;
            let mut cells = 0;
            for mode in MODES {
                for phase in PHASES {
                    for in_tx in [false, true] {
                        for has_writes in [false, true] {
                            for replayable in [false, true] {
                                for kind in KINDS {
                                    cells += 1;
                                    let a = ProxyServer::tr_decide(
                                        mode, phase, in_tx, has_writes, replayable, kind,
                                    );
                                    let ctx = format!(
                                        "{mode:?}/{phase:?}/in_tx={in_tx}/writes={has_writes}/replayable={replayable}/{kind:?} -> {a:?}"
                                    );
                                    // none: always one error, then close.
                                    if mode == TrMode::None {
                                        assert_eq!(a, CloseWithError("57P01"), "{ctx}");
                                        continue;
                                    }
                                    assert!(!matches!(a, CloseWithError(_)), "{ctx}");
                                    // A statement that never ran, outside a
                                    // transaction, is always just re-run.
                                    if phase == FaultPhase::NotDelivered && !in_tx {
                                        assert_eq!(a, Reexecute, "{ctx}");
                                    }
                                    // Never double-apply: an autocommit write/
                                    // opaque statement with unknown outcome is
                                    // never re-executed.
                                    if phase == FaultPhase::OutcomeUnknown
                                        && !in_tx
                                        && matches!(kind, StmtKind::Write | StmtKind::Other)
                                    {
                                        assert_eq!(a, ErrorAndContinue("08007"), "{ctx}");
                                    }
                                    // A COMMIT with unknown outcome is never retried.
                                    if phase == FaultPhase::OutcomeUnknown
                                        && in_tx
                                        && kind == StmtKind::Commit
                                    {
                                        assert_eq!(a, ErrorAndContinue("08007"), "{ctx}");
                                    }
                                    // Replay only ever happens inside a
                                    // transaction that is recorded & replayable.
                                    if a == ReplayThenReexecute {
                                        assert!(in_tx && replayable, "{ctx}");
                                        assert_ne!(mode, TrMode::Session, "{ctx}");
                                        if mode == TrMode::Select {
                                            assert!(!has_writes, "{ctx}");
                                        }
                                    }
                                    // Inside a transaction the ONLY transparent
                                    // outcome is a replay (the tx died with the
                                    // old backend; a bare re-execution would run
                                    // in autocommit).
                                    if in_tx {
                                        assert_ne!(a, Reexecute, "{ctx}");
                                    }
                                    // session mode never replays anything.
                                    if mode == TrMode::Session {
                                        assert!(
                                            matches!(a, ErrorAndContinue(_))
                                                || (phase == FaultPhase::NotDelivered && !in_tx),
                                            "{ctx}"
                                        );
                                    }
                                    // SQLSTATE follows the phase.
                                    if let ErrorAndContinue(code) = a {
                                        match phase {
                                            FaultPhase::NotDelivered => {
                                                assert_eq!(code, "57P01", "{ctx}")
                                            }
                                            FaultPhase::OutcomeUnknown => {
                                                assert_eq!(code, "08007", "{ctx}")
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            assert_eq!(cells, 4 * 2 * 2 * 2 * 2 * 5);
        }

        /// Spot rows straight from the specification table.
        #[test]
        fn tr_decide_spec_rows() {
            use FaultPhase::*;
            use StmtKind::*;
            use TrAction::*;
            let d = ProxyServer::tr_decide;
            // none
            assert_eq!(
                d(TrMode::None, OutcomeUnknown, true, true, true, Read),
                CloseWithError("57P01")
            );
            // session: not-delivered outside tx -> re-execute (any kind)
            assert_eq!(
                d(TrMode::Session, NotDelivered, false, false, false, Write),
                Reexecute
            );
            assert_eq!(
                d(TrMode::Session, NotDelivered, false, false, false, Commit),
                Reexecute
            );
            // session: not-delivered inside tx -> 57P01; unknown -> 08007
            assert_eq!(
                d(TrMode::Session, NotDelivered, true, true, true, Write),
                ErrorAndContinue("57P01")
            );
            assert_eq!(
                d(TrMode::Session, OutcomeUnknown, false, false, false, Read),
                ErrorAndContinue("08007")
            );
            assert_eq!(
                d(TrMode::Session, OutcomeUnknown, true, false, true, Read),
                ErrorAndContinue("08007")
            );
            // select: unknown-outcome read outside tx -> re-execute; write -> 08007
            assert_eq!(
                d(TrMode::Select, OutcomeUnknown, false, false, false, Read),
                Reexecute
            );
            assert_eq!(
                d(TrMode::Select, OutcomeUnknown, false, false, false, Control),
                Reexecute
            );
            assert_eq!(
                d(TrMode::Select, OutcomeUnknown, false, false, false, Write),
                ErrorAndContinue("08007")
            );
            // select: read-only replayable tx -> replay; tx with writes -> error
            assert_eq!(
                d(TrMode::Select, OutcomeUnknown, true, false, true, Read),
                ReplayThenReexecute
            );
            assert_eq!(
                d(TrMode::Select, NotDelivered, true, false, true, Write),
                ReplayThenReexecute
            );
            assert_eq!(
                d(TrMode::Select, OutcomeUnknown, true, true, true, Read),
                ErrorAndContinue("08007")
            );
            assert_eq!(
                d(TrMode::Select, NotDelivered, true, true, true, Read),
                ErrorAndContinue("57P01")
            );
            assert_eq!(
                d(TrMode::Select, OutcomeUnknown, true, false, false, Read),
                ErrorAndContinue("08007")
            );
            // transaction: replay uncommitted tx then re-execute (writes included)
            assert_eq!(
                d(TrMode::Transaction, NotDelivered, true, true, true, Write),
                ReplayThenReexecute
            );
            assert_eq!(
                d(TrMode::Transaction, OutcomeUnknown, true, true, true, Write),
                ReplayThenReexecute
            );
            assert_eq!(
                d(TrMode::Transaction, NotDelivered, true, true, true, Commit),
                ReplayThenReexecute
            );
            // transaction: COMMIT with unknown outcome -> 08007, never retried
            assert_eq!(
                d(
                    TrMode::Transaction,
                    OutcomeUnknown,
                    true,
                    true,
                    true,
                    Commit
                ),
                ErrorAndContinue("08007")
            );
            // transaction: autocommit write unknown -> 08007
            assert_eq!(
                d(
                    TrMode::Transaction,
                    OutcomeUnknown,
                    false,
                    false,
                    false,
                    Write
                ),
                ErrorAndContinue("08007")
            );
            // transaction: non-replayable (over cap) degrades to session behaviour
            assert_eq!(
                d(TrMode::Transaction, NotDelivered, true, true, false, Write),
                ErrorAndContinue("57P01")
            );
            assert_eq!(
                d(TrMode::Transaction, OutcomeUnknown, true, true, false, Read),
                ErrorAndContinue("08007")
            );
        }

        #[test]
        fn tr_classify_table() {
            use StmtKind::*;
            let c = |sql: &str| ProxyServer::tr_classify(sql, &TrReadPolicy::default());
            assert_eq!(c("SELECT 1"), Read);
            assert_eq!(c("  select * from t where x = 'a;b' "), Write);
            assert_eq!(c("SELECT count(*) FROM t;"), Read);
            assert_eq!(c("SELECT CASE WHEN x THEN 1 END FROM t"), Read);
            assert_eq!(c("SELECT * INTO t2 FROM t"), Other);
            assert_eq!(c("SELECT nextval('s')"), Other);
            assert_eq!(c("SELECT pg_sleep(0.5), 42"), Read);
            // TR-03: a read is re-executable only if every call is a known
            // side-effect-free built-in. Anything else may already have run.
            assert_eq!(c("SELECT audit_side_effect()"), Other);
            assert_eq!(c("SELECT my_schema.my_udf(1)"), Other);
            assert_eq!(c("SELECT lower(name), count(*) FROM t"), Read);
            assert_eq!(c("SELECT pg_catalog.upper('x')"), Read);
            assert_eq!(c("SELECT \"MyFn\"(1)"), Other);
            assert_eq!(c("SELECT currval('s')"), Other);
            assert_eq!(c("SELECT pg_notify('c', 'p')"), Other);
            assert_eq!(c("SELECT set_config('a', 'b', false)"), Other);
            assert_eq!(c("SELECT pg_advisory_lock(1)"), Other);
            assert_eq!(c("SELECT random(), now(), gen_random_uuid()"), Read);
            assert_eq!(c("SELECT 'f(x)' AS s, $$g()$$ FROM t"), Read); // calls only in literals
            assert_eq!(
                c("WITH x AS (SELECT audit_side_effect()) SELECT * FROM x"),
                Other
            );
            assert_eq!(
                c("SELECT * FROM t WHERE id IN (1, 2) AND EXISTS (SELECT 1)"),
                Read
            );
            assert_eq!(c("SELECT CAST(x AS int), COALESCE(a, b) FROM t"), Read);
            // Operator-listed UDFs become eligible.
            let policy = TrReadPolicy::from_config(&["Audit_Side_Effect".to_string()]);
            assert_eq!(
                ProxyServer::tr_classify("SELECT audit_side_effect()", &policy),
                Read
            );
            assert_eq!(
                ProxyServer::tr_classify("SELECT other_udf()", &policy),
                Other
            );
            assert_eq!(c("SHOW application_name"), Read);
            assert_eq!(c("VALUES (1)"), Read);
            assert_eq!(c("TABLE t"), Read);
            assert_eq!(c("WITH x AS (SELECT 1) SELECT * FROM x"), Read);
            assert_eq!(
                c("WITH d AS (DELETE FROM t RETURNING *) SELECT * FROM d"),
                Write
            );
            assert_eq!(c("with u as (update t set v=1) select 1"), Write);
            assert_eq!(c("EXPLAIN SELECT 1"), Read);
            assert_eq!(c("EXPLAIN ANALYZE DELETE FROM t"), Other);
            assert_eq!(c("COPY t TO STDOUT"), Read);
            assert_eq!(c("COPY t FROM STDIN"), Write);
            assert_eq!(c("INSERT INTO t VALUES (1)"), Write);
            assert_eq!(c("update t set v = 2"), Write);
            assert_eq!(c("DELETE FROM t"), Write);
            assert_eq!(
                c("MERGE INTO t USING s ON true WHEN MATCHED THEN DELETE"),
                Write
            );
            assert_eq!(c("CREATE TABLE x(i int)"), Write);
            assert_eq!(c("CALL p()"), Write);
            assert_eq!(c("DO $$ BEGIN END $$"), Write);
            assert_eq!(c("EXECUTE p(1)"), Write);
            assert_eq!(c("COMMIT"), Commit);
            assert_eq!(c("commit;"), Commit);
            assert_eq!(c("END"), Commit);
            assert_eq!(c("END TRANSACTION"), Commit);
            assert_eq!(c("COMMIT PREPARED 'x'"), Commit);
            assert_eq!(c("PREPARE TRANSACTION 'x'"), Commit);
            assert_eq!(c("PREPARE p AS SELECT 1"), Other); // server-side prepare, not a commit
            assert_eq!(c("INSERT INTO t VALUES (1); COMMIT"), Commit); // multi-stmt that may commit
            assert_eq!(
                c("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)"),
                Write
            );
            assert_eq!(c("BEGIN"), Control);
            assert_eq!(c("BEGIN; INSERT INTO t VALUES (1)"), Write);
            assert_eq!(c("START TRANSACTION"), Control);
            assert_eq!(c("SAVEPOINT s1"), Control);
            assert_eq!(c("RELEASE SAVEPOINT s1"), Control);
            assert_eq!(c("ROLLBACK"), Control);
            assert_eq!(c("ROLLBACK TO SAVEPOINT s1"), Control);
            assert_eq!(c("ABORT"), Control);
            assert_eq!(c("SET application_name = 'x'"), Control);
            assert_eq!(c("RESET ALL"), Control);
            assert_eq!(c("DISCARD ALL"), Control);
            assert_eq!(c(""), Control);
            assert_eq!(c("   ;  "), Control);
            assert_eq!(c("SETTINGS"), Other);
            assert_eq!(c("LISTEN ch"), Other);
            assert_eq!(c("NOTIFY ch"), Other);
            assert_eq!(c("LOCK TABLE t"), Other);
            assert_eq!(c("FETCH 10 FROM c"), Other);
        }

        #[test]
        fn tr_ends_transaction_recognises_single_statement_ends_only() {
            let f = ProxyServer::tr_ends_transaction;
            for s in [
                "ROLLBACK",
                "rollback;",
                "ABORT",
                "COMMIT",
                "END",
                "COMMIT WORK",
                "END TRANSACTION",
                " Rollback Work ; ",
            ] {
                assert!(f(s), "{s}");
            }
            for s in [
                "ROLLBACK TO SAVEPOINT a",
                "ROLLBACK PREPARED 'x'",
                "COMMIT PREPARED 'x'",
                "COMMIT AND CHAIN",
                "ROLLBACK; SELECT 1",
                "SELECT 1",
                "",
                "ENDING",
            ] {
                assert!(!f(s), "{s}");
            }
        }

        #[test]
        fn tr_session_set_tracking_filters() {
            let set = ProxyServer::tr_is_session_set;
            assert!(set("SET application_name = 'tr-f3'"));
            assert!(set("set search_path to a, b;"));
            assert!(set("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"));
            assert!(set("SET ROLE readonly"));
            assert!(set("RESET application_name"));
            assert!(!set("SET LOCAL statement_timeout = 1"));
            assert!(!set("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"));
            assert!(!set("SET CONSTRAINTS ALL DEFERRED"));
            assert!(!set("SET a = 1; SET b = 2"));
            assert!(!set("SELECT set_config('a','b',false)"));
            assert!(!set("SETTINGS"));
            let all = ProxyServer::tr_resets_all;
            assert!(all("RESET ALL"));
            assert!(all("discard all;"));
            assert!(!all("RESET application_name"));
            assert!(!all("DISCARD PLANS"));
        }

        #[test]
        fn starts_with_word_ci_requires_boundary() {
            assert!(ProxyServer::starts_with_word_ci("SET x", "SET"));
            assert!(ProxyServer::starts_with_word_ci("set", "SET"));
            assert!(ProxyServer::starts_with_word_ci("END;", "END"));
            assert!(!ProxyServer::starts_with_word_ci("SETTINGS", "SET"));
            assert!(!ProxyServer::starts_with_word_ci("SE", "SET"));
        }

        #[test]
        fn parse_msg_sql_extracts_query_from_encoded_parse() {
            let mut p = vec![b'P', 0, 0, 0, 0];
            p.extend_from_slice(&cstr("ps1"));
            p.extend_from_slice(&cstr("SELECT 42"));
            p.extend_from_slice(&[0, 0]);
            assert_eq!(ProxyServer::parse_msg_sql(&p), Some("SELECT 42"));
            assert_eq!(ProxyServer::parse_msg_sql(&p[..3]), None);
            let mut reg: HashMap<String, bytes::Bytes> = HashMap::new();
            reg.insert("ps1".to_string(), bytes::Bytes::from(p));
            let refs = vec!["ps1".to_string()];
            assert_eq!(
                ProxyServer::tr_extended_sql(None, &refs, &reg),
                Some("SELECT 42")
            );
            assert_eq!(
                ProxyServer::tr_extended_sql(Some("SELECT 1"), &refs, &reg),
                Some("SELECT 1")
            );
            assert_eq!(
                ProxyServer::tr_extended_sql(None, &["nope".to_string()], &reg),
                None
            );
        }

        fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
            let mut f = vec![tag];
            f.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
            f.extend_from_slice(body);
            f
        }

        #[test]
        fn tr_commit_classification_guards_every_query_statement() {
            for sql in [
                "/* audit */ COMMIT",
                "-- ignored\nEND WORK",
                "PREPARE/* nested /* x */ */TRANSACTION 'tx'",
                "SELECT ';COMMIT'; COMMIT PREPARED 'tx'",
                "COMMIT AND CHAIN",
                "ROLLBACK; INSERT INTO t VALUES (1)",
                "SELECT $x$COMMIT$x$;END",
            ] {
                let kind = ProxyServer::tr_classify(sql, &TrReadPolicy::default());
                assert_eq!(kind, StmtKind::Commit, "{sql}");
                assert_eq!(
                    ProxyServer::tr_decide(
                        TrMode::Transaction,
                        FaultPhase::OutcomeUnknown,
                        true,
                        true,
                        true,
                        kind
                    ),
                    TrAction::ErrorAndContinue("08007")
                );
            }
        }

        #[test]
        fn tr_visible_response_progress_never_reexecutes() {
            for action in [
                TrAction::Reexecute,
                TrAction::ReplayThenReexecute,
                TrAction::ErrorAndContinue("08007"),
                TrAction::CloseWithError("57P01"),
            ] {
                for raw in [false, true] {
                    for terminal in [false, true] {
                        let progress = ResponseProgress {
                            bytes: 7,
                            raw,
                            terminal,
                        };
                        let guarded = ProxyServer::tr_response_action(action, progress, false);
                        assert!(!matches!(
                            guarded,
                            TrAction::Reexecute | TrAction::ReplayThenReexecute
                        ));
                        if raw || terminal {
                            assert_eq!(guarded, TrAction::CloseIncompleteResponse);
                        }
                        // Independent of the byte count: an unfinished frame or a
                        // published terminal frame forbids injection even if this
                        // call recorded no bytes of its own.
                        let zero = ResponseProgress {
                            bytes: 0,
                            raw,
                            terminal,
                        };
                        if raw || terminal {
                            assert_eq!(
                                ProxyServer::tr_response_action(action, zero, false),
                                TrAction::CloseIncompleteResponse
                            );
                        }
                    }
                }
                // Rows published without a terminal frame: re-execution is
                // refused (it would append a second result), but the session can
                // still be told what happened.
                let published_rows = ResponseProgress {
                    bytes: 7,
                    raw: false,
                    terminal: false,
                };
                let guarded = ProxyServer::tr_response_action(action, published_rows, false);
                assert_ne!(guarded, TrAction::CloseIncompleteResponse);
                assert!(!matches!(
                    guarded,
                    TrAction::Reexecute | TrAction::ReplayThenReexecute
                ));
                // Backend-watch output may be a raw prefix from an earlier
                // Flush, even if this call has not sent any response bytes.
                assert_eq!(
                    ProxyServer::tr_response_action(action, ResponseProgress::default(), true),
                    TrAction::CloseIncompleteResponse
                );
                assert_eq!(
                    ProxyServer::tr_response_action(action, ResponseProgress::default(), false),
                    action
                );
            }
        }

        #[tokio::test]
        async fn tr_stream_fault_retains_visible_rows_and_terminal_frames() {
            for (bytes, terminal) in [
                (Vec::new(), false),
                (
                    [frame(b'T', b"description"), frame(b'D', b"row")].concat(),
                    false,
                ),
                (
                    [
                        frame(b'T', b"description"),
                        frame(b'D', b"row"),
                        frame(b'C', b"SELECT 1\0"),
                    ]
                    .concat(),
                    true,
                ),
                (frame(b'E', b"SERROR\0CXX000\0Mfailed\0\0"), true),
            ] {
                for capture in [false, true] {
                    #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
                    if capture {
                        continue;
                    }
                    let (mut backend, mut peer) = pair().await;
                    let (client, mut recipient) = pair().await;
                    let mut client = ClientStream::Plain(client);
                    let session = make_test_session();
                    let server = ProxyServer::new(test_config()).unwrap();
                    let sent = bytes.clone();
                    let feed = tokio::spawn(async move {
                        peer.write_all(&sent).await.unwrap();
                    });
                    let receive = tokio::spawn(async move {
                        let mut received = Vec::new();
                        recipient.read_to_end(&mut received).await.unwrap();
                        received
                    });
                    let failure = if capture {
                        #[cfg(any(feature = "query-cache", feature = "edge-proxy"))]
                        {
                            ProxyServer::stream_until_ready_capture(
                                &mut client,
                                &mut backend,
                                &session,
                                RelayLimits {
                                    client_write_timeout: Duration::from_secs(1),
                                    backend_read_timeout: Duration::from_secs(1),
                                    max_frame_bytes: usize::MAX,
                                },
                                16,
                                &server.state.metrics,
                            )
                            .await
                            .unwrap_err()
                        }
                        #[cfg(not(any(feature = "query-cache", feature = "edge-proxy")))]
                        {
                            unreachable!()
                        }
                    } else {
                        ProxyServer::stream_until_ready(
                            &mut client,
                            &mut backend,
                            &session,
                            &server.state,
                        )
                        .await
                        .unwrap_err()
                    };
                    drop(client);
                    feed.await.unwrap();
                    assert_eq!(receive.await.unwrap(), bytes);
                    assert_eq!(failure.progress.bytes, bytes.len() as u64);
                    assert_eq!(failure.progress.terminal, terminal);
                    assert!(!failure.progress.raw);
                    let mut fault = None;
                    BackendFault::set_response(&mut fault, "backend", &failure);
                    assert_eq!(fault.unwrap().progress.bytes, bytes.len() as u64);
                }
            }
        }

        #[test]
        fn tr_extended_execute_identity_and_commit_boundaries() {
            fn parse(name: &str, sql: &str) -> Vec<u8> {
                frame(b'P', &[cstr(name), cstr(sql), vec![0, 0]].concat())
            }
            fn bind(portal: &str, name: &str) -> Vec<u8> {
                // Binary int32 parameter and binary results. Safety inspection
                // must leave these opaque bytes intact, including embedded NULs.
                frame(
                    b'B',
                    &[
                        cstr(portal),
                        cstr(name),
                        vec![0, 1, 0, 1, 0, 1, 0, 0, 0, 4, 0, 0, 0, 7, 0, 1, 0, 1],
                    ]
                    .concat(),
                )
            }
            fn execute(portal: &str) -> Vec<u8> {
                frame(b'E', &[cstr(portal), vec![0; 4]].concat())
            }
            let classify = |frames: Vec<Vec<u8>>| {
                ProxyServer::tr_extended_kind(&frames.concat(), None, 256, &TrReadPolicy::default())
            };
            for end in ["COMMIT", "END", "PREPARE TRANSACTION 'x'", "ROLLBACK"] {
                assert_eq!(
                    classify(vec![
                        parse("a", "SELECT 1"),
                        bind("a", "a"),
                        execute("a"),
                        parse("b", end),
                        bind("b", "b"),
                        execute("b"),
                        frame(b'S', &[])
                    ]),
                    StmtKind::Commit,
                    "{end}"
                );
            }
            // A portal owns the definition at Bind time, even after the unnamed
            // statement is replaced. Looking up the last Parse would miss COMMIT.
            assert_eq!(
                classify(vec![
                    parse("", "COMMIT"),
                    bind("saved", ""),
                    parse("", "SELECT 1"),
                    execute("saved")
                ]),
                StmtKind::Commit
            );
            assert_eq!(
                classify(vec![
                    parse("", "SELECT 1"),
                    bind("saved", ""),
                    parse("", "COMMIT"),
                    execute("saved")
                ]),
                StmtKind::Read
            );
            // The unexecuted Parse isn't sufficient evidence of the executed SQL.
            assert_eq!(
                classify(vec![parse("read", "SELECT 1"), execute("older")]),
                StmtKind::Commit
            );
            assert_eq!(
                classify(vec![bind("p", "older"), execute("p")]),
                StmtKind::Commit
            );
            assert_eq!(
                classify(vec![parse("s", "SELECT $1"), bind("p", "s"), execute("p")]),
                StmtKind::Read
            );
            assert_eq!(
                classify(vec![
                    parse("s", "SELECT 1"),
                    bind("p", "s"),
                    frame(b'C', b"Pp\0"),
                    execute("p")
                ]),
                StmtKind::Commit
            );
            let held = parse("", "COMMIT");
            let batch = [bind("", ""), execute(""), frame(b'S', &[])].concat();
            assert_eq!(
                ProxyServer::tr_extended_kind(&batch, Some(&held), 256, &TrReadPolicy::default()),
                StmtKind::Commit
            );
            let unnamed_read = [parse("", "SELECT 1"), bind("", ""), execute("")].concat();
            assert_eq!(
                ProxyServer::tr_extended_kind(&unnamed_read, None, 0, &TrReadPolicy::default()),
                StmtKind::Read
            );
            let named_read = [parse("s", "SELECT 1"), bind("p", "s"), execute("p")].concat();
            assert_eq!(
                ProxyServer::tr_extended_kind(&named_read, None, 0, &TrReadPolicy::default()),
                StmtKind::Commit
            );
            assert_eq!(
                ProxyServer::tr_extended_kind(&named_read, None, 1, &TrReadPolicy::default()),
                StmtKind::Read
            );
            let too_many_portals = [
                parse("s", "SELECT 1"),
                bind("p", "s"),
                bind("q", "s"),
                execute("q"),
            ]
            .concat();
            assert_eq!(
                ProxyServer::tr_extended_kind(&too_many_portals, None, 1, &TrReadPolicy::default()),
                StmtKind::Commit
            );
            // Every truncated header/body is opaque; a complete prefix that
            // already Executes COMMIT must remain so, even before final Sync.
            let wire = [held, batch].concat();
            let commit_end = wire.len() - 5;
            for offset in commit_end..=wire.len() {
                assert_eq!(
                    ProxyServer::tr_extended_kind(
                        &wire[..offset],
                        None,
                        256,
                        &TrReadPolicy::default()
                    ),
                    StmtKind::Commit
                );
            }
            for bytes in [
                vec![b'E'],
                vec![b'E', 0, 0, 0, 3],
                vec![b'P', 255, 255, 255, 255],
            ] {
                assert_eq!(
                    ProxyServer::tr_extended_kind(&bytes, None, 256, &TrReadPolicy::default()),
                    StmtKind::Commit
                );
            }
        }

        /// A transport that accepts exactly `limit` bytes, then errors, stalls,
        /// or returns zero. This reproduces write_all losing its partial count.
        struct PrefixWriter {
            received: Vec<u8>,
            limit: usize,
            failure: u8,
        }

        impl tokio::io::AsyncWrite for PrefixWriter {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                bytes: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                use std::task::Poll;
                let count = bytes.len().min(self.limit - self.received.len());
                if count == 0 {
                    return match self.failure {
                        b'T' => Poll::Pending,
                        b'Z' => Poll::Ready(Ok(0)),
                        _ => Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into())),
                    };
                }
                self.received.extend_from_slice(&bytes[..count]);
                Poll::Ready(Ok(count))
            }
            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        #[tokio::test]
        async fn tr_partial_extended_writes_never_authorize_commit_reexecution() {
            let mut batch = Vec::new();
            for sql in ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"] {
                batch.extend(frame(b'P', &[cstr(""), cstr(sql), vec![0, 0]].concat()));
                batch.extend(frame(b'B', &[0; 8]));
                batch.extend(frame(b'E', &[0; 5]));
            }
            batch.extend(frame(b'S', &[]));
            let kind = ProxyServer::tr_extended_kind(&batch, None, 16, &TrReadPolicy::default());
            assert_eq!(kind, StmtKind::Commit);
            // Every possible byte offset includes each frontend frame boundary
            // and a complete COMMIT Execute followed by an incomplete final Sync.
            for limit in 0..batch.len() {
                let mut writer = PrefixWriter {
                    received: Vec::new(),
                    limit,
                    failure: b'E',
                };
                let (_, phase) =
                    ProxyServer::tr_write_batch(&mut writer, &batch, Duration::from_secs(1))
                        .await
                        .unwrap_err();
                assert_eq!(writer.received, batch[..limit]);
                for in_tx in [false, true] {
                    assert_eq!(
                        ProxyServer::tr_decide(TrMode::Transaction, phase, in_tx, true, true, kind),
                        TrAction::ErrorAndContinue("08007"),
                        "offset={limit}, in_tx={in_tx}"
                    );
                }
            }
            for failure in [b'T', b'Z'] {
                let mut writer = PrefixWriter {
                    received: Vec::new(),
                    limit: batch.len() - 5,
                    failure,
                };
                let (_, phase) =
                    ProxyServer::tr_write_batch(&mut writer, &batch, Duration::from_millis(5))
                        .await
                        .unwrap_err();
                assert_eq!(phase, FaultPhase::OutcomeUnknown);
                assert_eq!(writer.received, batch[..batch.len() - 5]);
            }
            let mut writer = PrefixWriter {
                received: Vec::new(),
                limit: batch.len(),
                failure: b'E',
            };
            assert!(
                ProxyServer::tr_write_batch(&mut writer, &batch, Duration::from_secs(1))
                    .await
                    .is_ok()
            );
            assert_eq!(writer.received, batch);
            // A real pre-dispatch connection failure remains safe to retry.
            assert_eq!(
                ProxyServer::tr_decide(
                    TrMode::Transaction,
                    FaultPhase::NotDelivered,
                    false,
                    false,
                    false,
                    kind
                ),
                TrAction::Reexecute
            );
        }

        /// One response per call (the replay/restore paths only ever have ONE
        /// response outstanding — statement, drain, statement, drain), with
        /// the status byte and error flag reported.
        #[tokio::test]
        async fn drain_until_ready_reports_status_and_errors() {
            let (mut a, mut b) = tokio::io::duplex(4096);
            let mut wire = frame(b'C', &cstr("INSERT 0 1"));
            wire.extend_from_slice(&frame(b'Z', b"T"));
            b.write_all(&wire).await.unwrap();
            let (status, err) =
                ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX)
                    .await
                    .unwrap();
            assert_eq!((status, err), (b'T', false));
            // Error frames are reported, not fatal.
            let mut wire = frame(b'E', &[b'S', 0, b'C', 0, 0]);
            wire.extend_from_slice(&frame(b'Z', b"E"));
            b.write_all(&wire).await.unwrap();
            let (status, err) =
                ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX)
                    .await
                    .unwrap();
            assert_eq!((status, err), (b'E', true));
            // A COPY-in request cannot be satisfied during a replay.
            b.write_all(&frame(b'G', &[0, 0, 0])).await.unwrap();
            assert!(
                ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX)
                    .await
                    .is_err()
            );
            // EOF is an error.
            drop(b);
            assert!(
                ProxyServer::drain_until_ready(&mut a, Duration::from_secs(5), usize::MAX)
                    .await
                    .is_err()
            );
        }

        #[tokio::test]
        async fn drain_until_ready_times_out_on_silent_backend() {
            let (mut a, _b) = tokio::io::duplex(64);
            let r =
                ProxyServer::drain_until_ready(&mut a, Duration::from_millis(50), usize::MAX).await;
            assert!(matches!(r, Err(ProxyError::Network(_))));
        }

        #[tokio::test]
        async fn tr_run_discard_and_restore_session_state() {
            let (mut a, mut b) = tokio::io::duplex(4096);
            // Backend: answers the first statement OK, rejects the second —
            // one response per received Query, like a real server.
            let backend = tokio::spawn(async move {
                let mut seen = Vec::new();
                for i in 0..2 {
                    let mut hdr = [0u8; 5];
                    b.read_exact(&mut hdr).await.unwrap();
                    let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
                    let mut body = vec![0u8; len - 4];
                    b.read_exact(&mut body).await.unwrap();
                    seen.push((
                        hdr[0],
                        crate::protocol::query_text(&body).unwrap().to_string(),
                    ));
                    let mut out = if i == 0 {
                        frame(b'C', &cstr("SET"))
                    } else {
                        frame(b'E', &[b'C', 0, 0])
                    };
                    out.extend_from_slice(&frame(b'Z', b"I"));
                    b.write_all(&out).await.unwrap();
                }
                seen
            });
            let gucs = vec!["SET a = 1".to_string(), "SET b = 2".to_string()];
            let r = ProxyServer::tr_restore_session_state(
                &mut a,
                &gucs,
                Duration::from_secs(5),
                Duration::from_secs(5),
                usize::MAX,
            )
            .await;
            assert!(matches!(r, Err(ProxyError::Protocol(_))), "{r:?}");
            // Both Query frames reached the backend, in order.
            let seen = backend.await.unwrap();
            assert_eq!(
                seen,
                vec![
                    (b'Q', "SET a = 1".to_string()),
                    (b'Q', "SET b = 2".to_string())
                ]
            );
            // Empty list restores nothing and succeeds.
            let (mut a2, _b2) = tokio::io::duplex(64);
            assert_eq!(
                ProxyServer::tr_restore_session_state(
                    &mut a2,
                    &[],
                    Duration::from_secs(1),
                    Duration::from_secs(1),
                    usize::MAX,
                )
                .await
                .unwrap(),
                0
            );
        }

        /// Fake PG backend: answers every simple Query with `CommandComplete` +
        /// `ReadyForQuery('T')`; a query containing "boom" gets ErrorResponse +
        /// RFQ('E'). Received query texts are pushed to `seen`.
        async fn fake_backend(
            listener: tokio::net::TcpListener,
            seen: Arc<std::sync::Mutex<Vec<String>>>,
        ) {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = BytesMut::with_capacity(4096);
            loop {
                buf.reserve(4096);
                match sock.read_buf(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
                while buf.len() >= 5 {
                    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
                    if buf.len() < len + 1 {
                        break;
                    }
                    let f = buf.split_to(len + 1);
                    if f[0] == b'Q' {
                        let q = crate::protocol::query_text(&f[5..])
                            .unwrap_or("")
                            .to_string();
                        let fail = q.contains("boom");
                        seen.lock().unwrap().push(q);
                        let mut out = if fail {
                            frame(b'E', &[b'C', b'4', b'2', b'0', b'0', b'0', 0, 0])
                        } else {
                            frame(b'C', &cstr("OK"))
                        };
                        out.extend_from_slice(&frame(b'Z', if fail { b"E" } else { b"T" }));
                        sock.write_all(&out).await.unwrap();
                    }
                }
            }
        }

        fn simple_log(sql: &str) -> StatementLog {
            StatementLog {
                sql: sql.to_string(),
                params: Vec::new(),
                result_checksum: None,
                executed_at: chrono::Utc::now(),
                extended: None,
            }
        }

        #[tokio::test]
        async fn tr_replay_transaction_replays_in_order_and_reports_rejections() {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            tokio::spawn(fake_backend(listener, seen.clone()));
            let server = ProxyServer::new(test_config()).unwrap();
            let state = server.state.clone();
            let mut conns: HashMap<String, BackendConn> = HashMap::new();
            conns.insert(
                addr.clone(),
                BackendConn::new(TcpStream::connect(&addr).await.unwrap()),
            );
            let registry: HashMap<String, bytes::Bytes> = HashMap::new();
            let entries = vec![
                simple_log("BEGIN"),
                simple_log("INSERT INTO t VALUES (1)"),
                simple_log("SAVEPOINT a"),
            ];
            let r =
                ProxyServer::tr_replay_transaction(&mut conns, &addr, &entries, &registry, &state)
                    .await;
            assert!(r.is_ok());
            assert_eq!(
                *seen.lock().unwrap(),
                vec!["BEGIN", "INSERT INTO t VALUES (1)", "SAVEPOINT a"]
            );
            // A rejected statement stops the replay with a Statement failure.
            let entries = vec![
                simple_log("BEGIN"),
                simple_log("INSERT boom"),
                simple_log("SELECT 1"),
            ];
            let r =
                ProxyServer::tr_replay_transaction(&mut conns, &addr, &entries, &registry, &state)
                    .await;
            match r {
                Err(ReplayFailure::Statement(d)) => assert!(d.contains("2/3"), "{d}"),
                _ => panic!("expected statement failure"),
            }
            assert_eq!(
                seen.lock().unwrap().len(),
                5,
                "replay stopped at the failure"
            );
            // A missing connection is a Backend failure.
            let r = ProxyServer::tr_replay_transaction(
                &mut conns,
                "127.0.0.1:1",
                &entries,
                &registry,
                &state,
            )
            .await;
            assert!(matches!(r, Err(ReplayFailure::Backend(_))));
        }

        fn qmsg(sql: &str) -> Message {
            QueryMessage {
                query: sql.to_string(),
            }
            .encode()
        }

        /// Drive the recorder through a transaction: BEGIN/INSERT/SET are
        /// recorded (with write tracking), the statement cap marks the
        /// transaction non-replayable (+ metric), COMMIT releases the record
        /// and promotes transaction-scoped SETs, and the SET cap stops tracking.
        /// A transaction that ends in ROLLBACK discards its session state, so
        /// SETs made inside it must never be restored on the replacement backend.
        /// `ROLLBACK; <DML>` is the trap: for replay safety it classifies as a
        /// possible commit (the trailing statement commits in autocommit), and
        /// reusing that classification for the GUC decision would restore
        /// settings the database threw away.
        #[tokio::test]
        async fn tr_recorder_does_not_promote_gucs_of_a_rolled_back_transaction() {
            let server = ProxyServer::new(test_config()).unwrap();
            let state = server.state.clone();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Transaction);

            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
            ProxyServer::tr_after_simple(
                &mut tr,
                &qmsg("SET work_mem = '512MB'"),
                &session,
                &state,
            )
            .await;
            assert_eq!(tr.pending_tx_gucs, vec!["SET work_mem = '512MB'"]);

            // Ends the transaction as a ROLLBACK, while the trailing statement
            // commits on its own — `StmtKind::Commit`, but nothing of the
            // transaction's own session state survived.
            ProxyServer::note_ready_for_query(&session, b'I', false);
            ProxyServer::tr_after_simple(
                &mut tr,
                &qmsg("ROLLBACK; INSERT INTO t VALUES (1)"),
                &session,
                &state,
            )
            .await;
            assert!(
                tr.gucs.is_empty(),
                "a rolled-back transaction's SETs must not be restored: {:?}",
                tr.gucs
            );
            assert!(tr.pending_tx_gucs.is_empty(), "pending set must be dropped");

            // The same shape ending in a real COMMIT still promotes.
            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SET work_mem = '64MB'"), &session, &state)
                .await;
            ProxyServer::note_ready_for_query(&session, b'I', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("COMMIT"), &session, &state).await;
            assert_eq!(tr.gucs, vec!["SET work_mem = '64MB'"]);
        }

        #[tokio::test]
        async fn tr_recorder_tracks_transaction_gucs_and_caps() {
            let mut config = test_config();
            config.limits.tr_max_replay_statements = 3;
            config.limits.tr_max_session_set_statements = 2;
            let server = ProxyServer::new(config).unwrap();
            let state = server.state.clone();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Transaction);

            // Autocommit SET -> tracked immediately. SET LOCAL -> ignored.
            ProxyServer::note_ready_for_query(&session, b'I', false);
            ProxyServer::tr_after_simple(
                &mut tr,
                &qmsg("SET application_name = 'x'"),
                &session,
                &state,
            )
            .await;
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SET LOCAL a = 1"), &session, &state).await;
            assert_eq!(tr.gucs, vec!["SET application_name = 'x'"]);
            // A rejected SET is not tracked.
            ProxyServer::note_ready_for_query(&session, b'I', true);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SET bogus = 1"), &session, &state).await;
            assert_eq!(tr.gucs.len(), 1);
            assert!(session.tx_state.read().await.statements.is_empty());

            // BEGIN opens the record; INSERT marks writes.
            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
            {
                let ts = session.tx_state.read().await;
                assert!(ts.in_transaction && ts.tx_id.is_some());
                assert_eq!(ts.statements.len(), 1);
                assert!(!ts.has_writes && ts.read_only && !ts.non_replayable);
            }
            ProxyServer::tr_after_simple(
                &mut tr,
                &qmsg("INSERT INTO t VALUES (1)"),
                &session,
                &state,
            )
            .await;
            // SET inside the transaction is pending until COMMIT.
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SET b = 2"), &session, &state).await;
            assert_eq!(tr.pending_tx_gucs, vec!["SET b = 2"]);
            {
                let ts = session.tx_state.read().await;
                assert_eq!(ts.statements.len(), 3);
                assert!(ts.has_writes && !ts.read_only);
                assert_eq!(
                    ts.replay_bytes,
                    "BEGIN".len() + "INSERT INTO t VALUES (1)".len() + "SET b = 2".len()
                );
            }
            // Fourth statement exceeds tr_max_replay_statements = 3.
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &state).await;
            {
                let ts = session.tx_state.read().await;
                assert!(ts.non_replayable);
                assert!(ts.statements.is_empty(), "record released at the cap");
                assert!(ts.in_transaction, "still inside the transaction");
            }
            assert_eq!(
                state.metrics.tr.replay_cap_exceeded.load(Ordering::Relaxed),
                1
            );
            // COMMIT -> record released, pending SET promoted (cap 2 reached).
            ProxyServer::note_ready_for_query(&session, b'I', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("COMMIT"), &session, &state).await;
            assert!(!session.tx_state.read().await.in_transaction);
            assert_eq!(tr.gucs, vec!["SET application_name = 'x'", "SET b = 2"]);
            assert!(tr.pending_tx_gucs.is_empty());
            // Third SET hits tr_max_session_set_statements = 2 -> cap metric.
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SET c = 3"), &session, &state).await;
            assert!(tr.guc_cap_hit);
            assert_eq!(tr.gucs.len(), 2);
            assert_eq!(
                state
                    .metrics
                    .tr
                    .session_set_cap_exceeded
                    .load(Ordering::Relaxed),
                1
            );
            // RESET ALL wipes tracking and lifts the cap.
            ProxyServer::tr_after_simple(&mut tr, &qmsg("RESET ALL"), &session, &state).await;
            assert!(tr.gucs.is_empty() && !tr.guc_cap_hit);

            // A rolled-back transaction drops its pending SETs, and a failed
            // transaction ('E') is never replayable.
            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SET d = 4"), &session, &state).await;
            ProxyServer::note_ready_for_query(&session, b'E', true);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("INSERT boom"), &session, &state).await;
            assert!(session.tx_state.read().await.non_replayable);
            ProxyServer::note_ready_for_query(&session, b'I', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("ROLLBACK"), &session, &state).await;
            assert!(tr.gucs.is_empty() && tr.pending_tx_gucs.is_empty());
        }

        /// `session` mode records no transaction statements (no lock, no
        /// allocation on the in-transaction path) but still tracks SETs;
        /// `none` tracks nothing.
        #[tokio::test]
        async fn tr_recorder_mode_gating() {
            let server = ProxyServer::new(test_config()).unwrap();
            let state = server.state.clone();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Session);
            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
            ProxyServer::tr_after_simple(
                &mut tr,
                &qmsg("INSERT INTO t VALUES (1)"),
                &session,
                &state,
            )
            .await;
            assert!(session.tx_state.read().await.statements.is_empty());
            ProxyServer::note_ready_for_query(&session, b'I', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SET a = 1"), &session, &state).await;
            assert_eq!(tr.gucs, vec!["SET a = 1"]);

            let mut none = TrSession::new(TrMode::None);
            ProxyServer::tr_after_simple(&mut none, &qmsg("SET a = 1"), &session, &state).await;
            assert!(none.gucs.is_empty());
        }

        /// A tenant/rewrite transform taints the transaction: recorded text is
        /// not what executed, so it must not be replayed.
        #[tokio::test]
        async fn tr_recorder_taint_marks_non_replayable() {
            let server = ProxyServer::new(test_config()).unwrap();
            let state = server.state.clone();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Transaction);
            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
            session
                .tr_replay_tainted
                .store(true, std::sync::atomic::Ordering::Relaxed);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("SELECT 1"), &session, &state).await;
            let ts = session.tx_state.read().await;
            assert!(ts.non_replayable && ts.statements.is_empty());
            assert!(!session
                .tr_replay_tainted
                .load(std::sync::atomic::Ordering::Relaxed));
        }

        /// Extended-protocol cycles: Flush-terminated batches accumulate and the
        /// Sync closes them into one replay entry carrying the raw frames.
        #[tokio::test]
        async fn tr_recorder_extended_cycle_accumulates_until_sync() {
            let server = ProxyServer::new(test_config()).unwrap();
            let state = server.state.clone();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Transaction);
            let registry: HashMap<String, bytes::Bytes> = HashMap::new();
            // Already inside a transaction (BEGIN recorded via simple protocol).
            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &state).await;
            let flush_batch = bytes::Bytes::from(
                [
                    frame(b'B', &[b"\0\0".to_vec(), vec![0; 6]].concat()),
                    frame(b'E', &[0; 5]),
                    frame(b'H', &[]),
                ]
                .concat(),
            );
            let sync_batch = bytes::Bytes::from(frame(b'S', &[]));
            let unnamed = (
                bytes::Bytes::from(frame(
                    b'P',
                    &[cstr(""), cstr("INSERT INTO t VALUES ($1)"), vec![0; 2]].concat(),
                )),
                bytes::Bytes::from_static(b"sig"),
            );
            ProxyServer::tr_after_extended(
                &mut tr,
                &flush_batch,
                Some(&unnamed),
                Some("INSERT INTO t VALUES ($1)"),
                false,
                &[],
                &[],
                &registry,
                &session,
                &state,
            )
            .await;
            assert!(tr.ext_cycle.is_some());
            assert_eq!(session.tx_state.read().await.statements.len(), 1);
            ProxyServer::tr_after_extended(
                &mut tr,
                &sync_batch,
                None,
                None,
                true,
                &["s1".to_string()],
                &["s1".to_string()],
                &registry,
                &session,
                &state,
            )
            .await;
            assert!(tr.ext_cycle.is_none());
            let ts = session.tx_state.read().await;
            assert_eq!(ts.statements.len(), 2);
            assert!(ts.has_writes);
            let ext = ts.statements[1].extended.as_ref().expect("extended entry");
            assert_eq!(
                ext.frames,
                [flush_batch.as_ref(), sync_batch.as_ref()].concat()
            );
            assert_eq!(ext.unnamed_parse.as_ref(), Some(&unnamed.0));
            assert_eq!(ext.defines, vec!["s1"]);
            assert_eq!(ts.statements[1].sql, "INSERT INTO t VALUES ($1)");
            assert_eq!(
                ts.replay_bytes,
                "BEGIN".len() + flush_batch.len() + sync_batch.len() + unnamed.0.len()
            );
        }

        #[tokio::test]
        async fn tr_recorder_does_not_retain_a_committed_transaction_prefix() {
            let server = ProxyServer::new(test_config()).unwrap();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Transaction);
            ProxyServer::note_ready_for_query(&session, b'T', false);
            for sql in ["BEGIN", "INSERT INTO t VALUES (1)", "COMMIT; BEGIN"] {
                ProxyServer::tr_after_simple(&mut tr, &qmsg(sql), &session, &server.state).await;
            }
            let ts = session.tx_state.read().await;
            assert!(ts.non_replayable);
            assert!(ts.statements.is_empty());
            assert_eq!(ts.replay_bytes, 0);
        }

        #[tokio::test]
        async fn tr_recorder_preserves_later_held_parse_positions() {
            for first_held in [false, true] {
                let server = ProxyServer::new(test_config()).unwrap();
                let session = make_test_session();
                let mut tr = TrSession::new(TrMode::Transaction);
                let registry = HashMap::new();
                ProxyServer::note_ready_for_query(&session, b'T', false);
                ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &server.state)
                    .await;
                let mut expected = Vec::new();
                for (index, sql) in ["SELECT 1", "SELECT 2", "SELECT 3"].iter().enumerate() {
                    let parse = bytes::Bytes::from(frame(
                        b'P',
                        &[cstr(""), cstr(sql), vec![0; 2]].concat(),
                    ));
                    let mut frames = Vec::new();
                    let held = if first_held || index > 0 {
                        Some((parse.clone(), bytes::Bytes::new()))
                    } else {
                        frames.extend_from_slice(&parse);
                        None
                    };
                    frames.extend_from_slice(&frame(b'B', &[0; 8]));
                    frames.extend_from_slice(&frame(b'E', &[0; 5]));
                    frames.extend_from_slice(&frame(if index == 2 { b'S' } else { b'H' }, &[]));
                    if held.is_some() {
                        expected.extend_from_slice(&parse);
                    }
                    expected.extend_from_slice(&frames);
                    ProxyServer::tr_after_extended(
                        &mut tr,
                        &bytes::Bytes::from(frames),
                        held.as_ref(),
                        Some(sql),
                        index == 2,
                        &[],
                        &[],
                        &registry,
                        &session,
                        &server.state,
                    )
                    .await;
                }
                let ts = session.tx_state.read().await;
                assert!(!ts.non_replayable);
                assert_eq!(ts.statements.len(), 2);
                let recorded = ts.statements[1].extended.as_ref().unwrap();
                assert_eq!(
                    [
                        recorded.unnamed_parse.as_deref().unwrap_or(&[]),
                        &recorded.frames
                    ]
                    .concat(),
                    expected,
                );
                assert_eq!(ts.replay_bytes, "BEGIN".len() + expected.len());
            }
        }

        #[tokio::test]
        async fn tr_recorder_bounds_open_flush_cycle_before_sync() {
            for statement_cap in [1, 256] {
                let mut config = test_config();
                config.limits.tr_max_replay_bytes = 64;
                config.limits.tr_max_replay_statements = statement_cap;
                let server = ProxyServer::new(config).unwrap();
                let session = make_test_session();
                let mut tr = TrSession::new(TrMode::Transaction);
                ProxyServer::note_ready_for_query(&session, b'T', false);
                ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &server.state)
                    .await;
                let batch = bytes::Bytes::from(frame(b'H', &[]));
                for _ in 0..32 {
                    ProxyServer::tr_after_extended(
                        &mut tr,
                        &batch,
                        None,
                        None,
                        false,
                        &[],
                        &[],
                        &HashMap::new(),
                        &session,
                        &server.state,
                    )
                    .await;
                    let retained = tr.ext_cycle.as_ref().map_or(0, |c| c.frames.len());
                    assert!(retained + session.tx_state.read().await.replay_bytes <= 64);
                }
                assert!(tr.ext_cycle.is_none());
                let ts = session.tx_state.read().await;
                assert!(ts.non_replayable && ts.statements.is_empty());
                assert_eq!(ts.replay_bytes, 0);
                assert_eq!(
                    server
                        .state
                        .metrics
                        .tr
                        .replay_cap_exceeded
                        .load(Ordering::Relaxed),
                    1
                );
            }
        }

        #[tokio::test]
        async fn tr_recorder_retains_begin_before_first_sync_or_refuses_incomplete_history() {
            for (cap, simple_end) in [(4096, false), (1, false), (4096, true)] {
                let mut config = test_config();
                config.limits.tr_max_replay_bytes = cap;
                let server = ProxyServer::new(config).unwrap();
                let session = make_test_session();
                let mut tr = TrSession::new(TrMode::Transaction);
                let parse = bytes::Bytes::from(frame(
                    b'P',
                    &[cstr(""), cstr("BEGIN"), vec![0; 2]].concat(),
                ));
                let held = (parse.clone(), bytes::Bytes::new());
                let flush = bytes::Bytes::from(
                    [frame(b'B', &[0; 8]), frame(b'E', &[0; 5]), frame(b'H', &[])].concat(),
                );
                // No RFQ has exposed BEGIN yet: both the client-visible and
                // recorder status still say Idle when the Flush is retained.
                ProxyServer::note_ready_for_query(&session, b'I', false);
                ProxyServer::tr_after_extended(
                    &mut tr,
                    &flush,
                    Some(&held),
                    Some("BEGIN"),
                    false,
                    &[],
                    &[],
                    &HashMap::new(),
                    &session,
                    &server.state,
                )
                .await;
                ProxyServer::note_ready_for_query(&session, b'T', false);
                let sync = bytes::Bytes::from(frame(b'S', &[]));
                if simple_end {
                    ProxyServer::tr_after_simple(
                        &mut tr,
                        &qmsg("SELECT 1"),
                        &session,
                        &server.state,
                    )
                    .await;
                } else {
                    ProxyServer::tr_after_extended(
                        &mut tr,
                        &sync,
                        None,
                        None,
                        true,
                        &[],
                        &[],
                        &HashMap::new(),
                        &session,
                        &server.state,
                    )
                    .await;
                }
                let ts = session.tx_state.read().await;
                if cap == 1 || simple_end {
                    assert!(ts.non_replayable && ts.statements.is_empty());
                } else {
                    assert!(!ts.non_replayable);
                    assert_eq!(ts.statements.len(), 1);
                    let entry = ts.statements[0].extended.as_ref().unwrap();
                    assert_eq!(
                        [entry.unnamed_parse.as_deref().unwrap_or(&[]), &entry.frames].concat(),
                        [parse.as_ref(), flush.as_ref(), sync.as_ref()].concat(),
                    );
                }
                assert!(tr.ext_cycle.is_none());
                assert!(!tr.ext_cycle_dropped);
            }
        }

        #[tokio::test]
        async fn tr_recorder_idle_flush_cap_does_not_taint_next_transaction() {
            let mut config = test_config();
            config.limits.tr_max_replay_bytes = 64;
            let server = ProxyServer::new(config).unwrap();
            let session = make_test_session();
            let mut tr = TrSession::new(TrMode::Transaction);
            ProxyServer::note_ready_for_query(&session, b'I', false);
            for _ in 0..16 {
                ProxyServer::tr_after_extended(
                    &mut tr,
                    &bytes::Bytes::from(frame(b'H', &[])),
                    None,
                    None,
                    false,
                    &[],
                    &[],
                    &HashMap::new(),
                    &session,
                    &server.state,
                )
                .await;
            }
            assert!(tr.ext_cycle_dropped);
            ProxyServer::tr_after_extended(
                &mut tr,
                &bytes::Bytes::from(frame(b'S', &[])),
                None,
                None,
                true,
                &[],
                &[],
                &HashMap::new(),
                &session,
                &server.state,
            )
            .await;
            assert!(!session.tx_state.read().await.non_replayable);
            ProxyServer::note_ready_for_query(&session, b'T', false);
            ProxyServer::tr_after_simple(&mut tr, &qmsg("BEGIN"), &session, &server.state).await;
            let ts = session.tx_state.read().await;
            assert!(!ts.non_replayable);
            assert_eq!(ts.statements.len(), 1);
        }

        #[test]
        fn backend_fault_set_ignores_client_errors() {
            let mut slot = None;
            BackendFault::set(
                &mut slot,
                "n",
                FaultPhase::OutcomeUnknown,
                &ProxyError::Network("Client write error: x".into()),
            );
            assert!(slot.is_none());
            BackendFault::set(
                &mut slot,
                "n",
                FaultPhase::OutcomeUnknown,
                &ProxyError::Network("Backend read error: reset".into()),
            );
            let f = slot.unwrap();
            assert_eq!(
                (f.node.as_str(), f.phase),
                ("n", FaultPhase::OutcomeUnknown)
            );
        }

        #[test]
        fn tr_metrics_snapshot_roundtrip() {
            let m = TrMetrics::default();
            m.failovers.fetch_add(2, Ordering::Relaxed);
            m.unknown_outcome_errors.fetch_add(3, Ordering::Relaxed);
            let s = m.snapshot();
            assert_eq!(s.failovers, 2);
            assert_eq!(s.unknown_outcome_errors, 3);
            assert_eq!(s.transactions_replayed, 0);
        }

        // ---- backend authentication on a fresh (redial/failover) connection ----

        /// Read one complete frame (tag + body) from a stream.
        async fn read_frame<S: AsyncReadExt + Unpin>(s: &mut S) -> (u8, Vec<u8>) {
            let mut hdr = [0u8; 5];
            s.read_exact(&mut hdr).await.unwrap();
            let len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
            let mut body = vec![0u8; len - 4];
            s.read_exact(&mut body).await.unwrap();
            (hdr[0], body)
        }

        fn auth_frame(kind: u32, body: &[u8]) -> Vec<u8> {
            let mut b = kind.to_be_bytes().to_vec();
            b.extend_from_slice(body);
            frame(b'R', &b)
        }

        /// A SCRAM-SHA-256 backend (driven by the tested `ScramServer`)
        /// accepts the proxy's client exchange; the post-auth frames are
        /// returned for the caller to forward.
        #[tokio::test]
        async fn complete_backend_auth_completes_scram_with_credential() {
            use crate::auth_scram::{ScramServer, ScramVerifier};
            let (mut proxy_side, mut backend_side) = tokio::io::duplex(8192);
            let password = "benchpass";
            let verifier =
                ScramVerifier::from_password(password, b"saltsaltsaltsalt".to_vec(), 4096);
            let backend = tokio::spawn(async move {
                // AuthenticationSASL: mechanism list.
                backend_side
                    .write_all(&auth_frame(10, b"SCRAM-SHA-256\0\0"))
                    .await
                    .unwrap();
                let (tag, body) = read_frame(&mut backend_side).await;
                assert_eq!(tag, b'p');
                let mech_end = body.iter().position(|&b| b == 0).unwrap() + 1;
                let client_first = std::str::from_utf8(&body[mech_end + 4..]).unwrap();
                let (server, server_first) =
                    ScramServer::start(verifier, client_first, "serverNONCE").unwrap();
                backend_side
                    .write_all(&auth_frame(11, server_first.as_bytes()))
                    .await
                    .unwrap();
                let (tag, body) = read_frame(&mut backend_side).await;
                assert_eq!(tag, b'p');
                let server_final = server.finish(std::str::from_utf8(&body).unwrap()).unwrap();
                let mut out = auth_frame(12, server_final.as_bytes());
                out.extend_from_slice(&auth_frame(0, b""));
                out.extend_from_slice(&frame(b'S', b"server_version\0"));
                out.extend_from_slice(&frame(b'K', &[0, 0, 0, 7, 0, 0, 0, 9]));
                out.extend_from_slice(&frame(b'Z', b"I"));
                backend_side.write_all(&out).await.unwrap();
                backend_side
            });
            let forwarded = ProxyServer::complete_backend_auth(
                &mut proxy_side,
                1 << 20,
                "bench",
                Some(password),
            )
            .await
            .unwrap();
            let _ = backend.await.unwrap();
            // Only non-auth frames are handed back: ParameterStatus,
            // BackendKeyData, ReadyForQuery.
            assert_eq!(forwarded[0], b'S');
            assert!(forwarded.ends_with(&frame(b'Z', b"I")));
            assert!(!forwarded.contains(&b'R') || forwarded[0] != b'R');
            let tags: Vec<u8> = {
                let mut v = Vec::new();
                let mut off = 0;
                while off < forwarded.len() {
                    v.push(forwarded[off]);
                    let len = u32::from_be_bytes([
                        forwarded[off + 1],
                        forwarded[off + 2],
                        forwarded[off + 3],
                        forwarded[off + 4],
                    ]) as usize;
                    off += 1 + len;
                }
                v
            };
            assert_eq!(tags, vec![b'S', b'K', b'Z']);
        }

        /// A wrong password is rejected by the SCRAM server -> ErrorResponse
        /// -> `ProxyError::Auth`.
        #[tokio::test]
        async fn complete_backend_auth_reports_scram_rejection() {
            use crate::auth_scram::{ScramServer, ScramVerifier};
            let (mut proxy_side, mut backend_side) = tokio::io::duplex(8192);
            let verifier =
                ScramVerifier::from_password("right", b"saltsaltsaltsalt".to_vec(), 4096);
            tokio::spawn(async move {
                backend_side
                    .write_all(&auth_frame(10, b"SCRAM-SHA-256\0\0"))
                    .await
                    .unwrap();
                let (_, body) = read_frame(&mut backend_side).await;
                let mech_end = body.iter().position(|&b| b == 0).unwrap() + 1;
                let client_first = std::str::from_utf8(&body[mech_end + 4..]).unwrap();
                let (server, server_first) =
                    ScramServer::start(verifier, client_first, "serverNONCE").unwrap();
                backend_side
                    .write_all(&auth_frame(11, server_first.as_bytes()))
                    .await
                    .unwrap();
                let (_, body) = read_frame(&mut backend_side).await;
                assert!(server.finish(std::str::from_utf8(&body).unwrap()).is_err());
                let mut err = vec![b'S'];
                err.extend_from_slice(b"FATAL\0C28P01\0Mpassword authentication failed\0\0");
                backend_side.write_all(&frame(b'E', &err)).await.unwrap();
            });
            let r = ProxyServer::complete_backend_auth(
                &mut proxy_side,
                1 << 20,
                "bench",
                Some("wrong"),
            )
            .await;
            match r {
                Err(ProxyError::Auth(m)) => {
                    assert!(m.contains("password authentication failed"), "{m}")
                }
                other => panic!("expected Auth error, got {other:?}"),
            }
        }

        /// Without a credential (pass-through mode) a challenge fails fast with
        /// a clear error instead of a timeout; a trust backend still completes.
        #[tokio::test]
        async fn complete_backend_auth_without_credential() {
            let (mut proxy_side, mut backend_side) = tokio::io::duplex(4096);
            backend_side
                .write_all(&auth_frame(10, b"SCRAM-SHA-256\0\0"))
                .await
                .unwrap();
            let r =
                ProxyServer::complete_backend_auth(&mut proxy_side, 1 << 20, "bench", None).await;
            match r {
                Err(ProxyError::Auth(m)) => assert!(m.contains("holds no credential"), "{m}"),
                other => panic!("expected Auth error, got {other:?}"),
            }
            // Trust backend: AuthenticationOk straight away.
            let (mut p2, mut b2) = tokio::io::duplex(4096);
            let mut out = auth_frame(0, b"");
            out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
            out.extend_from_slice(&frame(b'Z', b"I"));
            b2.write_all(&out).await.unwrap();
            let fwd = ProxyServer::complete_backend_auth(&mut p2, 1 << 20, "bench", None)
                .await
                .unwrap();
            assert_eq!(fwd[0], b'K');
            assert!(fwd.ends_with(&frame(b'Z', b"I")));
        }

        /// Cleartext and MD5 challenges are answered from the credential.
        #[tokio::test]
        async fn complete_backend_auth_answers_cleartext_and_md5() {
            // Cleartext.
            let (mut p, mut b) = tokio::io::duplex(4096);
            let backend = tokio::spawn(async move {
                b.write_all(&auth_frame(3, b"")).await.unwrap();
                let (tag, body) = read_frame(&mut b).await;
                assert_eq!((tag, body.as_slice()), (b'p', &b"pw\0"[..]));
                let mut out = auth_frame(0, b"");
                out.extend_from_slice(&frame(b'Z', b"I"));
                b.write_all(&out).await.unwrap();
            });
            ProxyServer::complete_backend_auth(&mut p, 1 << 20, "u", Some("pw"))
                .await
                .unwrap();
            backend.await.unwrap();
            // MD5.
            let (mut p, mut b) = tokio::io::duplex(4096);
            let backend = tokio::spawn(async move {
                b.write_all(&auth_frame(5, &[1, 2, 3, 4])).await.unwrap();
                let (tag, body) = read_frame(&mut b).await;
                assert_eq!(tag, b'p');
                assert_eq!(
                    body,
                    crate::backend::auth::md5_password_response("u", "pw", &[1, 2, 3, 4])
                );
                let mut out = auth_frame(0, b"");
                out.extend_from_slice(&frame(b'Z', b"I"));
                b.write_all(&out).await.unwrap();
            });
            ProxyServer::complete_backend_auth(&mut p, 1 << 20, "u", Some("pw"))
                .await
                .unwrap();
            backend.await.unwrap();
        }
    }
}
