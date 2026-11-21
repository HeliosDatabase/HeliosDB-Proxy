# Changelog

All notable changes to HeliosProxy will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.0.0] - 2026-11-21

This is a major release because the Rust library API has breaking changes (see Library
API below: configuration and metrics structs gain public fields, and several signatures
change) and because several defaults and behaviours change for existing deployments (see
Upgrade notes). The daemon's command line, the PostgreSQL wire protocol and the admin API
stay compatible: a 2.2 `proxy.toml` loads unchanged (unless its `[schema_routing]
analytics_node` names no configured node), but read the Upgrade notes before rolling it out.

Highlights: plugin hooks, query rewrite rules and the tenant row filter now cover the
extended protocol, so prepared statements and ORMs are filtered like simple queries.
SIGTERM and SIGINT drain the proxy gracefully, and the SIGUSR2 handoff drains the same way.
WASM plugins ship as signed `.tar.gz` artefacts (`helios-plugin keygen`/`pack`/`sign`/
`inspect`/`verify`), reload from the plugin directory without a restart, can move a session
to a branch database, and are refused at load when they could never run. The query cache
answers extended-protocol reads, proves each backend connection's settings and keeps that
proof for `[cache] proof_ttl_ms` (1 s), and gains tier sizes, L2 compression,
refresh-ahead and a per-table heatmap. The MCP gateway gains per-agent principals, a
tool-result cache and opt-in agent memory tools, and `GET /api/stats` gives pollers one
versioned document. Failover no longer continues a session that held state only the
failed backend had, statement pooling behaves as statement pooling, an open circuit
recovers, the rate limiter enforces rates above 1 000 a second, and routing targets must
be configured nodes. Several announced capabilities (auth methods beyond passthrough and
SCRAM, per-tenant pools, view-to-base-table cache dependencies and others) are not part of
3.0.0 and are planned for later 3.x releases; `docs/releases/3.0.0.md` lists them.

### Upgrade notes (behaviour changes)

- **SIGTERM and SIGINT drain instead of killing the process:** each session ends with
  FATAL `57P01` at its next point with nothing in flight, `/health/ready` answers 503, and
  the process exits at `shutdown_drain_timeout_secs` at the latest; a second signal exits
  at once. SIGUSR2 handoffs drain the same way.
- **The extended protocol passes the statement hooks:** a `Parse` now runs the pre-query
  and route plugin hooks, the rewrite rules and the tenant row filter, so a prepared
  statement can be blocked, rewritten, filtered or refused where it used to pass.
- **Non-UTF-8 statement text is refused (`22021`)** while a plugin hook, rewrite rules or
  the tenant row filter is configured. Use `client_encoding` UTF8.
- **Routing targets must be configured nodes:** a `node=` hint or plugin answer naming no
  `[[nodes]]` entry is refused with `42704`; an unknown `[schema_routing] analytics_node`
  is a startup error.
- **Plugin loading is stricter:** a manifest hook the module does not export, a hook the
  proxy never calls, symlinked files, `enable_threads` and `cache_dir` are refused;
  `.tar.gz` artefacts in `plugin_dir` are loaded; without `trust_root` signatures are not
  checked and startup warns.
- **Cached reads reuse the settings proof for `[cache] proof_ttl_ms` (default 1000):** a
  backend configuration reload reaches cached reads within that time. `proof_ttl_ms = 0`
  keeps 2.2's proof before every read.
- **Sessions holding backend-only state end on a lost backend** (`08006`, or the loss
  reported under `tr_mode = none`) instead of continuing without their temporary
  tables, cursors, prepared statements, `LISTEN` or advisory locks.
- **`[pool_mode] mode = "statement"`** refuses a lone `BEGIN` (`0A000`) and ends a
  session that leaves a transaction block open.
- **`[rate_limit]` has no hidden per-minute window:** a `default_qps` above 1 000 is now
  enforced as configured; the window is opt-in (`window_max_requests`, `window_secs`).
- **Release binaries are built with `postgres-topology`.**
- **The result caches cache only literal scalar reads unless you vouch for the backend:**
  without `[cache] trust_backend_read_effects = true` the query cache and the `[edge]`
  cache store only `SELECT <integer literal>`; every other read runs on the backend.
  Each cached read is keyed by the backend's effective settings (`SHOW ALL`), and a
  session that runs a write, a `SET`, `SET ROLE` or any statement that may change its
  execution context stops using the cache. Builtin function names no longer make a read
  safe to retry or offload: list trusted functions in `tr_read_functions`.
- **`[multi_tenancy] unknown_tenant` defaults to `"refuse"`:** under row isolation a
  session with no identified tenant gets `42501` on a statement naming a tenant table;
  statements of a shape the row filter cannot cover (`WITH`, `INSERT ... SELECT`,
  `ON CONFLICT ... DO UPDATE`, an alias on a tenant table, ...) are refused as well.
  `[[hba]]` rules without `@` now also cover `<db>@<branch>`.
- **Legacy `FunctionCall` messages are checked:** `function_call = "large-objects"` (the
  default) forwards the built-in large-object functions and refuses every other function
  with `0A000`; set `"any"` to forward all.
- **Client startups wait for a healthy node** up to `[limits] startup_node_wait_ms`
  (default 5000) instead of failing at once; `0` restores the immediate `08006`.
- **`[rate_limit] max_concurrent` is enforced** (`53400`), where it was ignored.
- **Gateway results are capped:** HTTP `/sql` and GraphQL refuse a result over 10000 rows
  or 16 MiB (`max_result_rows`, `max_result_bytes`); the MCP tools truncate it.
- **The `cache_refresh` hint (and `cache=refresh`) is active:** such reads always go to
  the backend and refresh the entry.
- **Library API:** see Library API below: `CacheToml`, `CacheConfig`, `L2Config`,
  `PluginToml`, `BranchConfig`, `McpConfig`, `MultiTenancyToml`, `LimitsToml`,
  `RateLimitToml`, `RateLimitConfig`, `ProxyConfig`, `ServerMetricsSnapshot` and other
  structs gain public fields (struct literals must set them), and some signatures change.
  The daemon's CLI, wire protocol and admin API stay compatible.

### Security

- **Plugins, rewrite rules and the tenant row filter now apply to the extended protocol.**
  The pre-query and route plugin hooks, the `[query_rewrite]` rules and the
  `[multi_tenancy]` row filter (with `unknown_tenant`) read simple queries only: a
  statement sent as `Parse`/`Bind`/`Execute`, which is how most drivers send every
  statement, passed a plugin's Block or rewrite, the rewrite rules and the row filter
  untouched. Every `Parse` now passes them before the proxy records anything of it: a
  rewrite replaces the SQL of the `Parse` the proxy registers, routes by, caches,
  journals and re-prepares; a refusal (42000 from a plugin, 42501 from the row filter)
  refuses the whole cycle, registers nothing and never reaches a backend. A `Bind` that
  executes a statement prepared in an earlier cycle runs the plugin hooks again for
  their Block, so a plugin that limits executions sees each one. `[schema_routing]`, the
  route plugin's node choice and the post-query hook cover extended cycles as well. See
  "Statement hooks on the extended protocol" in `docs/configuration.md` for the exact
  semantics and limits.
  **Upgrade note:** deployments with `[plugins]`, `[query_rewrite]` or
  `[multi_tenancy] tenant_tables` will see prepared statements blocked, rewritten or
  filtered that previously ran as sent.
- **The first-party plugins' answers take effect.** They answer the pre-query hook in
  the plugin ABI's spelling (`{"kind": "block", "reason": ...}`), which the proxy did not
  read: every Block of the cost governor, token budget and LLM guardrail and every
  Rewrite of the column mask failed to decode, was logged as a hook error, and the
  statement ran. Both spellings are read now, and an answer the proxy does not know is a
  failed hook call instead of a silent pass. The plugins' end-to-end tests looked for
  the plugin build in a path that does not exist and skipped; a CI job now builds the
  plugins and runs them.
- **Plugins see who and where a statement comes from.** `hook_context` carried only the
  session id, so the plugins that decide by tenant, roles, region, client address or AI
  agent decided without them. It now carries the identity, database, branch and the
  attributes they read (see "What a hook call carries" in `docs/configuration.md`), and
  `normalized` and `tables` are filled.
- **A plugin's memory limit is enforced.** `[plugins] memory_limit_mb` (and a lower
  `max_memory` in a plugin's manifest) bounded nothing: a call could grow its linear
  memory without limit. Each call's store is now limited: growing past the limit fails
  inside the plugin, and a module whose initial memory is above it cannot be called.
- **Plugin signatures can bind the manifest.** A signed module could be repacked under
  another plugin's name, and so reach that plugin's KV namespace, without breaking its
  signature. Artefacts packed with `helios-plugin pack --sign-with` (schema 1.1) also
  carry `manifest.sig`, a signature over `manifest.json`, which pins the module's
  SHA-256; a present `manifest.sig` must verify, and `[plugins] require_signed_manifest
  = true` (with `trust_root`) loads only artefacts that have one.
- **A plugin artefact must contain exactly what the format allows.** The loader ignored
  unknown archive entries, never validated an artefact's plugin name or version, and
  read archives without a size bound. An artefact with any entry but `manifest.json`,
  `plugin.wasm`, `plugin.sig` and `manifest.sig` (or one twice, or a non-file entry), an
  invalid name, version or hook list, or a file or decompressed content over `[plugins]
  max_artefact_bytes` (default 64 MiB) is refused.
- **A plugin that would never run is refused at load.** A manifest hook name the proxy
  does not know was dropped silently, a hook the proxy never calls (`on_connect`,
  `cache_get`, ...) was registered and never ran, and a module missing a declared hook's
  export (or `alloc`, `dealloc`, `memory`, or with the wrong signature) loaded and then
  failed every call — for a pre-query guard, letting every statement through. Each of
  these now refuses the plugin when it is loaded, with the reason in the log and in
  `GET /plugins/files`.
- **Plugin files in the plugin directory are not followed through symlinks**, so a
  plugin is always a file of that directory.
- **A routing target must be a configured node.** A `node=` routing hint, a route
  plugin's `node` answer or `[schema_routing] analytics_node` that named no `[[nodes]]`
  entry was dialed as a raw `host:port`: a client with routing hints enabled could make
  the proxy connect to any address, and with an `auth_file` authenticate there with the
  proxy-held credentials. Such a target is now refused with `42704` and the statement is
  not run; an unknown `analytics_node` is a startup error.
- **A statement no statement hook can read is refused while one is active.** A statement
  whose text is not valid UTF-8 (a `LATIN1` or other non-UTF-8 `client_encoding` sending
  non-ASCII characters) passed the plugins, the rewrite rules, the tenant row filter and
  `unknown_tenant` unread, on both protocols. While any of them is configured it is now
  refused with `22021` (use `client_encoding` UTF8); without them nothing changes.
- **Extended-protocol routing reads every statement of a cycle.** `[schema_routing]` sent
  a whole cycle to `analytics_node` when its first statement was an analytical read,
  also when a write or a point read followed in the same cycle; it now needs every
  statement of an unsplit cycle to be one. The route plugin's answers for all
  statements of a cycle (not only the first `Parse`'s) are combined, a cycle that only
  executes prepared statements is routed by the plugin's answer for them instead of
  staying on the session's current node, and answers that cannot be honoured together
  refuse the cycle (42000). The anomaly detector reads every `Parse`, as the client
  sent it, instead of the first one per cycle after the hooks rewrote it.
- **At a later execution of a prepared statement the plugin hooks read the client's
  text**, as at its `Parse`, instead of the text the rewrite rules or the row filter
  registered (a plugin loaded after the `Parse` included). A pre-query plugin that now
  rewrites the statement differently than when it was prepared refuses that execution
  with `0A000` (prepare it again) instead of being ignored.

- Default query/edge result caching to a tiny scalar-literal subset unless the operator explicitly vouches for backend read effects and dependencies. MCP tool-result caching requires a separate effects and pooled-context-equivalence contract, including manually attached caches. Native SQL forwarding remains available. Scope rejection publication now obeys the configured client write timeout and retires lost sockets even when the client write fails.

- Verify daemon query/edge cache execution scope on the selected backend with bounded effective-settings probes, clear L1 when that scope changes, and validate a native fill again before publication. Backend role/database defaults now partition shared entries; unsupported proof responses execute natively. The default adds backend round trips to cache reads (two `SHOW ALL` per miss, one per hit: pgbench's select-only script over the simple protocol at 16 clients measured 4 200 reads per second with the cache on, against 68 000 before the proof and 66 000 with `backend_context_immutable = true`); `cache.backend_context_immutable` permits proof reuse only under an explicit immutable-backend-context contract, and `[cache] proof_ttl_ms` (see Changed) now reuses a connection's proof for one second by default. Opaque view/RLS dependencies remain unvalidated.
- Refuse function-name-only cache context proof, including schema-qualified builtin names, overloads and comment-separated calls; those sessions keep executing natively after the unverified call.

- Namespace daemon SQL-result cache keys by every startup parameter and permanently bypass lookup/store after statements that may change execution context on either protocol. Mixed read/write sessions may have fewer hits. Opaque backend catalog dependencies still require validation.


- **Routing hints are read only from comments outside literals.** With
  `[routing_hints]` on and `strip_hints = true` (the default), a
  `/*helios:…*/` sequence inside a `'…'` or `E'…'` string, a `$$…$$` or
  `$tag$…$tag$` literal, or a `"quoted identifier"` was parsed as a hint and
  stripped from the statement, so `INSERT INTO notes(body) VALUES ('see
  /*helios:route=standby*/ here')` stored `'see  here'` and could route the
  statement. Hints are now found with the crate's quote-aware lexer
  (`crate::sql_text`): text inside those literals is neither parsed nor
  changed, and real hint comments keep their behaviour. Tracked as Sprinter
  6c6503dd6d1e.
- **Row-level tenant filter: OR, comments and unrewritable shapes (2b3c3706f133,
  40fae8854295, a497b9ae7c97).** Under `[multi_tenancy]` row isolation the filter is
  now placed by a quote- and comment-aware lexer, not by searching raw text.
  - An existing `WHERE` is rewritten as `WHERE <filter> AND <condition>`, with the
    condition parenthesised when it has a top-level `OR`. Before, `WHERE a OR b` became
    `WHERE <filter> AND a OR b`, which returned other tenants' rows (SELECT) or deleted
    them (UPDATE, DELETE).
  - A missing `WHERE` goes before the first top-level clause or trailing `;`, never
    after a `--` comment, so the filter is no longer commented out.
  - Leading comments, `WITH` through a comment, and other shapes are no longer skipped.
    A statement that touches a tenant table in a shape the rewrite does not cover (a
    `WITH`, a parenthesised query, `TABLE t`, a `VALUES`, several statements, a tenant
    table inside a subquery) is refused with SQLSTATE 42501 and never reaches the
    backend. The HTTP, MCP and GraphQL gateways refuse it the same way.
  - Statements that touch no tenant table, and tenant statements of the shapes listed
    in `docs/configuration.md`, are unchanged.
- **Row-level tenant filter: remaining bypasses.** Unicode-escaped identifiers
  (`U&"…"`, `UESCAPE`) are refused, since they can spell a tenant table the lexer does not
  read. `INSERT` into a tenant table is rewritten only for `INSERT INTO <table> (<columns>)
  VALUES (<row>)[, (<row>)...]` with optional `ON CONFLICT (...) DO NOTHING` and
  `RETURNING`: the tenant column and value are added to the list and to every row. Other
  `INSERT` shapes are refused (the old text rewrite put the tenant id into the first column,
  left `INSERT ... SELECT` and `DEFAULT VALUES` unfiltered, and filled only the first row).
  `ON CONFLICT ... DO UPDATE` is refused, since a conflicting row may belong to another
  tenant. An `UPDATE` that assigns the tenant column is refused. A tenant id is escaped in
  every generated literal (`'` doubled), and one containing a NUL byte is refused.
- **Row-level tenant filter: round 3.** A tenant table followed by an alias is refused
  (42501); a derived table reusing a tenant table's name no longer shadows it. `DO`, `CALL`
  and the functions in the new `[multi_tenancy] refused_functions` key (default: the XML
  exports, dblink, large-object import/export and server-side file reads) are refused under
  row isolation. An identified tenant with no configuration is filtered by the default row
  column, or refused. Identifiers are matched as PostgreSQL truncates them (63 bytes). Writes
  use the table's own tenant column. A tenant id containing a backslash is refused. Proxy-side
  filtering is documented as not a hard boundary.
- **Branch names no longer escape `[[hba]]` or tenancy (69ae2a33ff96).** A startup
  `database=<db>@<branch>` (HeliosDB-Nano branch selection) was matched as a whole
  string, so a `database = "prod"` rule with `action = "reject"` did not refuse
  `prod@feature`, and `[multi_tenancy] identify_by = "database"` treated `acme@b` as
  an unknown tenant: its session was forwarded unfiltered and held none of acme's caps.
  An `[[hba]]` rule without `@` now covers the full value and its base (a rule with `@`
  matches only that exact value). `identify_by = "database"` resolves a name like Nano:
  the whole value if it is a configured tenant, else its base before the first `@`. Row
  filtering and the per-tenant caps now share that resolution (`src/database_name.rs`).

### Added

- **`[cache] l2_evict_headroom_pct`** (default 5, `0..=50`): how much of `l2_max_bytes`
  an L2 eviction pass frees beyond what the store needs, so one scan of the map makes
  room for many stores. `cache::L2Config::evict_headroom_pct` in the library.
- **Plugin hot reload works, and the plugin directory loads `.tar.gz` artefacts.**
  `[plugins] hot_reload = true` built a watcher nothing ever polled, and the startup scan
  skipped packed artefacts (the distribution format) entirely. The plugin directory is
  now reconciled at startup, on every SIGHUP, on `POST /plugins/reload` and — with
  `hot_reload` — every `hot_reload_interval_ms` (default 1000): every `.wasm` module
  (with its `.sig`, `.yaml` / `.json` sidecars) and `.tar.gz` artefact whose version has
  been stable for `hot_reload_debounce_ms` (default 500) is loaded; one file provides
  each plugin name (an artefact before a bare module, then file-name order); a new
  version replaces the running one only once it has loaded, so a version that fails
  (bad signature, bad module) leaves the old one running; a removed file unloads its
  plugin, and a file that failed is retried when it, a sidecar or the trust root
  changes. After a trust-root change a plugin that no longer verifies is unloaded.
  Changes are detected by size, time, inode and content digest. Plugins of a hook run in
  file-name order. A missing plugin directory holds no plugins instead of disabling the
  subsystem, and `max_plugins` is enforced. `GET /plugins/files` shows each file's state
  (loaded, stale, shadowed, failed, pending) and `GET /plugins` each plugin's file and
  SHA-256. A hook call no longer holds a lock or a map entry across the WASM call, and a
  reload of unchanged bytes reuses the compiled module.
- **Branch routing.** A route plugin's `branch` answer was logged and refused, and the
  `/*helios:branch=<name>*/` routing hint was parsed and ignored (so the statement ran on
  the session's own database). With `[branch] route_enabled = true` either now moves the
  session to the branch database before the statement runs — `<name>` on PostgreSQL,
  `<base>@<name>` on HeliosDB-Nano (`route_mode`), limited to `route_allow` — and the
  session stays there; `branch=` with no name moves it back. A move is made only at a
  request boundary outside a transaction (`25001`), when the session holds no state a
  connection to another database would lack (`0A000`), and when `[[hba]]` admits the new
  database (`28000`). Without `route_enabled`, a branch request refuses its statement
  (`42000`) instead of running it on the wrong database. New metric
  `heliosdb_proxy_branch_moves_total`.
- **A plugin's cached result answers extended-protocol statements.** A pre-query plugin's
  `cached` answer was honoured for simple queries only: the same statement sent as
  `Parse`/`Bind`/`Execute` ran on the backend. A cycle that is one execution the answer
  can stand for (unnamed portal, no parameter values, text results, the portal described,
  no row limit, ending with `Sync`) is now answered by the proxy — for a statement the
  cycle Parses, a named statement prepared earlier, or the unnamed statement of an
  earlier cycle — without any backend. When the plugin stops answering, a later cycle
  binding an unnamed statement it had answered gives the backend that statement first.
  See "Statement hooks on the extended protocol" in `docs/configuration.md`.
- **`helios-plugin` packs, signs, inspects and verifies artefacts.** New verbs: `keygen`
  (an Ed25519 key pair: PKCS#8 PEM signing key, trust-root public key), `pack` (from
  flags or a `plugin.yaml` manifest; `--sign-with` signs module and manifest), `sign`
  (writes a module's `.sig`, re-signs an artefact), `inspect` (manifest, entries, sizes,
  digests; a module's exported hooks; `--json`). `verify` accepts artefacts and
  `--require-signed-manifest`. Keys and trust roots are read as PEM (what `openssl
  genpkey -algorithm Ed25519` and `openssl pkey -pubout` write), base64 or hex, and
  `[plugins] trust_root` takes a directory of `*.pub` keys, one key file, or a hex key.
- **A bare plugin module may carry its manifest** in a `helios_manifest` custom section,
  and one with no manifest at all loads (for development) under its file name with the
  hooks it exports.

- **SIGTERM and SIGINT shut the proxy down gracefully.** Until now only SIGHUP (reload)
  and SIGUSR2 (binary handoff) were handled, so `docker stop`, a Kubernetes pod
  termination or `systemctl stop` ended every session at once, mid-statement. On SIGTERM
  or SIGINT the listener now closes, `/health/ready` answers 503 `Proxy is draining`, and
  each session is ended with FATAL `57P01` as soon as nothing of it is in flight: idle
  sessions at once, the others when their transaction, extended cycle or COPY is over.
  The process exits 0 when the last session is gone or after
  `shutdown_drain_timeout_secs` (default 60, env `HELIOS_DRAIN_TIMEOUT_SECS`); a second
  signal exits at once. New counters `sessions_drained` /
  `heliosdb_proxy_sessions_drained_total` and the `draining` flag in `GET /metrics`.
  The HTTP SQL, MCP and GraphQL gateways take part: they stop accepting with the
  PG-wire listener and answer the requests they already accepted before the process
  exits (they used to be cut). A connection accepted just before the listener closed
  is ended at its startup message instead of after authenticating, and the recovery
  journal's queued records are written out before exit.
- **Tenant active-query permits: extended-protocol unlocks and two-phase completions.**
  Under session pooling a session holding a session-level advisory lock keeps its
  tenant permit until it releases the locks; `pg_advisory_unlock_all()` and `DISCARD ALL`
  sent over the extended protocol (as most drivers send everything) did not release it,
  so such a session held its permit until it disconnected. They now do once their cycle
  succeeds. The `COMMIT PREPARED` / `ROLLBACK PREPARED` exemption from the tenant cap now
  also covers the shapes drivers send: a named statement prepared earlier and executed
  with `Bind`/`Execute`/`Sync`, a `Describe` of the portal (pgjdbc), result format codes,
  and sessions with plugins, `[query_rewrite]` or tenant tables configured (the statement
  is read after those hooks, as it runs).
- **Credential-stuffing detection is fed.** The anomaly detector's failed-auth burst
  detection (`[anomaly] auth_window_secs`, `auth_warning_count`, `auth_critical_count`)
  was never given an authentication event, so it could not fire. Every client
  authentication outcome now reaches it: the proxy's own SCRAM check, a plugin's
  Authenticate denial, and in pass-through mode the backend's class-28 refusal of the
  client's credentials. New key `[anomaly] max_auth_windows` (default 10000) bounds the
  per-`(user, address)` counts it keeps.
- **`[pool_mode] mode = "statement"` behaves as statement pooling.** It behaved exactly
  like transaction pooling: a client could hold a backend connection across a whole
  transaction block. A lone `BEGIN` / `START TRANSACTION` is now refused with `0A000`
  (the session goes on), and a request that leaves a block open anyway ends its session
  (FATAL `0A000`, connection closed, block rolled back), as in PgBouncer's statement
  mode; a block one request opens and ends still runs.
- **A failover no longer continues a session without state only the failed backend had.**
  Transaction replay restored a session's `SET`s on the replacement but quietly dropped
  its temporary tables, `WITH HOLD` cursors, SQL `PREPARE`d statements, `LISTEN`
  registrations and session-level advisory locks (the last released when the backend
  died, so the application's mutual exclusion was gone without a sign). A session that
  holds any of them is now ended with `08006` naming what it held; what a transaction
  replay recreates and what the session cleared (`DISCARD ALL` and the like) does not
  count. Under `tr_mode = none` an idle session whose backend died was redialed even
  when it held settings or such state; it now gets `57P01` and is closed, as documented.
- **An open circuit recovers.** The daemon and the gateways only read a node's
  circuit state, and the move from open to half-open happened only in a library call
  they never made: once `[circuit_breaker]` opened a circuit, every request to that node
  was fast-failed until the proxy restarted, and `open_secs` and `success_threshold`
  had no effect. After `open_secs` a request is now admitted as a half-open probe (at
  most two at once), `success_threshold` successes close the circuit and a failed probe
  opens it again; reads stop avoiding the node once it may be probed.
- **`[rate_limit]` enforces the configured rate, and its memory is bounded.** Every
  bucket also carried a hidden 60 000-requests-per-minute window, so a `default_qps`
  above 1 000 was cut to 1 000 a second after the first minute's 60 000 requests, and
  each bucket preallocated room for 60 000 timestamps; no bucket was ever dropped, so
  `key_by = "client_ip"` grew without bound. The window is now opt-in
  (`window_max_requests`, `window_secs`), and idle buckets are dropped every
  `cleanup_interval_secs` without changing any decision.
- **The HTTP SQL gateway's result cache follows the relay's rules.** It cached
  statements the PG-wire relay refuses (a system catalog, a `[cache]` excluded table,
  time-travel or ambiguous text) and read only the built-in volatile-function list, not
  `extra_volatile_functions`. It now applies the same checks.
- **The SIGUSR2 binary handoff drains like a shutdown.** It used to wait for clients to
  disconnect on their own and then cut every session still open at
  `shutdown_drain_timeout_secs`, in-flight transactions included; a pooled client
  rarely disconnects, so most handoffs ended that way. It now ends each session at its
  next point with nothing in flight, as SIGTERM does, so clients reconnect to the new
  process without losing work.
- **Release binaries include the `postgres` and `patroni` topology providers.** The
  prebuilt binaries were built with `all-features`, which carries no topology provider,
  so `[topology] provider = "postgres"` was refused by them while the Docker image
  accepted it. They are now built with `all-features,postgres-topology`, like the image
  and the CI matrix.
- **SIGHUP resizes per-tenant caps.** A reload that only changes the size of caps or
  byte budgets tenants already run under (`[multi_tenancy]` client caps, active-query
  caps, replay-byte and recovery-journal budgets, defaults and per-tenant overrides)
  now applies to the running process instead of being kept back for a restart. A raised
  cap admits waiters in FIFO order; a lowered cap ends nothing and admits nobody new
  until the tenant's holders are back under it. Any other change to the section, and a
  cap a tenant would gain or lose, still refuses the whole section with a warning that
  names the reason (unchanged behaviour for those).
- The HTTP gateway's `POST /sql` can use the daemon's SQL result cache. It is consulted only when writes to the gateway's backend are visible to cache invalidation (the backend is a `[[nodes]]` target or this proxy's own listener, or `[cache] invalidation_source = "logical"`); otherwise reads execute natively and startup logs the bypass. Keys bind the backend endpoint, user, database, tenant, typed parameter values and the Neon array/object result shape. Results over the HTTP row/byte limits, errors, session-dependent statements and function calls are never cached, and without `[cache] trust_backend_read_effects` only literal scalar `SELECT`s are considered. The MCP tool-result cache skips its function-call scan for statements with no `(`.

- Optional `[limits] copy_idle_timeout_secs` (default 0/off) bounds stalled COPY input, including partial frames and the final extended Sync after backend completion. Backend processing time is excluded. Expiry cancels and drops the COPY backend, rolls back unfinished input, closes with FATAL 57014 and returns tenant capacity. JSON/Prometheus counters expose COPY expiries and query-cache context bypasses without changing the public metrics snapshot shape.


- **`GET /api/stats`, schema_version 1, for HeliosDB-Lite and other pollers (Sprinter
  c3397d898b25).** One versioned JSON document on the admin listener, token-gated like
  every other `/api/*` route: `schema_version`, `proxy_version`, `generated_at`,
  `uptime_secs`, and the sections `nodes` (role, health, lag, override, circuit state,
  per-node backend connections at a `max_connections` cap), `pools`, `sessions`,
  `traffic`, `cache` (per tier entries, bytes, hits, evictions; heatmap summary),
  `tenants`, `rate_limit`, `transaction_replay` and `gateways` (HTTP, GraphQL, MCP and its
  tool cache). Every section is always present: one whose feature is compiled out or
  switched off is `{"enabled": false, "reason": "..."}`. It is built from the snapshots
  `/metrics`, `/nodes`, `/topology`, `/api/circuit`, `/api/cache/heatmap` and
  `/api/mcp/cache` already take, plus configuration values and the existing counters of
  the transaction/statement-mode idle backend pool; no counter is added to the data path.
  Lists are sorted and cut at the new `[admin] stats_max_items` (default 1000, must be
  >= 1) with `count` and `truncated`. Contract: within version 1 fields are only ever
  added, a breaking change bumps `schema_version`; documented field by field in
  docs/admin-api.md and pinned by `tests/fixtures/api-stats-v1.schema.json` (JSON Schema)
  and `tests/fixtures/api-stats-v1.example.json`, against which a test validates the live
  response strictly. Lite's view poller (S2) reads this instead of linking the proxy
  crate.
- **MCP agent memory tools (`[mcp] memory_*`, Sprinter 20bcc218385f).** Off by default
  (`memory_enabled = false`: the tool catalog and responses are unchanged). With it on, the
  MCP gateway adds `memory_put` (key, any JSON value, optional `ttl_secs` and `tags`),
  `memory_get`, `memory_list` (keys, tags and times; `prefix`, `tag`, `limit`, `cursor`),
  `memory_search` (the same page with values; exact prefix and tag match, no similarity)
  and `memory_delete`. Entries are rows of `memory_table` (default `heliosdb_mcp_memory`)
  on the MCP backend, so they survive restarts and are shared by every proxy instance on
  it. Every row is owned by the request's principal and tenant, which the proxy binds as
  `$1` and `$2` of every statement and no argument can set. The statements are a fixed set
  sent through the extended protocol with every value bound (new
  `BackendClient::query_bound_bounded`), as `memory_backend_user` (default `backend_user`)
  on connections of their own. The shared principal `default` is refused, and
  `memory_enabled` needs a `[[mcp.principals]]` entry. `memory_put` and `memory_delete` are
  writes: refused by `read_only` or a read-only contract. They run after admission, the
  read-only guard, the rate limit, the circuit breaker and the tenant's active-query permit,
  and move the C-02 write generations of the memory table. Entries expire
  (`memory_default_ttl_secs = 604800`, `memory_max_ttl_secs = 2592000`); reads filter
  expired rows and each put deletes its owner's. Bounds: `memory_max_key_bytes = 256`,
  `memory_max_value_bytes = 65536`, `memory_max_entries_per_owner = 1000`,
  `memory_max_tags = 16`, `memory_max_tag_bytes = 64`, `memory_list_max_limit = 100`.
  `memory_auto_create = false` checks the table at startup and refuses to start, naming the
  DDL, when it is missing; `true` creates it at the first call. While memory is on, `query`
  and `explain` refuse statements that name the memory table. `strict_config` refuses a
  memory role equal to `backend_user` (startup warns otherwise), because that role could
  read every owner's rows through `query`. Needs a PostgreSQL MCP backend: HeliosDB-Nano and
  HeliosDB-Lite lack the interval arithmetic, and Lite a bound `LIMIT`. New metrics:
  `heliosdb_proxy_mcp_memory_ops_total{op}`, `_refused_total{reason}`,
  `_entries_quota_hits_total` (no principal or tenant labels), `mcp_memory` on `/metrics`,
  and an `mcp-memory` row on `/capabilities`. Documented under "MCP agent memory tools" in
  `docs/configuration.md`, with the DDL. Library API: `McpConfig` gains the `memory_*`
  fields (struct literals must set them); new module `mcp_memory`.
- **MCP tool-result cache (`[mcp] tool_cache_*`, Sprinter 20bcc218385f).** Off by default
  (`tool_cache_enabled = false`; needs the `query-cache` feature). With it on, the MCP
  gateway answers a repeated `query` or `list_tables` from memory; `explain` is never
  cached. Entries are keyed by owner (principal, tenant), tool and the final backend SQL
  (after `[query_rewrite]` and the tenant filter), compared on every byte; only a single
  plain `SELECT` naming at least one table and no system catalog, excluded table,
  backend-ambiguous text or volatile function (the result caches' scan, with `[cache]
  extra_volatile_functions`) is cached. Entries are retired by the shared C-02 write
  generations (writes through the PG-wire listener and every gateway, with or without
  `[cache]`), with the fill-race rule, and by a TTL (`tool_cache_ttl_secs = 60`, a shorter
  `[[cache.tables]] ttl_secs` applying; `list_tables` follows DDL and
  `tool_cache_catalog_ttl_secs = 30`). Admission, the read-only guard or contract and the
  rate limit run first (hits consume rate-limit tokens); the circuit check and the
  tenant's active-query permit apply only on a miss (a hit runs nothing on the backend,
  so it is served while its tenant is at its cap). Bounded per owner and overall in
  entries and bytes (`tool_cache_max_entries = 10000`, `tool_cache_max_bytes = 64 MiB`,
  `tool_cache_max_entries_per_principal = 1000`, `tool_cache_max_bytes_per_principal =
  8 MiB`, `tool_cache_max_entry_bytes = 1 MiB`, `tool_cache_max_statement_bytes = 16384`),
  with O(1) amortized eviction. `query` and `list_tables` gain an optional `"fresh": true`
  that bypasses and refreshes the entry; a cacheable call's result carries
  `_meta["heliosdb.com/cache"]` (hit, age, stored, skip reason; never data). With the cache
  off, responses are unchanged. `strict_config` refuses the cache when the MCP backend is
  neither a `[[nodes]]` address nor the proxy's listener and no logical source runs
  (startup warns otherwise). New admin endpoints `GET /api/mcp/cache` and
  `POST /api/mcp/cache/clear`; new metrics `heliosdb_proxy_mcp_cache_*` (hits, misses,
  stores, stale, expired, raced fills, skips by reason, evictions by scope, entries,
  bytes, owners; no principal or tenant labels); a `mcp-tool-cache` row on
  `/capabilities`. Documented under `[mcp]` in `docs/configuration.md`.
- **MCP: per-agent principals (`[[mcp.principals]]`, Sprinter 20bcc218385f).** Each
  entry has an `id` (`[A-Za-z0-9_.-]{1,64}`, unique; `default` reserved) and a `token`
  (non-empty, unique, not `auth_token`). The request's bearer token selects its
  principal: the presented token's SHA-256 is compared in constant time against every
  configured one, with no early exit; an unknown or missing token gets 401 once any
  token is configured. Backward compatible: `[mcp] auth_token` keeps working, and its
  holders (or every caller, with no token configured) are the principal `default`. The
  principal, with the request's tenant, is the isolation unit for per-agent MCP state; it
  is logged with each tool call, and tokens are never logged, serialised or shown by
  `Debug`. Startup-only, like the rest of `[mcp]`. Library API: `config::McpPrincipal`,
  `McpConfig::principals` (struct literals must set it), `MCP_DEFAULT_PRINCIPAL`.
- **Query-cache refresh-ahead** (`[cache] refresh_ahead_ratio`, default `0.0` = off;
  `refresh_ahead_claim_timeout_ms`, default `5000`). Once an L1/L2 entry is at least
  ratio × its TTL old, exactly one read of it claims the refill: it is treated as a miss
  (bypassing its own L1 copy), runs on its own backend connection, and its result replaces
  the entry through the normal store path, while every other read is still served the
  entry until the refill lands or the TTL ends; connections whose L1 copy is past the
  refresh point pick the refill up from L2. A hot key no longer stampedes the backend at
  expiry, and its readers see no latency cliff there, for one extra backend read per hot
  entry per TTL. A claim ends when its refill is stored or, for any other outcome (error
  response, oversized result, a write racing the fetch, a standby's answer, a lost client),
  is abandoned; a claim older than the timeout is taken over, so a lost claimant never
  blocks refreshes. New counters `heliosdb_proxy_cache_refresh_ahead_claims_total`,
  `_completed_total`, `_abandoned_total` (JSON `query_cache.refresh_ahead`). Off, the
  lookup pays one branch. Validated at startup (an error names the key); `[cache]` stays
  startup-only on SIGHUP. Library API: `CacheConfig` gains `refresh_ahead_ratio` and
  `refresh_ahead_claim_timeout`, and `CacheToml` the two keys (struct literals must set
  them).
- **Query cache: tier sizes and L2 payload compression** (distribcache merge P1,
  7c860e8a2e14). New flat `[cache]` keys, defaults unchanged behaviour:
  `l1_max_entries = 500` and `l1_max_bytes = 33554432` (per client connection; were
  hardcoded), `l2_max_bytes = 268435456` (shared L2; was hardcoded as 256 MiB),
  `l2_compression = "none" | "lz4" | "zstd"` (default `"none"`),
  `l2_compression_min_bytes = 4096` and `l2_zstd_level = 3` (1..=19). With a codec, L2
  stores payloads of at least the threshold compressed when that shrinks them (a
  marker byte names the codec; an entry that does not decompress is dropped and the
  read misses), counts the compressed size against `l2_max_bytes`, and decompresses on
  every L2 hit before serving or promoting to L1, which keeps raw bytes. zstd runs on the
  blocking thread pool, before the C-02 generation check, which still runs last before
  the stores; LZ4 runs inline. It only pays when the hot set does not fit `l2_max_bytes`
  uncompressed. Every key is validated at startup and an error names it. Startup logs
  `l2_max_bytes` and `l2_compression`; `/capabilities` reports `cache.l2_compression`
  on a build without `query-cache`.
- **Query cache metrics:** `heliosdb_proxy_cache_evictions_total{tier="l1|l2|l3"}`
  (evictions were counted only in tests), `heliosdb_proxy_cache_entries{tier}` and
  `heliosdb_proxy_cache_compression_saved_bytes` (raw minus stored bytes L2 retains),
  with `entries`, `evictions` and `compression_saved_bytes` under `query_cache` in
  `GET /metrics`.
- **Config hints:** `[cache.l1]`, `[cache.l2]` and `[distribcache]` name the new flat
  keys.
- Criterion: `cache/l2/compress_{lz4,zstd}_{4k,64k,1m}`,
  `cache/l2/hit_{lz4,zstd}_{4k,64k,1m}` and the control `cache/l2/hit_none_64k`, on
  DataRow payloads.
- **Query-cache heatmap with recommendations** (distribcache merge P2). With
  `[cache] heatmap_enabled = true` the cache counts, per table and per time bucket
  (`heatmap_bucket_secs = 300` × `heatmap_buckets = 288`, a 24 h window), hits, misses
  (and of those, expired L2 entries), stores and their bytes, C-02 write invalidations,
  stale rejections and raced fills. `GET /api/cache/heatmap` (token-gated like every
  `/api/*` route) lists the tables hottest first with hit ratio, temperature and last
  access, and recommends `cache.exclude_tables` for a table with more invalidations than
  hits, or a higher `[[cache.tables]] ttl_secs` when most of its misses found the entry
  expired, once it has `heatmap_min_samples = 100` reads in the window. The table set is
  bounded by `heatmap_max_tables = 1024` (samples beyond it count in `overflow`; idle
  tables make room once per bucket). Recording takes no lock (per-table atomic rings, a
  copy-on-write table set); off (the default), each cache access pays one branch. A
  `[cache.heatmap]` table gets a hint naming the flat keys. Per-table retained bytes are
  not reported (the cache keeps no per-table byte count). Library API: `CacheConfig`
  gains `heatmap` (`cache::HeatmapConfig`) and `CacheToml` the five keys (struct
  literals must set them); new `QueryCache::heatmap_snapshot` and
  `L2WarmCache::get_noting_expiry`. Criterion cases
  `cache/heatmap/record_hit_{off,on,contended_8}`.

### Changed

- **Cached reads reuse the backend settings proof (`[cache] proof_ttl_ms`, default
  1000) (perf).** Every cached read proved the backend's effective settings with a
  `SHOW ALL` round trip before the lookup and again before storing a miss, which cut
  cheap cached reads to about 4 200 a second at 16 clients, against 68 000 without the
  cache. A connection's proof is now kept and reused by the reads after it on that
  connection until `proof_ttl_ms` passes, anything other than a proven cacheable read
  reaches the connection (a write, a `SET`, a `FunctionCall`, an extended batch that is
  not a cached read), or a ParameterStatus is relayed to the session. Under
  transaction and statement pooling the proof is parked with the connection: one taken
  while the connection held its reset settings survives the `DISCARD ALL` reset and is
  reused by the next lease of a session with the same startup parameters, and a lease
  that served only a cache hit is parked again without the reset (v2.2.0 served such a
  hit without leasing a connection). The `backend_context_immutable` proof is parked
  with its connection as well. `0` restores the per-read proof;
  `backend_context_immutable` still never re-proves. The proof's reply is read into one
  buffer with as few reads as the socket allows and its rows are borrowed from it, not
  copied (it used two reads and three allocations per row); the namespace it proves is
  unchanged. New counters `heliosdb_proxy_cache_scope_proofs_total` and
  `heliosdb_proxy_cache_scope_proofs_reused_total`.
- **Recovery journal: the per-statement size cap no longer re-walks the transaction
  (perf).** Every statement appended to an explicit transaction's journal checked
  `[journal] max_bytes_per_transaction` by summing all earlier entries again, so a
  transaction of `n` statements measured `n²/2` entries (up to the 10,000-entry
  default). The append path now keeps a running size and measures only what was
  appended since the last check; a savepoint rollback, a cleared journal or a decoded
  one is measured again once. The cap is enforced at the same point (tested);
  `total_size()` still walks and stays exact. New Criterion cases
  `journal/append_entry/after/{16,1024}`. No behaviour change.
- **Capture relay: a write cycle that arrives whole skips the response stash (perf).**
  With capture armed, each backend read chunk's completion metadata was stashed under
  the session's response-prefix lock and merged back at `ReadyForQuery`, even when the
  `ReadyForQuery` was in the same chunk and nothing was held from an earlier
  Flush/COPY boundary. That common cycle now hands its metadata to the observation
  directly, under the same `limits.max_pending_bytes` bound. Cycles that span chunks,
  hold earlier metadata or carry an explicit `COMMIT` keep the stash and its
  commit-prefix proof. No behaviour change.
- **Recovery journal: `SAVEPOINT` without a tenant journal budget no longer measures
  the whole transaction (perf).** The retained-size walk it took to undo a refused
  reservation now runs only when `[multi_tenancy]` configures a journal budget.
- **Recovery journal: the size walk reads scalar parameters inline (perf).** The
  per-parameter size estimate is inlined for scalars and kept out of line only for
  arrays, which removes the call per parameter that `total_size()` and the running
  size paid (`journal/total_size/{1,16,128}`). Same estimate for every value.
- **L2 cache: a compressed payload no longer pins its worst-case buffer.** LZ4 and
  zstd payloads were compressed into a buffer sized for the worst case and stored
  truncated, so each kept an allocation at least as large as the uncompressed rows
  while L2 charged only the stored length against `l2_size_mb`. The buffer is now
  shrunk to the stored length before it is shared. No format or config change.
- **Statement-mode safety scan: one keyword compare instead of nine (perf).**
  `StatementModeHandler::is_safe_query` / `get_query_warning` tested nine keyword
  prefixes in turn; they now pick the candidate keyword from the statement's first
  byte and compare it with a case-bit mask, and skip the trim when the statement
  already starts and ends on a visible ASCII byte. Same answers for every input
  (differential test against the sequential scan, ~80k statements).
- **Journal manager: beginning a transaction hashes its id once (perf).** The
  active-journal store inserted the journal and then looked the same key up again
  to return its handle. No behaviour change (replacement and eviction order tested).
- **Semantic cache (L3): a slot is 216 bytes again, not 288 (perf).** The cache
  namespace added to every slot grew it from 256 to 288 bytes, which took a bucket's
  first block of four slots (and the list a store evicts into) from 1024 to 1152
  bytes, past what glibc serves from its per-thread cache: a store into a full tier
  ran 6 % slower and eight threads storing at once 21 % slower
  (`cache/l3/store_full_5000`, `cache/l3/store_lookup_contended_8`). The namespace
  and the context strings now sit behind one pointer shared by the slots of a
  bucket; a compile-time check keeps the block within 1024 bytes. Same isolation
  (namespace and context tests).
- **Pool-mode transaction detection reads keywords, not prefixes.**
  `TransactionEvent::detect` (the library pool's lease state; the daemon follows the
  backend's ReadyForQuery status) compared prefixes of the trimmed text. It now
  reads the statement's leading words, separated by any whitespace PostgreSQL's
  lexer skips: `START\tTRANSACTION` and `ROLLBACK\nTO sp` are recognised (the
  second was read as a full rollback, ending the lease's transaction); `ABORT` is a
  rollback; `COMMIT` / `END` / `ROLLBACK` / `ABORT ... AND CHAIN` keep the session in
  a transaction (`Begin`), where they were read as its end; a keyword is a whole
  word (`BEGINNING`, `ENDPOINT`, `START SLAVE` and `START WORK TRANSACTION` are
  statements); and a `TO` elsewhere in a `ROLLBACK` no longer names a savepoint.
  Statements written with single spaces classify as before (reference test against
  the prefix form). Allocation-free as before; it reads at most four words.
- **Cache lookup: the comment and hint searchers are built once (perf).** The cache
  hint parser and the lexer's comment tests called `memchr::memmem::find`, which
  builds its searcher on every call: about 30 ns each, more than the search of a
  statement-sized text. Building them was 34 ns of a 480 ns L1 hit, 84 ns of a
  2.6 us L2 hit and 174 ns of a 5.5 us miss and store (sampled profile,
  gpc001ca). The searchers for `--`, `/*`, `*/`,
  `&"` and `helios:` are now statics, as the routing hint parser's already was.
  Same answers (generated-text test against a window scan).
- **Router write detection reads the first word without allocating (perf).**
  `QueryRouter::is_write_query` upper-cased the whole statement into a new string to
  compare its first word. An ASCII first word is now upper-cased in a ten-byte stack
  buffer; text with a non-ASCII byte before the end of its first word takes the old
  path, so Unicode case and whitespace rules decide exactly as before (reference
  test against the upper-casing form). The journal classifier's call-free shortcut
  searches for `(` with `memchr` instead of the slice's byte loop.
- **An unnamed `Parse` inside a batch ends the promotion of the one before it.**
  `optimize_unnamed_parse` records the unnamed statement a backend connection
  holds when its `Parse` is the first message of a batch. An unnamed `Parse` later
  in a batch (after a named `Parse`, for instance) replaced that statement on the
  backend without replacing the record, so an identical `Parse` of the earlier text
  was answered by the proxy and its `Bind` ran the newer statement. The record is
  now dropped when such a batch is delivered.
- **Query cache and journal classifier: two restructurings, no measured gain.** The
  query cache merges a hint's dependencies into the caller's normalized query instead
  of returning a new one inside an `Option`, and the journal classifier's
  function-call scan is out of line. Both were meant to take back per-call costs of
  this release's changes (6-12 ns per L2 lookup, 5-10 ns per classified statement);
  the Criterion gate of 2026-11-17 separated neither. No behaviour change.
- **Journal classifier: the unterminated-nested-comment check looks at one byte
  first (perf).** The `/* /* */` tail test trimmed every statement; it now runs only
  when the last non-blank byte could end such a comment. Same classification
  (tested with ASCII and non-ASCII trailing blanks).

- **Retry safety:** builtin-looking function names no longer imply safe retry or
  standby offload. Call-shaped reads require explicit `tr_read_functions` trust.
  Calls separated from parentheses by comments are recognized; EXPLAIN and COPY
  query wrappers use the same eligibility check. Ordinary execution remains
  native; an untrusted call returns an error on an uncertain outcome.

- **C-02 write path: the plain-ASCII check stops at the first quote (perf).** The
  plain-ASCII fast path of `write_tables` (query cache and edge) and the journal's
  classifier ran its whole-text ASCII test first, so every statement that is not plain
  (a bulk `INSERT ... VALUES ('...')` of 1 MiB) paid a full pass over its text, about
  1.1 ms per MiB, before it took the lexer anyway. The quote scan now runs first and the
  ASCII test last; the answers are the same (tested). No behaviour change.
- **Routing hints: parse and strip cost restored (perf).** Reading hints only from
  comments outside literals (the `[routing_hints]` literal fix) lexed every hinted
  statement whole and ran the hint regex with captures per comment, so parse and strip
  cost up to 3x what they did. The reader now stops after the last `helios:` in the
  statement, finds the comments of a statement without `'`, `"` or `$` with a byte scan
  (no literal or quoted identifier can exist there; the lexer's comment rules, rests
  included), matches a block-comment hint without a regex search, and strip trims in
  place. Hints inside literals are still never read or stripped; property tests compare
  the scanner with the lexer and the new reader with the old one on generated statements,
  with and without literals. No behaviour change.
- The C-02 write path (`QueryCache::write_tables`, the edge `write_tables`) and the
  journal's statement classifier skip the lexer pass for plain ASCII statements: no
  quote, `$`, backslash, `-`, `/`, CR, and no `;` but a final one. The answers are the
  same (a property test compares both paths on generated statements). No behaviour change.
- The `[cache] exclude_tables` scan builds its automaton as a DFA (one table lookup per
  byte) instead of the crate's default pick, which is slower for small name sets. The
  matches are the same (a test compares the two automata on generated names and text).

- The SQL lexer core (`Piece`, `Pieces`, comment and statement-start reads, the
  multi-tenancy identifier scan, the routing hint comments) moved from `sql_text` to a new
  `sql_lex` module, compiled only with the features that read it. Single-feature builds of
  `multi-tenancy`, `routing-hints`, `query-cache`, `edge-proxy` and `rate-limiting` now
  compile without dead-code warnings. No behaviour change.

- **Upgrade note: the `cache_refresh` hint, documented as a no-op, is now active.**
  `/*helios:cache_refresh*/` (also `refresh`, `nocache_read`, and `cache=refresh`,
  `cache=force`, `cache=update`, the routing parser's refresh words, which it strips) used
  to be parsed and ignored, so such a read was served from cache. It now looks nothing up
  on any tier (L1, L2, L3): the read goes to the backend and its result is stored through
  the normal store path (cacheable read outside a transaction, exclusions,
  `max_result_bytes`, the C-02 write generation), replacing the shared entry and this
  text's L1 copy. A `cache=skip` next to it still wins. Applications that left the hint in
  their SQL now send those reads to the backend every time. Counted in
  `heliosdb_proxy_cache_refresh_hint_total` (JSON `query_cache.refresh_hint`), not as
  misses.
- **Dependencies:** `zstd` and `lz4_flex` are optional, pulled by the `query-cache` and
  `distribcache` features; a build with neither no longer compiles them.
- **Library API** (crates that use `heliosdb-proxy` as a library): `cache::L2Config`'s
  `size_mb` is replaced by `max_bytes` (bytes) and it gains `compression`,
  `compression_min_bytes` and `zstd_level`; `CacheToml` gains the six keys above and
  `L2Entry` gains `raw_len` (struct literals must set them). `L1HotCache::put`,
  `L2WarmCache::put_evicting`, `shed` and `shed_paced` return the number of entries
  they evicted or removed. New: `cache::codec` (`L2Codec`, `L2Payload`),
  `L2WarmCache::{encode_payload, put_payload_evicting, compression_saved_bytes}`,
  `QueryCache::{entry_counts, compression_saved_bytes}`,
  `CacheMetrics::record_evictions`.
- **Docs: branch databases.** `[branch]` (`CREATE DATABASE … TEMPLATE`) is documented as a PostgreSQL copy that is not instant, not as an instant branch; Nano branches are selected per connection with `database=<db>@<branch>` (Sprinter 45ab08af3f00).
- **New client connections wait for a healthy node instead of failing at once** (sprint
  f4ae895f36ad). A client startup that finds no eligible healthy node, as happens for about
  a second after a promotion until the health checker re-marks the new leader, now waits up
  to `[limits] startup_node_wait_ms` (default 5000) before being refused with 08006
  `Startup failed: No healthy nodes available`. The wait re-checks with the reconnect
  backoff and serves the connection as soon as a node is eligible. Upgrade note: a client
  that used to get an immediate refusal during an outage now waits up to 5 s for an answer;
  set `startup_node_wait_ms = 0` to restore the immediate refusal. Counted by
  `heliosdb_proxy_startup_waits_total` and `heliosdb_proxy_startup_wait_timeouts_total`.
- **Semantic cache (L3) hinted-miss CPU (perf).** The canonical parse borrows token
  text from the statement and serialises the form into pre-sized buffers, and the
  store shares each slot's database, user and branch strings as `Arc<str>` (no
  behaviour change; `f96d0d9a13bc`).
- **`[multi_tenancy] unknown_tenant` (new key, default `"refuse"`).** Under row isolation
  (`tenant_tables`), a session whose tenant cannot be identified is refused with SQLSTATE
  42501 when a simple-protocol statement references a tenant table, instead of being
  forwarded unfiltered. A name inside a string literal or a comment does not count; identifiers
  are read as PostgreSQL truncates them (63 bytes), as by the row filter. Extended-protocol
  statements are checked at their `Parse` (see Security). **Upgrade note:** this changes behaviour for deployments with
  `enabled = true`, non-empty `tenant_tables` and sessions that name no configured tenant;
  set `unknown_tenant = "passthrough"` to keep the previous behaviour. Also, with
  `identify_by = "database"` a session with no `database` parameter now uses its user
  name as the tenant, as PostgreSQL does.
- **Query-cache read and store cost** (Criterion `cache/query/l1_hit_1k`,
  `miss_store_1k`, `cache/l2/memory_hit_1k`): no cache tier waits, so the lookup behind
  `QueryCache::{get, get_with_prep, get_with_prep_hinted}` and the L2 operations behind
  their async methods are plain calls, and a store waits only for zstd compression on the
  blocking pool. The futures the relay awaits per cacheable read are 40-56 bytes again
  (the compression, refresh-ahead and heatmap work above had grown the lookup's to
  1.2 KiB, and every await copies a future whole: an L1 hit cost 13 % more); the store's
  is 912 bytes, below 2.2.0's. No behaviour change.

### Fixed

- **The query cache's backend-settings proof matches setting names ignoring case.** A
  backend that reports `datestyle` / `timezone` (HeliosDB-Nano 4.42.0) where PostgreSQL
  reports `DateStyle` / `TimeZone` no longer has every cached read refused; the same
  name twice in different case is still refused as a duplicate.
- **A cached result no longer keeps its 4 KiB capture buffer.** The response of a cache
  miss is read into a buffer that starts at 4 KiB and doubles, and the entry stored from it
  kept the whole buffer while `l2_max_bytes` counted only the response's length: a cache of
  60-byte point reads held 4 KiB per entry (3.5 GB resident for an L2 reporting 268 MB,
  645 000 entries, in the 3.0.0 miss-path profile). Present since the query cache existed,
  on the simple protocol too. A response that fills less than half its buffer is now
  copied into an allocation of its own size; a larger one is shrunk in place.
- **The query cache no longer keeps a key per stored read in the invalidation index.**
  Every store registered its key under each table it read, and only a write to that
  table released the set, so a table that is read but never written collected one key per
  distinct read ever stored, outside every byte bound. Nothing read those sets: writes
  retire entries through the table generations. `InvalidationManager::register` and its
  lookups remain for callers that register keys themselves (see Library API).
- **A full, large L2 no longer makes room on a client's connection.** The store that
  found L2 full ran the eviction pass itself: two scans of every entry, the first reading
  the write generation of each (tens of milliseconds for 645 000 entries, about every
  30 000 stores). Above 2 048 entries the pass now runs on the `cache-reclaim` thread and
  reads the generation once per table set; the size bound stays soft while a pass runs, as
  before. Smaller maps evict inline as before.
- **An extended-protocol read's statement text is analysed once per worker thread, not at
  every execution** (normalization and table extraction, the volatile-function and
  exclusion scans), when the text repeats: a prepared statement, or an unnamed one that
  binds parameters.

  Together, in the miss-path profile (pgbench prepared select-only over random keys, 64
  clients, `extended_protocol = "on"`, about half the reads hits): proxy CPU per read
  72–75 µs before, 54–55 µs after (53–55 µs with the cache not consulted); resident memory
  after the read phase 3.5 GB before, 0.54 GB after; 130 000 reads per second against
  98 000 with the cache not consulted (108 000–118 000 before).
- **A full L2 no longer stalls a session for minutes.** When the shared L2 reached
  `l2_max_bytes`, the store that won the eviction pass scanned the whole map once for
  every entry it evicted, while concurrent stores kept inserting without evicting. With a
  few hundred thousand entries, one session's query stalled for 20 s to 23 min after a
  read-heavy run (measured by the 3.0.0 user-path gate with the cache on, 64 clients;
  v2.2.0 has the same loop). A pass now scans the map once, selects the least recently
  used entries (expired ones first) until the store fits with
  `[cache] l2_evict_headroom_pct` (default 5) of the budget to spare, and keeps entries
  read since its scan. Criterion case `cache/l2/store_at_capacity_256b`.
- **Ed25519 signing keys in PKCS#8 v2 form (with the public key) are accepted.** Their
  length was checked as 83 bytes instead of 85, so every such key was refused.
- **A plugin waiting for a `max_plugins` slot loads in the rescan that frees one.**
  Removed plugins were unloaded after the new ones were tried.
- **`SELECT set_config(name, value, false)` counts as session state** for branch moves
  and failover: the `SELECT` was classified before the call was looked at.
- **Plugins see the tenant of a tenant without a client cap** in `hook_context`
  (`tenant_id`); it was set only for tenants with `max_clients_per_tenant`.
- **`helios-plugin install` installs working plugins.** It always wrote `<name>.wasm`
  with no manifest, so an installed module loaded with no hooks and never ran, and a
  packed artefact from a registry was saved as `.wasm` and refused. It now installs an
  artefact as `<name>.tar.gz` (checking it unpacks, names the requested plugin and, with
  a trust root, is signed), a module as `<name>.wasm` with its `.sig`, writes through a
  temporary file and a rename (signature first), and removes the other format's files
  for that plugin.
- **`helios-plugin new` scaffolds a plugin that builds and loads.** The scaffold's
  `pre_query` had the wrong signature and no `alloc`/`dealloc`, so the runtime could
  never call it, and it had no `Cargo.toml`. It is now a complete crate implementing
  the ABI, with the manifest `pack --manifest` reads and the build, pack and load steps.
- **Plugin runtime options do what they say.** `enable_simd = false` now disables the
  SIMD proposals; `enable_threads = true` and an on-disk `cache_dir`, which this build
  cannot provide, are refused when the runtime is created instead of being ignored; and
  `cache_modules` keeps compiled modules by content hash (it kept every path's module
  forever and never read them).
- Unloading a plugin no longer calls its `on_disconnect` export with an empty payload.

- Reject malformed simple-Query CStrings with bounded FATAL 08P01 before hooks, rewriting or cache lookup; preserve empty queries, native byte encodings and typed Parse payloads.

- Allow proven standalone unnamed extended COMMIT PREPARED and ROLLBACK PREPARED cycles through a saturated tenant query cap, including fragmented wire input, without admitting mixed work.

- Keep edge reconnect cursors at the last complete, successfully applied invalidation. Reserve sequence zero for legacy/hello frames, preserve replay order across chunks, and retain the prior checkpoint while a warm replay tail is still queued.

- Check the executed extended-cycle shape before granting a rate-concurrency completion exemption; a first COMMIT Parse cannot exempt a different executed statement.

- Honor L1/L2 cache-tier restrictions across lookup, promotion, compression and store; combine dependency hints with inferred relations and protect their fill generations. Rewritten/client hints take the shortest TTL and intersect restrictions. Automatic opaque catalog dependencies remain open.

- Compile shared SQL-lexer helpers only for their consuming features and guard the edge-cache regression by its feature, keeping isolated SQL-text test builds warning-free.

- **Query cache on the extended protocol** (`[cache] extended_protocol`: `"hint"`, the default, for statements that carry `/*helios:cache=on*/` after their text; `"on"` for every cacheable read; `"off"` for none; `true`/`false` read as `"on"`/`"off"`). The default leaves unhinted extended batches to the backend as 2.2.0 did, because a looked-up read that misses costs more than an unread one: with pgbench's prepared select-only script over random keys `"on"` raised read p99 by 11–24 % and concurrent write p99 by 4–16 % against 2.2.0. The query cache now answers reads sent as `Parse`/`Bind`/`Execute`/`Sync` with an unnamed statement, and the `Bind`/`Execute`/`Sync` of a prepared (named) statement, each optionally with a `Describe` of the portal, under every condition a simple query's lookup has. An entry is keyed by the statement text, the declared parameter types, the bound parameter values and their format codes, the result format codes and the batch's shape, so text and binary results, and different parameter values, never share one; the statement's name is not part of the key. A hit runs nothing on the backend but leaves it holding the unnamed statement the client was told it parsed. Other batch shapes, transactions and sessions that have written go to the backend as before. See "Extended-protocol reads" in `docs/configuration.md`. Like a simple query's lookup, each one needs the backend settings proof (`SHOW ALL`), reused per connection for `proof_ttl_ms` (see Changed) or, with `backend_context_immutable = true`, for the connection's life: measured with pgbench's prepared select-only script at 16 clients, 4 400 reads per second with the per-read proof against 84 000 with the proof reused per connection (81 000 with the cache bypassed).

- Borrow existing named-statement references through extended-protocol forwarding and recovery, avoiding a second vector and String clones per batch.

- Legacy FunctionCall (fastpath) messages no longer bypass tenant admission, the rate limiter, the circuit breaker, cache invalidation and replay bookkeeping, and the large-object client API keeps working. New top-level key `function_call`: `"large-objects"` (default) forwards PostgreSQL's built-in large-object functions (`lo_open`, `lo_close`, `loread`, `lowrite`, `lo_lseek`, `lo_lseek64`, `lo_tell`, `lo_tell64`, `lo_creat`, `lo_create`, `lo_unlink`, `lo_truncate`, `lo_truncate64`, by catalog OID) as admitted requests on the primary and refuses every other function with `0A000` and ReadyForQuery; `"any"` forwards every OID and treats each call as a write of unknown scope (all cached results retired, reads pinned to the primary, the tenant permit kept as for a session-level lock); `"refuse"` refuses all. A transaction that ran a forwarded call is never replayed after a failover, and a journaled one that ran a writing call is marked incomplete. Plugins, rewrite rules, tenant row filters, anomaly detection and analytics do not see these calls (they carry no SQL). A malformed FunctionCall and unexpected frontend messages end the session with `08P01`. libpq `lo_*`, JDBC `LargeObject` and `pg_dump` of large objects work through the proxy by default.

- Reject node, pool and reconnect caps above Tokio's semaphore limit during config validation, before startup or a reconnect can panic.

- Replace the multi-tenant example and full-config comments with supported startup-parameter identity, row filtering and tenant budgets; document restart requirements and validate the example through the real config loader.

- **Bounded backend reads drain an oversized row without allocating it (Sprinter 2a43c1f4d4d7).**
  The bounded readers behind the MCP, HTTP `/sql` and GraphQL gateways read a whole
  frame into memory before the result caps applied, so one huge DataRow allocated
  without bound even with `max_result_bytes` set. A DataRow whose payload cannot fit the
  call's remaining byte budget is now drained in 64 KiB chunks and the cap is reported.
  The connection stays in sync. The unbounded `simple_query` is unchanged.

- **EXPLAIN ANALYZE writes and gateway writes invalidate every cache (Sprinter 73f84de3ded3).**
  `EXPLAIN ANALYZE UPDATE t ...` (also `ANALYSE`, `EXPLAIN (ANALYZE, ...)`) was classified
  as a read: the C-02 table generations were not bumped although the write ran, and a
  cached read of `t` stayed stale. It is now classified as the statement it runs (fail
  closed when that cannot be read). `EXPLAIN` without ANALYZE stays a read. A write
  through the HTTP, MCP or GraphQL gateway on an `[edge] role = "home"` proxy now also
  invalidates the home's edge cache and broadcasts to the edges, as a PG-wire write does.
  `docs/configuration.md` documents that a gateway statement which commits after its
  timeout can leave a refill stale until the next write or the TTL.

- **Gateway writes retire cached reads (C-02, Sprinter 6a0c6c34d27a).** The HTTP `/sql`,
  MCP and GraphQL gateways run SQL on their own backend connections, which the PG-wire
  relay's journal capture never sees, so a write through them moved no `[cache]`
  generation and a cached PG-wire read of the written table stayed stale until its TTL.
  The write generations now live in one clock the daemon owns (`ServerState`), which the
  query cache reads with unchanged semantics (the fill-time snapshot checked at store,
  `fills_raced`, the relay's statement- and commit-time marks, the unseen-write signal,
  `COMMIT PREPARED`, the logical source); each gateway moves it after every statement
  that may have written, once the backend answered, failed or timed out (a statement
  that timed out may still commit): the tables the text as sent names, by the relay's
  extraction, or every entry when they are unknown (fail-closed; an `EXPLAIN ... ANALYZE`
  counts as unknown). Reads move nothing. Counted in the new
  `heliosdb_proxy_cache_gateway_write_invalidations_total{gateway="http|mcp|graphql"}`.
  With `[cache]` off nothing changes (no clock, no capture). Library API: new
  `cache::TableGenerations` (`mark_written`, `invalidate_all`, `generation_of`,
  `global_generation`, `write_tables`, `note_write`), `cache::GatewayWrites`,
  `cache::WriteScope`, `gateway_policy::GatewayKind`,
  `GatewayPolicy::{with_cache_writes, note_statement}`,
  `QueryCache::{with_generations, generations}`; `QueryCache::new` keeps its signature
  (its own clock).
- **L1 byte account after an entry-bound eviction:** when a connection's L1 dropped its
  least recently used entry to stay within its entry bound, the entry's bytes stayed in
  the byte account, so on a long-lived connection running many distinct reads the
  phantom total grew until the 32 MiB byte budget evicted every live entry and the
  connection's L1 held next to nothing.
- **`[rate_limit] max_concurrent` is enforced (0a9d45da9c50).** The key was documented as the
  in-flight cap per rate-limit bucket, but nothing on the data path checked it. Now each
  `key_by` bucket admits at most that many requests in flight; a request over the cap gets
  `53400` (not queued), counted in `heliosdb_proxy_rate_limit_concurrency_refused_total`.
  `COMMIT`, `END`, `ROLLBACK` and `ABORT` run past the cap. The default `0` is unchanged.
  **Upgrade note:** a config that already sets `[rate_limit] max_concurrent` to a value above
  0 now takes effect; a bucket busier than that value is refused where it was served before.
  Raise the value or set it to `0` to keep the old behaviour. Requires `[rate_limit]
  enabled = true`.
- **TR: refused statements are not recorded or replayed.** A simple Query or an
  extended batch the proxy refused before any backend (rate limit, open circuit,
  route-plugin Block) was recorded in the transaction replay log as if it had run,
  so a failover replayed a statement the client was told failed. Refused statements
  now leave the record and the session settings untouched; an open extended prefix
  they close makes the transaction non-replayable. Test: `server::tests::tr_refusal`.
  (d7350c9b1c93)
- **MCP gateway result caps (memory DoS).** The `query`, `list_tables` and `explain` tools
  buffered a whole result set with no limit, so one agent query could hold unbounded memory
  in the proxy. The new `[mcp] max_result_rows` (default `10000`) and `max_result_bytes`
  (default `16777216`, 16 MiB) stop storing rows while the result is read, keep the backend
  connection in sync, and mark the result `truncated` (`structuredContent` on the tool result,
  documented in `docs/configuration.md`). Upgrade note: results above 10000 rows or 16 MiB of
  cell data are now truncated; raise the keys (up to 1000000 rows and 1 GiB) if agents depend
  on larger results. Counted by `heliosdb_proxy_mcp_results_truncated_total`. Sprinter
  b2cbd0f6f29d.
- **Result cache: volatile reads were served stale.** The refusal list missed functions and
  literals whose value changes per call or per session, so a cached `SELECT` of one returned
  a stale or another session's value. Now refused, in both the query cache and the edge
  cache: `localtime`/`localtimestamp`, `transaction_timestamp`, `timeofday`,
  `txid_current*`, `pg_current_xact_id*`, `pg_current_snapshot`, `setseed`, `currval`,
  `lastval`, `pg_backend_pid`, `inet_client_*`/`inet_server_*`, the session-state reads
  `current_setting`, `current_user`, `current_role`, `session_user` (session state the
  cache does not read per statement), and the special timestamp
  literals `'now'`, `'today'`, `'tomorrow'`, `'yesterday'` (over-refusal when such a literal
  is plain data only means that read is not cached). The scan is one Aho-Corasick pass over
  the statement with no lower-casing allocation. Sprinter 87c5caf119f8.
- **Result cache: `[cache] extra_volatile_functions`.** New key (default `[]`): names or
  prefixes of user-defined volatile functions that also make both result caches refuse a
  read. Validated at startup; restart to change. Sprinter 87c5caf119f8.

- **Per-tenant active-query caps apply to the HTTP, MCP and GraphQL gateways.**
  `[multi_tenancy] max_active_queries_per_tenant` (and its per-tenant override and
  `active_query_wait_ms`) bounded only PG-wire sessions. A gateway request of a tenant
  named by `tenant_header` now takes that tenant's active-query permit, the same permit
  the PG-wire path takes: it waits up to `active_query_wait_ms` and is refused when the
  tenant is still at its cap (HTTP 429, MCP tool error, GraphQL 429 with an error). The
  permit is released when the request ends, however it ends. `max_clients_per_tenant`
  does not apply to stateless gateway requests (it counts PG-wire sessions only).
  Sprinter 1d73ba306c92.
- **HTTP `/sql` and GraphQL results are capped.** Both gateways buffered every row of a
  result. They now read through the bounded query path and refuse a result past
  `[http_gateway] max_result_rows` (default `10000`) or `max_result_bytes` (default
  `16777216`), and `[graphql_gateway]`'s same two keys. A refused `/sql` request gets
  HTTP 413 with `{"error": "result exceeds http_gateway.max_result_rows (N)"}`; a refused
  GraphQL request gets an error with `extensions.code` `RESULT_TOO_LARGE`. The connection
  is kept, and results at or below the caps are served unchanged. Counted in
  `heliosdb_proxy_gateway_results_refused_total{gateway="http"|"graphql"}`. A
  single DataRow larger than the byte cap is drained without being allocated (see
  "Bounded backend reads drain an oversized row" above). Sprinter 2a43c1f4d4d7.
  - **Upgrade note:** a `/sql` or GraphQL query that returned more than 10000 rows or
    16 MiB of cell data before now fails with the refusal above. Raise the caps (up to
    1000000 rows and 1 GiB) for such a workload, or narrow the query.

### Library API (breaking)

- `CacheToml::extended_protocol` is the enum `config::ExtendedProtocolCache`
  (`Off` / `Hint` / `On`, default `Hint`), not a `bool`; `CacheHint` gains the public
  field `opt_in`. `QueryCache` no longer registers its entries with its
  `InvalidationManager`: `get_keys_for_table`, `get_tables_for_key` and
  `InvalidationStats::tracked_keys` report only keys a caller registered.
- `TenantQueryTransformer::with_refused_functions`, the `TransformResult::refusal` field
  and the `TransformResult::refused` constructor are new; `PolicyDenied` gains the variants
  `TenantStatement` and `TenantQueryLimit` (exhaustive matches must handle them);
  `GatewayPolicy::tenant_sql` now returns `Result<Cow<str>, PolicyDenied>` (it was
  `Cow<str>`), so a refused statement is an `Err` (`PolicyDenied::TenantStatement`, HTTP 403)
  and never a passthrough. Downstream callers must handle the `Err`; the release is a
  breaking change for the library API.
- `routing::HintParser::strip` returns `Cow<'_, str>` (it was `String`): a statement
  without a hint is returned borrowed. `QueryRouter::strip_hints` keeps its `String`.
- `cache::L2Config::size_mb` is replaced by `max_bytes` (see ### Changed above for the
  other cache structs that gained fields).
- New public items: `pool::{SettingsProof, ParkedSettings, PooledConn}` and
  `BackendIdlePool::{checkin_conn, checkout_conn}`, which park a connection with what
  is known about its settings (`checkin_keyed` / `checkout_keyed` are unchanged).
- New public fields: `config::CacheToml::proof_ttl_ms`; `ServerMetricsSnapshot::{branch_moves,
  cache_scope_proofs, cache_scope_proofs_reused}`; `config::PluginToml::{hot_reload_interval_ms,
  hot_reload_debounce_ms, max_artefact_bytes, require_signed_manifest}`;
  `config::BranchConfig::{route_enabled, route_mode, route_allow}`;
  `rate_limit::RateLimitConfig::{window_size, window_max_requests, max_in_flight}`. New
  module `plugin_artefact` (artefact format, signing, trust roots).
- Further structs gain public fields (struct literals must set them, or use
  `..Default::default()`): `config::ProxyConfig::{function_call, admin}`,
  `config::McpConfig` (`principals`, `max_result_*`, `trust_backend_read_effects`,
  `tool_cache_*`, `memory_*`), `config::MultiTenancyToml::{refused_functions,
  max_journal_bytes_per_tenant, tenant_max_journal_bytes, unknown_tenant}`,
  `config::LimitsToml::{copy_idle_timeout_secs, startup_node_wait_ms}`,
  `config::AnomalyToml` / `anomaly::AnomalyConfig::max_auth_windows`,
  `config::RateLimitToml::{window_max_requests, window_secs, cleanup_interval_secs}`,
  `config::{HttpGatewayConfig, GraphqlGatewayConfig}::{max_result_rows, max_result_bytes}`,
  `config::CacheToml` (the cache keys listed under Added), `cache::CacheConfig::{refresh_ahead_ratio,
  refresh_ahead_claim_timeout, heatmap}`, `cache::L2Config::evict_headroom_pct`,
  `plugins::PluginRuntimeConfig::{hot_reload_interval, hot_reload_debounce,
  max_artefact_bytes, require_signed_manifest}`, `ServerMetricsSnapshot::{rate_limit_concurrency_refused,
  startup_waits, startup_wait_timeouts}`, `tenant_slots::TenantBudgetStats` (journal
  fields), `plugin_registry::VerifyReport::manifest_signed_by`, `admin::AdminState::{mcp_cache,
  backend_pool}`.
- Changed signatures: `GatewayPolicy::check_node` returns `Result<NodeAdmission,
  PolicyDenied>` (hold the admission for the request: it carries a half-open circuit's
  probe); `plugins::HotReloader::watch_dir` returns `PathBuf`; `L1HotCache::put`,
  `L2WarmCache::{put_evicting, shed, shed_paced}` return a count. New public modules:
  `database_name`, `gateway_drain`, `mcp_memory`, `mcp_cache` (with `query-cache`).
- **Library modules fail closed instead of simulating success.** Several library modules
  that the daemon does not call used to return made-up values or report success for work
  that never happened when they had no backend to talk to. In 3.0.0 each one either does
  the work on a connection it is given or returns an explicit error; none of them
  fabricates data. The daemon's behaviour is unchanged.
  - `auth::oauth`: `OAuthClient::introspect` / `validate_to_identity` and
    `TokenExchange::{exchange_code, refresh_token}` return `OAuthError::NotSupported`
    (they returned fabricated tokens); nothing is cached. `auth::jwt`:
    `JwtValidator::refresh_jwks` returns `JwtError::NotSupported` (it installed a dummy
    key that could replace a configured HS256 key); a validator without a JWKS URL needs
    no refresh. `auth::credentials`: the Vault and AWS Secrets Manager providers return
    `CredentialError::NotSupported` (they fabricated credentials);
    `CredentialManager::list_credentials` returns `Result` and reports provider errors.
  - `failover_controller`: without a backend template, `initiate_failover`,
    `manual_failover` and `coordinate_failover_replay` fail with `FailoverFailed` (they
    reported a failover that promoted nothing); a failed promotion closes its history entry
    and leaves the state `Failed` (it stayed `InProgress`); the LSN catch-up reads the
    node's WAL position (it ignored the target LSN); coordinated replay reaches the new
    primary through the controller's template.
  - `cursor_restore`: `restore_cursor` / `restore_session_cursors` take the session's
    connection (`&mut BackendClient`, `in_transaction`) and recreate the cursor on it; the
    old path declared it on a throwaway connection and closed it, reporting success. New
    `restore_statements`; `with_backend_template` and `register_endpoint` are removed.
  - `session_migrate`: `migrate_session` takes the session's connection and runs the
    restore on it (`set_config` for parameters, `PREPARE`, `CREATE TEMP TABLE`), stopping at
    the first refused statement; it refuses to drop temp tables or their rows silently.
    `with_backend_template` / `register_endpoint` are removed.
  - `failover_replay`: replay without a backend fails every statement (as before), with
    error text that names the cause.
  - `upgrade_orchestrator`: the drain stage checks the subscription slot's
    `confirmed_flush_lsn` against the source WAL (it slept and advanced); retire replays the
    buffered writes on the new primary (they waited forever); failures and `cancel()` answer
    buffered writes with `SwitchoverFailed`.
  - `connection_pool`, `pool::ConnectionPoolManager`, `health_checker`: without a backend
    template, acquire and health checks fail (`ProxyError::Pool` / `HealthCheck`) instead of
    handing out connections that only looked alive or reporting nodes healthy;
    `validate_connection` peeks the socket; `ConnectionPoolManager::with_backend_template`
    and `resets_skipped()` are new.
  - `schema_routing::SchemaDiscovery`: discovery against a backend returns
    `DiscoveryError::BackendDiscoveryUnsupported` (it returned mock tables). `graphql`:
    `GraphQLEngine` without a backend refuses a query that generates SQL (it returned empty
    data). `rewriter`: `Condition::{SessionVar, ClientType, TableExists}`,
    `AstPattern::NPlusOne`, `SqlParser::to_sql`, the `RewriterConfig` fields that were never
    read (`expand_select_star`, `add_default_limit`, `default_limit`, `max_query_length`,
    `cache_*`, `agent_safety`) and `AgentSafetyConfig` / `BuiltinRule` are removed.
    `routing::metrics::RoutingMetrics::target_distribution` returns real counts.
  - `distribcache`: the L2 tier is documented as the in-memory compressed tier it is
    (`l2_path` removed); inserts return `CacheResult` (oversize entries were dropped
    silently); L3 replication and invalidation are awaited and reported per peer
    (`ReplicationReport`, `PeerOpReport`; the peer protocol acknowledges both, so peers must
    run the same version); WAL invalidation fails to start when it cannot subscribe and is
    wired to the tiers; LRU/FIFO eviction is real (`EvictionPolicy::Adaptive` removed); the
    prefetcher queue is bounded; `invalidate` / `invalidate_table` are async.

### Development

- **Nano pair drill: online promotion.** `scripts/regress/nano-pair-drill.py
  --promote-mode online` / `online-nowait` promote the running standby with
  `SELECT pg_promote(true, 60)` / `pg_promote(false, 60)` on its own PG port (HeliosDB-Nano
  P5.2) instead of the fixture's offline `--promote standby`: the call answers `t`,
  `pg_promote` on the primary and a second call answer 55000, a session held on the
  standby gets FATAL 57P01, and the promoted node's timeline is above the fenced node's;
  then the same proxy-side checks. First run against Nano P5.2 (869df830, fixture
  445d9c67): every mode 9/9, plain and TLS.

## [2.2.0] - 2026-10-30

Tenants can be capped on the requests they run at once and on the bytes their sessions hold
for transaction replay. Client sockets get TCP keepalive, and client TLS certificates rotate
on SIGHUP. The query cache gains an opt-in semantic tier (statements of one small SELECT
grammar matched exactly or by canonical form, never by embedding similarity of the SQL),
table exclusions and per-table TTLs; its keys and invalidation now read literals, comments
and the places where PostgreSQL and HeliosDB-Nano/Lite read a statement differently, so
statements that return different rows no longer share an entry (C-07), and text the
backends read differently is never cached. Admin node controls reach routing, and unknown
config keys name the key they most likely meant.

### Upgrade notes (behaviour changes)

- **TCP keepalive on every client socket:** `[limits] client_tcp_keepalive_secs = 60` by
  default, so a client that vanished without a FIN or RST ends its session after the OS's
  probes go unanswered. `0` keeps the 2.1 behaviour (no keepalive).
- **Refusals inside a transaction report its status:** a rate-limit, circuit fast-fail,
  plugin-block or wasm-cached reply now ends in ReadyForQuery `T` or `E`, not `I`.
- **`[cache]` is read at startup only:** a SIGHUP that changes it keeps the running section
  with a warning (except `max_cacheable_response_bytes`). A `cache_ttl` hint can only
  shorten a TTL, and `cache_ttl=0` skips the cache.
- **Query-cache keys changed:** fingerprints keep words as written (case variants are
  separate entries), and reads of system catalogs, statements with a time-travel clause in
  a comment and statements PostgreSQL and HeliosDB-Nano/Lite read differently are never
  cached; a multi-statement write or a write with such text invalidates the whole cache.
  Case-variant spellings of one statement now miss each other.
- **`exclude_columns` under `[cache]` is a startup error**, with or without
  `strict_config`: column-level exclusion cannot be enforced.
- **Library API** (crates that use `heliosdb-proxy` as a library): see Changed below —
  `L3Config`, `CacheToml`, `LimitsToml` and `ServerMetricsSnapshot` gain public fields
  (struct literals must set them), `L3Config`'s embedding timeout and concurrency defaults
  change from 30 s / 10 to 5 s / 4, and `QueryNormalizer` fingerprints are no longer
  upper-cased (HeliosDB-Lite's cache tests that expect upper-cased fingerprints must
  change). The daemon's CLI, wire protocol and admin API stay compatible.
- **Dependencies:** `aho-corasick` 1 is a direct dependency; a crate that builds this one
  against its own lock file needs `cargo update -p heliosdb-proxy`.

### Added

- **Per-tenant active-query and replay-byte budgets** (H-05 follow-up, Sprinter
  ac858d61400b; `multi-tenancy` feature). New `[multi_tenancy]` keys:
  `max_active_queries_per_tenant` (default 0 = no cap) with `tenant_max_active_queries`
  overrides, `active_query_wait_ms` (default 5000), `idle_permit_timeout_ms` (default 0 =
  off), and `max_replay_bytes_per_tenant` (default 0 = no budget) with
  `tenant_max_replay_bytes` overrides. A PG-wire request that starts while its session is
  idle takes one of its tenant's permits and holds it until the session is idle again (an
  explicit transaction holds one permit from `BEGIN` to its end; under session pooling, so
  does a session holding session-level advisory locks (taken by a statement naming
  `pg_advisory_lock[_shared]` or `pg_try_advisory_lock[_shared]` as a whole word, literals
  and comments included; a table such as `job_advisory_locks` takes none), until a simple-protocol
  `pg_advisory_unlock_all()` or `DISCARD ALL` succeeds on the backend connection that took
  them or that connection ends: one refused before it releases nothing, a lock statement a
  failover answers without re-running it takes none, and the extended protocol never clears
  it, so a session that uses only the extended protocol keeps its permit until it ends or
  loses that connection; one lost while the session idles returns the permit at once, not at
  the client's next message).
  At the cap a request waits FIFO,
  then is refused with 53300 `too many active queries for tenant "X" (limit N)`; the
  session stays usable, and a refused extended cycle is discarded up to its `Sync`. A
  CancelRequest during the wait answers 57014 `canceling statement due to user request`
  (a key that several live sessions were relayed, from a backend that issues colliding
  BackendKeyData, ends no wait and is forwarded to the backend as before), and under transaction/statement pooling a waiting session parks its idle backend
  connections first. A single-statement simple `COMMIT PREPARED` / `ROLLBACK PREPARED` is
  exempt. `idle_permit_timeout_ms` terminates a session idle while holding a permit (FATAL
  25P03 in a transaction, 57P05 otherwise). The replay budget bounds the transaction-replay
  bytes all sessions of a tenant hold (`tr_mode` `select`/`transaction`); the statement
  that would exceed it marks its transaction non-replayable like `[limits]
  tr_max_replay_bytes`. A request the proxy abandons while its backend may still run it
  (silent past `[limits] backend_read_timeout_secs` or `backend_response_timeout_secs`, its
  client gone mid-response, or a session that ends with a `Flush`-dispatched `Execute` or an
  extended COPY's end open) now gets a CancelRequest there first: the backend connection keeps the
  BackendKeyData its backend reported (also while parked in the pool), and the proxy sends
  that backend a CancelRequest, bounded by the new `[limits] backend_cancel_timeout_ms`
  (default 5000, which also bounds a forwarded client CancelRequest, now including the
  backend's close), before the connection closes and the session's permit returns. Before
  this, such a statement ran on outside the cap until it next wrote to its closed socket.
  That cancel, and a client's forwarded CancelRequest, go to the address the backend
  connection is open to (the node's `host:port` is dialled only when that is unknown), so
  a node name with several addresses, or one a failover moved, no longer sends them to a
  server that ignores the key. The cancel stops the statement only on a backend that
  implements CancelRequest (PostgreSQL; PgBouncer forwards it): HeliosDB-Nano ignores one,
  so there such a statement still runs to completion outside the cap.
  Only a session whose tenant has an active-query cap waits for that cancel (after the
  fault is recorded against the node); every other session sends it in the background, so
  its failover latency is unchanged. Library: `pool::backend_pool::BackendKey`,
  `BackendIdlePool::{checkin_keyed, checkout_keyed}`; `LimitsToml` gains the public field
  `backend_cancel_timeout_ms` (default 5000), so struct literals without
  `..Default::default()` must set it; `metrics_hist::ProxyLatency` gains
  `tenant_query_wait`. Metrics: `tenant_budgets` in `GET /metrics`, twelve
  `heliosdb_proxy_tenant_*` series labelled by tenant and the
  `heliosdb_proxy_tenant_query_wait_seconds` histogram (one series for all tenants). The reconnect-wave lab drill runs a
  heavy tenant against both budgets. Library: `tenant_slots::{TenantBudget, TenantCharge,
  QueryHold, QueryWait, TenantBudgetStats, TenantQueryRefusal}`, `TenantSlots::{budget,
  budget_of, budget_stats, has_budgets, has_replay_budgets, register_query_wait,
  unregister_query_wait, cancel_query_wait, session_key_relayed, session_key_ended}`, `MultiTenancyToml::{active_query_cap,
  replay_byte_cap}` and the new public `MultiTenancyToml` fields (struct literals without
  `..Default::default()` no longer compile).
- **TCP keepalive on client sockets** (`[limits] client_tcp_keepalive_secs`, default 60;
  0 = off). Every accepted PG-wire client socket now probes its peer after that many idle
  seconds, at the same interval, so a client that vanished without a FIN or RST (a NAT or
  load-balancer flow dropped, a host frozen or powered off) ends its session once the OS's
  probe count goes unanswered, and what the session held returns: a `[multi_tenancy]`
  active-query permit (an idle transaction, advisory locks, a stalled COPY), its client
  slot, its backend connections. Before this the proxy enabled no keepalive, and such a
  session waited on its dead client until the proxy restarted. With keepalive off, a tenant
  active-query cap now logs a warning at startup: `idle_permit_timeout_ms` ends only an idle
  permit holder (in a transaction, holding advisory locks), `client_idle_timeout_secs` also
  a client mid-cycle (after a `Flush`), and nothing ends a session in a stalled COPY or
  with a message partly received. Read at startup; a SIGHUP change is ignored with a
  warning.
  `LimitsToml` gains the public field `client_tcp_keepalive_secs` (struct literals without
  `..Default::default()` must set it) and `config::MAX_TCP_KEEPALIVE_SECS` (32767).
- **Client TLS follows a SIGHUP reload.** `[tls]` and the files it names are reloaded with
  the configuration: replace the certificate and key on disk and send `SIGHUP` to rotate
  them. New connections handshake with the new certificate, established sessions keep
  theirs, a certificate or key that fails to load keeps the running TLS configuration
  (logged as an error) instead of dropping to plaintext, and the section can be switched
  on or off the same way. Before this a rotation needed the `SIGUSR2` drain and a restart.
- **Five more chaos verbs in the HA lab and the Patroni demo** (Sprinter 967e1db11c29):
  lab scenarios `drain-restart` (`SIGUSR2` drain and restart of a proxy under the
  ledger), `stale-sighup` (an unparsable file is refused, a stale one is applied while
  writes go on) and `tls-rotation`; demo modes `dcs` (loss of one, then two, of a
  three-member etcd under load) and `delay` (latency on the proxy's Patroni control path
  below and above `patroni_request_timeout_ms`). `scripts/regress/tcp-relay.py` gained a
  `--control FILE` with `delay_ms` and `drop` for the latter. The demo's etcd is now three
  members.
- **Nano physical-pair drill.** `scripts/regress/nano-pair-drill.py` runs the proxy
  (`scripts/regress/proxy-nano-pair.toml`: `[topology] provider = "postgres"`,
  `[health] check_query = "SELECT heliosdb_require_ready()"`, lag routing in Nano sequence
  units) in front of HeliosDB-Nano's physical primary + standby fixture and checks startup,
  read offload, the chaos overrides, SIGKILL and restart of either side, and a sustained
  load. First run against Nano P2.1 (d9d49e90): 8/8. A ninth case, `promote`, runs when the
  fixture has `--promote standby` (Nano P5.1): a transaction left open on the primary, a
  fenced promotion of the standby, the leader moving to it, the fenced node leaving the read
  set, and the open transaction failing without its row appearing; older fixtures skip it.
- **Semantic cache tier (L3) in `[cache]`** (Sprinter 91c4af7cc20e): `semantic_enabled`,
  `semantic_match` (default `"verbatim"`), `semantic_similarity_threshold` (default `1.0`;
  below it needs `semantic_match = "canonical"`, else a startup error names both keys),
  `semantic_max_entries`,
  `semantic_max_variants`, `semantic_max_bytes`, `semantic_ttl_secs`,
  `semantic_embedding_{endpoint,model,dim,timeout_ms,concurrency,retry_secs}` and
  `semantic_max_statement_bytes` (default 16 KiB, at most 1 MiB since the parse takes about a
  hundred times the statement; longer statements are not parsed). Only reads
  that carry `/*helios:cache=semantic*/` use it, after L1 and L2 miss, and only
  statements of one single-table SELECT grammar (an allow-list: columns and the five
  aggregates, one relation, `AND`/`OR`/`NOT` comparisons with literals, `IS`, `IN`,
  `BETWEEN`, `LIKE`, `GROUP BY`/`ORDER BY` on columns, `LIMIT`/`OFFSET`); anything else
  uses L1/L2 and is counted as ineligible. With `semantic_match = "verbatim"` (the
  default, sound on every backend) it serves a read only from an entry stored for the same
  statement text as the backend receives it, hint comments included (removing a hint
  would glue the words around it, and HeliosDB-Nano reads a statement holding a comment
  through another path). With `semantic_match = "canonical"` (PostgreSQL semantics only)
  it serves a read from an entry with the same
  canonical form — hint comments, whitespace, keyword case, `!=`/`<>`, the relation's
  alias and `AS`, and which alias a qualifier uses do not matter (a qualified `t.f` is
  never a bare `f`, since it may call the function `f(t)`; names keep their case:
  HeliosDB-Lite does not fold them, and HeliosDB-Nano reports an output column's label as
  written, so case variants only cost hit rate);
  literals, operators, the relation and its schema, `ORDER BY`/`GROUP BY` keys as written
  (a lone key named like an output alias is refused: PostgreSQL and HeliosDB-Nano resolve
  it differently) and limits must be identical — with no embedding service. Shapes
  HeliosDB-Nano refuses or rewrites while their form is that of one it runs are refused
  too: a relation alias without `AS` its parser reserves (`FROM kv sort`), the names
  `exists`, `interval`, `struct`, `trim` and `top`, a statement one of its
  spacing-exact text rewrites may fire on (`AS OF` / `VERSIONS BETWEEN`, the removal of
  `SECURITY DEFINER` / `SECURITY INVOKER`, which reads `FROM xsecurity definer` as the
  table `x`, and of the list after `AS IDENTITY`), one naming a catalog it answers from a
  raw-text router (any `pg_…` or `information_schema` text: `name='x'` keeps every
  `pg_settings` row, `name = 'x'` one), and one with a comment other than a plain
  `helios:` hint (Nano's planner and HeliosDB-Lite read `AS OF` in a comment, and a quote
  in one flips Lite's literal tracking). Once a backend connection's startup reports a
  `server_version` naming HeliosDB (Nano, Lite), a process-wide sticky flag turns a
  canonical tier off — it neither stores nor serves, hinted reads count as ineligible —
  with one warning naming `cache.semantic_match` and the node: those engines re-read the
  raw text, so statements PostgreSQL reads alike may read differently there (the alias
  decides between Nano's point-lookup fast path and its planner). With the canonical form
  and a threshold below 1.0 it may also serve a bare column
  of a statement without `DISTINCT` or `GROUP BY` from an entry listing another column
  whose name embeds at or above the threshold (approximate: that column's values, never
  other rows; counted; a qualified `t.f`, which may call `f(t)`, is never substituted).
  Only the column names of stored results are embedded (never a name only a lookup
  saw), in the background, and remembered within a quarter of `semantic_max_bytes` (embeddings never
  evict an entry, and none is fetched while the entries leave no room for it and the
  names it must be compared with); no read
  waits on the endpoint, which is probed once at startup and
  backed off for `retry_secs` after a failure. Metrics: `query_cache.semantic` on `/metrics`,
  `heliosdb_proxy_cache_semantic_{endpoint_state, embed_requests_total,
  embed_errors_total, embed_skipped_total, embed_seconds, approximate_hits_total,
  ineligible_total, too_large_total, entries, embeddings}`; a result whose entry alone
  exceeds `semantic_max_bytes` stays out of the tier (counted; startup warns when the
  budget is below `max_result_bytes`). The entries are sharded by statement shape and
  session (`semantic_max_entries / 32` rounded down to a power of two, 1 to 64): a lookup
  or store locks only its shard and builds what it stores before taking the lock, while
  `semantic_max_entries` and `semantic_max_bytes` bound the whole tier (eviction is
  oldest-first within a shard, approximate across shards). A hinted read that misses pays
  the tier's parse, lookup and store (about 3 µs single-threaded); on random-key reads that
  never repeat, hinted reads ran 5-10 % below unhinted ones (docs/configuration.md, "Cost of
  a hinted miss"): hint the reads that repeat. Under `strict_config` a build without
  `query-cache` refuses `cache.semantic_enabled`.
- **Table exclusions and per-table TTLs.** `[cache] exclude_tables` and `[[cache.tables]]`
  (`name` and exactly one of `ttl_secs` or `exclude = true`). An excluded read is neither
  looked up, captured nor stored by either result cache (the query cache and `[edge]`),
  found by a fail-closed whole-identifier scan of the statement text (any `U&"…"`
  identifier counts as a match, a name next to a Unicode space, which HeliosDB-Nano and
  HeliosDB-Lite read as whitespace, is whole, and a name of 60 bytes or more also matches
  the start of a longer identifier, which PostgreSQL truncates to 63 bytes), and counted in
  `heliosdb_proxy_cache_excluded_total` (JSON `cache_excluded`). A PostgreSQL reserved
  word (`order`, `user`, `left`, …) is refused as a table name at load: such a table is
  only ever written quoted, and the scan would match the keyword in nearly every
  statement. A table TTL longer than
  `ttl_secs` applies only to a single-table read of the semantic tier's grammar (its table
  spelled in lower case), hinted or not; a shorter one also holds for any other read whose text names the table (the
  same scan), and caps the `[edge]` entries of such reads (`[edge] default_ttl_secs`
  otherwise; a longer table TTL never extends one). The single-table check is syntactic: writes that reach a table without
  naming it (partitions, inheritance children, foreign-key cascades, triggers, a function
  called from a `SELECT`, which the proxy treats as a read, and the base tables of a view
  or the tables a row-level security policy reads) do not retire its reads: give such a
  table or view no longer TTL (a table written through functions only with
  `invalidation_source = "logical"`), and exclude its partitions or children with it
  (documented).
- **Live drill `scripts/regress/cache-semantic-drill.py`** (psycopg 3, no Docker) with the
  embedding stub `scripts/regress/embed-stub.py` and
  `scripts/regress/proxy-pg-cache-semantic.toml` (phases 1 and 2 set
  `semantic_match = "canonical"`; a verbatim phase checks the default serves the same
  text only; on HeliosDB-Nano the canonical tier must neither store nor serve);
  `scripts/regress/cache-test.sh` checks a
  `cache=skip` inside a multi-key hint comment; the drill's differential corpus runs
  pairs of statements (every wrong-row pair the reviews found, and legitimate
  paraphrases) through the proxy and the backend and fails when the second statement's
  rows or column names through the proxy differ from the backend's, whichever tier (L2 or
  L3; every statement runs on a new session, so L1 never serves it, and the drill runs
  no `[edge]`) served it, and when a paraphrase is not an L3 hit, on PostgreSQL and (with
  `REGRESS_NANO_BACKEND`) on HeliosDB-Nano; its approximate-refusal corpus (threshold
  0.92, the two column names embedded synonyms) fails when a whole-row reference, a
  qualified column (attribute notation), or a `DISTINCT` or `GROUP BY` statement is
  filled approximately, next to a control pair that must be; a pair meant to read
  differently that the backend now answers alike (Nano P4 sorts by an output alias like
  PostgreSQL) is reported as premise gone (listed under `premise_gone`, never a pass)
  when the backend served it with its own result, and still fails when any tier did;
  `scripts/regress/bench-usertp.sh` takes a `READ_SCRIPT` pgbench script for its read
  cells (run with the scale read from `pgbench_branches`, which pgbench detects for its
  built-in scripts only, so `:scale` covers the accounts `-S` reads).
- **Criterion cases** `cache/query/{l1_hit_1k, l2_hit_1k, hinted_l2_hit_1k,
  miss_store_1k, write_tables_short, write_tables_insert_1m}`,
  `cache/edge/{fingerprint_short, write_tables_insert_1m}`, `cache/l3/{lookup_exact_5000,
  lookup_approx_16, store_full_5000}` and `cache/excl/{scan_3_names_1k,
  scan_1100_names_1k, catalog_time_travel_scan_1k}` (the `write_tables` cases time the
  C-02 write path's lexer pass, a 1 MiB `INSERT` included).

### Changed

- **Cache hints reach the cache when the forwarded text lost them.** With `[routing_hints]`
  `strip_hints` (the default), a rewrite rule or the tenant filter, the client's own text
  is read too, so `cache=skip`, `cache_ttl` and `cache=semantic` take effect (a leading
  `/*helios:cache=…*/` works through the strip; `cache=bypass` and `cache=none`, which the
  routing parser strips as skips, are skips too). Hints are read from comma-separated
  lists (`/*helios:cache=semantic,cache_ttl=60*/`) and only from comments outside string
  literals and quoted identifiers. A skipped read arms no response capture. Cache hints
  do nothing next to `route=primary` or `consistency=strong`, which make the read a write
  that bypasses every cache tier (documented).
- **A `cache_ttl` hint can only shorten a TTL** (L2 used to keep a hinted entry far beyond
  `ttl_secs`), and it bounds what the read is served as well: an entry older than the
  hint (stored by an unhinted read, or under a longer hint) is a miss on every tier and
  is refilled. A `cache_ttl=0` hint is a skip (nothing looked up, captured or stored). A `[[cache.tables]]` TTL may now exceed `ttl_secs` (only for a
  single-table read of the semantic tier's grammar whose table was extracted and that has
  no qualified column; a read of
  several tables uses the shortest, and a shorter table TTL also holds where the
  extraction missed the table but the text names it).
- **`[cache]` is applied at startup only:** a SIGHUP that changes it keeps the running
  section with a warning (`live_config` and `/config` stay truthful), except
  `max_cacheable_response_bytes`, which reads take from the reloaded config, on
  connections opened before the reload too (the capture ceiling, which such connections
  used to keep from their own snapshot, and now also the `[edge]` cache's per-entry
  ceiling, which used to stay at its startup value, so a larger response was buffered and
  then refused as oversize).
- **`[ai_cache]` and `[cache.query]` hints name the new keys** (`cache.semantic_enabled`,
  `cache.semantic_similarity_threshold`, `cache.exclude_tables`), as do new hints for
  `[cache.semantic]`, `[cache.l3]`, `similarity_threshold`, `semantic_cache_enabled`,
  `exclude_patterns` and `[[cache.tables]]` `ttl`/`table`. `exclude_columns` is refused
  at load in any spelling under `[cache]` (`cache.exclude_columns`,
  `cache.tables.exclude_columns`, or inside any other table under `[cache]`, such as the
  website sample's `[cache.query]`, at any depth), even without `strict_config`:
  column-level exclusion cannot be enforced.
- **Library API** (precedents: `L1Config.max_bytes`, `CachedResult.generation`):
  `L3Config` gains `max_variants`, `embedding_timeout`, `embedding_concurrency`,
  `embedding_retry_after` and `max_statement_bytes`, and its timeout and concurrency defaults change from 30 s / 10
  to 5 s / 4; `CacheToml` gains the keys above (and `PartialEq`); `L3SemanticCache::get`
  and `put` match by canonical form (statements of the tier's grammar only), never wait on the embedding endpoint and embed column
  names only; `EmbeddingClient::embed` rejects a wrong dimension instead of padding it;
  `parse_cache_hints` reads comma lists, only from comments outside quotes;
  `QueryNormalizer` fingerprints and hashes change (below): `NormalizedQuery.fingerprint`
  and `QueryNormalizer::fingerprint` (which `quick_fingerprint` hashes) keep every word as
  written and are no longer upper-cased (only literals become `?`), so a downstream
  assertion on an upper-cased fingerprint (`"SELECT * FROM USERS WHERE ID = ?"`) must
  expect the statement's own spelling (`"SELECT * FROM users WHERE id = ?"`; in
  HeliosDB-Lite, `tests/ha_tests/cache_tests.rs` `test_normalize_literals`,
  `test_normalize_strings` and `test_strip_comments`, Sprinter 4c642ae77401), and
  `are_equivalent` no longer holds between case variants; `ServerMetricsSnapshot` gains
  the public fields `cache_excluded`, `cache_catalog_skipped`,
  `cache_time_travel_skipped` and `cache_ambiguous_skipped`, so struct literals must set
  them; `L3Config` gains
  `semantic_match` (library default `SemanticMatch::Canonical`, which its 0.92 threshold
  needs; `CacheConfig::validate` refuses a threshold below 1.0 with `Verbatim`);
  `L3Entry` is no longer used.
  New: `QueryCache::get_with_prep_hinted`, `semantic_cache`, `semantic_health`,
  `CacheHint::merge`, `L3SemanticCache::health`, `SemanticHealth`, `EndpointState`,
  `EmbeddingClient::with_options`/`try_embed`, `CacheToml::to_cache_config`,
  `excluded_table_names`, `validate_semantic_and_tables`, `CacheTableToml`,
  `EdgeCache::set_max_entry_bytes`, `sql_text::TableExclusions`,
  `config::SemanticMatch`, `QueryCache::note_heliosdb_backend` / `heliosdb_backend`,
  `QueryNormalizer::normalize_with`, `edge::fingerprint::analyze_with`,
  `edge::fingerprint::write_tables`,
  `sql_text::names_system_catalog`, `sql_text::uncacheable_text` /
  `UncacheableText`,
  `QueryCache::invalidate_all_for_unseen_write` / `unseen_write_invalidations` and
  `config::SEMANTIC_MAX_STATEMENT_BYTES_CEILING`; `CacheConfig::validate` refuses an
  `L3Config::max_statement_bytes` above it, enabled or not.
- **Dependencies:** `aho-corasick` 1 is now a direct dependency (it was already in the tree
  through `regex`), for the `[cache]` table-name scans. A crate that builds this one
  against its own lock file needs `cargo update -p heliosdb-proxy`.

- **Unknown config keys name the key they most likely meant**, in the startup warning
  and in the `strict_config` error: a known rename or never-existed key
  (`rate_limit.global_qps` -> `default_qps`, `pool_mode.validation_query` -> there is
  none, `[pool] test_on_acquire` is a socket check), else the closest key of the same
  section for a small typo (`cache.enabed` -> `cache.enabled`). The ignored keys stay
  reported after startup: `GET /config` lists `ignored_config_keys`, `GET /health` adds
  their count, `/metrics/prometheus` adds the gauge
  `heliosdb_proxy_config_ignored_keys`; all follow a SIGHUP reload. Field report
  2026-10-16 (Sprinter ab502f27cbe2): a `[rate_limit]` policy written with the wrong
  key names ran at the defaults with only a startup warning to show for it. Library:
  `config::known_config_keys`, `config::ignored_key_hint`,
  `config::describe_ignored_key`, `ProxyConfig::ignored_keys`,
  `ConfigSnapshot::ignored_config_keys`.
- **Hints for the sections the website sample showed that never existed** (Sprinter
  18b23fd60e82): `[ai_cache]`, `[cache.query]`, `[tenancy]`, `[transparent_write_routing]`,
  `[upgrade]`, `[auth] enabled` / `[auth.api_key]` / `[auth.ldap]`, `[[rate_limit.tenant]]`,
  `rate_limit.global_connections`, `circuit_breaker.reset_timeout` / `half_open_requests`,
  each pointing at the real key; a pinned test loads that sample and expects exactly those
  unknown keys. `docs/configuration.md` gains the rows.

### Fixed

- **Synthesized refusals report the backend's transaction status.** Rate-limit, circuit
  fast-fail, plugin-block and wasm-cached replies inside a transaction ended in
  ReadyForQuery 'I' while the backend transaction stayed open; they now report its real
  status ('T' or 'E'). As before, such a refusal does not abort the transaction the way a
  PostgreSQL error does: a `COMMIT` sent after it commits the statements that ran before
  the refused one, so a client that continues after the error must roll back itself
  (documented under `[rate_limit]`, `[circuit_breaker]` and `[plugins]`). A transaction end
  refused this way while the session emulates a transaction a failover aborted reports 'E'
  and keeps the emulation (later statements get 25P02, never autocommit).
- **The unnamed statement and portal follow PostgreSQL's lifetime.** PostgreSQL destroys
  the unnamed prepared statement at every simple Query and before it parses an unnamed
  `Parse` (so even one that fails leaves none), and the unnamed portal at every simple
  Query. A request the proxy refused or answered without the backend (the rate limiter,
  circuit fast-fail, the tenant active-query cap or a cancel of its wait, a query- or
  edge-cache hit, a plugin answer, the aborted-transaction emulation), or a Query another
  of the session's connections ran, left the older ones on the backend connection: a later
  `Bind` or `Describe` of the unnamed statement, or `Execute` of the unnamed portal, with
  no `Parse` or `Bind` of its own (for example a pipelined prepare-only cycle, then its
  execution) ran the older statement with the new parameters where PostgreSQL answers
  26000 or 34000. The proxy now closes them on that connection before such a batch (one
  extra round trip, only then). Conversely the unnamed-`Parse` promotion
  (`optimize_unnamed_parse`, on by default) skipped an unnamed `Parse` identical to the one
  before a simple Query the backend ran, or before settings the proxy itself mirrored or
  restored on that connection, so its `Bind` failed 26000 where PostgreSQL runs it (the
  same parameterized statement on either side of a simple `BEGIN`, say). Residual: a Query
  a plugin `Block` refuses while a `Flush`-dispatched cycle stays open leaves that cycle's
  own later batches unchecked until it settles.
- **A pooled connection holding an unnamed statement is reset before it is parked.** With
  `[pool_mode] skip_clean_reset = true` and `optimize_unnamed_parse = false` (neither is
  the default), a transaction- or statement-pooled connection whose requests had only
  Parsed the unnamed statement (no named one, no `SET`, no `Close`) counted as clean and
  was parked without `reset_query`, as was one whose unnamed `Parse` did not lead its
  batch with the promotion on. The next session leasing it (same node, user, database
  and identity parameters) could `Bind` that statement without a `Parse` of its own and
  run it with its own parameters, where PostgreSQL answers 26000. Such a connection is
  now reset.
- **A message continuing an open extended cycle is no longer refused or sent elsewhere.**
  Anything after a Flush-dispatched part of an extended cycle (further `Bind`/`Execute`/
  `Flush` batches, the `Sync`, or a Query), or after an extended `COPY` ended, could be
  refused before it reached the backend (rate limit, circuit fast-fail), answered from the
  query or edge cache, or routed to another node (read offload, a routing hint, the node
  marked unhealthy or disabled meanwhile). The client was told the cycle failed (or
  finished) while the backend kept it open: a flushed chunk that had failed made the backend
  skip the next cycle (the client got a bare ReadyForQuery), one that had succeeded was
  committed by the next cycle's Sync after the client saw an error, under
  transaction/statement pooling the connection was reset and parked mid-cycle, and a write
  with its row locks stayed uncommitted on the primary while the session (and its tenant
  permit) moved on. Such a message now belongs to the request admitted at the cycle's first
  forwarded batch: it goes to the connection holding the cycle (a fault on that node when
  the connection is gone), is never refused nor answered from a cache, and that backend's
  reply settles the cycle. The rate limiter still charges one that runs statements (a batch
  with an `Execute`, or a Query): with the bucket empty it waits for the token, at most 5 s,
  instead of being refused, so a `Flush`-chained pipeline runs at the bucket's rate. Only a
  plugin `Block` of such a Query still leaves the cycle open; the session then keeps its
  connection and tenant permit until a later request settles it (or that connection is
  lost: the permit returns at once, the cycle having died with it), and counts as an idle
  permit holder meanwhile (`idle_permit_timeout_ms` bounds it; a bare `Flush` sent then is
  consumed, as at idle). A session that ends with a cycle still open on its backend (its
  client gone after a `Flush` or an extended `COPY`'s end, an idle or idle-permit timeout)
  closes that connection, so the backend rolls the cycle back, instead of resetting it
  mid-cycle (which committed the unsynced work, or waited out the reset's drain timeout
  with the tenant permit held) and parking it. A `Sync` (or a Query in its
  place) continuing an extended `COPY` whose connection was lost before it (the backend
  died while the session was idle, or the session moved to a new write leader) is answered
  `08007` in every `tr_mode` but `none`, instead of being re-run alone on a replacement
  connection, whose bare ReadyForQuery told the client the COPY had completed while its rows
  died uncommitted. A `Flush` sent there gets that `08007`, and the `Sync` after it a
  ReadyForQuery that leaves the session idle: its tenant permit returns and, under
  transaction/statement pooling, its replacement connection is parked.
- **A `Flush`-dispatched cycle a routing hint sent to a standby is relayed.** With read
  offload, a pipelining client's extended cycle that a `route=standby` (or `node=`) hint
  sent to a standby while the session's home stayed on the primary (libpq
  `PQsendFlushRequest`, a psycopg 3 pipeline) never got the rest of its results: the idle
  backend watch covered only the home connection, so they sat in the standby's socket
  while the proxy waited for the client and the client for them, and a capped tenant's
  session held its active-query permit until the client gave up. While a cycle its client
  is still in is open, the watch now covers the connection holding it, wherever routing
  sent it, and that connection's loss ends the session with 08006 as on the home node.
- **A stray CopyData, CopyDone or CopyFail outside a COPY is ignored**, as PostgreSQL
  ignores it (pgx, for one, sends CopyDone after a COPY that failed or was refused at once).
  On the simple protocol the session waited for a ReadyForQuery that never came; after an
  extended-protocol request it was taken for the end of an extended COPY, which let a
  following bare Flush dispatch a cycle (and a capped tenant's session run requests and
  open transactions) without an active-query permit; and a session holding no backend
  connection (idle under transaction/statement pooling) answered it with an unsolicited
  `57000 COPY aborted: backend connection lost` and ReadyForQuery, which put every later
  reply on that connection one query behind. A COPY whose connection was lost before its
  end is still answered 57000 at its CopyDone or CopyFail.
- **A message that ends a COPY in copy-in leaves the session idle again.** PostgreSQL ends
  copy-in at any message other than CopyData, CopyDone and CopyFail (and the Flush and
  Sync it ignores there): it fails the COPY (08P01) without running that message, and
  answers ReadyForQuery (a COPY a Query started) or skips to the Sync that does. The proxy
  relayed that ReadyForQuery but still took the session for one mid-COPY until it ended,
  so for the rest of the session its tenant permit (and, under transaction/statement
  pooling, its backend connection) stayed pinned, its idle backend watch and both idle
  timeouts stayed off, and `idle_permit_timeout_ms` never applied. A backend's
  ReadyForQuery now ends the session's COPY. Such a message also goes to the COPY's own
  connection, like one continuing an open extended cycle (never refused, answered from a
  cache, or routed elsewhere: read offload sent a read to a standby, whose ReadyForQuery
  ended no COPY on the primary); one a plugin `Block`s leaves the COPY open there and the
  session an idle permit holder, which `idle_permit_timeout_ms` bounds.
- **A session whose backend died while its client waited inside an extended cycle no
  longer hangs.** When the connection holding a cycle its client was still in died while
  the session waited for that client (`pg_terminate_backend`, a crash, a failover), the
  client could be waiting for answers no backend would give: the rest of a
  `Flush`-dispatched cycle's results (a libpq or psycopg pipeline, which only the idle
  backend watch relays), or the answer to an extended `COPY`'s end. Under a recording
  `tr_mode` the backend's FATAL was dropped (the session survives other losses), with
  `tr_mode = none` a crash sent none, and the proxy then waited for the client in turn
  with the tenant permit held, which `idle_permit_timeout_ms` does not cover; two such
  sessions could take a tenant's last permits for good. Such a session now ends at once
  with FATAL 08006 `backend ... connection lost while an extended-protocol cycle was open
  on it (its outcome is unknown)`, and its permit returns. A client whose COPY end was
  answered still gets 08007 at the `Sync` it owes and goes on.
- **A cycle refused at a `Flush` batch is discarded up to its `Sync`.** A `Flush`-terminated
  batch that started an extended cycle and was refused before it reached any backend (an
  empty rate-limit bucket, an open circuit, the error a failover answers it with when it
  does not re-run it) got its ErrorResponse, but what the client pipelined after it was
  taken for a new request: once the bucket refilled or the circuit closed, a later
  `Bind`/`Execute` ran and the cycle's `Sync` committed it although the client had been
  told the cycle failed (a libpq pipeline or an asyncpg `executemany` committed partly),
  and each refused chunk got an error of its own. As PostgreSQL skips to the `Sync` after
  an error, the rest of such a cycle (a `Query` included) is now discarded, and its `Sync`
  is answered with ReadyForQuery reporting the backend's status, without being charged,
  and returns the cycle's tenant permit. A bare `Flush` with no extended cycle open is
  consumed in every session, as PostgreSQL answers nothing to one: it runs nothing, so a
  rate limit, an open circuit or a lost backend's recovery never refuses it (which would
  discard a simple `Query` sent after it, and hang a client that never sends a `Sync`),
  and it never makes the next request look like part of a cycle.
- **A SIGHUP that changed `[multi_tenancy]` was silently ignored**; it now logs a warning
  and keeps the running section (it is applied at startup only).
- **A tenant cap above `Semaphore::MAX_PERMITS` panicked at startup**; it is now a config
  error.
- **`[multi_tenancy] enabled = true` in a build without the `multi-tenancy` feature** now
  logs a startup warning.
- **Reads of system catalogs were cached** by L1, L2 and `[edge]`: a monitoring probe's
  `SELECT count(*) FROM pg_stat_activity` was served frozen for `ttl_secs`, and
  `SELECT setting FROM pg_settings WHERE name = 'work_mem'` returned another session's
  `SET` value (no write retires those tables, and DDL outside the proxy changes
  `pg_class` / `information_schema` silently). No tier (L1, L2, L3, `[edge]`) looks up,
  captures or stores a read with a word starting with `pg_` or `information_schema`, in
  any case, anywhere in its text (literals and comments included: fail-closed), or with a
  Unicode-escaped quoted identifier (`U&"\0070g_settings"` is `pg_settings`), counted
  in `heliosdb_proxy_cache_catalog_skipped_total` (JSON `cache_catalog_skipped`).
  HeliosDB-Nano's own live system views (`helios_topology`, `helios_ha_status`, …) are
  not recognised: the docs say to list them in `exclude_tables`.
- **A write after a Unicode space was not seen as one on HeliosDB-Nano and
  HeliosDB-Lite**, which read U+00A0, U+3000 and the like as whitespace and run the
  statement after it: the capture took an `UPDATE kv …` after a leading U+00A0 for a
  read (no invalidation), and the invalidation took a DDL statement or branch merge after
  one for a plain write of the tables it names. The first word is now read past a Unicode space too, and the
  table extraction separates words at one (a keyword after one counts, and so does the
  dot of a dotted name). Nor was a write whose first word holds another non-ASCII
  character: HeliosDB-Lite matches keywords upper-cased the Unicode way and runs
  `ınsert ınto kv …` (a dotless `ı`, as a Turkish-locale `toLowerCase()` writes it) as an
  INSERT, which the capture took for a read and `[edge]` for nothing to flush. Such a
  statement is now one the capture cannot reproduce (its transaction is not replayed),
  which invalidates everything, `[edge]` flushes on it, and a data-modifying CTE verb is
  found upper-cased the same way.
- **Writes a later statement or an `INSERT` without `INTO` hid from the invalidation**
  (both caches): HeliosDB-Nano and HeliosDB-Lite accept `INSERT orders SELECT … FROM
  staging` and write `orders`, which only moved `staging`; `UPDATE a …; TRUNCATE b` moved
  only `a`. An `INSERT` now names its table with or without `INTO`, and a leading
  `INSERT` without `INTO` and a write of more than one statement invalidate everything
  (statements end at a `;` outside literals, quoted identifiers, dollar quotes and
  comments: `UPDATE posts SET body = 'Hi; do you know?'` retires `posts` alone). The
  journal capture now ends a `--` comment at a lone `\r` (`SELECT 1 -- x`, `\r`, `;
  DELETE FROM b`) and reads a `$` that continues an identifier as part of it
  (`SELECT 1 AS a$b$ ; UPDATE b …`, which it took for a dollar quote running to the
  end), as PostgreSQL does, so it journals the write after either.
- **An `UPDATE OR IGNORE kv …` / `UPDATE REPLACE kv …` or `INSERT INTO LOCAL kv …`
  invalidated a table `or`, `replace` or `local`** (both caches): HeliosDB-Nano and
  HeliosDB-Lite (sqlparser) read the conflict clause and Hive's `LOCAL` before the
  target and write `kv`, whose cached reads stayed current. Such a write now
  invalidates everything.
- **A comment holding a time-travel clause was dropped from the L2 and edge keys:**
  `SELECT val FROM kv /* as\tof now */ WHERE id = 1` shared the key of the comment-free
  read although HeliosDB-Lite cuts an `AS OF` clause out of the raw text across the end
  of a comment. A read whose comment holds `AS OF` or `VERSIONS BETWEEN` with any run of whitespace
  between the words, in any ASCII case, is now cached by no tier, counted in
  `heliosdb_proxy_cache_time_travel_skipped_total`.
- **On a HeliosDB backend, L2 and edge keys merged statements the engine reads
  differently**: they dropped comments and collapsed whitespace, while HeliosDB-Lite's
  `SIMILAR TO` rewrite takes a quote inside a comment for a literal (`… /* don't */ WHERE
  body = 'SIMILAR TO'` reads `'LIKE'` there and shared the key of the comment-free read)
  and matches one exact spacing, and HeliosDB-Nano's point-lookup fast path declines any
  statement holding a comment and reads the table `Post` as written while its planner
  reads `post` (`SELECT * FROM Post WHERE id = 1` and the same with a trailing
  `/*helios:cache_ttl=30*/` shared a key). Once a backend's startup reports a HeliosDB
  `server_version`, both keys keep every run of whitespace, every comment and every
  dollar quote's delimiters (literals still fold: sqlparser there fails `$ab$x$a$ab$`
  and reads `$$x$a$$`), and the entries keyed before are retired.
- **Statements PostgreSQL and HeliosDB-Nano/Lite read differently are never cached**
  (L1, L2, L3, `[edge]`): a backslash before a quote in a plain literal (where the
  literal ends depends on `standard_conforming_strings`, which the proxy does not
  track), a nested `/*` (PostgreSQL nests block comments, HeliosDB-Nano ends one at its
  first `*/`), a `--` comment ended by a lone `\r` (HeliosDB-Nano runs it on to `\n`), a
  dollar quote holding a quote or a comment marker (HeliosDB-Nano strips comments
  without knowing dollar quotes), a `$` where HeliosDB-Nano and HeliosDB-Lite open a
  dollar quote and PostgreSQL does not (`a$$`, `a$b$`, `$1$`) or the reverse (`$é$`,
  a tag Nano's splitter does not know), a comment holding a `;` or a `$` or leaving a
  quote open (`-- it's`) before a later `;` (HeliosDB-Lite's splitter knows no comments;
  a comment whose quotes all close, such as a sqlcommenter tag
  `/*controller='users',action='index'*/`, leaves it where PostgreSQL is), or an
  unterminated token; so is a quoted literal where a table name is read
  (`SELECT v FROM public.'kv'`, which HeliosDB-Nano and HeliosDB-Lite read as a read of
  `kv` and the table extraction as one of `public`, so a write of `kv` left it served;
  a literal after a function's `FROM`, as in `substring(s FROM 'x')`, or after
  `IS DISTINCT FROM` counts too, since `SELECT kv.is DISTINCT FROM 'kv'` reads `kv`
  there, and so does one after a number running into a word, `SELECT 1from 'kv'`).
  Such a read is counted in
  `heliosdb_proxy_cache_ambiguous_text_skipped_total` (JSON `cache_ambiguous_skipped`);
  such a write (for a literal: one where a table the write may change is named)
  invalidates everything in both caches; and a `;` inside such text, which
  may end a statement the capture never sees, invalidates every cached result at its
  `ReadyForQuery`, read or write, counted in
  `heliosdb_proxy_cache_unseen_write_invalidations_total` (`SELECT E'\'' ; UPDATE b SET
  v = 2` runs the UPDATE on PostgreSQL, `SELECT 1 -- ; UPDATE b SET v = 2` on
  HeliosDB-Lite; the capture read each as one read, and the query cache retired nothing
  for either). Nothing else does: a `;` inside a plain literal or a quoted identifier
  ends no statement. A statement whose first word follows a leading comment the
  backends end in different places (`/* purge /tmp/* entries */ DELETE FROM sessions …`,
  a DELETE on HeliosDB-Nano and an unterminated comment on PostgreSQL) used to be
  classified by PostgreSQL's reading and never journaled, and one whose leading comment
  leaves a quote open before a later `;` (`/* it's */ UPDATE t SET v = 'a; DELETE FROM
  b; --'`, whose DELETE HeliosDB-Lite runs) was journaled as a plain, replayable UPDATE:
  in builds with a result cache (`query-cache` or `edge-proxy`) the capture now journals
  both as statements it cannot reproduce (their transactions are not replayed), which
  invalidates everything, and `[edge]` flushes on them. A leading query tag whose quotes all close (`/*controller='posts'*/ SELECT …
  WHERE body = 'a; b'`) is none of these: the statement is read by its first word, and
  journaled, replayed and invalidated as before. The capture also splits at a `;` after
  a dollar quote with a non-ASCII tag (`SELECT $é$ $$ $é$; DELETE FROM b` used to hide
  the DELETE).
- **L2 and edge cache keys merged statements that return different rows** (C-07,
  Sprinter ebee53b26919): a `--` or `/*` inside a literal truncated the key
  (`… ILIKE '%a -- b%' AND owner_id = 7` and `= 8` shared a slot), a removed comment
  glued its neighbours, literals lost their order and kind (`a = '1' AND b = 2` vs
  `a = 2 AND b = '1'`, `'1'` vs `1`), dollar-quoted literals and quoted identifiers were
  upper-cased, and so was every other word, although HeliosDB-Nano labels an output
  column as written (`SELECT Name …` and `SELECT name …` shared a slot, and the second
  client got the first one's column names). Both keys now come from one quote-aware
  scan, and every word keeps its spelling (in a `client_encoding` such as SJIS a letter
  byte can also belong to a multi-byte character: `C2 83 41` and `C2 83 61` are two
  identifiers); case variants of a statement no longer share an L2 or edge entry. Two
  string literals PostgreSQL reads as one (`'ab'`, a newline, `'c'`, also with a `\v`
  next to the newline) no longer share a key with the same two on one line, which
  PostgreSQL rejects. Where HeliosDB-Nano reads comments otherwise (it does not nest block
  comments, ends a `--` comment at `\n` only and strips comments without knowing dollar
  quotes) the statement is not cached at all (above). The key keeps the text as written
  for a statement whose `AS OF` / `VERSIONS BETWEEN`,
  `SECURITY DEFINER` / `SECURITY INVOKER` or `AS IDENTITY (…)` rewrite on HeliosDB-Nano
  depends on its whitespace (`SELECT v AS of FROM t` vs `AS  of`; `FROM xsecurity definer`,
  which reads the table `x` there, vs `FROM xsecurity\tdefiner`), whose literal, quoted
  identifier or comment holds one of those texts (HeliosDB-Nano's planner and HeliosDB-Lite
  read `AS OF` in the raw statement: `… WHERE id = 1 /* was offline */` fails there and
  no longer shares the key of `… WHERE id = 1`), or that names a catalog HeliosDB-Nano
  answers by matching the raw text (`pg_settings … name IN ('x')` vs `IN  ('x')`).
- **Writes whose scope the query cache misread:** HeliosDB-Nano's
  `MERGE BRANCH fix INTO main` moved only a table `main`, in the query cache and the
  edge cache, leaving the merged branch's reads current; a write naming a quoted
  identifier where a table name is read moved the qualifier or another table it names
  (`UPDATE s."t"` the table `s`, `UPDATE "t" … FROM u` only `u`), one naming a quoted
  literal there moved the qualifier or another table too (`UPDATE public.'kv' …`, a write
  of `kv` on HeliosDB-Nano and HeliosDB-Lite, moved `public`), and one naming a `$` name
  a cut name (`UPDATE t$x` the table `t`). All now invalidate everything, as a write
  naming nothing else the extraction could read already did (`INSERT INTO "app_book" …`,
  `INSERT INTO été …`). A quoted identifier, a literal or a non-ASCII name anywhere else
  (a column, a conflict target, an alias: `UPDATE accounts SET "lastLogin" = now() …`,
  `UPDATE customers SET 名前 = 'x' …`, `UPDATE kunden SET straße = 'x' …`), a literal
  where a table the write only reads would be named (`UPDATE … FROM`, a subquery, a
  function's `FROM`) in a write without a `DELETE` (after one, any `FROM` may name the
  table it deletes from: HeliosDB-Nano and HeliosDB-Lite run `DELETE x FROM 'kv' …` as a
  delete from `kv`) and anything after the `FROM` of `IS [NOT] DISTINCT FROM`
  (`… AND status IS DISTINCT FROM 'done'`) leave the write's tables read as before; a
  Unicode-escaped one (`U&"t"`) is unknown wherever it stands, and so is a non-ASCII
  word whose upper case is one of sqlparser's keywords, or, starting with a digit, is
  ASCII text (HeliosDB-Lite reads it as that keyword, `1ınsert` as `1` and `INSERT`). On the simple protocol
  `[edge]` invalidated nothing for a `MERGE` or a write after a leading comment
  (`/* job */ UPDATE …`; `-- v2`, a lone `\r`, `CREATE OR REPLACE VIEW …`), and for DDL
  only the tables its text reads
  (`CREATE OR REPLACE VIEW v AS SELECT * FROM t` left `v`'s entries served):
  it now reads the first word past leading comments, and DDL, `EXECUTE`, `CALL` and `DO`
  flush every edge entry. Neither protocol invalidated anything for
  `REFRESH MATERIALIZED VIEW`, `COMMENT`, `SECURITY LABEL`, `IMPORT FOREIGN SCHEMA` or
  `REASSIGN OWNED` (each now flushes every edge entry) or for an `EXPLAIN ANALYZE`, which
  runs the write it explains (now the tables its text names). A string of more than one
  statement is read one statement at a time: one holding a write, or any statement other
  than a read (`SELECT`, `VALUES`, `TABLE`, `SHOW`, a read-only `WITH`, an `EXPLAIN`
  without `ANALYZE`) or bare `BEGIN`/`START`/`SAVEPOINT`/`RELEASE`/`ROLLBACK`, flushes
  every edge entry, and a string of reads alone
  (`SELECT a FROM t1; SELECT b FROM t2`) no longer invalidates the tables it reads; a
  `;` inside a literal or a quoted identifier ends no statement there
  (`COPY t TO STDOUT (DELIMITER ';')` invalidates nothing).
- **`[edge]` took a `WITH` read for a write** when a literal, a comment or a quoted
  identifier held a data-modifying verb (`WITH r AS (SELECT … WHERE op = 'delete')
  SELECT …` flushed its tables on every execution through the extended protocol): only
  words outside literals, quoted identifiers and comments count now, on both protocols.
- **A write through `ONLY` did not invalidate its table** in the query cache or the edge
  cache: `UPDATE ONLY kv …`, `DELETE FROM ONLY kv …` and `MERGE INTO ONLY kv …` were read
  as writes to a table named `only`, so kv's cached reads (every tier) were served until
  their TTL ended. The table extraction now skips `ONLY` (and `ONLY (…)`).
- **A three-part name and a parenthesized join lost their table** in the query cache and
  the edge cache: `benchdb.public.kv` was read as the table `public`, and
  `FROM (orders JOIN items ON …)` as a read of `items` alone, so `UPDATE kv …` or
  `UPDATE orders …` left those reads served until their TTL ended. A name's last part is
  the table, and `(` before a table is skipped (a subquery's `SELECT` is no table).
- **A schema-qualified table spelled with whitespace around the dot** (`app . kv`) was
  read as the table `app` (query cache and edge cache), so its writes did not retire the
  reads of `app.kv` and a write to `app.kv` did not retire such a read. The qualifier's
  dot may now have whitespace or a comment on either side.
- **A read whose last select-list column ended in `from`, `join`, `into`, `update` or
  `table` was never invalidated** by a write (query cache and edge cache):
  `SELECT id, valid_from FROM prices …` matched at the `from` of `valid_from`, took the
  real `FROM` as the table and stepped over `prices`, so the read was keyed to a table
  `from` and `UPDATE prices …` left it served until its TTL ended (`last_update`,
  `data_table` alike). A keyword that ends an identifier is no longer one, and a match no
  longer swallows the keyword after it (`SELECT t.from FROM prices`).
- **A read that spelled a table name longer than 63 bytes was never invalidated** by a
  write naming the table as PostgreSQL stores it (truncated to 63 bytes), nor by a
  logical-decoding change (query cache and edge cache): the read was keyed to its whole
  spelling. Table names are now cut to 63 bytes, for reads and writes alike.
- **`cache=skip` and `cache_ttl` were lost** when `[routing_hints]` stripped them, and a
  multi-key hint comment (`/*helios:cache_ttl=60,cache=skip*/`) was read as one key.
- **A huge `cache_ttl` hint panicked the connection's task**
  (`SELECT 1 /*helios:cache_ttl=99999999999999999999*/`); it is now ignored.
- **The semantic tier never hit again after a write** (the stale entry shadowed every
  refill), embedded the raw SQL (literals included) and could serve another row's result,
  waited on its embedding endpoint on the read path (30 s timeout, unbounded permit wait),
  and kept an unbounded, uncounted embedding memo. Rewritten (above).
- **`CacheConfig::validate` accepted a NaN or 0 similarity threshold.**
- **The docs claimed `VALUES`, `TABLE` and `WITH` reads were cached;** only
  single-statement `SELECT` reads are.
- **Admin node controls reach routing.** `POST /api/chaos force_unhealthy` and
  `POST /nodes/{addr}/disable` only flipped the admin API's display copy of node health,
  which the server overwrote within a second, so neither ever took a node out of routing
  (field report 2026-10-16, finding F; Sprinter b251750f1d54). Both now set a per-node
  override the server owns and republish health and routing before the request returns:
  the node publishes as unhealthy and counts as `enabled = false` (`/nodes` shows the
  `override`, `/topology` drops it, no new query is routed to it, a session on it moves at
  its next request), probes cannot re-admit it, and a forced provider leader makes writes
  fail closed. `restore`, `reset` and `/enable` clear overrides; `reset` leaves operational
  disables alone. A cleared node is routed to again at once when its probe counters already
  prove it, otherwise when its next probe passes. Overrides are per proxy instance and do
  not survive a restart; the proxy still never promotes. Library: `node_overrides` module
  (`NodeOverrides`, `NodeOverride`, `OverrideKind`, trait `NodeControl`),
  `ProxyServer::node_control()`, `AdminState::with_node_control()`.

### Development

- **No lint suppressions left in `src/server/`** (Sprinter 7603358c5da2). The forward
  paths take a `ForwardCtx` (client stream, connection cache, current node, read routing,
  session/state/config, fault slot). The extended path takes an `ExtBatch` (batch frames,
  routing SQL, Sync/Flush, defined statements, held unnamed Parse), which
  `InFlight::Extended` and TR recording also carry. The TR fault path takes a
  `TrRecovery`. Parameter counts: `forward_extended_batch` 16→5, `forward_simple_query`
  9→2, `tr_handle_fault` 10→3, `tr_after_extended` 10→6. The plugin- and hint-only
  routing variants are compiled only where something builds them:
  `PreQueryAction::{Block, Cached}` and `RouteOverride::Block` under `wasm-plugins`;
  `RouteOverride::{Primary, Standby, Node}` under `wasm-plugins` or `routing-hints`. No
  behaviour change. The 16 remaining suppressions are on public API and wait for the next
  major release.
- **`scripts/regress/bench-usertp.sh` and `scripts/perf-gate-userpath.sh`:
  `PGBENCH_PROTOCOL`.** `PGBENCH_PROTOCOL=extended|prepared` runs the measured pgbench
  cells in that query protocol (`-M`), so the user-path gate measures the
  extended-protocol relay (`forward_extended_batch`). Unset keeps the simple protocol and
  today's command lines; any other value exits 2 before any heavy step. The harness runs
  `tr_mode = "session"`, so TR recording returns early in these cells. Each result row
  records its `protocol`, `compare-usertp.py` prints the protocols compared (and warns
  when they differ), and `perf-gate-userpath.sh` archives its settings in
  `<label>-env.txt`. `scripts/tests/compare_usertp_test.py` covers the comparator in CI.

## [2.1.0] - 2026-10-15

Read offload to standbys now works as documented and is on by default. Session settings
(`SET`, `RESET`, `DISCARD ALL`, `SET ROLE`) are tracked statement by statement on both wire
protocols, so offloaded reads and transaction-replay re-homes carry them, and a failover
that cannot restore them exactly is refused instead of continuing without them. A Patroni
leader counts only while it holds the DCS leader lock, under a lease no longer than
Patroni's own, and a session whose home stops being the leader is re-homed. Reconnect
waves, backend connections per node and client sessions per tenant can be capped; the
query cache can follow logical replication; transaction replay covers prepared statements
and portals from earlier cycles.

### Upgrade notes (behaviour changes)

- **Reads go to standbys by default.** With `[load_balancer] read_write_split = true` (the
  default), a side-effect-free read outside a transaction now runs on a standby
  (`standby_reads = "async"`); a session reads its own writes from the primary for
  `read_your_writes_ms` (1000). `standby_reads = "primary"` keeps the 2.0 routing.
  Standbys that require a password need `[auth] mode = "scram"` with an `auth_file`
  (otherwise those reads fall back to the primary). See Changed below.
- **A failover can refuse a session with `08006`** when the proxy cannot restore its
  settings exactly (`set_config(..., false)`, a cycle it cannot read statement by
  statement, more distinct variables than `[limits] tr_max_session_set_statements`) until
  the session sends `DISCARD ALL`, instead of re-homing it without them.
- **Reconnects are metered:** `[limits] max_concurrent_reconnects_per_node = 16` by
  default (0 = unlimited, the 2.0 behaviour).
- **Patroni:** `[topology] patroni_confirm_leader_lock = true` by default (the leader's own
  REST API must answer `GET /primary` with 200), and startup rejects a
  `lease_timeout_secs` that is not below `patroni_ttl_secs - patroni_loop_wait_secs`
  (defaults 30 and 10): set those to the cluster's Patroni values.
- **An empty `admin_token`** (config file or `--admin-token`) is a startup error.
- **Gateways** (HTTP SQL, GraphQL, MCP) now apply `[rate_limit]`, `[query_rewrite]` and
  `[circuit_breaker]`.
- **Library API** (crates that use `heliosdb-proxy` as a library): `JournalOp` gains a
  `PreparedCommitted` variant, so exhaustive matches must handle it; `L2Config`
  (`shed_batch`, `shed_pause`), `ServerMetricsSnapshot` (`admission_queue_depth`),
  `ExtendedBatchLog` (`outside`) and `TransactionState` (`replay_cap_exceeded`) gain
  public fields, so struct literals must set them. The daemon's config, CLI, wire
  protocol and admin API stay compatible.
- **Dependencies:** `dashmap` 6, `socket2` 0.6, `base64` 0.22. A crate that builds this one
  against its own lock file needs `cargo update -p dashmap -p socket2 -p base64`.

### Fixed

- Read offload and transaction replay now see a statement behind comments, such as
  sqlcommenter tags (`/* app=shop */ SET search_path TO tenant1`). Such a `SET` was
  neither tracked nor did it keep the session's reads on the primary, so offloaded reads
  ran with the default settings, for example another tenant's `search_path`. A tagged
  `CREATE TEMP`, `DECLARE` or `LISTEN` did not pin the session either, a tagged
  `SAVEPOINT` was not seen by the transactional `SET` tracking, and `RESET x /* all */`
  was taken for `RESET ALL`. Comments nest and a `--` comment ends at a line feed or
  carriage return, as in PostgreSQL. Tagged reads now offload like untagged ones. Under
  the aborted-transaction emulation after a failover, a tagged `ROLLBACK` is now accepted.
- `RESET ALL` no longer forgets a tracked `SET ROLE` or `SET SESSION AUTHORIZATION`:
  PostgreSQL's `RESET ALL` leaves both in place, so after `SET ROLE tenant_a; RESET ALL`
  offloaded reads and a transaction-replay re-home ran as the session user instead of
  `tenant_a`. `DISCARD ALL` still resets them. A standby connection that took a mirrored
  role is reset with `RESET SESSION AUTHORIZATION` and `RESET ROLE` as well, so `RESET ROLE`
  reaches it too. The role and the session authorization always fit under
  `[limits] tr_max_session_set_statements`.
- Reads are no longer offloaded while the session's default isolation level is
  SERIALIZABLE (`default_transaction_isolation` or `SET SESSION CHARACTERISTICS`): a hot
  standby refuses them ("cannot use serializable mode in a hot standby").
- An extended-protocol `SET`/`RESET`/`DISCARD ALL` sent alone outside a transaction (one
  Bind and one Execute of the same portal in the cycle, the way JDBC, Npgsql and
  tokio-postgres send a `SET`) is now tracked like a simple-protocol one. It is mirrored
  onto standby connections, so such sessions keep offloading reads instead of being
  pinned to the primary after their first `SET`, and transaction replay restores it on a
  replacement backend; it now counts toward `tr_max_session_set_statements`, and a
  replacement backend that rejects it aborts the re-home like any tracked `SET`. A cycle
  after which the proxy cannot be sure which prepared statements the backend holds keeps
  the session's reads on the primary.
- Extended-protocol cycles (Parse/Bind/Execute up to a Sync) are now read statement by
  statement, the way PostgreSQL runs them. Statements up to a Sync share one transaction
  unless they control it.
  - Session-level `SET`, `RESET` and `DISCARD ALL` apply when that transaction commits.
  - Inside an explicit transaction they are deferred to its end, applied if it commits,
    dropped if it rolls back, and undone by `ROLLBACK TO SAVEPOINT`, as in the simple
    protocol.
  - `COMMIT`, `ROLLBACK`, their `AND CHAIN` forms, `PREPARE TRANSACTION`, `SAVEPOINT`,
    `RELEASE` and `ROLLBACK TO` settle the deferred settings, whichever protocol
    deferred them.

  Covered patterns:
  - Spring or Hibernate on JDBC with autocommit off (`BEGIN` sent in front of a
    transaction's first statement).
  - pgjdbc `autosave` (a `SAVEPOINT` in front of each statement).
  - Batches of several statements.
  - `SET LOCAL`, now recognised as no session setting.

  Such sessions keep offloading reads, and transaction replay restores their `SET`s.

  What was wrong before:
  - An extended-protocol `SET` inside a transaction or batched with other statements kept
    the session's reads on the primary and was never restored.
  - A simple-protocol `SET` inside a transaction that an extended-protocol `COMMIT` ended
    (drivers that mix the two) stayed pending. Offloaded reads ran without it, and a
    failover re-homed the session without it.
  - With `tr_mode = session` (the default), an extended-protocol cycle did not advance the
    transaction status this tracking compares against.
- For a cycle that failed, the proxy considers every place the error could have come from:
  - a statement that ran and failed, or one whose Parse or Bind failed;
  - a Parse, Describe or Close after the last Execute;
  - an error answering a Flush, which never reaches the Sync's response.

  It keeps the cases that end in the transaction status the server reports, and uses the
  outcome they agree on. PostgreSQL semantics this follows:
  - `DISCARD ALL` as the first statement of a cycle commits at once.
  - Settings made in a prepared transaction persist, as at `COMMIT`.
  - A transaction a cycle opens and rolls back leaves nothing behind.
  - `SET`s that ran before a `CALL` or `DO` outside a transaction block in the same
    cycle are unknown: PostgreSQL refuses transaction control there (2D000), but other
    PG-wire backends may let the procedure commit or roll them back.

  When the cases disagree, or a cycle cannot be read statement by statement, the
  session's settings become unknown (inside an open transaction, once it commits). A
  cycle cannot be read statement by statement when it has:
  - a statement the proxy has no record of;
  - an Execute of a portal not bound just before it;
  - a COPY still running;
  - a simple Query arriving before its Sync;
  - more statements than the new `[limits] tr_max_cycle_statements` (default 16).

  A statement calling `set_config(..., false)` (or with an `is_local` the proxy cannot
  read) also makes them unknown, once it commits: the proxy cannot record what it set.

  Such a session keeps its reads on the primary. A failover is refused with `08006` until
  `DISCARD ALL`, rather than re-homing the session without those settings. New counter:
  `tr_session_set_untracked_total`.
- Failed cycles are read exactly (Sprinter 8bd78d5947e5). The proxy counts the statements
  of each request that completed (`CommandComplete`, `EmptyQueryResponse`,
  `PortalSuspended`, output answering a `Flush` included) and notes any error in its
  responses, so it knows which statement failed instead of considering every position.
  Common driver patterns that made the settings unknown now stay known:
  - pgjdbc `autosave=always` with a `SET` in the transaction and a recovered error
    (`[SAVEPOINT PGJDBC_AUTOSAVE, failing INSERT]`, then `ROLLBACK TO`): the transaction's
    settings were tainted, and after `COMMIT` a failover was refused until `DISCARD ALL`,
    which HikariCP never sends;
  - a psycopg pipeline in autocommit with a `SET` and a mid-pipeline fetch (`Flush`);
  - a cycle with a Parse, Describe or Close after a `COMMIT` that failed.

  The count is not used when a backend fault or re-home interrupted the request, or when
  a `[query_rewrite]` rule changed its text.
- `SET`, `RESET` and `DISCARD ALL` inside a multi-statement simple query are now tracked:
  the query's statements are read in order, like an extended-protocol cycle. Before, only
  a query's first statement was looked at, and a multi-statement query was not tracked
  at all. So after a connection pool's reset query (Npgsql's
  `SET SESSION AUTHORIZATION DEFAULT;RESET ALL;...`, asyncpg's `...;RESET ALL;`) the proxy
  still held the previous logical user's settings, `SET ROLE` included, and a
  transaction-replay re-home restored them onto the connection now serving someone else.
  A query whose statement boundaries depend on `standard_conforming_strings` makes the
  settings unknown. Simple-protocol `COMMIT AND CHAIN` and `ROLLBACK AND CHAIN` now
  settle deferred settings. A transaction that ends on a statement that cannot have ended
  it leaves them unknown instead of dropping them.
- `SET SESSION AUTHORIZATION` and `RESET SESSION AUTHORIZATION` now also end a tracked
  `SET ROLE`, as in PostgreSQL. A `SET ROLE` that follows is restored after them: two
  pool resets on one connection, each followed by another tenant's `SET ROLE`, no longer
  restore the role switched off. A simple query that is not valid UTF-8 (for example
  under `client_encoding` LATIN1) is read with replacement characters: its statement
  structure still counts, but a `SET` in it makes the settings unknown, since its text
  cannot be restored. The statements of a simple query that yields for COPY are read when
  the COPY ends.
- A simple query with more than one `COPY ... FROM STDIN` (for example
  `SET ...; COPY a FROM STDIN; COPY b FROM STDIN`): after the first COPY ended, the
  proxy marked the session as no longer copying while the server waited for the next
  COPY's data. It now stays mid-COPY until the query's ReadyForQuery, and the query's
  settings are read then.
- `PREPARE p AS ... set_config(..., false)` makes the session's settings unknown when it
  runs, even inside a transaction that rolls back: the prepared statement survives, and a
  later `EXECUTE p` changes the setting.
- Savepoint names in this tracking are read like PostgreSQL reads them:
  - comments inside the statement are skipped;
  - quoted names keep their spaces;
  - `ROLLBACK WORK TO` and `ROLLBACK TRANSACTION TO` count as `ROLLBACK TO`.

  A `ROLLBACK TO` a savepoint the proxy has no record of, while `SET`s are deferred, makes
  them unknown. The tracked savepoints per transaction are capped by the new
  `[limits] tr_max_tracked_savepoints` (default 1024). A `SET` whose value contains a
  quoted `;` is now tracked.
- A backend that ends an idle session (`pg_terminate_backend`, a fast shutdown during a
  switchover, `idle_session_timeout`) sends a FATAL `ErrorResponse` before it closes.
  The idle backend-watch forwarded that frame to the client although the proxy keeps the
  client session and re-homes it, so a driver could close a connection that transaction
  replay was about to recover. With `tr_mode` other than `none` the frame is now dropped
  and the backend treated as gone. With `tr_mode = "none"` nothing is restored, so the
  FATAL still reaches the client as its notice that the session state is lost.
- Topology authority lease (H-02): the tracker refreshed the lease to "now" whenever the
  provider still reported the same leader, but a provider keeps its previous answer while
  a poll is waiting on an unreachable endpoint. With the Patroni control path cut, writes
  continued about 4 s (two endpoint timeouts) past `lease_timeout_secs`. The lease now
  runs from the start of the probe or request that last confirmed the leader. Startup
  warns when `poll_interval_secs` is not below `lease_timeout_secs`.
- Named prepared statements across a session's backend connections (F2). A `Close`
  reaches only the connection its batch is routed to, but the proxy kept listing the
  name as prepared on the session's other connections. When a session's batches reach
  more than one node (a `/*helios:node=...*/` routing hint, a read re-routed after its
  node failed), a statement closed and re-Parsed on a standby could still run its old
  definition when a later `Bind` stayed on the primary, a `Bind` of a closed name ran
  instead of failing with `26000`, and a new `Parse` of the name failed with `42P05`.
  Those connections now close the name before a batch uses or redefines it (bounded by
  `reprepare_timeout_secs`). Separately, `Close s; Parse s ...` in one batch (the Npgsql
  replacement pattern) dropped `s` from the session's record, so it could not be
  re-prepared on another connection; the record now keeps the new definition. Live
  check: `tests/integration/prepared_close_routing.py`.
- Extended-protocol `COPY ... FROM STDIN` (Parse/Bind/Execute/Sync, CopyData, CopyDone,
  Sync, as libpq's `PQexecParams` and tokio-postgres `copy_in` send it) no longer stalls
  (Sprinter 03fb06edf4b8). PostgreSQL ignores the cycle's Sync during copy-in and answers
  the Sync the client sends after CopyDone, but the proxy waited for that ReadyForQuery at
  CopyDone without reading the client's Sync, until the backend read timeout. An extended
  COPY's end is now relayed and its bookkeeping settled at the client's Sync. Simple-query
  COPY (psql, pgjdbc, psycopg, asyncpg) is unchanged.
- A session whose home connection to the former primary was still open when the topology
  provider moved the leader (a switchover that left the old primary running) sent its next
  write to a fresh connection on the new leader without its settings, or ran the rest of
  an open transaction outside it (Sprinter 480d8084f833). It now leaves that connection
  before its next request: under transaction replay it re-homes like a session whose
  backend was lost while idle (settings restored, or `08006` when they are unknown), and
  an open transaction is reported lost (`57P01`). Live check:
  `tests/integration/home_move.py`.
- A transaction-replay re-home during a Patroni switchover could replay onto the new
  leader before it had left recovery: Patroni takes the leader lock and starts the
  promotion about 100 ms before PostgreSQL accepts writes, and the replayed `INSERT`
  failed read-only, so the client got `40001` instead of a transparent failover. A
  replacement connection whose server reports `in_hot_standby = on` (PostgreSQL 14+) is
  now dropped and redialed until the promotion has completed, within the recovery
  deadline (`write_timeout_secs`), without marking the node unhealthy. Backends that do
  not report `in_hot_standby` are unaffected.

### Changed

- **Read offload to standbys now works as documented** (behaviour change). With
  `[load_balancer] read_write_split = true` (the default) a session's reads used to stay on
  the node the session was on — the primary, from its first statement — so standbys only
  served reads after a failover or with a `node=` hint. Now a read the proxy can show is
  side-effect-free (one `SELECT`/`VALUES`/`TABLE`/`WITH` of side-effect-free built-ins
  and `tr_read_functions`, no `INTO`, no row locks, outside a transaction; for the
  extended protocol, every statement of the batch) runs on a standby, chosen by the new
  `[load_balancer] standby_reads`: `"async"` (default) any healthy standby or read
  replica, `"sync"` only synchronous standbys (new `[[nodes]] synchronous = true`, or
  Patroni's `sync_standby`), `"primary"` none — the previous behaviour. Every other
  statement, and everything inside a transaction, runs on the primary. Session `SET`s are
  mirrored onto the standby connection; temp tables, cursors, SQL `PREPARE`, `LISTEN`,
  `set_config()`, multi-statement strings and extended-protocol `SET`s keep that
  session's reads on the primary. After a session's write its reads stay on the primary
  for `[load_balancer] read_your_writes_ms` (new, default 1000), so it reads back its own
  writes; other sessions' recent writes can still be missing on an asynchronous standby
  (bound the lag with `[lag_routing]`, use `"sync"` with `synchronous_commit =
  remote_apply`, or set `standby_reads = "primary"` to keep the old routing). A
  `/*helios:route=standby*/` hint now sends the statement to a standby (it only cleared
  the write flag before). The proxy opens standby connections itself: with pass-through auth
  it holds no password, so a standby that requires one refuses it, and the read is served
  by the primary instead (the session then keeps its reads there, and later sessions of
  that user and database without a credential skip that standby until a SIGHUP reload).
  Offload to password-protected standbys needs `[auth] mode = "scram"` with an
  `auth_file`, or a standby that trusts the proxy.
- Dependencies: `dashmap` 5 → 6 (6.2.1), `socket2` 0.5 → 0.6 (0.6.5, the line tokio
  already uses), `base64` 0.21 → 0.22 (the version `reqwest` and `hyper-util` use; 0.23
  would add a third copy). Duplicate crate versions in the all-features tree drop from 45
  to 43; `base64` 0.21 and `hashbrown` 0.14 remain through the `config` crate. A crate
  that builds against this one with its own lock file needs
  `cargo update -p dashmap -p socket2 -p base64`.

### Added

- `[limits] max_concurrent_reconnects_per_node` (H-05, default 16 per node, 0 = unlimited): the most backend
  connections the proxy opens at once to one node on a session's behalf (a transaction-replay
  re-home, a reconnect after a lost backend, a standby connection), from TCP connect through
  authentication, so a failover's reconnect wave reaches the new primary metered. Over the
  limit a dial waits up to `backend_slot_wait_ms`, then the request is refused with 53300; a
  transaction-replay re-home keeps retrying until its recovery deadline.
  Metrics: `heliosdb_proxy_backend_reconnects_in_progress`, `_reconnects_peak` (the most
  opened at once to one node), `_reconnect_limit`, `_reconnect_waits_total`,
  `_reconnect_refusals_total`; `/metrics` `reconnect_slots`.

- Paced L2 shed (`[cache] l2_shed_batch`, default 1024, and `l2_shed_pause_us`, default
  2000): the `cache-reclaim` thread now removes write-retired L2 entries in steps with a
  pause between them, instead of removing a whole retired working set at full rate while
  concurrent readers wait on the same map shards. `L2Config` gains public `shed_batch` and
  `shed_pause` fields.

- Per-request tenant identity on the HTTP SQL, MCP and GraphQL gateways (H-06): with
  `[multi_tenancy]` enabled, a gateway's new `tenant_header` key names the request header
  that carries the tenant. A request must name a known tenant (403 otherwise), and every
  statement it runs gets that tenant's row filter after the rewrite rules, as on the
  PG-wire listener. `strict_config` now refuses `[multi_tenancy]` only together with a
  gateway that has no `tenant_header`.

- Per-tenant client admission (H-05 slice 2): `[multi_tenancy] max_clients_per_tenant`
  (default 0 = off) and per-tenant overrides in `[multi_tenancy.tenant_max_clients]` cap
  the client sessions each known tenant holds. The tenant is read from the startup packet;
  a tenant at its cap waits up to `[limits] client_admission_wait_secs`, then gets 53300
  naming it, without affecting other tenants. The tenant slot is taken before the
  process-wide one. Metrics: `tenant_slots` on `/metrics`,
  `heliosdb_proxy_tenant_{clients,client_limit,admission_queue_depth}` and
  `heliosdb_proxy_tenant_admission_{waits,refusals}_total{tenant}`, plus
  `heliosdb_proxy_admission_queue_depth`. `ServerMetricsSnapshot` gains a public
  `admission_queue_depth` field.

- Opt-in holding of small re-executable reads (TR-09, GH #51): `[limits]
  tr_buffer_small_reads_bytes` (default 0 = off). Under `tr_mode = "select"` or
  `"transaction"`, the response of a read the proxy may re-execute is held until its
  `ReadyForQuery`, up to that many bytes, so a backend fault before then retries the read
  transparently instead of closing the session. A larger response is published at the
  bound and streams on as before. Costs time to first row and the bound in memory per
  session.

- In-session transaction replay covers named prepared statements and portals from earlier
  protocol cycles (TR-08, GH #50). A transaction that binds a statement the session
  prepared in an earlier cycle, or fetches more rows from a portal an earlier cycle bound,
  is now replayed after a backend loss instead of being refused with `57P01`/`08007`. The
  proxy keeps the named statements the backend acknowledged (their batch reached
  `ReadyForQuery` without an error), records each cycle with the `Parse` it relied on, and
  redefines that statement on the replacement connection before replaying the cycle.
  Names it cannot vouch for (failed `Parse`/`Close`, SQL `PREPARE`/`DEALLOCATE`/`DISCARD`,
  redefinition after use) still refuse. Bounded by `[limits] max_prepared_statements`.
  `ExtendedBatchLog` gains a public `outside` field. Live check:
  `tests/integration/tr_prepared_replay.py`.

- Opt-in automatic standby promotion, `[auto_promote]` (off by default, single-proxy).
  After the topology has had no primary for `down_after_secs` and a direct probe finds
  no writable node, the proxy promotes the standby with the highest replay LSN (lag
  within `max_lag_bytes`) via `pg_promote()`, a `promote_command` or a `promote_url`,
  after an optional `fence_command`. Requires `[topology] provider = "postgres"`.
  Status at `GET /api/promotion`. The `[ha]` rejection message now points here.
- Latency histograms and trust metrics (P-03) on `/metrics` and `/metrics/prometheus`:
  failover reconnect, transaction replay, admission wait, journal apply and operator
  replay durations (`heliosdb_proxy_*_seconds`, buckets from the new
  `[limits] latency_buckets_ms`); `heliosdb_proxy_tr_replay_refusals_total{reason}`;
  query cache hits per tier, misses, puts, invalidations, stale-hit refusals, raced
  fills and retained bytes per tier; edge cache retained bytes and gap flushes;
  gateway pool reuse/miss/discard counters and idle gauge per gateway (H-06).
  `TransactionState` gains a public `replay_cap_exceeded` field.
- Per-node backend connection caps (H-05): `[[nodes]] max_connections` (default 0 =
  unlimited) bounds the backend connections this proxy holds to a node, in use and parked
  idle alike. A full node gives up one idle pooled connection, then waits up to the new
  `[limits] backend_slot_wait_ms` (default 5000), then refuses with 53300 without marking
  the node unhealthy. `/metrics` reports `node_slots`; Prometheus
  `heliosdb_proxy_node_backend_*{node}` and `heliosdb_proxy_backend_slot_wait_seconds`.
- `[cache] invalidation_source = "logical"` (C-02 slice 2, opt-in): the query cache also
  follows the database's committed changes from a `pgoutput` logical replication slot
  (`logical_dsn`, `logical_slot`, `logical_publication`, `logical_feedback_ms`,
  `logical_max_changes_per_poll`, `logical_max_slot_lag_bytes`), so writes that bypass the
  proxy — other proxies, direct connections, triggers — invalidate within about one poll.
  Unknown relations, a lost connection or slot, and excessive slot lag invalidate the
  whole cache. The proxy refuses to start if the slot cannot be attached.

### Changed

- The HTTP SQL, GraphQL and MCP gateways now apply the PG-wire request policies (H-06):
  `[rate_limit]` per request (keyed by `key_by`; denial = HTTP 429), `[query_rewrite]`
  rules on every statement, and `[circuit_breaker]` (fast-fail with HTTP 503 while the
  backend's circuit is open; connect outcomes recorded). `[multi_tenancy]` together with a
  gateway is refused under `strict_config` and warned about otherwise (gateways have no
  per-request tenant identity). `[mcp] read_only = true` probes the backend role at
  startup: a role that can write user tables is refused under `strict_config` and warned
  about otherwise.
- With `[topology] provider = "patroni"`, the leader `/cluster` names now counts only
  while it holds Patroni's DCS leader lock: its own REST API must answer `GET /primary`
  with 200 (`patroni_confirm_leader_lock`, default on). Startup now rejects a
  `lease_timeout_secs` that is not below the new `patroni_ttl_secs` (default 30) minus
  `patroni_loop_wait_secs` (default 10), so the proxy never trusts a leader longer than
  Patroni does; set them to the cluster's Patroni values. `/topology` lists Patroni's
  members (role incl. `sync_standby`, state, lag) under `members` (H-02).
- One routing snapshot for writes, reads, pool eviction and `GET /topology` (H-01
  step 3). Under a topology provider, reads now follow it too: the read set is every
  enabled, healthy node except the provider's leader, so after a promotion the demoted
  former primary serves reads and the promoted former standby is no longer treated as
  one (with no valid leader, reads use the configured standbys/replicas). Replica lag
  is measured against the leader. When the leader moves to another node, idle pooled
  connections to the former leader are closed. `/topology` gains a `routing` object
  (leader, lease bound, read set, leader-change and eviction counters), and
  `currentPrimary` is the snapshot's write target, `null` while a provider's lease has
  lapsed. Static topology routes as before.
- `JournalOp` gains a `PreparedCommitted` variant (cache notification for a
  successful `COMMIT PREPARED`). Downstream exhaustive matches on this public enum
  must handle it; it never creates replayable journal history.

### Fixed

- With a topology provider (`patroni`, `postgres`), a new client session opened
  after a promotion now connects to the provider's leader, and the write path
  checks the target against that leader instead of the configured
  `role = "primary"`. Previously the session started on the demoted node and its
  first write failed read-only. With no valid leader, writes wait (fail closed).
  Static topology is unchanged.
- A backend login rejection now reaches the client as exactly one terminal
  startup error; the proxy no longer appends a second `08006` after forwarding the
  backend's `28P01`. Proxy-generated startup errors are unchanged.
- A successful `COMMIT PREPARED` invalidates the whole query cache before its
  completion reaches the client, including a transaction prepared by another
  session and Execute/Flush without Sync. Failed commits and `ROLLBACK PREPARED`
  do not invalidate. Partial backend frames now stay with their own connection
  across Flush, the idle backend watch and Sync.
- Cache fills no longer grow an unused key-to-tables reverse index that was
  never cleaned up after table invalidation. The public lookup and unregister
  APIs remain, scanning the forward index.
- File-backed L2 query-cache entries retain their original age and precise TTL
  across demotion, disk reads and L1 promotion. Expired entries are no longer
  revived by these transitions; wall-clock changes do not extend their lifetime.
  The file index remains process-local, with no cache recovery after restart.

### Development

- Split the proxy server into focused modules under `src/server/`, with unit tests
  grouped by area under `src/server/tests/`. Public `crate::server::` paths and
  runtime behavior are unchanged. Updated contributor and architecture references
  to the new module locations.

- `scripts/bench-gate.sh` fails closed and records attribution evidence. The
  comparison moves to `scripts/bench-gate-compare.py`. A run is now INVALID (exit 2),
  never a PASS, if:
  - any case the executables declare (`--list`) is missing, duplicated, malformed,
    non-finite or stale in any round;
  - outputs for the label already exist;
  - the executables that ran differ from the ones built.

  A benchmark removed from the candidate fails the gate. New candidate-only cases are
  reported. `scripts/bench-gate-fde.py` classifies each base/candidate bench executable
  pair by its function bodies (`.eh_frame` ranges, address-normalized instructions):
  `body-identical` (only placement moved) or `code-changed`. The verdict rules are
  unchanged and the reviewer still attributes every separated regression; the
  classification is the evidence for that.

### Security

- wasmtime 36.0.15 → 36.0.16 (the 36.0.x wasmtime/cranelift/pulley family),
  fixing RUSTSEC-2026-0316 / GHSA-jqpg-j7w6-42pr: dynamic record lifting can
  allocate beyond the hostcall fuel limit. Only builds with the optional
  `wasm-plugins` feature include wasmtime. HeliosProxy builds wasmtime without
  `component-model`, so the vulnerable path is not compiled in; the bump is defence
  in depth and clears the advisory for downstream audits. The manifest now requires
  `>=36.0.16`, so dependents cannot resolve the vulnerable release.

### Added

- Minimal TOML configurations and partial pool, health, and load-balancer sections
  now use the same defaults as the CLI. Backend `host` and `role` remain required;
  omitted node port, weight, and enabled fields default to 5432, 100, and true.
- CLI-only deployments can set `--admin-token` or explicitly opt into
  `--admin-allow-insecure`. The new security flags cannot be combined with
  `--config` (rejected rather than silently ignored).

### Changed

- An empty or whitespace-only `admin_token` (config file or `--admin-token`) is
  now a startup error instead of an accepted empty credential.

### Fixed

- Journal replay preserves simple-query batches instead of submitting them as
  prepared statements. Extended entries retain their captured bindings and OIDs.
  Replay refuses transaction-ending or ambiguous SQL before sending it, including
  historical entries, and refuses incomplete failover journals.
- Ordinary complete `BEGIN; INSERT/UPDATE/DELETE/SELECT ...; COMMIT/ROLLBACK;`
  batches no longer trigger the anomaly detector's stacked-query heuristic.
  Suspicious statements and other injection heuristics remain active; ambiguous
  quoting keeps the conservative alert. The detector also scans faster (−4 % to −63 %
  per query). Operator note: the false positive also wrote one WARN line per such
  transaction, about 370 MB in 12 s at 64 clients. That logging slowed clients down. On a
  backend that is already CPU-saturated, removing it lets them push harder, so p99 for
  this workload can rise even though TPS does not drop. Measured: +10–29 % at 64 clients
  against a backend capped at 4 CPUs, and −32–35 % at 16 clients. With detection compiled
  out of both builds the difference disappears.
- The TR commit-boundary test fixture (`scripts/regress/tr-boundary-test.py`) enables TR
  explicitly. Since 1.8.0, `tr_enabled = false` forces `tr_mode` to `none`, so the fixture
  had silently stopped testing the mode it names; 7 of its 26 cases and all 7
  commit-outcome cases failed for that reason. They pass on 2.0.0 and on this change.
- Unsupported `[ha]` settings now fail startup with external-promotion and
  topology-provider guidance. Documentation distinguishes database promotion
  from proxy routing recovery and clarifies native WASM runtime requirements.

## [2.0.0] - 2026-09-25

A new major version: the HA topology, the result cache and Transaction Replay now share
one model of what the backend actually committed. The query cache invalidates on commit
for both wire protocols and every tier, a Patroni cluster can be the write authority,
and conflicting primaries fail closed. The query cache stays **opt-in**
(`[cache] enabled = false` by default): cached results are keyed by the SQL, database,
user and branch, not by session state such as `SET ROLE`, `search_path` or a tenant
setting read by row-level-security policies, and writes that bypass the proxy are only
bounded by `ttl_secs` (see docs/configuration.md `[cache]`).

### Upgrade notes (behaviour changes)

- **Gateways have a concurrency cap by default.** `[graphql_gateway]`, `[mcp]` and
  `[http_gateway]` now admit at most `max_concurrent_requests = 64` requests at once;
  beyond that the HTTP and GraphQL gateways answer `503` + `Retry-After` and MCP answers a
  JSON-RPC error for `tools/call`. Raise the key if you run more concurrent gateway
  requests. `query_timeout_ms` (default `30000`) replaces the fixed 30 s timeout.
- **Unknown config keys are reported at any nesting depth.** A typo inside a section
  (`[cache] l1_max_byts`) now logs a warning at startup, and fails startup with
  `strict_config = true` (previously only top-level keys were checked).
- **Conflicting primaries fail closed** with `[topology] provider = "postgres"`: several
  writable nodes are resolved by the strictly highest timeline, otherwise nothing is
  authorized (previously the first writable node probed won).
- **Query cache (when enabled):** writes on the extended protocol now invalidate it, a
  stale entry is refused on every tier (L1 and L3 used to be served until TTL), and
  DDL, `TRUNCATE`, `COPY ... FROM`, `EXECUTE`, `CALL` and `DO` invalidate the whole
  cache. The transaction-journal capture now also runs when Transaction Replay is off
  but the cache is on.
- **Library API:** `TopologyProvider` gains defaulted `conflicts_total()` and
  `leader_timeline()` (existing implementations keep compiling);
  `QueryCache::purge_tables` no longer removes L2 entries itself (they are refused by
  their write generation and replaced or evicted); new
  `InvalidationManager::take_table`/`forget_keys`, `L2WarmCache::put_evicting`/`shed`/
  `end_eviction`.

### Added

- **`strict_config` now catches unknown config keys at any nesting depth, not just
  top-level sections.** A typo inside a known section (`[cache] l1_max_byts = 1`,
  `[journal] fsyncc = "commit"`) was previously silently dropped by serde with no
  warning at all; the top-level-only `unknown_top_level_keys` check could never see
  it. Config loading now runs the whole `ProxyConfig` deserialization through
  `serde_ignored`, which reports the full dotted path of every ignored key at any
  depth. `strict_config = true` fails startup listing every offending path;
  `strict_config = false` (default) logs one `WARN unknown config key '<path>'
  ignored` per path, unchanged in spirit but now complete. The shipped
  `config/*.toml` examples are asserted to have zero ignored keys (they are
  documentation, so an ignored key there is a doc bug, not a parser gap).
- **H-06 slice 1: configurable gateway timeouts and per-gateway concurrency caps.**
  `[graphql_gateway]`, `[mcp]`, and `[http_gateway]` each gained `query_timeout_ms`
  (default `30000`, matching the previous hardcoded 30 s) and `max_concurrent_requests`
  (default `64`, a new cap — there was none before). Once `max_concurrent_requests`
  in-flight requests are outstanding, the HTTP and GraphQL gateways answer further
  requests with `503 Service Unavailable` + `Retry-After`, and MCP answers `tools/call`
  with a JSON-RPC error (code `-32000`); in every gateway, health/liveness checks and
  cheap protocol calls (MCP `initialize`/`ping`/`tools/list`, notifications) are never
  gated. Both knobs are validated to be `> 0`. `src/gateway_pool.rs`'s
  `BackendClientPool` also gained `reuse_count`/`discard_count`/`miss_count` counters
  (plus the existing `idle_count` as the idle gauge) so pool effectiveness can be
  inspected per gateway; not yet exported through `/metrics` (that needs the pool
  instances threaded into `src/admin.rs`, which lives in `src/server.rs`).
- **Patroni topology provider (H-01, #52).** `[topology] provider = "patroni"` with
  `patroni_endpoints` and `patroni_request_timeout_ms`: the cluster's running leader (as
  Patroni's DCS reports it on `GET /cluster`) becomes the authoritative write primary; a
  `standby_leader`, a leader that is not running, or one outside `[[nodes]]` is never
  authorized; no reachable endpoint means no primary (writes stop at the lease
  boundary). Requires the `postgres-topology` feature. `GET /topology` reports the
  leader's timeline (`authoritative.timeline`) and, for any provider,
  `conflictingPrimariesTotal`, also exported as the Prometheus counter
  `heliosdb_proxy_topology_conflicting_primaries_total`.

### Changed

- **Conflicting primaries are resolved by timeline, or fail closed (H-01).** With
  `provider = "postgres"`, more than one node reporting `pg_is_in_recovery() = false` no
  longer picks the first node probed: the node on the strictly highest timeline wins, and
  a tie or an unreadable timeline authorizes nothing. A provider that reports conflicting
  authority now makes the tracker drop its leader immediately instead of waiting for the
  lease.
- **The primary tracker follows a provider's new leader from its periodic check too
  (H-01).** Only the provider's change event moved it; a missed or lagged event left the
  old leader in place, its lease stopped being refreshed, and authoritative writes then
  stalled until the next leader change. A duplicate change event no longer starts a
  second authority epoch.

### Fixed

- **Query-cache invalidation is commit-aware on both protocols and every tier (C-02,
  #56).** Extended-protocol writes (every prepared statement, i.e. most drivers and
  ORMs) did not invalidate the query cache at all, and L1/L3 entries were never
  invalidated by writes, only by TTL — so a read could return a value older than a
  committed write until the entry expired. Each cached result now records the write
  generation of the tables it read, taken before its backend fetch; an autocommit write
  moves its tables' generations before the client sees the `ReadyForQuery` that
  acknowledges it, an explicit transaction's tables move when the backend reports the
  commit, before the client sees it (a rollback drops them), a result whose tables were
  written during its fetch is not stored, and every L1/L2/L3 hit is checked. Writes are
  taken from the transaction journal's capture of what the backend executed, which now
  also runs when TR is off but the cache is on. DDL, `TRUNCATE`, `COPY`, `EXECUTE`,
  `CALL` and `DO` invalidate the whole cache. L2 entries now keep their tables across
  the serialized form (they were dropped, so an L2 entry promoted to L1 escaped table
  invalidation). Writes that bypass the proxy, and two-phase commits, are still only
  bounded by `ttl_secs` (documented).

- **The first writes after a read-heavy phase no longer stall for seconds with the cache
  on.** Every writer purging a table cloned the table's whole cached-key set (10^5-10^6
  keys after a busy read phase), so the first concurrent commits paid for it once each: a
  3-4 s maximum transaction latency on the user-path harness. A purge now only takes the
  set out of the invalidation index (one purger per table gets it) and a dedicated
  `cache-reclaim` thread drops it; the entries are not walked, since a stale one is
  refused by its write generation and replaced by the next fill of its key or evicted by
  the L2 size bound and TTL. When L2 has to make room, one eviction pass runs at a time
  (every concurrent insert used to scan the whole cache and shed the same entries, a
  multi-second stall for all readers): it collects every entry a write has retired along
  with expired ones, instead of evicting live entries one full scan at a time, and the
  `cache-reclaim` thread drops them (inserts meanwhile proceed, so the size bound is
  soft for that long). A refill that replaces an entry for the same key no longer
  triggers an eviction at all. L2's usage account no longer keeps the bytes of a replaced entry or of
  an expired entry dropped on lookup (it only grew, so a full L2 evicted early and, on
  every insert, scanned the whole cache).

- **`--tr false` / `--tr=false` now turn Transaction Replay off.** The flag was declared
  as a plain `bool`, which clap treats as a presence switch: `--tr false` was rejected
  ("unrecognized subcommand") and `--tr=false` too, so TR could only be disabled through
  `tr_enabled` in `proxy.toml`, contrary to the documented CLI. A bare `--tr` still means
  `true`. Found by the new CLI parsing tests.
- The GraphQL gateway's HTTP status line now carries the matching reason phrase for
  404, 413, 429, 500 and 503 (it said `413 OK`).

- **A burst of new clients no longer fails with a false "Authentication failed"
  when the backend is at `max_connections`.** Every pass-through login opens its own
  backend connection, while the transaction/statement pool keeps idle connections
  parked; under a login burst PostgreSQL refused the excess with `53300 sorry, too many
  clients already`, and the proxy relayed it and logged it as an authentication failure
  (64-client burst after a 16-client run: 0 TPS in transaction mode). The backend's
  refusal arrives before any authentication exchange, so the proxy now holds it back,
  closes one idle pooled connection to that node and redials with jittered backoff,
  bounded by `[pool] acquire_timeout_secs`; only then does the client receive the
  53300. The same applies to transaction-mode redials. The same burst now completes
  (68.8k read / 17.9k committed TPS at 64 clients). Startup errors are also reported
  with their SQLSTATE: only class 28 is logged as an authentication failure, and a
  full backend no longer demotes the node on the `auth_file` path. New Prometheus
  counters `heliosdb_proxy_backend_capacity_waits_total` and
  `heliosdb_proxy_backend_capacity_refusals_total`.

## [1.9.0] - 2026-09-22

### Added

- **Recovery-grade transaction journal and committed-history replay (TR-07, #49).**
  The write path no longer journals post-response SQL text as never-committed synthetic
  transactions. A per-session capture state machine (`src/journal_capture.rs`) registers
  what is sent (simple-query strings, and extended-protocol `Parse`/`Bind`/`Execute` with
  the bound parameter values byte for byte) and reconciles it with what the backend
  answered (each `CommandComplete` tag, `ErrorResponse`, `ReadyForQuery` status), so the
  journal now holds real transactions: `BEGIN`…`COMMIT` boundaries as the backend
  reported them, per-statement outcomes, `SAVEPOINT`/`ROLLBACK TO` structure, source
  identity (client, user, database, backend, tenant) and a global commit sequence.
  Rejected statements, rolled-back transactions and two-phase transactions never enter
  committed history; a `COPY … FROM STDIN` or `EXECUTE` marks its transaction incomplete.
  Committed transactions are retained in a bounded store and, with `[journal] dir`
  configured, appended to a segmented on-disk journal (`src/journal_store.rs`:
  CRC-checked records, torn-tail truncation, rotation, size-based retention, `commit`/
  `interval`/`none` fsync policy, non-blocking bounded writer queue) and reloaded at
  startup. `POST /api/replay` gains `mode: "committed_history"`: committed transactions
  in commit order, one per `BEGIN…COMMIT` on one connection, parameters re-sent in their
  captured text/binary format through the extended protocol, stop-on-first-failure with
  rollback and a `last_commit_seq` resume point; the previous `501` is gone.
  `time_window` replay now reads committed history too (it no longer replays failed or
  rolled-back statements). The library `FailoverReplay` applies a journaled transaction
  on one connection inside one transaction and stops at the first failure instead of
  opening a connection per statement; arrays render as PostgreSQL array literals instead
  of degrading to `NULL`. New `[journal]` section (retention caps, `max_statement_bytes`,
  `dir`, `segment_bytes`, `retain_bytes`, `fsync`, `fsync_interval_ms`, `writer_queue`);
  new metrics `journal_committed_total`, `journal_rolled_back_total`,
  `journal_statements_total`; the replay `coverage` block reports the real guarantees
  plus committed counts, `commit_seq_high` and `dropped_transactions`.

- User-path benchmark harness for P-01: `scripts/regress/bench-usertp.sh` measures
  committed TPS (`pgbench -b simple-update`), read TPS with p50/p95/p99/p999 tails
  (per-transaction log), a first-row latency proxy, failed/unknown client outcomes and
  proxy RSS/CPU — against direct PostgreSQL and HeliosProxy in session/transaction pool
  modes, writing a machine-readable JSON per run. The comparison rules (identical
  capacity/TLS/durability/timeouts, same-window baseline/candidate, per-critical-path
  gates plus the 3% cumulative budget) live in `docs/internal/P-01-measurement-plan.md`.

- Admission and recovery-wave metrics (P-03). `/metrics/prometheus` now exports
  `heliosdb_proxy_admission_waited_total` (admissions that queued on the bounded H-05
  cap), `heliosdb_proxy_admission_timeout_total` (bounded waits that expired) and
  `heliosdb_proxy_reconnect_attempts_total` (jittered waits in the primary-select
  recovery loops) — the signals that explain a failover's reconnect wave, alongside the
  existing connection/query/failover/TR/cache counters.

- Capability reachability test and explicit live-test skips (V-01). A new test asserts
  that every advertised capability the config enables is actually wired on the running
  state (`pool-modes`, `query-cache`, `routing-hints`, `rate-limiting`,
  `circuit-breaker`, `query-analytics`) — removing a construction hook fails the test,
  not just a config struct. `HELIOS_REQUIRE_LIVE=1` turns a missing backend env var in
  `tests/integration/fixture.rs` into a hard failure instead of a silent skip, so a
  live-validation job cannot pass by skipping. The claim → evidence map lives in
  `docs/internal/V-01-claim-map.md`.

- Capabilities manifest and opt-in strict configuration (D-05). `GET /capabilities`
  (and `/api/capabilities`) reports every subsystem's `compiled` / `enabled` / `wired`
  state plus the `strict_config` flag. With `strict_config = true`, startup fails when
  an enabled subsystem was not compiled into the binary (e.g. `[cache] enabled = true`
  without the `query-cache` feature) or when an unknown top-level section/key is present;
  the default `false` keeps the historical warn-and-continue behaviour so existing
  configs are unaffected.

- `[topology]` config section (H-01). `provider = "postgres"` makes topology discovery
  authoritative for the write path: the daemon polls `pg_is_in_recovery()` on every
  configured node (requires the `postgres-topology` cargo feature), routes writes only
  to the provider's leader (which must be an enabled `[[nodes]]` entry), waits rather
  than falling back to a configured role while no leader is known, and reports the
  leader plus an authority epoch in `GET /topology`'s new `authoritative` block. A
  promotion therefore moves the write destination without hand-editing `proxy.toml`.
  The default `provider = "static"` preserves the historical role+health selection.
- `PrimaryTracker`/`PrimaryInfo` now carry an authority `epoch`, incremented on every
  observed leader change; the tracker is wired into the daemon for provider-backed
  configurations.
- `[topology] lease_timeout_secs` (default 10, H-02): a provider observation is a lease
  heartbeat. If the provider cannot be reached within the lease, the tracker drops the
  stale leader and the write path fails closed (`NoHealthyNodes`) instead of authorizing
  on stale knowledge. `GET /topology`'s `authoritative` block now reports `valid` and
  `leaseRemainingMs`. Docs state the fencing limit explicitly: a proxy-side lease cannot
  fence direct clients and is not a zero-RPO guarantee for async replicas.

- Cursor-aware invalidation resumption on the home side (C-03, first half): the edge
  registry now assigns every broadcast a monotonic stream offset (`seq`), keeps the
  last 256 events in a bounded replay ring, and accepts a `last_event_id=<boot>:<seq>`
  cursor on `GET /api/edge/subscribe`. A reconnect inside the ring gets the missed
  tail replayed (`: resume warm replayed=N` comment marker, hello carries version 0 so
  a warm cache is not wildcard-flushed); anything else — first connect, unknown format,
  foreign home boot, evicted history, or nothing streamed yet — falls back to
  `: resume gap` plus the existing wildcard hello flush. Pre-C-03 edges ignore the
  comment/id lines and keep flushing on reconnect. The edge client now sends its
  `last_event_id` cursor on reconnect and honours the marker: `warm` keeps the cache and
  applies the replayed tail, `gap` flushes, and a first connect (or a pre-C-03 home)
  still starts cold — closing C-03 end-to-end.

- Release-time downstream consumer check: `scripts/release/downstream-check.sh` reads
  the version in `Cargo.toml`, verifies each known consumer's exact `heliosdb-proxy`
  pin (HeliosDB-Lite today; `--apply` rewrites it), and compiles the consumer's
  proxy-backed feature under the fleet build lock, so a version bump or a
  library-API change that only breaks external constructors is caught before the
  tag instead of hours after (sprinter `c57ebe1d1ae9`).

### Changed

- **Binary codec: postcard replaces bincode 1** (RUSTSEC-2025-0141, bincode is unmaintained
  in every version). Used for the durable journal segments (`[journal] dir`) and the
  DistribCache L2/L3 blobs. Journal records now carry the magic `HJ02`; a directory written
  by a pre-release 1.9.0 build (`HJ01`) is refused at startup with an explicit error instead
  of being truncated as corrupt. No released version ever wrote `HJ01`.
- **TLS PEM loading uses `rustls-pki-types`** (`PemObject`) instead of the unmaintained
  `rustls-pemfile` (RUSTSEC-2025-0134). Certificates and PKCS#8, SEC1 and PKCS#1 keys load as
  before; `src/client_tls.rs` gained unit tests for every key encoding, missing files, wrong
  item types, key/certificate mismatch and the mTLS CA path. `cargo deny --all-features check
  advisories` now reports no findings.

- **Allocation-free statement classifiers (perf, sprinter `1d70b68fa7cc`).** The
  per-statement classifiers on the write and pool-mode paths no longer build an
  uppercased copy of every statement: `StatementType::from_sql` (transaction journal),
  the statement-mode safety check/warning (`is_safe_query`, `get_query_warning`) and the
  transaction-mode `PREPARE`/`DEALLOCATE` parsers now use ASCII case-insensitive
  prefix/substring tests on the trimmed input. Classification is unchanged (same bare
  prefix semantics; non-ASCII bytes compared verbatim). Controlled 3-round A/B against
  the pre-change tree: `journal/statement_type/*` −58..−70%, `pool_mode/statement_safety/*`
  −41..−80%, `pool_mode/prepared_parse/*` −22..−74%; this closes the 1.8.0 codegen
  regression cluster with margin (details in `benches/BASELINE.md`).

- `scripts/bench-gate.sh`: reproducible Criterion A/B gate — baseline and candidate
  trees interleaved over N rounds, own target dir per tree, fleet build lock + bounded
  scope per heavy step, per-case medians across rounds, and a verdict that fails on
  a mean delta above 3% or any *separated* regression above 2% (every candidate round
  slower than every baseline round). Archives per-round Criterion estimates, a per-case
  table and JSON under `sprint/baselines/proxy/<label>/`.

### Fixed

- **Write-path throughput under concurrent writers (TR-07 follow-up, P-01).** The
  recovery-journal capture took the shared async journal lock once per statement of an
  explicit transaction (begin, every logged statement, commit), and under 16 concurrent
  writers each acquisition parked the task behind the scheduler's wake-up latency:
  `pgbench -b simple-update` through the proxy fell from 12.5k to 7.5k committed TPS
  (session and transaction pool modes, `synchronous_commit=off`) while the proxy sat
  idle. An explicit transaction is now journaled session-locally — begin, statements,
  savepoints and incomplete marks touch nothing shared — and reaches the journal
  exactly once, at the backend-reported commit, through a short sync mutex on the
  committed store. Auto-commit statements take the same path. Committed TPS at 16
  clients is now above the pre-TR-07 baseline (15.2k vs 13.3k) with write p99 down
  from 2.3 ms to 1.5 ms; read paths unchanged. The pre-TR-07 baseline paid a smaller
  version of the same convoy (15.8k with journaling off → 12.5k on), which is why the
  fix lands above it. Evidence: `sprint/baselines/proxy/1.8.1-tr07h-usertp/`.

- `scripts/regress/bench-usertp.sh` (P-01 harness) produced no usable results: the
  generated proxy config lacked the required `[pool] test_on_acquire` key and doubled
  the admin listen address (`127.0.0.1:127.0.0.1:9099`, so the admin API never started);
  latency percentiles were written space-separated into a JSON array (the results file
  failed to parse) and treated pgbench's microsecond log as seconds (values off by
  1000×); the "unknown outcome" count grepped `08007` and matched digits inside floats
  in anomaly log lines; and the first-row probe timed one `docker run` per round-trip.
  The harness now reads `tr_unknown_outcome_errors_total` from the admin API, reports
  percentiles in milliseconds, and times the first-row probe inside one container.
  New knobs for cost isolation: `PGOPTIONS` is passed through to pgbench (e.g.
  `-c synchronous_commit=off` to measure the write path CPU-bound rather than
  fsync-bound), `TR_ENABLED=false` and `EXTRA_TOML` splice into the generated proxy
  config.

- **Cache byte budgets (C-01, part 2):** L1 per-connection and L3 semantic
  query caches now enforce aggregate retained-byte budgets (defaults 32 MiB and
  64 MiB) in addition to entry counts. Byte accounting is exact across
  insert/replace/remove/expiry and both tiers evict oldest-first when the
  budget is exceeded; previously only L2 (`size_mb`) and the edge cache were
  byte-bounded. `max_bytes = 0` on `L1Config`/`L3Config` means unbounded (the
  entry count still applies).
- The edge cache now has an aggregate retained-byte budget (C-01). `[edge]
  max_total_bytes` (default 256 MiB, `0` = off) bounds the payload across all entries;
  on insert the cache evicts LRU entries until the retained response bodies plus
  per-entry overhead fit, and the accounting is exact across insert, replacement,
  removal and invalidation. Previously `max_entries` alone allowed
  `max_entries × max_cacheable_response_bytes` of payload (defaults ≈ 39 GiB).

- The non-PG-wire gateways now reuse backend connections instead of dialing per
  request (H-06 first slice). The HTTP SQL, MCP and GraphQL gateways each own a small
  idle pool (`[limits] gateway_pool_max_idle`, default 16) of authenticated
  `BackendClient`s; a connection is returned only for session-neutral SQL and is
  discarded otherwise, so a request cannot inherit another request's transaction or
  session state. Pools are separate per gateway so a session policy (MCP read-only GUC)
  cannot leak across interfaces. `0` disables pooling (the previous behaviour). The
  per-interface contract and remaining parity gaps (rate limiter, circuit breaker,
  tenant identity) are documented in `docs/internal/H-06-interface-contract.md`.

- Fault-tolerant client admission and reconnect (H-05). `[limits]
  client_admission_wait_secs` (default 0 = immediate refusal, unchanged) makes the
  `max_client_connections` cap a bounded fair queue: a connection arriving at the cap
  waits up to the configured budget for a permit instead of being refused with 53300 at
  once. The primary-wait loops now use per-session full-jitter exponential backoff (base
  100 ms, 2 s cap) instead of a fixed 100 ms poll, so a fleet that lost the same primary
  does not reconnect in lockstep. Health/admin capacity is unaffected (health probes hold
  no client slot; admin is a separate listener).

- The daemon now honors `[load_balancer] read_strategy` on the read path (H-03).
  `select_read_node` used to hardcode round-robin while the configured strategy was set
  by every caller and read by none. `round_robin`, `weighted_round_robin`,
  `least_connections` (fewest attached sessions), `latency_based` (measured health-probe
  latency), `random` and the new `power_of_two` (sample two, prefer fewer attached
  sessions then lower latency) are all applied to the already-eligible standby set; the
  default configuration is unchanged. (The earlier note in this file that health policy
  was "re-verified" referred to the library checker; the daemon-side fix is below.)

- Daemon health policy is now real (H-03 follow-up). The health sweep applies
  `success_threshold` — an unhealthy node must pass N consecutive probes to return —
  and runs `[health] check_query` when optional `[health] user/password/database`
  credentials are configured; without them it keeps the credential-less SSLRequest
  probe. `success_threshold` previously did not exist in the daemon's health state.

- Real replica-lag measurement and strict freshness (H-04). With health credentials and
  `[lag_routing] enabled`, each sweep samples the primary's `pg_current_wal_lsn()` once
  and each standby's `pg_last_wal_replay_lsn()`, recording byte lag plus sample time per
  node (surfaced at `/nodes`). `[lag_routing] require_known_lag = true` excludes a
  standby whose lag was never measured instead of treating unknown as fresh.

- Shadow execution is actually concurrent and budget-bounded (O-03). The shadow side now
  runs in its own task/connection while the primary is awaited (the doc had claimed
  concurrency but the code awaited the primary first), the two full result sets are no
  longer cloned for comparison, and `POST /api/shadow` accepts `max_rows` (default 10000)
  / `max_bytes` (default 16 MiB): a result over either ceiling returns
  `budget_exceeded: true` and is not certified clean.

- `BackendConfig.query_timeout` is now honored on every management query (O-02).
  `BackendClient::run_query` used a hardcoded 30 s while the configured value was set by
  every caller and never read; the resolved timeout is captured at connect and a zero
  value keeps the 30 s default.

- Operator replay has an overall deadline and no longer stalls live journal writers
  (O-04). `TransactionJournal::entries_in_window` clones under the lock and sorts after
  releasing it; `[limits] replay_deadline_secs` (default 300, 0 = off) bounds the whole
  `POST /api/replay` run, which stops cleanly at the current statement and reports
  `deadline_exceeded` + partial progress.

- Extended-batch tracking is bounded and error frames allocate once (O-05).
  `batch_refs`/`batch_defines`/`batch_closes` now de-duplicate and cap at the configured
  `max_prepared_statements` (a never-Sync client can no longer grow them without bound,
  and the re-prepare filter stays linear), and `create_severity_response` builds the
  `ErrorResponse` frame directly instead of a `HashMap` plus four `String`s.

- Operator/failover replay no longer reports success without a backend (TR-07).
  `FailoverReplay::execute_statement` returns an explicit failure when no backend
  template/endpoint is configured, `wait_for_wal_sync` refuses a non-zero LSN it cannot
  verify, and `/api/replay` rejects a blank target. `ReplaySummary` now labels the run
  `mode: "time_window"` with `partial: true` on failures, and the docs state the journal's
  real retention (in-memory, bounded, nothing survives restart) and keep operator
  time-window replay explicitly distinct from committed-history replay.

- `/api/replay` refuses `"mode": "committed_history"` instead of silently running the
  weaker time-window replay (TR-07). The stronger mode does not exist yet, so the request
  gets `501` naming the requested and available modes; unknown modes get `400`. Every
  successful response now carries a `coverage` block — retained transactions / entries /
  bytes and the journal's global cap, plus explicit `false` flags for transaction
  boundaries, parameter values, per-statement outcomes and restart durability — so the
  summary cannot be read as a committed ledger. Retention documentation now states the
  exact journal coverage: simple-protocol statements only, recorded after the response is
  relayed and without inspecting it for a backend error (a rejected statement is
  journaled too), extended-protocol writes not journaled at all, SQL text only, one
  synthetic auto-commit transaction per statement.

- Query-cache invalidation is now commit-aware for simple-query writes (C-02, first
  slice). The tables a write touches inside an explicit transaction are staged per
  session and re-invalidated when that transaction COMMITs (discarded on
  ROLLBACK/ABORT), closing the window where a concurrent reader could refill an entry
  between the write's response and its commit. Extended-protocol writes, `COPY`, and
  DDL/unknown dependencies are not yet staged; invalidation for those remains
  statement-observation plus TTL.

### Security

- **rustls 0.23.45** (from 0.23.40): fixes RUSTSEC-2026-0285, TLS 1.3 handshake messages
  accepted at the wrong encryption level. Affects the client-facing TLS listener (`[tls]`)
  and every rustls client in the proxy (backend TLS, gateways' HTTPS clients). The yanked
  `chacha20 0.10.0` is refreshed to 0.10.2.

## [1.8.0] - 2026-09-12

### Changed

- **Transaction Replay now ships in the default build.** The `ha-tr` cargo feature
  becomes a deprecated no-op (kept so downstream feature mappings still resolve);
  the TR modules (transaction journal, replay engine, cursor restore, session
  migrate, upgrade orchestrator, shadow execution) are always compiled.
  `tr_enabled` is the runtime master switch: setting it to `false` stops write
  journaling, forces the effective `tr_mode` to `none` (so in-session recovery is
  disabled), and makes `POST /api/replay` return `503`. `POST /api/shadow` remains
  available because it is an explicit validation tool, not part of failover.

- Dependency security updates: `reqwest` 0.11 → 0.12 (rustls 0.23 stack), which
  removes rustls-webpki 0.101.7 and h2 0.3.27 from the graph; `wasmtime` 26 → 36.0.15,
  which clears all 17 wasmtime advisories; `lru` 0.12 → 0.18.4 (RUSTSEC-2026-0002,
  RUSTSEC-2026-0253); `anyhow` ≥ 1.0.103 and `crossbeam-epoch` ≥ 0.9.20. `socket2`
  now declares the `all` feature explicitly instead of relying on another
  dependency's feature unification.

- Removed the unused `prometheus` and `opentelemetry` optional dependencies (they
  were declared but referenced nowhere in `src/`, and `prometheus` pulled
  `protobuf` 2.28.0 with RUSTSEC-2024-0437). The `observability` feature name is
  kept as a documented no-op; `/metrics` and `/metrics/prometheus` were always
  served independently of it.

- `docs/transaction-replay.md` no longer claims Transaction Replay lives behind the
  `ha-tr` feature; the Feature Gating section now describes build and runtime gating.

### Fixed

- `GET /api/migration/status` no longer masks apply errors: `lag` is reported as
  accepted-but-not-applied without subtracting failed applies, and
  `migration_ready` now requires zero errors and zero drops. A mirror whose writes
  failed to apply can no longer report safe-to-cut-over (#44).

- Removed stray `</content>` / `</invoke>` tool-call tags from the end of four docs
  and corrected the stale `ha-tr` description in `Cargo.toml` (#55).

## [1.7.0] - 2026-09-11

Transaction Replay correctness is complete. Replay now verifies its own results,
refuses what it cannot reproduce, restores session state transactionally, and runs
the whole recovery under one deadline. Every P0 safety item in the Transaction
Replay group of the September 2026 audit is closed; what remains there is the
`ha-tr` journal and operator replay tooling (TR-07) and two availability
follow-ups (TR-08, TR-09).

### Added

- `tr_read_functions` (top level) — extra function names that an interrupted read
  may call and still be re-executed on a replacement backend.
- `[limits] tr_max_observation_bytes` (default 1 MiB) — per-statement budget for the
  response digest replay is verified against.
- `[limits] backend_response_timeout_secs` (default 0 = off) — whole-response
  deadline for a backend that drips bytes inside one response.
- `[limits] max_backend_frame_bytes` (default 100 MiB) — ceiling on any single
  backend protocol frame, applied on every streaming relay.

### Fixed

- In-session replay now verifies what it replays. While a transaction is recorded
  for `tr_mode = transaction`, each statement's response frames are hashed (up to
  the new `[limits] tr_max_observation_bytes`, default 1 MiB); the replacement
  backend's responses are hashed the same way during replay and any divergence
  rolls the replay back and returns `40001`, instead of continuing a transaction
  whose earlier results the client saw from a different snapshot. Transactions at
  `SERIALIZABLE`/`REPEATABLE READ` are marked non-replayable. One recovery deadline,
  `write_timeout_secs`, now bounds the whole failover — primary wait, connect/auth,
  session restore, replay and re-execution — where per-operation timeouts could
  previously add up beyond it (TR-06).

- New `[limits] backend_response_timeout_secs` (default 0 = off) bounds one backend
  response as a whole on every streaming relay. The per-read
  `backend_read_timeout_secs` re-arms on each read, so a backend dripping bytes
  inside a single response was never timed out (H-07 slow-drip).

- Session `SET` tracking for failover restore is now transactional, mirroring
  PostgreSQL: a `SET` inside a transaction is restored only if that transaction
  commits, `ROLLBACK TO SAVEPOINT` discards the `SET`s made after the savepoint,
  and `RESET name` / `RESET ALL` / `DISCARD ALL` inside a transaction are deferred
  until commit instead of taking effect immediately (a rolled-back `RESET ALL` no
  longer wipes the restore set). Variables are tracked by name, so a repeated `SET`
  replaces the earlier value rather than consuming another slot of
  `tr_max_session_set_statements`, and when that cap has been exceeded a failover
  is refused with `08006` rather than re-homing the session with incomplete state
  (TR-04).

- In-session Transaction Replay no longer treats every `SELECT` as re-executable.
  An interrupted read is re-run on the replacement backend only when every function
  it calls is a PostgreSQL built-in known to be side-effect-free (or is listed in the
  new top-level `tr_read_functions`); a read calling a user-defined function,
  `nextval`, `pg_notify`, `set_config`, an advisory lock or any unlisted name is
  classified opaque, returns `08007` on an unknown outcome and is never run twice.
  The supported subset is documented in `docs/configuration.md` (TR-03).

- Every backend streaming relay (query responses, cache capture, replay drain, pool
  reset, and the idle backend-watch) now validates each response frame header
  immediately against the new `[limits] max_backend_frame_bytes` (default 100 MiB,
  the existing frontend message cap). A declared length above it, or below the
  4-byte protocol minimum, closes the backend connection as malformed. Previously a
  length below four looped waiting for bytes that could never complete the frame,
  and an excessive length grew the accumulator toward the advertised size; the
  out-of-band re-prepare reader also allocated the advertised body size up front,
  which it now discards in bounded chunks (H-07).

## [1.6.1] - 2026-09-09

Patch release: Transaction Replay correctness. An independent audit of 1.6.0 showed the
released binary re-executing possibly-committed statements (4 of 7 real-PostgreSQL
commit-outcome probes) and appending re-executed rows to responses the client had
already partly received (12 of 13 streaming probes). This release closes those paths
and the further gaps found reviewing the fix; the probes now pass 7/7 and 13/13 and
ship as `tests/integration/tr_commit_outcome.py` and `scripts/regress/tr-boundary-test.py`.
No configuration changes. See `docs/internal/audit-2026-09/` for the audit and the
remaining backlog (replay eligibility policy, transactional session restoration,
cross-cycle statement identity).

### Fixed

- Extended-batch write failures and timeouts now preserve an unknown outcome,
  preventing retries based on the false assumption that no prefix frames executed.
  Connection failures before dispatch retain their safe retry behavior.

- Live transaction replay detects commented and batched commit boundaries and
  classifies every extended-protocol Execute using its bound statement. Unknown
  identities refuse replay; batches that already committed cannot remain in a
  replayable transaction history. An uncertain commit reports `08007` and asks
  clients to verify its outcome before retrying. See the [TR-01 implementation
  notes](docs/internal/audit-2026-09/TR-01.md) for supported cases and remaining work.

- Recovery no longer appends a second result to a response the client has already
  partly received. After published rows an interrupted statement returns one error
  instead of re-running; after a published `ErrorResponse`, or after a `Flush`
  fragment that may have ended mid-frame, the connection closes rather than
  injecting protocol bytes the client cannot place. Closing is deliberate: an
  `ErrorResponse` appended after a `CommandComplete` would report failure for a
  statement the backend actually finished, and a client that then retried an
  `INSERT` would double-apply it. See the [TR-02 implementation
  notes](docs/internal/audit-2026-09/TR-02.md).

- The idle backend-watch relay forwards only COMPLETE backend frames. A large
  asynchronous frame (`NotificationResponse` from `LISTEN`/`NOTIFY`, `NoticeResponse`,
  `ParameterStatus`) split across reads is now reassembled instead of being published
  truncated: a client holding half a frame blocks forever, and nothing can safely be
  written after it. A frame declaring a length below the 4-byte minimum is rejected,
  and reassembly is bounded by `[limits] max_pending_bytes`.

- Session `SET`s made inside a transaction that was rolled back are no longer restored
  after a failover. `ROLLBACK; <DML>` classifies as a possible commit for replay safety
  (the trailing statement commits in autocommit), but the transaction's own session
  state was discarded, so promoting its GUCs restored settings the database never kept.

- An uncertain autocommit write that finds no replacement primary now reports `08007`
  with the same "verify the database outcome" guidance as an uncertain `COMMIT`, rather
  than a bare `08006` connection failure a client would reasonably retry.

- The held unnamed `Parse` write is bounded by `[limits] backend_write_timeout_secs`;
  it was the one remaining unbounded backend write on the extended-protocol path.

### Changed

- Replay eligibility no longer depends on a statement being free of backslashes.
  A backslash inside a single-quoted literal is session-dependent (it escapes only
  when `standard_conforming_strings` is off, which the proxy does not track), and
  refusing every such literal silently stripped `select`/`transaction` replay from
  ordinary regexes, Windows paths and JSON escapes. The commit-boundary scan now runs
  under both readings and refuses only when they disagree — which is exactly the
  case where a backslash could hide a statement separator.

## [1.6.0] - 2026-09-04

Performance & stability batch (21 fixes) from the September 2026 audit, plus **Transaction
Replay made real**: `tr_mode` was previously accepted but never read on the data path — a
backend failure mid-query dropped the client socket with no error frame. It now drives
in-session failover (`none` / `session` / `select` / `transaction`, see the entry below), with
a live kill-the-primary regression (`scripts/regress/tr-failover-test.sh`, 77 checks).
Highlights: per-connection `Arc<ProxyConfig>` (no deep clone per accept), lazily-memoized
statement facts (one lexical pass per simple Query), zero-copy `Message::encode_into` on the
extended-protocol path, O(1) L1 hot-cache LRU, analytics ingest moved off the connection task
(bounded queue + fingerprint memo), single-lowercase anomaly scan, `[limits]`
`max_client_connections` / `client_idle_timeout_secs` / `tr_*` caps, `[cache]`
`max_cacheable_response_bytes`, health-probe in-flight guard, bounded failover history,
backend auth-frame length cap, pre-auth CancelRequest panic fix, admin `forward_sql_request`
timeouts + body cap, legacy pool gauge leak + idle-checkout validation, rate-limiter hit-path
mutual exclusion preserved without key clones, journal `begin_and_log` single-lock path.
Every change is `proxy.toml`-tunable where it introduces a threshold; defaults reproduce
prior behavior. Behavior changes to note: backend faults now yield a PostgreSQL
`ErrorResponse` (57P01 / 08007 / 08006) instead of a bare disconnect; primary selection
considers any healthy `role = "primary"` node (previously only the first listed);
`anomaly::QueryObservation` is now borrowed (`Cow`) — library-API change.

### Fixed

- **`FailoverController` history is now a bounded ring buffer, and entries are
  closed out by id.** `history` was an unbounded `Vec` pushed on every failover
  attempt and never trimmed, so repeated health flapping grew it without bound;
  closing out a completed/failed attempt used `.last_mut()`, which could stamp
  the wrong record when two failovers overlapped. History is now a `VecDeque`
  capped at `FailoverConfig::max_history` (default 100, FIFO-evicted past
  capacity), and completion looks the entry up by its `id: Uuid` instead of
  assuming it's last. `FailoverConfig` is library-only (`FailoverController`
  has no construction site in the daemon yet — `src/server.rs` runs its own
  independent failover path); `max_history` is a library-API knob, not a
  `proxy.toml` parameter. Library consumers note: `max_history` is a new public
  field on `FailoverConfig`, so an exhaustive `FailoverConfig { .. }` literal
  must either add it or fall back on `..Default::default()`.

### Changed

- **BREAKING (semver-major for library consumers)**: `anomaly::QueryObservation`
  is now `QueryObservation<'a>`, with `fingerprint` and `sql` changed from
  `String` to `Cow<'a, str>`, and `AnomalyDetector::record_query` now takes
  `&QueryObservation<'_>`. This lets the per-query hot path lend the detector
  a view into a buffer it already owns (the wire frame, a fingerprint scratch
  buffer) instead of copying every statement, at zero cost to callers that
  already own a `String`/`&str` (`.into()` still builds the `Cow` for them).
  Any out-of-tree code constructing `QueryObservation` with owned `String`
  fields needs a version bump / `.into()` to keep compiling.

### Added

- **`tr_mode` now delivers in-session transparent failover (F3).** The
  `tr_mode` setting (`none | session | select | transaction`) was parsed and
  shown on `/config` but never read on the data path: a backend that failed
  mid-query dropped the client socket with no `ErrorResponse`, and a backend
  that died while a session sat idle inside a transaction let the next
  statement run in autocommit on a fresh connection. Every mode is now real,
  for both the simple and the extended query protocol, with a pure decision
  table (`tr_decide`, exhaustively unit-tested over mode × fault phase ×
  transaction state × statement kind):
  - `none`: one `ErrorResponse` (SQLSTATE `57P01`, naming the failed node) +
    `ReadyForQuery`, then close.
  - `session` (default): the connection stays open; the proxy waits for a
    healthy primary (`write_timeout_secs`), reconnects, replays the session's
    tracked `SET`/`RESET` statements (named prepared statements are
    re-prepared lazily), transparently re-runs a not-delivered autocommit
    statement, and otherwise returns ONE error (`57P01` not delivered inside a
    transaction / `08007 transaction_resolution_unknown`) while aborting the
    client-visible transaction (`25P02` until the client issues `ROLLBACK`).
  - `select`: `session` + unknown-outcome reads are re-executed transparently;
    read-only explicit transactions are replayed from `BEGIN`.
  - `transaction`: `select` + uncommitted explicit transactions are replayed
    from `BEGIN` (recorded statement text / raw extended-protocol batches,
    responses discarded) and the in-flight statement re-executed inside them;
    a replay failure yields `40001`; a `COMMIT` with unknown outcome is never
    retried. Documented as opt-in because blind replay assumes deterministic
    statements.
  A `COPY` in progress at the fault returns `08006` and closes in every mode;
  a routing timeout (`NoHealthyNodes`) now also returns `08006` instead of a
  bare disconnect.
- `[limits] tr_max_replay_statements` (default 1000), `tr_max_replay_bytes`
  (default 4 MiB) and `tr_max_session_set_statements` (default 256) bound the
  per-transaction replay record and the per-session `SET` tracking; over a cap
  the transaction is marked non-replayable (`transaction` degrades to `session`
  behaviour for it) / tracking stops, with a counter each.
- Metrics (`/metrics`, `/metrics/prometheus`): `tr_failovers_total`,
  `tr_statements_reexecuted_total`, `tr_transactions_replayed_total`,
  `tr_replay_failures_total`, `tr_unknown_outcome_errors_total`,
  `tr_replay_cap_exceeded_total`, `tr_session_set_cap_exceeded_total`.
- **Backend SCRAM-SHA-256 / MD5 / cleartext client authentication for fresh
  backend connections.** `complete_backend_auth` (used by every redial: route
  switch, pooled-connection miss, in-session failover, and the first backend
  connection in `[auth] mode = "scram"`) previously only completed against
  trust backends — a challenging backend timed out after 5 s. It now answers
  SASL/MD5/cleartext challenges with the user's plaintext `auth_file` entry
  when the proxy is the auth boundary (`[auth] mode = "scram"`), and fails fast
  with a clear `ProxyError::Auth` when it holds no credential (pass-through
  mode). In scram mode the backend's auth frames are no longer relayed to the
  already-authenticated client (a synthesized `AuthenticationOk` precedes the
  backend's ParameterStatus/BackendKeyData/ReadyForQuery), so scram mode no
  longer requires a trust backend.
- `scripts/regress/tr-failover-test.sh` + `scripts/regress/tcp-relay.py`: live
  battery that kills a TCP relay in front of the backend mid-transaction and
  checks every `tr_mode` (no second PostgreSQL required).

### Fixed

- `select_primary_with_timeout` only ever inspected the *first* enabled
  `role = "primary"` node, so a configuration listing a second primary could
  never fail over to it once the first was demoted in-band; it now picks any
  healthy enabled primary in config order (matching `select_node_for_startup`).

- **Criterion coverage for the relay / failover / pool-contention paths**
  (dev tooling only; no shipped runtime change). Closes the "known gap" recorded
  in `benches/BASELINE.md` for the in-process HA and mode-aware pooling hot paths
  that never touch a live backend:
  - `benches/pooling.rs` gains `pool/acquire_release/contention` — the skeleton
    `ConnectionPool` acquire/return cycle under K concurrent tasks on the
    multi-thread runtime (real `RwLock<HashMap>` + per-node `Semaphore`
    contention), extending the existing single-threaded acquire bench.
  - `benches/protocol.rs` gains the connection-setup and relay framing not
    covered by the Query-frame bench: `decode_startup` (startup params /
    SSLRequest / CancelRequest), the extended-query `Parse`/`Bind` parsers, the
    backend-response `ErrorResponse` / `CommandComplete`+`rows_affected` /
    `AuthRequest` (SASL, MD5) parsers, and per-frame tag dispatch
    (`MessageType::from_tag`, `starts_with_ci`, `contains_ci`).
  - New `benches/relay.rs` (self-gated, compiles under every feature set): the
    `switchover_buffer` enqueue (`buffer_query`) and no-op replay-drain (`drain`)
    paths; under `ha-tr`, the `transaction_journal` classifier
    (`StatementType::from_sql`), size walk (`total_size`), in-memory mutation
    (`add_entry` / `rollback_to_savepoint`), the async begin+log+commit
    journaling lifecycle (single-threaded and under write-lock contention), and
    the time-travel `entries_in_window` scan; under `pool-modes`, the
    allocation-free classifiers (`TransactionEvent::detect`, `pool_key`,
    statement-mode safety, PREPARE/DEALLOCATE parsers) and the
    `ConnectionPoolManager` acquire/release cycle under client concurrency plus
    the per-statement `on_statement_complete` decision.

### Changed

- **L1 hot cache now uses `lru::LruCache` for O(1) get/put/eviction**,
  replacing the previous `HashMap` + `Vec<(String, Instant)>` pair whose LRU
  bookkeeping did an O(n) scan-and-allocate on every hit and put. This trades
  away the read-lock-only hit path added in 0.3.1 (`L1 hot cache: hits take
  only a read lock`, above): `get()` now takes a brief write lock to promote
  the entry to most-recently-used, so concurrent hits on the *same* cached
  query serialize briefly on that lock instead of running fully in parallel
  under a shared read lock. Puts on a full cache no longer prefer evicting an
  already-expired entry over a live LRU one; expired entries are still
  purged, just lazily — on the `get()` that finds them, or by the periodic
  `evict_expired()` sweep — rather than being scanned for preferentially on
  every insert. Public API and hit/access-count metrics are unchanged.
  `test_concurrent_hits_read_lock_only` is renamed
  `test_concurrent_hits_no_lost_updates` to describe what it actually
  verifies now.

## [1.5.0] - 2026-07-11

### Added

- **`[limits]` config section** — eleven operational safety bounds (ten
  formerly-named consts plus the previously-inline 30s backend read timeout)
  that were hardcoded in `src/server.rs` are now tunable via `proxy.toml`. Each
  default reproduces the prior constant exactly, resolved once at startup, so a
  config without a `[limits]` block is byte-for-byte unchanged. Keys (default):
  `max_cancel_keys` (100000), `startup_timeout_secs` (30),
  `backend_write_timeout_secs` (30), `backend_read_timeout_secs` (30),
  `client_write_timeout_secs` (60), `reprepare_timeout_secs` (15),
  `max_prepared_statements` (8192), `max_prepared_bytes` (67108864 / 64 MiB),
  `max_pending_bytes` (67108864 / 64 MiB),
  `max_total_idle_backend_conns` (8192, pool-modes), and
  `pool_reap_interval_secs` (30). `validate()` rejects a `0` for any of these
  (a safety bound, not "unbounded") with a key-named error, and caps every
  `*_secs` timeout at one year (`31536000`) — a value above that would overflow
  the connect-time `Instant + Duration` deadline and panic the per-connection
  task, so it is refused up front.
- **`[anomaly]` config section** — the in-process anomaly detector previously
  ran on a hardcoded `AnomalyConfig::default()` plus a `MAX_SEEN_FINGERPRINTS`
  module const with no way to tune it. Its eight tunables are now exposed via
  `proxy.toml`, defaults reproducing the prior behavior exactly: `rate_window_secs`
  (60), `spike_z_threshold` (3.0), `auth_window_secs` (60), `auth_critical_count`
  (10), `auth_warning_count` (5), `event_buffer_size` (1024), `emit_novel_queries`
  (true), and `max_seen_fingerprints` (100000). `validate()` rejects degenerate
  values (windows/buffer/fingerprint-cap `> 0`, `spike_z_threshold` finite and
  `> 0`, `auth_critical_count >= 1`, `auth_warning_count <= auth_critical_count`).
  The detector is built once at startup, so changing `[anomaly]` requires a
  restart (a SIGHUP reload does not rebuild it).
- **`/admin/kv/<plugin>/<key>` admin endpoints** — the per-plugin KV store
  (`KvBackend`, read by plugins through their `kv_get`/`kv_set` host imports)
  can now be read, written, listed, and deleted from outside the WASM sandbox,
  so operators can push a plugin's runtime config (budgets, region maps, mask
  rules, allowlists) without a restart. `GET /admin/kv/<plugin>/<key>` returns
  `{"plugin","key","value"}` (404 if absent), `GET /admin/kv/<plugin>/`
  (trailing slash) lists the namespace as `{"plugin","keys":[...]}`,
  `PUT` sets a value (UTF-8 body via `from_utf8_lossy`), and `DELETE` removes one
  (idempotent 200). A trailing-slash list accepts an optional `?prefix=` filter;
  any query string is stripped before the plugin/key split, so `?…` never leaks
  into a stored key, and an empty `<plugin>` segment (`/admin/kv//<key>`) is
  rejected `400`. All four sit behind the normal admin bearer gate; the build
  returns `501` without `--features wasm-plugins` and `503` when no plugin
  manager is attached. Four `[plugins]` caps bound writes and are tunable
  (`0` = unlimited): `kv_max_value_bytes` (default 65536, now bounds a single
  key OR value), `kv_max_keys_per_plugin` (default 1024), `kv_max_plugins`
  (default 256, bounds how many `<plugin>` namespaces can exist so a token-holder
  cannot exhaust memory by writing to unboundedly-many namespace names), and
  `kv_max_total_bytes` (default 67108864 / 64 MiB) — a total-footprint backstop
  that sums each entry's key + value bytes plus each live namespace's name bytes
  and keeps the whole store within a survivable ceiling regardless of the
  per-axis product (which could otherwise retain tens of GiB). A PUT
  past a cap returns `413` (and the in-WASM `kv_set` returns `-1`); an oversized
  body is rejected before it is copied. Overwriting an existing key never trips
  the key-count cap, writing to an existing namespace never trips the namespace
  cap, and deleting a namespace's last key frees its slot (the reclaimed bytes
  are subtracted from the total-footprint counter too).
  Keys must not contain `?`: a query string is stripped before the plugin/key
  split (so `?prefix=` can filter a listing), which means a plugin-created key
  containing `?` is listable but not addressable via GET/DELETE over the admin
  surface.
- **`benches/protocol.rs`** — a Criterion benchmark covering the PG-wire
  per-query hot path that every client frame and backend response flows
  through, previously uncovered by the pool/routing benches (so a regression
  there was invisible to quality gate 3). Three groups —
  `protocol/decode_message`, `protocol/encode_message`, and
  `protocol/query_text` — each run over three payload sizes (a trivial
  `SELECT 1`, a ~60-char `WHERE` query, and a deterministically-built ~1 KiB
  `IN (...)` statement) with `Throughput::Bytes` so a regression surfaces as
  both a per-call delta and a bytes/sec change. Feature-free: it exercises only
  the always-public `protocol` API, so it compiles under every feature set.

### Changed

- **Plugin `kv_set` is now bounded (was unbounded in 1.4.0).** The per-plugin KV
  store went from an infallible unbounded write to a capped one. The in-WASM
  `kv_set` import keeps its `i32` ABI — `0` on success, `-1` when the write is
  refused (a cap breach now joins the internal-error case in returning `-1`);
  the internal store `set()` method that backs it changed from returning `()` to
  returning a `bool` (`false` = refused). The new `[plugins]` caps default to
  `kv_max_value_bytes` 65536,
  `kv_max_keys_per_plugin` 1024, `kv_max_plugins` 256, and `kv_max_total_bytes`
  67108864 (64 MiB). **Upgrade impact:** a plugin deployed under 1.4.0 that
  stored values larger than 64 KiB, or more than 1024 keys in its namespace,
  will silently start receiving `-1` from `kv_set` after upgrade — writes past a
  cap fail instead of succeeding. Setting any `kv_*` cap to `0` disables that
  cap and restores the unbounded 1.4.0 behavior for that axis (set all four to
  `0` for byte-for-byte 1.4.0 semantics).
- **CI now lints test code (`cargo clippy --tests`)** — both clippy invocations
  in `.github/workflows/ci.yml` gained `--tests`, so `#[cfg(test)]` modules and
  the `tests/` integration crate are held to the same `-D warnings` bar as the
  library. Pre-existing `clippy::field_reassign_with_default` warnings in test
  code (converted `let mut x = T::default(); x.field = …;` sequences into
  struct-literal `T { field: …, ..Default::default() }` initializers, behavior
  unchanged) were cleared so the gate starts green.
- **Rewrote `docs/transaction-replay.md` and `docs/topology-providers.md` against
  the current code.** Both documents were conceptually dated. The TR deep dive is now
  grounded in `src/transaction_journal.rs`, `src/failover_replay.rs`,
  `src/failover_controller.rs`, `src/switchover_buffer.rs`, `src/replay/mod.rs`, and the
  `tr_enabled` / `tr_mode` / `write_timeout_secs` keys — correcting invented keys
  (`tr_max_journal_bytes`, `switchover_drain_timeout_secs` never existed), the
  `write_timeout_secs` default (30, not 15), the default `tr_mode` (`session`), the
  text-format (not binary) replay parameter path, and the fact that session-state/cursor
  migration exists only as unwired library modules (`src/cursor_restore.rs`,
  `src/session_migrate.rs`) not reachable from the replay path, while
  `FailoverController`/`PrimaryTracker` are library components rather than
  daemon-wired. The topology doc now separates the daemon's
  static-role-plus-health primary tracking (surfaced at `/topology`) from the
  `TopologyProvider` library abstraction, and notes the PostgreSQL provider is constructed
  programmatically (not from `[[nodes]]`). Each doc carries a "last verified against"
  commit line.

### Fixed

- **`/healthz`, `/livez`, `/readyz` admin routes** — these three
  Kubernetes-style probe paths were already token-exempt in the admin auth gate
  but had no handler, so they fell through to the catch-all and returned `404`.
  They now route to the same handlers as their slash-form twins (`/healthz` →
  `/health`, `/livez` → `/health/live`, `/readyz` → `/health/ready`), returning
  byte-for-byte identical responses. Because they are token-exempt, orchestrators
  can use `/livez` and `/readyz` for unauthenticated liveness/readiness probes
  even when `admin_token` is set (the slash-form `/health/live` and
  `/health/ready` remain token-gated, unchanged).
- **Embedded admin dashboard usable with `admin_token` set** — v1.4.0 made
  token-gating the recommended posture, but the embedded web UI
  (`src/admin_ui.html`, served at `/` and `/ui`) sent no `Authorization`
  header on any of its `fetch()` calls, so with a token set every panel
  `401`ed — the secure configuration broke the dashboard. The UI now wraps
  `window.fetch` once to inject `Authorization: Bearer <token>` (from the tab's
  `sessionStorage`, key `helios_admin_token`) into every request; on a `401`
  it prompts once per page load for the token and reloads. A **token** button
  in the header bar clears the saved token so a wrong one can be re-entered.
  The static shell (`GET /`, `/ui`) is now token-exempt so the page can load
  and prompt — it carries no privileged data, and every API call it makes is
  still individually gated. Without a token, behavior is unchanged (no prompt).

### Security

- **Stored XSS in the embedded admin dashboard (present in 1.4.0 — prioritize
  this upgrade).** The admin web UI (`src/admin_ui.html`, served at `/` and
  `/ui`) interpolated backend- and attacker-derived strings into `innerHTML`
  with only a partial `<`-escape. A crafted SQL query whose text flowed into an
  anomaly `fingerprint` (or `sql_excerpt`) — surfaced on the `/anomalies` panel
  — could therefore inject script that runs in an authenticated operator's
  dashboard, an admin-API-takeover vector. The same gap affected the other
  anomaly fields (`tenant`, `user`, `client_ip`, each `patterns_matched`
  entry) and the node (`address`), plugin (`name`/`version`/`hooks`/`state`/
  error), edge (`edge_id`/`region`/error), and topology (`currentPrimary`)
  strings rendered into `innerHTML`. Every backend/attacker-derived string
  interpolated into an `innerHTML` template now passes through a single `esc()`
  HTML-escape helper; numeric/boolean/hardcoded values and `textContent` sinks
  were already safe and are unchanged. This path shipped live in the published
  1.4.0, so 1.4.0 operators should upgrade.

## [1.4.0] - 2026-07-09

Minor release — data-path performance, relay robustness, security hardening
across the admin API and the HTTP/MCP/GraphQL gateways, an edge/geo result-cache
mode, and config-loader correctness. Covers the 2026-07 performance & stability
program (PRs #26–#37) plus a security/stability follow-up sweep.

### ⚠️ Breaking / behavior changes — read before upgrading

- **The admin API now binds loopback by default.** The `--admin` CLI default and
  the `admin_address` default changed from `0.0.0.0:9090` to `127.0.0.1:9090`,
  and binding the admin API to a **non-loopback** address without an
  `admin_token` is now **refused at startup**. To keep an off-loopback admin
  bind, set `admin_token` (recommended) or `admin_allow_insecure = true`.
- **`[health] check_interval_secs = 0` is now rejected at startup.** Previously
  it started and silently disabled the health-check task; set a positive value.
- **Config files now undergo `${VAR}` / `${VAR:-default}` substitution at load.**
  A `${VAR}` with no default and an unset environment variable now fails fast
  (naming the variable) instead of being passed through literally.

### Added

- **Edge / geo result-cache mode** (`edge-proxy` feature, `[edge]` config section):
  a two-region result cache with a home-authoritative clock, SSE invalidation
  push, and a PG-wire data plane. Admin surface: `GET /api/edge/subscribe` (SSE
  stream), `POST /api/edge/register`, `POST /api/edge/invalidate`.
- **`[pool_mode] skip_clean_reset`** — skip `DISCARD ALL` for provably
  session-neutral pooled connections (opt-in; default off).
- **`mcp.auth_token`** — bearer authentication for the MCP agent gateway, which
  was previously open by default.
- **`admin_allow_insecure`** — opt in to an anonymous non-loopback admin bind
  (see the breaking-changes note above).
- **Environment-variable substitution in config files** — `${NAME}` and
  `${NAME:-default}` are expanded by `ProxyConfig::from_file` (env lookup only,
  no shell). Unrecognized top-level TOML keys/sections are now logged at startup
  (`tracing::warn!`) instead of being silently ignored.

### Changed

- **LISTEN/NOTIFY notifications are delivered to idle sessions immediately**
  (previously deferred to the session's next query); idle-relay `Flush`
  round-trip latency dropped from ~202 ms to ~0.8 ms.
- Pooled-connection reset is now conditional for clean connections (see
  `skip_clean_reset`).

### Fixed

- **MCP `read_only` guardrail bypass.** The gateway inspected only a statement's
  leading verb, so a multi-statement batch (`SELECT 1; DROP TABLE t`) or a
  data-modifying CTE (`WITH x AS (INSERT … RETURNING *) SELECT …`) slipped past
  it — and multi-statement input also defeated agent-contract allow-lists. It is
  now closed by a comment/quote/dollar-quote-aware lexical guard (reject
  multi-statement, scan for data-modifying CTEs) **and** a backend
  `default_transaction_read_only` backstop on the fresh per-call connection.
  (Also listed under Security.)
- **Underflow panics on the live query path.** `Instant − Duration` panics when a
  configured window exceeds process uptime (the monotonic clock starts near zero
  at boot); ten sites across the circuit breaker (`sliding_counter`, `adaptive`)
  and analytics (`patterns`), plus a wall-clock age in the distribcache heatmap,
  now use `checked_sub` / `saturating_sub`.
- **Auth relay** slow-client deadlock and `ErrorResponse` blindness (the auth
  relay is now event-driven).
- **Connection pool** COPY-hang, poisoned-park, and pool-key identity leakage.
- **Session leaks** — RAII session guard and a DashMap-backed session table;
  the health-check loop is now supervised.
- **`ha-tr` transaction journal** is bounded (50 000 journals, oldest-first
  eviction at capacity) — previously an unbounded leak; note the replay-window
  implication for `/api/replay`. Per-session L1 caches are reclaimed.
- **The shipped example configs now load.** `${VAR:-default}` was documented but
  unimplemented and several files were invalid TOML; the loader now substitutes,
  and a CI test loads every `config/*.toml` through the real `from_file` path.

### Security

- **Gateway request hardening (HTTP SQL / MCP / GraphQL):** an 8 MiB body cap
  (rejected with 413 before allocation), a 15 s request deadline, 100-header /
  64 KiB header caps, constant-time bearer comparison, and the new
  `mcp.auth_token`.
- **Admin-API exposure hardening:** loopback-by-default, refusal of anonymous
  non-loopback binds, a `MAX_ADMIN_CONNS = 256` connection cap, a 15 s admin
  read timeout, an 8 MiB admin body cap, and header caps.
- **Pre-auth protocol crash guards:** undersized startup / message frames
  (`len < 8` / `len < 4`) that previously panicked the pre-auth handler are
  rejected; a 30 s pre-auth startup timeout bounds the handshake; per-session
  resource caps bound the prepared-statement registry (count and retained
  bytes), the un-flushed extended-protocol buffer, and a single inbound message.

### Performance

- **Hot-path per-query allocation elimination** — the relay previously allocated
  two fresh 16 KiB buffers per query (~1 GB/s of churn at 64 clients); buffers
  are now reused.
- **Conditional reset** — transaction-pooling throughput +48 % at 16 clients and
  +31 % at 64 clients versus always-reset (`pgbench -S`, evidence host).
- **Idle relay** — `Flush` stall reduced from ~202 ms to ~0.8 ms.

## [1.3.1] - 2026-06-28

Patch release — feature-gating fix.

### Fixed

- **`query-analytics` now compiles standalone.** `parse_limit_query` (in
  `src/admin.rs`) was gated `#[cfg(feature = "anomaly-detection")]`, but the
  `#[cfg(feature = "query-analytics")]` `/api/analytics` handler also calls it.
  Because the two features are declared independently, building with
  `--features query-analytics` alone failed to compile. The helper (and its unit
  test) are now gated `any(feature = "anomaly-detection", feature =
  "query-analytics")`, so it is available whenever either consumer is compiled —
  without coupling the two features.

## [1.3.0] - 2026-06-27

Minor release — reliability hardening across the data path, the pool, and
observability.

### Added

- **In-band failure detection.** A query that fails against a backend now
  demotes that node's health *immediately* — both on the forward path and at
  connection establishment — instead of waiting up to a full health-check
  interval. The demotion is narrowly scoped: a client-side error, or a merely
  slow-but-healthy query (a backend read-timeout, indistinguishable from a large
  sort / lock wait / bulk DML), never takes a healthy backend out of rotation;
  only genuine backend faults do. Concurrent health writers are serialized so a
  demotion can never be lost to a racing update. Live-verified: the node is
  marked unhealthy within ~1 query, far ahead of the periodic checker.
- **Protocol-level health probe.** The health check now performs a PostgreSQL
  `SSLRequest` handshake (auth-free, un-logged) instead of a bare TCP connect,
  so a wedged-but-connectable backend (stuck postmaster, out of slots) is
  detected.
- **`/api/circuit` admin endpoint** reporting live per-node circuit-breaker
  state (closed / open / half-open).
- **Idle-connection reaper + global ceiling** for the data-path backend pool:
  parked connections older than the idle timeout are reaped, and a hard global
  cap (enforced via an atomic reservation, so concurrent check-ins cannot
  overshoot) bounds total file descriptors across all `(node,user,db)`
  identities. An `idle_timeout_secs` of `0` disables the TTL reaper (PgBouncer
  convention) rather than reaping every connection each cycle.

### Changed

- **Timeout coverage** added to backend forward writes, client streaming
  writes, and the prepared-statement re-prepare exchange — a hung backend or
  wedged client can no longer pin a session indefinitely (backend reads were
  already bounded).
- **Per-session resource caps** bound `stmt_registry` (prepared statements, by
  both count *and* aggregate retained bytes), the un-flushed extended-protocol
  `pending` buffer, and a single inbound message — a misbehaving client can no
  longer grow proxy memory without limit.
- **Cancel-key map** now FIFO-evicts its oldest entries at capacity instead of
  clearing the whole map, so a busy proxy no longer loses every in-flight
  cancel registration at once.

### Notes

- New live test `scripts/regress/reliability-test.sh` (5/5 vs PostgreSQL 18.4):
  `/api/circuit` reporting, in-band demotion on backend death, fast clean-error
  (no hang), and recovery after the backend returns.
- Transparent *mid-query* failover remains out of scope for a streaming proxy
  (a partially-streamed response cannot be retried); the proxy surfaces a clean
  error and reroutes the next query. Single-primary-without-standby still
  requires an external orchestrator.

## [1.2.0] - 2026-06-26

Minor release — real LDAP authentication.

### Added

- **LDAP authentication (`ldap-auth` feature).** A new optional feature (off by
  default; implies `auth-proxy`) implements the standard search-then-bind flow:
  service-bind + search to resolve the user DN, then bind as that DN with the
  supplied password to verify it. Group memberships become identity groups;
  StartTLS / ldaps are supported. Previously `authenticate_ldap` denied by
  default. Without the feature it still denies — never a fabricated success.

### Notes

- Live-verified against a real OpenLDAP container
  (`scripts/regress/ldap-test.sh`, local-only): correct credentials
  authenticate, wrong password and unknown user are denied.
- All implemented auth methods are now genuinely real: JWT (HS256), API key,
  OAuth RFC 7662 introspection, and **LDAP**.

## [1.1.0] - 2026-06-26

Minor release — `pool-modes` now does real work on the data path.

### Added

- **Transaction / Statement connection pooling (`pool-modes`).** Previously the
  pool manager was never exercised on the query path — every client stayed 1:1
  session-pinned regardless of the configured mode. A new raw-stream
  `BackendIdlePool`, keyed by `(node, user, database)` identity, now backs the
  data path: `ensure_conn` leases a parked, identity-matched connection before
  dialing fresh; at each idle transaction boundary the connection is
  `DISCARD ALL`-reset and parked for reuse; on disconnect idle connections are
  retained (not dropped) for cross-session reuse. Session mode and feature-off
  builds are unchanged.

### Notes

- Live-verified vs PostgreSQL 18.4 (`scripts/regress/pool-modes-test.sh`):
  connection reuse fires on every post-first query, the `DISCARD ALL` reset is
  proven by a temp table vanishing between pooled statements, results are
  correct, and connections are parked on disconnect.
- Bounded scope (reported, not faded): connection reuse + a shared identity-keyed
  idle pool with correct reset are delivered. The first connection of each
  session still authenticates through the startup path, so concurrent backend
  connections are not yet reduced below the concurrent-client count; true N:M
  startup-level multiplexing needs proxy-terminated backend auth and is the
  documented next increment.

## [1.0.0] - 2026-06-26

First stable release. Every feature flag now ships genuinely-working, tested
functionality — no scaffolding, no stubs that fake success. Where a capability
is intentionally bounded, it is **documented here**, not hidden.

This release is the culmination of the 0.7.0–0.19.0 wiring series: each proxy
feature was taken from "compiles but inert" to wired onto the real data path (or,
for standalone subsystems, to genuinely functional internals), with a unit test
plus — wherever a backend was reachable — a live verification against
PostgreSQL 18.4.

### What is real and verified

- **Routing & resilience** — routing hints (`/*+ route= */`), replica lag-aware
  routing + read-your-writes, rate limiting, circuit breaker, schema-aware
  (OLAP) routing. All on the per-query path; live-verified via the proxy log.
- **Caching** — query cache (L1/L2/L3) on the path; DistribCache subsystem with
  real LZ4 (`lz4_flex`) + zstd compression and a working L3 peer server
  (replication/remote-reads over TCP).
- **Query handling** — query rewriting (rules engine on the path), query
  analytics / slow-query / N+1 detection, GraphQL-to-SQL gateway (real engine
  execution + HTTP listener).
- **Multi-tenancy** — tenant isolation + per-tenant query transformation on the
  path.
- **HA / Transaction Replay (`ha-tr`)** — failover replay, cursor restore,
  session migration, and the zero-downtime major-version upgrade orchestrator
  (real logical-replication setup, row-count parity validation, `pg_promote`
  cutover, artefact cleanup).
- **Auth (`auth-proxy`)** — JWT (HS256, constant-time verify), API keys
  (SHA-256, constant-time compare), and OAuth RFC 7662 token introspection.
- **Plugins, anomaly detection, edge proxy, observability** — as documented in
  their modules.

### Documented limitations (reported, not faded)

- **LDAP auth** is deny-by-default: `authenticate_ldap` returns an error rather
  than faking acceptance. A real bind needs an `ldap3` client and a live
  directory.
- **DistribCache** and the **INSERT batcher** are genuinely functional but are
  standalone subsystems, not yet mounted on the proxy's per-query data path
  (the `query-cache` feature is the data-path cache).
- **Pool modes** (Session/Transaction/Statement) and the **auth connection
  path** have working components; full data-path integration is staged for a
  later minor.
- The GraphQL generator keys the FROM table off the (pluralized) query field
  name and assumes an `id` primary key; nested-relationship shaping and
  mutations are follow-ons.
- `/api/pools` reports live active/idle/total per node; per-node
  pending/created/closed counters are not yet tracked.

### Engineering

- CI gate clean: `cargo clippy -D warnings` on `""`, `ha-tr`, `all-features`,
  `all-features,postgres-topology` (and `--no-default-features`). 1391
  all-features lib tests pass.

## [0.19.0] - 2026-06-26

Minor release — OAuth token introspection is now real.

### Added

- **OAuth RFC 7662 introspection (`auth-proxy`).** `authenticate_oauth`
  previously denied every request as "not implemented". It now POSTs the bearer
  token to the configured introspection endpoint (HTTP Basic client auth),
  requires an explicit `"active": true`, enforces optional issuer / audience /
  required-scope checks, and mints an `Identity` (subject + scopes→roles +
  expiry). Results are cached. A token is never trusted without the round-trip.
- `reqwest` now enables `rustls-tls`, so HTTPS identity providers work without
  OpenSSL (also benefits the L3 semantic cache).

### Notes

- Implemented + real auth methods: **JWT (HS256)**, **API key** (SHA-256 +
  constant-time compare), **OAuth introspection**. **LDAP** remains
  deny-by-default — `authenticate_ldap` returns an error rather than faking
  acceptance; a real bind needs an `ldap3` client + a live directory.

## [0.18.0] - 2026-06-26

Minor release — three orphan stubs made real.

### Changed

- **INSERT batcher executes for real (`batch.rs`).** The flush built the
  combined bulk INSERT, discarded it, and reported a fabricated success to every
  waiter. It now executes the combined statement against an attached backend
  (`with_backend`) and reports the true outcome — an honest failure (no backend
  / connect / SQL error) instead of a silent success. Live-verified: batched
  rows actually land in PostgreSQL.
- **`PreferLocal` routing is real (`load_balancer.rs`).** Was "return the first
  node". Now prefers the least-loaded node co-located with the proxy (loopback /
  localhost), falling back to least-connections overall when none is local.
- **`/api/pools` returns real pool stats (`admin.rs`).** `get_pool_stats`
  returned an empty list; `AdminState` now holds the `ConnectionPoolManager`
  (wired at startup) and reports real per-node active/idle/total connections.

### Notes

- The `InsertBatcher` is a standalone utility (not yet on the proxy's per-query
  path); these changes make its execution real and honest. Per-node
  pending/created/closed pool counters aren't tracked separately, so those
  `/api/pools` fields are 0 while active/idle/total are live.

## [0.17.0] - 2026-06-26

Minor release — fourth of the 1.0.0 wiring set: the upgrade orchestrator's
validation stage is now real.

### Changed

- **Upgrade-orchestrator stage 3 validation (`ha-tr`).** The validation stage
  advertised a source≡target sample comparison but only slept and advanced. It
  now lists a deterministic, bounded sample of the source's user tables and
  compares `count(*)` on each between source and target over the backend
  client; a mismatch fails the job, and the rows checked are recorded in
  `validated_rows`. Stages 1/2/4/6 were already real. The stale module
  doc-comment (which called every stage a stub) is corrected.

### Notes

- Row-count parity is the portable PG 14–18 check; a per-row checksum on top is
  a follow-on. The validation SQL path is live-verified via self-compare
  (`HELIOS_LIVE_PG`-gated test); a full two-major-version replication run needs
  a two-node harness.

## [0.16.0] - 2026-06-26

Minor release — third of the 1.0.0 wiring set: the DistribCache internals are
now real.

### Added

- **L3 distributed-cache peer server (`distribcache`).** `DistributedCache`
  now serves the GET/PUT/PING/INVALIDATE wire protocol it already spoke as a
  client (`serve` / `serve_on`). Replication PUTs and remote GETs from peers
  previously reached nothing — the mesh can now actually replicate and serve
  cross-node reads.

### Changed

- **L2 LZ4 compression is real.** The `Lz4` tier compressor (the default) was a
  no-op that copied bytes behind a marker; it now uses `lz4_flex` block
  compression. Round-trip + measurable shrink are covered by tests.

### Notes

- These make the DistribCache subsystem genuinely functional. It is not yet
  mounted on the proxy's per-query data path — the `query-cache` feature is the
  data-path cache; wiring DistribCache in as an optional proxy backend is a
  follow-on.

## [0.15.0] - 2026-06-26

Minor release — second of the 1.0.0 wiring set: the GraphQL gateway is now real.

### Added

- **GraphQL-to-SQL gateway (`graphql-gateway`).** `[graphql_gateway]` config
  (off by default; `listen_address`, `backend_*`, `tables`). A new HTTP listener
  accepts `POST {"query": "<graphql>"}`, generates SQL from the configured
  schema, executes it over the backend PG-wire client, and returns a real
  GraphQL response. The engine previously returned `null`/empty for every field
  and had no listener at all.

### Notes

- Real SQL execution + flat top-level response shaping is delivered and
  live-verified. Nested-relationship shaping (joins), mutations, and the SQL
  generator's table-name derivation + `id` primary-key assumption are follow-ons.

## [0.14.0] - 2026-06-26

Minor release — first of the 1.0.0 wiring set: schema/workload-aware routing.

### Added

- **Schema/workload-aware routing (`schema-routing`).** `[schema_routing]`
  config (off by default): a read classified as analytical (OLAP — aggregations,
  GROUP BY, window functions) is routed to the configured `analytics_node`,
  offloading analytics from the primary. Other queries use default routing.
  Decisions log to the `helios::schema` target.

### Notes

- This wires the real (query-shape) workload classification. Table-level schema
  discovery (data-temperature routing from `pg_catalog`/`information_schema`
  introspection) needs a configured introspection credential and is a follow-on.

## [0.13.0] - 2026-06-26

Minor release — platform-tier wiring wave 6: Transaction Replay journaling.

### Added

- **Transaction Replay journaling (`ha-tr`).** With `tr_enabled`, write
  statements are now journaled on the query path (they previously weren't, so
  the replay engine had nothing to replay). The existing replay machinery —
  `POST /api/replay` and the failover coordinator — can now re-apply journaled
  writes onto a promoted primary or a staging target.

### Notes

- The journal-population half and the replay **mechanism** are live-verified
  (writes journaled → table emptied → `/api/replay` re-applies them → rows
  reappear). The failover-**triggered** auto-replay is coordinated by the
  failover controller but is not live-verified here (no real standby in the dev
  env). Follow-ons: extended-protocol write journaling, explicit-transaction
  grouping, journal retention, array bind params in replay, cursor reposition.

## [0.12.0] - 2026-06-26

Minor release — platform-tier wiring wave 5: multi-tenancy.

### Added

- **Multi-tenancy row isolation (`multi-tenancy`).** `[multi_tenancy]` config
  (off by default): `identify_by` (a startup parameter such as
  `application_name`/`user`, or `database`) selects the tenant; queries against
  the configured `tenant_tables` get a `WHERE <tenant_column> = '<tenant>'`
  filter injected so each tenant sees only its own rows. The filter is applied
  before the cache lookup, so cached results are tenant-scoped (no cross-tenant
  leakage). Injection logs to the `helios::tenant` target.

## [0.11.0] - 2026-06-26

**Security release** — the auth subsystem (`auth-proxy`) no longer accepts
forged or arbitrary credentials.

### Security

- **JWT signatures are now actually verified.** `verify_signature` previously
  returned success for *every* token. It now performs a constant-time HS256
  (HMAC-SHA256) verification against a symmetric key, and **rejects any other
  algorithm** (RS256/ES256 etc.) rather than trusting it — so a forged or
  tampered JWT no longer validates.
- **OAuth / LDAP / HTTP-Basic deny by default.** These previously synthesized an
  accepted identity for any input (any password, any bearer token). They now
  deny; real OIDC introspection / LDAP bind / user-store verification are
  follow-ons requiring external infrastructure.
- **API keys** are hashed with SHA-256 and compared in constant time (was the
  non-cryptographic `DefaultHasher` with a `==` compare).

### Breaking

- `auth::jwt::Jwk` gains a `k` (symmetric key) field, and JWT validation now
  rejects algorithms it cannot verify.

### Notes

- This closes the client-auth **crypto** holes (the deep-audit's #1 finding),
  verified by unit tests. Wiring proxy-terminated client auth into the
  connection path (a `[auth] mode` that verifies a client-presented JWT on
  connect), RS256/ES256, and real OAuth/LDAP remain follow-ons. That
  connection-path work is also the prerequisite for `pool-modes`.

## [0.10.0] - 2026-06-26

Minor release — platform-tier wiring wave 4: SQL query rewriting.

### Added

- **SQL query rewriting (`query-rewriting`).** `[query_rewrite]` config (off by
  default) with a list of rules — each `{ match_table | match_regex }` selects
  which queries it applies to and `{ replace_table_with | append_where |
  add_limit }` is the transformation. Matching simple queries are rewritten on
  the path before forwarding; the cache and backend both see the rewritten SQL.
  Rewrites log to the `helios::rewrite` target.

### Notes

- Rewriting covers the simple-query path and the real string-based
  transformations. The extended-protocol (Parse) path and the
  AST-reserialization transform are follow-ons.
- `pool-modes` has been resequenced to **after** `auth-proxy`: real connection
  multiplexing requires proxy-held backend credentials (which `auth-proxy`
  provides — passthrough auth cannot share a backend connection across clients)
  plus a raw-connection transaction-pool on the data path.

## [0.9.0] - 2026-06-26

Minor release — platform-tier wiring wave 3: query-result caching.

### Added

- **Query-result cache (`query-cache`).** `[cache]` config (off by default;
  `ttl_secs`, `max_result_bytes`). A cacheable read (a plain, deterministic,
  non-transactional `SELECT` — not `WITH`/`FOR UPDATE`/volatile) is served from
  an in-memory L1 hot (per-connection) + L2 warm (shared, normalized) cache with
  no backend round-trip; on a miss the response is captured while streaming and
  stored. A write invalidates cached reads referencing its tables. Cache hits
  log to the `helios::cache` target.

### Notes

- L1 hot + L2 warm tiers are live-verified (a read served the stale cached value
  while the backend held a newer one; a write invalidated it). The L3 semantic
  (vector-similarity) tier remains opt-in via the `/*helios:cache=semantic*/`
  hint and is not yet live-verified — it needs an embedding source, tracked as a
  follow-on.

## [0.8.0] - 2026-06-26

Minor release — platform-tier wiring wave 2: observability + read-scaling. Two
more feature flags that previously did nothing on the query path are now active.

### Added

- **Query analytics (`query-analytics`) records every forwarded query.**
  `[analytics]` config (off by default). Each query is fingerprinted +
  normalized (literal-only differences collapse to one `select ?` fingerprint),
  accumulating per-fingerprint call/latency/p99/error stats with a slow-query
  log. New `GET /api/analytics` returns the top queries + slow-query count. The
  real query normalizer now runs on the live path (it was previously a
  stand-in).
- **Read-your-writes (`lag-routing`).** `[lag_routing]` config (off by default).
  Within `ryw_window_ms` after a write, a session's reads are pinned to the
  primary so the client always observes its own writes despite replica lag. A
  lag-aware read-exclusion filter (`max_lag_bytes`) drops standbys lagging
  beyond a threshold.

### Notes

- Read-your-writes is live-verified. The lag-**exclusion** decision logic is
  unit-tested and in place (safe-by-default, `max_lag_bytes=0`), but populating
  per-node replication lag requires a background monitor with configured backend
  credentials + a real standby — tracked as a follow-on.

## [0.7.0] - 2026-06-26

Minor release — the first wave of **platform-tier wiring**. Three feature flags
that previously compiled but did nothing on the query path are now genuinely
enforced, each live-verified against PostgreSQL 18.4. This release also formally
owns two prior unreleased source changes (an MSRV bump and one breaking
public-API rename).

### Added

- **SQL-comment routing hints (`routing-hints`) are now honored.**
  `[routing_hints]` config (off by default). `/*helios:route=primary|standby|…*/`,
  `/*helios:node=<name>*/`, and `consistency=strong` steer routing on both the
  simple- and extended-query paths, taking precedence over default verb routing
  (but never over a plugin `Block`). A `node=<name>` hint resolves the node name
  to its address; the hint comment is optionally stripped before forwarding.
- **Rate limiting (`rate-limiting`) is now enforced.** `[rate_limit]` config
  (off by default; token bucket + concurrency, keyed per user / client-ip /
  database / global). Over-limit queries receive a clean `ErrorResponse`
  (SQLSTATE 53400); throttle/queue verdicts apply real backpressure; the
  connection is not dropped.
- **Per-node circuit breaker (`circuit-breaker`) is now active.**
  `[circuit_breaker]` config (off by default). Repeated backend failures trip a
  node's circuit; while open the proxy fast-fails with `ErrorResponse` SQLSTATE
  08006 instead of retrying the dead node, and read selection routes around it.
  A half-open probe closes the circuit on recovery.

### Fixed

- **Write detection is no longer fooled by a leading hint comment.**
  `is_write_query` is evaluated on the hint-stripped SQL, so
  `/*helios:…*/ INSERT …` is correctly classified as a write. The `node=<name>`
  route path (hint and plugin) now resolves a node name to its address instead
  of dialing a literal hostname.

### Changed

- **MSRV is now Rust 1.86** (was 1.75), to match the transitive dependency floor.

### Breaking

- `rewriter::RewriteRule::new` (the builder entry point, under the
  `query-rewriting` feature) was renamed to `RewriteRule::build`. Callers using
  `RewriteRule::new(id)` must switch to `RewriteRule::build(id)`.

## [0.6.1] - 2026-06-15

Patch release — demo infrastructure fixes. No proxy code or runtime behavior changes.

### Fixed

- **All 5 legacy demos now work end-to-end with the published Docker image.**
  They previously assumed a local `build: context` that no longer exists.
  - Replaced `build: context: ../../` with `image: ghcr.io/heliosdatabase/hdb-heliosdb-proxy:0.6.1`
  - Fixed `pg_isready` healthcheck to probe TCP (`-h 127.0.0.1`) so standbys
    don't start `pg_basebackup` before the primary TCP listener is ready.
  - Added `init-hba.sh` to each demo's `initdb.d` to allow Docker bridge-network
    replication connections (pg_hba `host replication all all trust`).
  - Added `user: postgres` to standby containers (postgres refuses root execution).
  - Added `chmod 750` after `pg_basebackup` to satisfy PostgreSQL permissions check.
  - Removed `synchronous_standby_names` from primary command blocks — it blocks
    `CREATE DATABASE` indefinitely when no standbys exist at init time.
  - Updated `demo.sh` and `run-compare.sh` to drop `--build`; updated READMEs.
  - Rewrote `proxy.toml` in bank-ledger and vs-pgbouncer from stale PgBouncer
    format to current HeliosProxy format.

## [0.6.0] - 2026-06-15

Maintenance release — operational/packaging tooling and docs. No proxy code or
runtime behavior changes.

### Fixed

- **Docker image publishing now works.** The `docker.yml` workflow had three
  bugs that failed every build: a non-existent action name, an uppercase image
  name ghcr rejects, and the wrong Dockerfile path. It now builds and pushes
  `ghcr.io/heliosdatabase/hdb-heliosdb-proxy` (linux/amd64).

### Changed

- Demo / skill / IaC examples reference the current `:0.6.0` image (the prior
  `:0.4.x` tags were never built).
- Build linux/amd64 only (dropped the slow/flaky emulated linux/arm64 leg).
- README uses stable "Connection-Routing Tier" / "Platform Tier" wording instead
  of version-pinned module counts.
- Internal working docs consolidated under `docs/internal/` (excluded from the
  published crate).

## [0.5.1] - 2026-06-14

Docs/branding patch. No code or behavior changes.

### Changed

- Rebrand the project org from `Dimensigon` to `HeliosDatabase` across the
  README, embedded operator skills, demos, operator/Terraform guides, and the
  LICENSE copyright line. Registry/k8s/npm references use lowercase
  `heliosdatabase` where required (`ghcr.io` image refs, the k8s API group, the
  npm scope, `*.github.io` / `charts.*` hosts, the Terraform provider source);
  `github.com` URLs keep the `HeliosDatabase` display case.

### Added

- `AGENTS.md` — contributor guide (build/test commands, style, testing, and
  commit conventions).
- A README "Ecosystem" section linking the HeliosDatabase repositories.

## [0.5.0] - 2026-06-14

Minor release: the 2026-06 deep-audit batches (A–H + G2). Table-stakes
security/protocol features, an AI/agent data-plane, a continuous PostgreSQL→
HeliosDB-Nano migration mirror, zero-downtime operations, and hot-path
performance work — all live-verified against PostgreSQL 18.4 and HeliosDB-Nano.

### Added

- **Client-facing TLS termination + mTLS** — `[tls]` config; rustls server
  handshake, optional client-certificate verification.
- **Proxy-terminated SCRAM-SHA-256** — `[auth] mode = "scram"` with a pgbouncer-
  style auth file; the proxy authenticates clients itself.
- **pg_hba-style admission rules** — `[[hba]]` allow/reject by user / database /
  IP-CIDR, evaluated before any backend connection.
- **Query cancellation forwarding** — `CancelRequest` routed to the backend that
  owns the session's `BackendKeyData`.
- **Prepared statements survive backend switches** — named statements are
  transparently re-prepared on a new connection after a failover / redial.
- **Native MCP agent gateway** — JSON-RPC 2.0 over HTTP (`query` / `list_tables`
  / `explain`), backend-agnostic, read-only by default.
- **Per-agent scoped grants + SQL contract validator** — verb/table/predicate/
  row-limit policy with machine-readable repair hints.
- **Neon-serverless-compatible HTTP SQL gateway** — `POST /sql`.
- **Continuous traffic mirroring** and a **PostgreSQL→Nano migration mirror**:
  snapshot bootstrap (COPY-based bulk load with INSERT fallback + a non-empty-
  target idempotency fence), `migration_ready` status, and transparent cutover
  with rollback.
- **Instant branch databases** — `CREATE DATABASE … TEMPLATE` provisioning via
  `POST/GET/DELETE /api/branch`.
- **Admin API Bearer-token authentication** — closes the unauthenticated-admin
  release blocker.
- **Zero-downtime operations** — `SIGHUP` config reload (nodes / pools / limits /
  hba without dropping connections) and a `SIGUSR2` binary handoff over
  SO_REUSEPORT with graceful drain (client + admin listeners).
- **Plugin registry + `helios-plugin` CLI** — `install` (from a local/`file://`/
  `http://` index, SHA-256 + Ed25519 verified), `list`, `new`, `verify`.

### Changed

- **Extended-protocol streaming relay** — large result sets stream frame-by-frame
  with bounded proxy memory (≈100 MB result → flat RSS).
- **Per-session multi-node backend connection cache** — reuses authenticated
  connections across read/write routing switches.
- **WASM plugin runtime** — `InstancePre` reuse, epoch-based timeout enforcement,
  sharded metrics.
- **Lock-free hot path** — node-health map and live config behind `ArcSwap`;
  parallel health sweep.
- **Unnamed-`Parse` promotion** — an identical re-`Parse` is not re-forwarded to a
  backend that already holds that unnamed statement (fewer backend round trips).

### Fixed

- Wire-protocol tag mapping: `'R'` → `AuthRequest`, and server→client tag-collision
  remapping in the backend client (fixes management/replay queries against real
  backends).

## [0.4.2] - 2026-05-01

Patch release: ship the operator skill bundle inside the binary so
`cargo install heliosdb-proxy` users (no repo clone) can deploy
the bundle to Claude Code / Codex with one subcommand.

### Added

- **`heliosdb-proxy install skills`** — new subcommand that deploys
  the embedded `.claude/skills/` bundle (22 skills + index + template)
  into `~/.claude/skills` and `~/.codex/skills`.
- Flags: `--target claude|codex|both` (default `both`),
  `--symlink` (use a symlink into `~/.local/share/heliosdb-proxy/skills/`
  so re-running after a binary upgrade refreshes the bundle in place),
  `--force` (overwrite pre-existing entries), `--dry-run` (show
  planned actions without writing).
- New module `src/skills.rs` with `EMBEDDED_SKILLS` (a `Dir<'_>`
  populated by `include_dir!`), `install_skills()`, and a
  test-friendly `install_skills_at(home, …)` overload.
- Dependency: `include_dir = "0.7"` (~80 KiB binary growth).

### Changed

- `src/main.rs` now uses clap subcommands. The daemon path
  (no subcommand + flags) is unchanged for backward compatibility
  with v0.4.1 invocations.
- Package size up from 188 files / 3.0 MiB to 214 files / 3.2 MiB
  (compressed: 620 KiB → 673 KiB) — entirely from the embedded
  skill bundle.

## [0.4.1] - 2026-05-01

Patch release: internal cleanup + first tag-driven crates.io publish.
No API surface changes; default-feature lib lint count drops from 41
to 0. All 1 502 tests still green.

### Build & release

- Tag-driven `cargo publish` workflow (`.github/workflows/crates-io.yml`).
  Pushing a `vX.Y.Z` tag matching `Cargo.toml`'s version uploads the
  crate. `workflow_dispatch` retries against an existing tag.
- Cargo metadata for crates.io: `rust-version = "1.75"`, `homepage`,
  `documentation`, `exclude` (drops demos/docs/operator/terraform/docker
  from the `.crate` — 320 → 188 files), and
  `[package.metadata.docs.rs] all-features = true` so docs.rs renders
  the full feature surface.

### Internal — dead-code retirement

- Removed legacy connection-pool path in `server.rs` that
  `ConnectionPoolManager` superseded: `state.pools` field, `NodePool`
  and `BackendConnection` structs, `get_connection`, `cleanup_pools`,
  and `process_statement_for_pool_mode` methods. ~200 lines deleted.
- Removed unused fields across `pool/manager.rs`,
  `connection_pool.rs`, `health_checker.rs`, `failover_controller.rs`,
  `switchover_buffer.rs`, `pipeline.rs`, `batch.rs`, `load_balancer.rs`,
  `admin.rs` (and the now-empty `with_flush_channel` constructor on
  `InsertBatcher`).
- Cfg-gated imports / locals that only live under `wasm-plugins`,
  `ha-tr`, or `#[cfg(test)]` (chrono `DateTime`/`Utc` in admin.rs,
  `NodeRole` in failover_controller.rs, `forward_start` in server.rs).
- `MessageType::from_tag` had four unreachable server-side arms
  (`DataRow`/`ErrorResponse`/`CommandComplete`/`ParameterStatus`)
  that collide with client-side `Describe`/`Execute`/`Close`/`Sync`
  byte tags. Removed and documented that the function is
  direction-agnostic and resolves collisions to the client-side variant.
- Collapsed the `let mut backend_stream = None; ... match { ... =
  Some(...) }` pattern in `handle_client` to a single
  `let (mut backend_stream, mut backend_node) = match { ... }`.

### Licensing

- License changed from AGPL-3.0-only to Apache-2.0. (Already shipped
  in the 0.4.0 crates.io upload — recorded here for the changelog
  trail.)

## [0.4.0] - 2026-04-25

Major feature release: full delivery of the T1 / T2 / T3 roadmap shipped
in v0.3.1's audit, plus a critical correctness fix in the WASM plugin
hook serialization. 31 PRs across five repositories; 1 296 lib tests +
5 end-to-end WASM tests + 57 plugin tests + 17 operator tests, all green.

### ⚠ Critical fix
- **`QueryContext` serializer was dropping `hook_context`.** Every WASM
  plugin shipping in v0.3.1 received a context without the per-request
  attributes the proxy carefully populated (`tenant_id`, `agent_id`,
  `application_name`, `helios.region`, etc). All five attribute-driven
  plugins (cost-governor, token-budget, llm-guardrail, ai-classifier,
  residency-router) silently no-op'd their gates as a result. Fixed by
  adding `hook_context` to the custom `Serialize` impl and deriving
  `Serialize` on `HookContext`. Covered by the new
  `tests/wasm_plugin_e2e.rs` end-to-end tests that load real .wasm
  plugins through the production runtime.

### Added — admin REST API
- `GET /topology` — `{ currentPrimary, healthyNodes, unhealthyNodes,
  totalNodes, lastFailoverAt }`. Joins config (role) with node_health
  (healthy) so external controllers populate cluster topology in one
  poll. Field names match `HeliosProxyStatus` directly (camelCase).
- `POST /api/replay` + `GET /api/replay` — time-travel replay of a
  journal window against a target backend. Per-call credential
  overrides via `target_user` / `target_password` / `target_database`.
  503 when `ha-tr` feature off.
- `POST /api/shadow` — runs a query against source AND shadow backends,
  diffs results, returns `{ row_count_match, row_hash_match, is_clean,
  primary_elapsed_us, shadow_elapsed_us, primary_error, shadow_error }`.
  Order-independent row-set hash so non-deterministic orderings tolerate.
- `POST /api/chaos` — controlled fault injection: `force_unhealthy`,
  `restore`, `reset` actions. `GET /api/chaos` lists active overrides.
- `GET /plugins` — loaded WASM plugin list with name, version, hooks,
  state, invocation + error counts.
- `GET /anomalies?limit=N` — recent anomaly events (rate spikes,
  credential bursts, SQLi patterns, novel queries).
- `GET /api/edge` + `POST /api/edge/register` + `POST /api/edge/invalidate`
  — edge-mode cache stats, edge-node registration, manual invalidation
  broadcast.

### Added — admin Web UI (`/` + `/ui`)
Single embedded HTML file. Auto-refresh every 5 s. Panels:
Nodes · Topology · Plugins · Anomalies · Edge Mode · Chaos Mode ·
Shadow Execution · Time-Travel Replay · SQL Console · Traffic · Cluster.

### Added — WASM plugin ecosystem
- **Plugin host KV bridge.** `env.kv_get` / `env.kv_set` /
  `env.kv_delete` wasmtime imports. Per-plugin namespaced
  `Arc<RwLock<HashMap<plugin, HashMap<key, value>>>>`. Plugins can
  persist state across hook invocations.
- **`env.sha256_hex` host import.** Real SHA-256 via the audited
  `sha2` crate; plugins no longer embed their own (now ~25 KiB
  smaller). Replaces the FNV placeholder `audit-chain` shipped with
  in v0.3.1.
- **`PluginLoader.SignatureVerifier`.** Optional Ed25519 trust root
  (directory of `*.pub` files, base64 raw 32-byte keys). Every
  loaded `.wasm` requires a sidecar `.sig`. Wire-compatible with
  `openssl pkeyutl -sign` and `signify`.
- **OCI-style artefact loader.** `PluginLoader.load` now accepts
  `.tar.gz` artefacts produced by the new `helios-plugin pack` CLI:
  `manifest.json` + `plugin.wasm` + optional `plugin.sig`. Validates
  wasm SHA-256 against the manifest, verifies signature against trust
  root if attached.
- **`RouteResult::Block { reason }` ABI variant.** Plugin-side route
  rejection synthesises a PG `ErrorResponse` + `ReadyForQuery` —
  same wire shape as `PreQueryResult::Block`.
- **`trust_root` config knob** — plugins.trust_root in TOML wires the
  signature verifier automatically.

### Added — anomaly detection (T3.1)
- **`anomaly-detection` feature.** In-process sliding-window
  detector. Four families:
  1. **Rate spike** — z-score on per-tenant queries-per-second vs
     rolling EWMA baseline (default 60 s window, 3σ threshold).
  2. **Credential stuffing** — failed-auth bursts per (user, ip);
     Warning at 5, Critical at 10. Successful auth resets.
  3. **SQL injection** — six pattern classes (classic OR, UNION
     SELECT, comment escape, stacked queries, time-based blind,
     information_schema probe).
  4. **Novel query** — first-seen fingerprint, informational.
- Wired into the query path via `record_anomaly_observation`. Tenant
  identity from session vars / fallback to client IP.

### Added — edge / geo proxy mode (T3.2)
- **`edge-proxy` feature.** Cache-first edge mode with last-write-wins
  TTL coherence. `EdgeRole::Edge` terminates reads against a local
  LRU+TTL+version cache; `EdgeRole::Home` is authoritative and
  broadcasts invalidations. Pull-on-miss + invalidation push,
  no consensus, no central registrar.
- `EdgeCache` (LRU + TTL + monotonic version + per-table tags).
- `EdgeRegistry` (home-side fanout, bounded mpsc per edge,
  back-pressure on slow edges, prune-stale).
- Explicit "eventual consistency with bounded staleness via TTL"
  contract.

### Added — chaos engineering (T3.3)
- **`POST /api/chaos`.** `force_unhealthy` / `restore` / `reset`
  actions. Triggers the failover path the same way a real probe
  failure would.

### Added — query shadow execution (T3.4)
- **`POST /api/shadow`.** Built on the existing `src/shadow_execute/`
  module. Order-independent row-set hash for non-deterministic
  orderings. SLO: HTTP 500 only on source-connect failures; shadow
  failures land in the report.

### Added — Kubernetes operator (`HDB-HeliosDB-Proxy-Operator`)
- Reconciler renders ConfigMap + Deployment + Service from
  HeliosProxy CR. Owned objects use `SetControllerReference` so
  `kubectl delete` reaps the stack.
- `RefMissing` condition surfaces unresolved PoolProfile /
  RoutingRule / AuditPolicy / TenantQuota refs.
- Status polling: reconciler hits the proxy's `/topology` and
  populates `currentPrimary` / `healthyNodes` / `unhealthyNodes` /
  `lastFailover`.
- 17 Go unit tests (render helpers + condition merge + config hash).

### Added — Terraform + Pulumi providers
- `terraform-provider-HDB-HeliosDB-Proxy` — five resources
  (`heliosproxy_instance`, `_pool_profile`, `_routing_rule`,
  `_audit_policy`, `_tenant_quota`). Schema mirrors operator CRDs
  via local replace.
- `pulumi-HDB-HeliosDB-Proxy` — terraform-bridge wrapper, Node.js /
  Python / Go / .NET SDK metadata. Both providers wrap the same
  operator CRDs.

### Added — first-party plugins (`HDB-HeliosDB-Proxy-Plugins`)
All seven plugins on the new shared `helios-plugin-abi` crate +
host KV bridge:
- `cost-governor` — per-tenant query cost budgets (minute / hour /
  day windows). Real `kv_get` / `kv_set` for usage tracking.
- `ai-classifier` — detects LLM-generated SQL via
  `application_name` keywords, generated-by markers, opt-in.
  Best-effort `agent_id` + `model_id` extraction.
- `token-budget` — per-(agent, model) cost gating.
- `llm-guardrail` — blocks DROP/TRUNCATE in AI traffic, DELETE/UPDATE
  without WHERE, SELECT without LIMIT against large tables, missing
  tenant_id filter.
- `pgvector-router` — detects pgvector top-K (`<->`, `<#>`, `<=>`),
  routes to a topology-tagged vector replica via
  `RouteResult::Node`.
- `column-mask` — rewrites bare column refs to
  `mask_<fn>(<col>) AS <col>` based on user roles.
- `audit-chain` — hash-chained tamper-evident log. **Now uses real
  SHA-256** via the `env.sha256_hex` host import (was an FNV
  placeholder in v0.3.1).
- `residency-router` — routes by `helios.region` attribute; uses
  `RouteResult::Block` for cross-region rejections (was a sentinel
  hack in v0.3.1).

### Added — `helios-plugin` CLI
Pack / inspect / verify WASM plugin artefacts as portable `.tar.gz`:

```sh
helios-plugin pack    --wasm <path> --name X --version 1.0 \
                      --hooks pre_query,post_query [--sig <path>] \
                      --out <path>
helios-plugin inspect <artefact.tar.gz>
helios-plugin verify  <artefact.tar.gz> --trust-root <dir>
```

Uses the same Ed25519 + base64 trust-root format as the proxy's
loader so a single key directory works for both artefact
verification AND in-proxy signature checking.

### Changed
- New feature flags: `anomaly-detection`, `edge-proxy`. Both added
  to the `all-features` bundle.
- `RouteResult` deserialiser accepts the new `block` action variant.
- `PluginLoader.allowed_extensions` now accepts `gz` (for
  `.tar.gz` artefacts) in addition to `wasm`.

### Tests
- 1 296 lib tests pass with `--features all-features` (+58
  regression tests covering the additions above).
- 5 new end-to-end WASM tests load real plugin .wasm artefacts
  through the production runtime.
- All 7 plugins compile to wasm32 (~120-150 KiB each).
- All three feature configurations build clean: default,
  `--no-default-features`, `--features all-features`.

## [0.3.1] - 2026-04-22

### Fixed
- **Connection pool: `max_connections` now enforced while connections are in use.**
  The semaphore permit acquired during `get_connection` was previously dropped
  before the connection reached the caller, so the limit only gated concurrent
  calls to `get_connection` itself. The permit is now attached to the
  `PooledConnection` and released when the connection is dropped or closed,
  so `max_connections` bounds total (idle + in-use) connections as documented.
  Covered by `test_max_connections_enforced_while_in_use`.
- **Protocol parser: unterminated C-strings now return a protocol error.**
  The previous `read_cstring` loop silently consumed the entire remaining
  buffer on missing null terminators. It now returns
  `ProxyError::Protocol(...)` and leaves the buffer untouched. Covered by
  `test_read_cstring_unterminated`.

### Performance
- **Connection-pool checkout no longer serialises across nodes.**
  `ConnectionPool::get_connection` previously held `pools.write().await`
  across `semaphore.acquire_owned().await` and `create_connection().await`,
  serialising all checkouts through a single lock. The lock is now taken
  only to pop an idle connection and clone the per-node `Arc<Semaphore>`;
  both awaits happen after the lock is released.
- **Pool metrics are now lock-free.** Seven `self.metrics.write().await`
  call sites replaced with `fetch_add(1, Relaxed)` on a
  `PoolMetricsCounters` struct of `AtomicU64` fields. `pool.metrics()`
  snapshots the counters on demand; no lock, no `.await`.
- **Zero-copy protocol parsing.**
  - `read_cstring` scans for the null terminator in a single pass
    (`iter().position(|&b| b == 0)`) and hands the split-off `BytesMut`
    directly to `String` (zero-copy conversion when uniquely owned)
    instead of growing a `Vec<u8>` byte-by-byte via `get_u8()`.
  - `BindMessage::param_values` is now `Vec<Option<Bytes>>`; parse uses
    `split_to(len).freeze()` instead of `.to_vec()`, so parameter values
    are reference-counted slices into the original buffer rather than
    per-parameter heap allocations.
- **L1 hot cache: hits take only a read lock.**
  `L1Entry::access_count` is now an `AtomicU64`, so `touch()` takes `&self`
  and can run under a read lock on the entries map. The cache itself
  switches from `std::sync::RwLock` to `parking_lot::RwLock` (no
  poisoning, faster uncontended). Only expired entries escalate to a
  write lock for eviction. Covered by `test_concurrent_hits_read_lock_only`
  (16 threads × 500 iters on the same key).

### Changed
- `L1Entry` no longer derives `Clone` (atomics aren't trivially `Clone`).
  No external caller was cloning it; only the contained `CachedResult` is
  cloned on return from `get()`. `access_count()` accessor added for
  read-only consumers.

### Tests
- 1 102 tests pass with `--features all-features` (+6 regression tests
  covering the fixes above).

### Benchmarks
Criterion `--quick` on the `pooling` bench suite, comparing baseline
(v0.3.0, commit `8c43de9`) against v0.3.1 on the same machine:

| Benchmark                                  | v0.3.0     | v0.3.1     | Change       |
|--------------------------------------------|------------|------------|--------------|
| `pool/acquire_release/single`              | 471.81 ns  | 329.83 ns  | **−30 %**    |
| `pool/throughput/sequential_acquire/1`     | 490.36 ns  | 353.88 ns  | **−28 %**    |
| `pool/throughput/sequential_acquire/10`    | 5.354 µs   | 3.351 µs   | **−37 %**    |
| `pool/throughput/sequential_acquire/50`    | 25.36 µs   | 16.95 µs   | **−33 %**    |
| `pool/metrics/read_metrics`                | 35.90 ns   | 11.51 ns   | **−68 %** (3.1× faster) |

Single-threaded numbers — the concurrency gains from dropping
`RwLock<pools>` across `.await` and from lock-free metric reads
compound under contention and are not captured by this bench.

## [0.3.0] - 2026-03-26

### Added
- **24 feature modules** migrated to standalone, feature-gated architecture:
  - `pool-modes` - Session, Transaction, and Statement connection pooling
  - `ha-tr` - Transaction Replay with failover replay, cursor restore, session migration
  - `query-cache` - Multi-tier caching (L1 hot / L2 warm / L3 semantic)
  - `routing-hints` - SQL comment-based query routing (`/*helios:route=primary*/`)
  - `lag-routing` - Replica lag-aware routing with read-your-writes consistency
  - `rate-limiting` - Token bucket, sliding window, and concurrency limiters
  - `circuit-breaker` - Adaptive circuit breaker with sliding counter
  - `query-analytics` - Slow query log, N+1 detection, query fingerprinting
  - `multi-tenancy` - Tenant isolation with per-tenant connection pools
  - `auth-proxy` - JWT, OAuth, and API key authentication
  - `query-rewriting` - SQL rewriting rules engine
  - `wasm-plugins` - Hot-reload WASM plugin system (sandboxed)
  - `graphql-gateway` - GraphQL-to-SQL translation with introspection
  - `schema-routing` - Data temperature and workload classification routing
  - `distribcache` - Distributed intelligent caching with AI-driven prefetch
  - `observability` - Prometheus metrics and OpenTelemetry tracing
- **TopologyProvider trait** with two implementations:
  - `postgres-topology` - PostgreSQL discovery via `pg_is_in_recovery()` polling
  - `heliosdb-topology` - HeliosDB-Lite internal topology
- **`all-features` bundle** for enabling all proxy features at once
- PostgreSQL wire protocol forwarding with Transparent Write Routing (TWR)
- Request pipelining support (Parse/Bind/Execute pipeline, FIFO ordering)
- Batch INSERT coalescing for bulk write optimization
- Switchover buffer for zero-downtime planned failover
- Primary tracker with standalone and integrated modes
- Admin REST API with HTTP SQL endpoint for load-balanced queries
- Criterion benchmarks for pooling and routing performance
- Integration test skeleton for end-to-end verification
- CI workflow with feature matrix (default, ha-tr, all-features, all-features+postgres-topology)
- MSRV verification at Rust 1.75
- Release workflow for cross-platform binary builds (linux/macOS, x86_64/aarch64)
- Docker workflow with multi-arch images (amd64/arm64) pushed to GHCR

### Changed
- Restructured from embedded library to standalone binary + library crate
- All feature modules are now independently toggleable via Cargo feature flags
- Connection pool modes (`session`, `transaction`, `statement`) moved behind `pool-modes` feature

## [0.2.0] - 2026-02-15

### Added
- Connection pooling with Session, Transaction, and Statement modes
- Pool hardening: transaction leak detection, stale lease cleanup, exhaustion monitoring
- Prepared statement tracking across pool mode transitions
- Connection reset executor for clean state between leases
- Load balancer with round-robin, least-connections, and latency-based strategies
- Health checker with configurable check queries and failure thresholds
- Failover controller with sync-standby preference and automatic promotion
- Transaction journal for write tracking and replay

### Changed
- Upgraded to tokio 1.x full runtime
- Improved protocol codec for better PostgreSQL compatibility

## [0.1.0] - 2026-01-27

### Added
- Initial release of HeliosProxy as a standalone connection router
- PostgreSQL wire protocol support (startup, simple query, extended query)
- Basic connection pooling with configurable min/max connections
- Read/write splitting with automatic query classification
- Node health monitoring with configurable intervals and thresholds
- Admin API for runtime management and metrics
- Configuration via TOML file or command-line arguments
- Benchmark suite: HeliosProxy vs PgBouncer scalability comparison
- Docker support for containerized deployment

[0.3.1]: https://github.com/HeliosDatabase/HeliosDB-Proxy/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/HeliosDatabase/HeliosDB-Proxy/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/HeliosDatabase/HeliosDB-Proxy/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/HeliosDatabase/HeliosDB-Proxy/releases/tag/v0.1.0
