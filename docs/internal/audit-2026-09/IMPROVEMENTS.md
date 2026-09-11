# HeliosProxy reliability, performance and global-scale backlog

Source baseline: `1dca229`, 2026-09-08. Companion [audit and reproductions](README.md).
Items are proposals unless explicitly described as already implemented. Suggested
configuration/API names below **do not exist yet**. P0 means correctness before
stronger guarantees; P1 means next reliability/scale work; P2 means optimization
after measurement. Implementation status is recorded under each item; unchecked items retain acceptance work.

## Sonnet's six suggestions, checked against the code

| Suggestion | Current implementation | More precise next item |
|---|---|---|
| O(1) edge LRU + byte bound | `edge/cache.rs` already uses `lru::LruCache`, Arc/Bytes entries, reverse table index, entry count and per-entry response ceiling | Keep the LRU. Add aggregate retained/in-flight byte accounting and tenant budgets; measure its single-mutex contention before sharding it (C-01, P-02) |
| Finer invalidation | Table-targeted reverse indexing already avoids a full cache scan for known tables; a write still invalidates all dependent queries | First make invalidation commit-aware and recover missed events. Then add safe primary-key/range dependencies with conservative table fallback (C-02, C-03, C-07) |
| HLC or authoritative home clock | Home logical counter + per-boot epoch already defines ordering; edges use observed home versions, not regional wall time. Epoch uses time/pid as an opaque identity, not version order | Preserve authority; add elected ownership epochs per shard. HLC can carry causal timestamps but cannot fence writers or prove data freshness (D-01) |
| Single-flight misses | `QueryCache::pending_requests` is an unused field, not a working coalescer; no edge leader/follower fetch path | Implement bounded coalescing on the real request path with cancellation, invalidation and identity semantics (C-04) |
| Per-table freshness + metrics | Edge has one TTL plus hit/miss/eviction/oversize counters; query-cache library has table TTLs, but daemon `CacheToml` lacks them | Define freshness relative to commit/watermark, wire per-table policy, export age/gaps/bytes/region histograms (C-05, C-06) |
| Compression and warm-up | Edge retains encoded PG responses in Bytes; no edge compression/warm-up path | Optional thresholded compression and rate-limited, identity-scoped warming after ownership/freshness validation (C-08, C-09) |

The highest-value additions to that list are safe replay decisions, verified
promotion authority, commit-driven invalidation, security/session-aware cache
identity, and aggregate resource admission. Optimizing hit rate before these can
make incorrect responses faster or overload the surviving primary during failover.

## First: protect Transaction Replay

- [x] **TR-01 · P0 — classify every possible commit boundary.** *(2026-09-11: shipped in 1.6.1. Bounded lexer over leading/nested comments, quoted text, BEGIN/END, PREPARE/COMMIT PREPARED, multi-statement Query strings and every Execute in a pipelined batch; an unknown commit outcome stays unknown and is reported `08007`. Acceptance: both COMMIT probes pass and the real-PG commit-outcome suite is 7/7 where the pre-change binary failed 4 (`evidence/tr01-postgres.json`, `evidence/tr01-postgres-before.json`). The residual cross-cycle statement/portal identity work is availability, not safety, and is re-filed as TR-08.)* Reuse a bounded
  PostgreSQL-aware lexer/parser for leading/nested comments, quoted text, BEGIN/END,
  PREPARE/COMMIT PREPARED, multi-statement Query strings and every Execute in a
  pipelined batch. Preserve statement/portal identity, including binary Bind params.
  An unknown commit outcome must stay unknown until the database can resolve it.
  **Accept:** both failing COMMIT probes pass; fault injection at every frontend
  frame boundary never dispatches a possibly committed transaction a second time.
  Add real PG ledger checks that lose the COMMIT response *after* durable execution.
  **Started 2026-09-08:** commit-boundary guards, Execute/Bind inspection and
  committed-prefix exclusion implemented; validation and remaining acceptance work
  are tracked in [TR-01.md](TR-01.md). The item stays open until the listed protocol
  identity and transport-boundary coverage is complete.

- [x] **TR-02 · P0 — track client-visible response progress.** *(2026-09-11: shipped in 1.6.1 and completed by the 2026-09-09 review fix. `ResponseProgress` carries emitted RowDescription/DataRow/CommandComplete/ErrorResponse bytes into the fault context; a visible result prefix closes the session instead of concatenating a second result; delivery uncertainty is preserved across Flush chunks; the idle backend-watch relay now publishes only complete frames. Acceptance: real-PG streaming suite 13/13, synthetic boundary harness 26/26. The opt-in bounded response buffering is an enhancement, not part of the safety contract, and is re-filed as TR-09.)* Include emitted
  RowDescription/DataRow/CommandComplete/ErrorResponse progress in the fault context.
  Once a result prefix is visible, fail cleanly rather than concatenate a new result.
  Preserve delivery uncertainty across Flush chunks: failure before sending a later
  chunk does not establish the outcome of Executes already sent in that cycle.
  For explicitly opted-in small reads, bounded response buffering can allow retries
  before publication; streaming remains available without that guarantee.
  **Accept:** simple/extended, Flush/Sync and large-result faults never duplicate
  rows or protocol completion messages. Test with streaming clients and slow readers.
  Measure first-row latency and memory with/without optional buffering.

- [x] **TR-03 · P0 — establish replay eligibility, not a SELECT keyword heuristic.** *(2026-09-09: lexical call scan against a side-effect-free built-in allowlist + `tr_read_functions`; subset published in docs/configuration.md. Catalog-informed eligibility and table/UDF-change invalidation remain future work.)*
  Unknown functions, volatile operations, advisory/session locks, sequence calls,
  notifications, external effects and unsupported SQL constructs need an explicit
  policy. For PG, catalog metadata can inform a cached allowlist; PG-wire backends
  need capability declarations. Catalog information alone does not establish that
  a whole transaction will reproduce the same observations. Table/UDF changes must
  invalidate eligibility metadata. **Accept:** the volatile SELECT probe passes;
  WITH-wrapped sequence/UDF calls and data-modifying functions execute at most once
  on unknown autocommit outcomes. Publish the supported replay subset.

- [x] **TR-04 · P1 — restore session state transactionally and within a byte budget.** *(2026-09-09: deferred `GucOp`s applied at COMMIT, savepoint-scoped rollback, variables keyed by name, cap = distinct variables, failover refused (08006) when the cap was exceeded; all three `guc_*` probes green. Extended-protocol SETs, `PREPARE`, temp tables and cursors remain unrestored.)*
  Track effective GUC state plus savepoint undo, RESET/DISCARD and extended SET;
  apply only changes that survive transaction end. Maintain effective role,
  search_path, timezone and startup options consistently with cache identity.
  Bound pending transaction GUCs and strings by bytes as well as count. If state
  capture is incomplete, stop claiming transparent restoration and return a clear
  unrecoverable-session error. **Accept:** all three failing GUC probes pass,
  extended and savepoint variants agree with direct PG, and a long SET-only
  transaction cannot exceed its budget.

- [x] **TR-05 · P0 — distinguish unsent frames from partially sent extended batches.** *(2026-09-11: shipped in 1.6.1. Every extended-batch write failure preserves `OutcomeUnknown`; the deterministic partial-writer covers every byte offset, timeout and zero-length write; connect failure before any send stays eligible for the safe retry. The benchmark question that held acceptance open is resolved: the interrupted candidate run was superseded by the full 107-case post-release measurement on 1.6.0, which includes this code and passes gate 3 (92/92 comparable, mean +1.59%) — see `benches/BASELINE.md`.)*
  A failed `write_all(batch)` is not proof that earlier Execute messages never ran.
  Retain byte/frame delivery progress and classify uncertain prefixes conservatively.
  **Accept:** a deterministic writer that fails after each byte/frame offset cannot
  trigger a duplicate committed write; connect failure before any send remains
  eligible for the safe retry path. Cover an initially-autocommit batch containing
  BEGIN/DML/COMMIT and a lost final Sync.
  **Implemented, validation pending 2026-09-08:** all extended-batch write failures
  preserve OutcomeUnknown; deterministic partial-writer coverage includes every byte
  offset, timeout and zero-length write. See [TR-05.md](TR-05.md).

- [x] **TR-06 · P1 — verify replay observations and apply one recovery deadline.** *(2026-09-10: per-statement observation digest over T/D/C/I frames within `[limits] tr_max_observation_bytes`, re-verified at replay (40001 + ROLLBACK on divergence); over-budget responses make the transaction non-replayable; SERIALIZABLE/REPEATABLE READ (explicit or via default_transaction_isolation) non-replayable; ONE deadline (`write_timeout_secs`) carried through primary wait, connect/auth, restore and replay. `[limits] backend_response_timeout_secs` adds the whole-response bound for the H-07 slow-drip case. The final re-execution is bounded by the ordinary relay timeouts, not the recovery deadline.)*
  Capture result shape, ordered row digest and affected-row/command metadata for
  acknowledged statements within a configurable budget. Recompute during replay;
  divergence returns `40001` with the new transaction rolled back. Add eligibility
  for isolation level/snapshot-sensitive transactions. Carry one deadline through
  selection, connect/auth, GUC restore, replay and retry; current per-operation
  timeouts can exceed `write_timeout_secs`. **Accept:** mutate a prior SELECT result
  between attempts and verify refusal; large replay histories remain within the
  configured time/memory ceiling and do not claim the old snapshot was preserved.

- [ ] **TR-07 · P1 — separate and harden live replay, recovery journal and replay tools.**
  The current global journal is post-response SQL text, not a durable WAL. Preserve
  actual transaction boundaries, parameters, source identity, outcome, commit order
  and tenant/session context before offering recovery-grade retention. Exclude
  failed and rolled-back work from committed-history replay. Make missing backend
  configuration an error outside explicit test doubles. Use one transaction/connection
  for library transaction replay and stop on failure. **Accept:** rollback/savepoint,
  failed statements, binary/array parameters, interleaved clients and interrupted
  replay cannot produce partial committed ledger transfers; no backend means no
  successful replay report. Keep operator time-window replay explicitly distinct.

- [ ] **TR-08 · P2 — acknowledged statement and portal identity across cycles.**
  Residual of TR-01. Preserve named statement/portal identity, including binary
  Bind parameters, for statements acknowledged in an earlier extended cycle so a
  replay can re-establish them instead of refusing. Today the refusal is correct
  but costs availability: an interrupted session that cannot re-derive identity
  returns `08007` rather than re-homing. **Accept:** a pipelined client with named
  statements prepared in an earlier cycle survives a backend loss mid-transaction
  with identical results; no path gains a second dispatch of a possibly committed
  statement.

- [ ] **TR-09 · P2 — opt-in bounded response buffering for small reads.**
  Residual of TR-02. For explicitly opted-in small reads, buffer the response
  until it is complete so a fault before publication can be retried transparently;
  streaming remains the default and keeps today's no-duplicate guarantee.
  **Accept:** first-row latency and peak memory measured with and without the
  option; a fault before publication retries, a fault after publication still
  closes.

## Next: make HA and load-balancing contracts real

- [ ] **H-01 · P0 — wire authoritative topology into the daemon.** Connect providers
  to one immutable routing snapshot, including cluster/shard ID, leader identity,
  authority epoch, health freshness and replica positions. Consume it from startup,
  active-session recovery, reads, admin endpoints and pool eviction. Reject conflicting
  primaries instead of picking the first reachable address. Support an external
  Patroni/controller adapter first; an integrated controller can follow the same
  contract. **Accept:** real replica promotion changes the write destination without
  hand-editing roles; a partitioned old primary never receives new accepted writes
  after authority changes. Run with and without `ha-tr`.

- [ ] **H-02 · P0 — require quorum authority and fencing before promotion.** Backend
  liveness is not permission to become primary. Define lease expiry, fencing token
  enforcement, loss-of-quorum behavior, synchronous/asynchronous RPO, recovery and
  reintegration. A proxy-side epoch alone cannot fence independent direct clients:
  the database or a reliably exclusive backend/network control must enforce it.
  **Accept:** two sides of a partition cannot both authorize writes; isolate control
  traffic separately from data traffic; validate paused processes, old-primary
  return, witness loss and delayed messages. Never advertise zero-loss for an async
  replica that has not received an acknowledged commit.

- [ ] **H-03 · P1 — implement configured routing and health policy.** Daemon reads
  currently choose RR regardless of `read_strategy`; health uses SSLRequest rather
  than `check_query` and ignores recovery `success_threshold`. Honor existing knobs
  or reject unsupported settings. Add measured inflight-load and latency EWMA to
  an optional power-of-two-choices policy, with weights, locality and hysteresis.
  **Accept:** unequal response latency and backend load produce the configured
  distribution, unhealthy candidates are excluded, and N successes are required
  when N is configured. Benchmark primary work independently of read replicas.

- [ ] **H-04 · P1 — measure lag and carry causal positions.** Populate real backend
  replay/flush/apply positions, timeline and sample time; `None` must not mean
  “within lag threshold” for a strict policy. Use backend-specific probes behind
  a capability adapter. Preserve a per-session commit watermark and offer an
  authenticated reconnect token across proxies. **Accept:** pause replica apply,
  migrate the client to another proxy, and verify a read waits/routes primary or
  fails within its deadline; it never returns older data as causally fresh.

- [ ] **H-05 · P1 — fleet-aware connection and recovery admission.** Bound total
  clients, active/idle backend connections, outstanding queries and replay bytes
  per process, tenant and backend. Default max clients is currently unlimited;
  per-session bounds do not bound the process. Add fair queues, max wait time,
  per-destination reconnect concurrency and jittered retry budgets. **Accept:** a
  primary loss at the approved client counts produces a bounded reconnect wave,
  stable RSS and queue delay, with reserved capacity for health/admin traffic and
  no starvation of a small tenant. Multi-proxy totals respect backend capacity.

- [ ] **H-06 · P1 — one policy contract across PG, HTTP, GraphQL and MCP.** The HTTP
  SQL gateway dials/authenticates a BackendClient per request and gateways do not
  automatically inherit the wire path's pool/replay/policy behavior. Introduce a
  shared request executor without forcing binary PG results through JSON. Explicitly
  define transaction scope, auth/tenant identity, admission and retry semantics for
  each interface. **Accept:** the same forbidden write, timeout, tenant identity and
  resource limit yields equivalent enforcement across interfaces; gateway connection
  churn and throughput improve with measured pooling.

- [x] **H-07 · P0 — enforce backend frame bounds on streaming paths.** *(2026-09-09: `[limits] max_backend_frame_bytes`, applied on every relay; slow-drip whole-response deadline tracked under TR-06.)* Follow-up
  inspection during TR-01/05 implementation found that `stream_until_ready` and
  `stream_until_ready_capture` wait for `len + 1` bytes without applying
  `validate_backend_frame_len`. A malformed length below four loops waiting for
  more bytes; an excessive advertised length can grow the accumulator toward the
  backend's requested size. The startup frame cap does not protect these loops.
  Validate each header immediately against an explicit backend-response frame
  budget, use checked arithmetic, and close the offending backend on violation.
  Frame size and total result size are different limits: keep valid large results
  streamable while bounding each incomplete frame and total process memory.
  **Accept:** negative/oversized/truncated lengths and slow-drip payloads cannot
  allocate beyond the configured cap, panic, or remain live beyond the request
  deadline. Cover plain streaming, cache capture, Flush/backend-watch traffic and
  COPY under default/no-default/full feature profiles. Measure framing cost on
  small-result throughput and time-to-first-row; add a bounded-cardinality rejection
  counter. Do not substitute the cache capture cap for the frame accumulator cap.

## Cache work in correctness-first order

- [ ] **C-01 · P1 — aggregate byte budgets, not count × per-entry caps.** Add a
  configurable process cache budget, per-tier and per-tenant allocation, and bounded
  miss-capture/coalescer memory. Include keys, reverse indexes and decompression
  buffers; avoid double charging shared Bytes while retaining a conservative RSS
  guard. Current edge defaults allow 10,000 × 4 MiB ≈ **39.1 GiB of payload** before
  overhead. **Accept:** mixed small/large entries plus concurrency remain within
  budget; replacements, expiry, invalidation and failure reclaim accounting exactly.

- [ ] **C-02 · P0 — commit-aware invalidation for every result-cache tier.** The
  query cache invalidates matching L2 keys after statements; L1 and L3 rely on TTL.
  A read can refill before another session's transaction commits, and COMMIT has no
  table dependencies in `invalidate_query`. Stage write sets until successful
  commit and invalidate all relevant tiers; rolled-back work must not be presented
  as committed history. Subscribe to committed WAL/CDC or a backend event adapter
  for writes that bypass this proxy. **Accept:** two-client read/refill/commit and
  direct-backend-write schedules cannot serve stale values under a strict policy;
  cover simple, extended, COPY, triggers, DDL and unknown dependencies.

- [ ] **C-03 · P1 — recover gaps in invalidation delivery.** SSE buffers currently
  drop full-channel events and disconnected edges rely on TTL. Add monotonic stream
  offsets, cursor acknowledgements and a bounded durable invalidation log. On gaps,
  either replay events or flush affected caches before strict reads resume. Do not
  block the write path on every edge acknowledgement. **Accept:** dropped/duplicated/
  reordered events, saturated subscribers, home restart and replay-log truncation
  have deterministic recovery; strict reads bypass stale caches during recovery.

- [ ] **C-04 · P1 — active single-flight with correct follower semantics.** Elect
  one fetch per complete cache identity *and freshness requirement*, bound waiters
  and keys, and return shared immutable results only after generation validation.
  Cancellation of a follower must not kill the leader; leader cancellation/failure
  must wake followers without a second herd. Uncacheable streams must bypass shared
  replay. **Accept:** an approved burst of 64 identical cacheable requests causes
  one backend fetch per generation; mixed tenants/params never coalesce; invalidation
  during the fetch prevents publishing an obsolete result. Measure collapse ratio.

- [ ] **C-05 · P1 — define per-table freshness as an SLA.** Expose policies such as
  primary/causal, bounded staleness and eventual in daemon config and metrics. For a
  join use the strictest participating policy. A TTL measured from insertion does
  not prove the age of data read from a lagging replica or from an upstream cache;
  carry origin commit watermark and remaining freshness budget across tiers/hops.
  **Accept:** a result's age cannot reset when promoted from home to edge or L2 to
  L1; aged lag samples, missed events and unavailable authority cause the configured
  wait/bypass/failure, never an undocumented stale success.

- [ ] **C-06 · P1 — complete query/session/security cache identity.** L2 CacheKey
  has query hash/database/user/branch but not effective role, search_path, RLS tenant
  state, schema version or session GUC generation; edge uses startup variables but
  these are not a general runtime session-state model. Track identity changes or
  bypass caching when they cannot be captured. Use exact parameter types/values and
  protocol result format in semantic identity. **Accept:** equal SQL under SET ROLE,
  SET search_path, tenant GUC, schema change and different Bind formats cannot share
  an incompatible result. Couple C-04 to this work.

- [ ] **C-07 · P2 — narrower dependencies with conservative fallback.** After C-02/03,
  cache primary-key lookups and bounded ranges with old/new row-key invalidations.
  Handle rows entering/leaving predicates, negative results, joins, aggregates,
  LIMIT/ORDER BY, trigger effects, deletes, updates moving keys and collation. Do not
  infer general SQL predicate validity from string matching. Unknown dependencies
  retain table/schema invalidation. **Accept:** differential tests against uncached
  PG show identical results while unrelated point writes measurably preserve hits.
  Track fan-out and invalidated-bytes per committed write, not only hit rate.

- [ ] **C-08 · P2 — adaptive compression with a CPU break-even gate.** Compress
  sufficiently large cold/warm or cross-region payloads; keep small hot entries
  uncompressed. Use bounded decompression and exact original PG framing. Measure
  LZ4/zstd alternatives and enable only when saved memory/WAN bytes outweigh CPU
  and tail-latency cost. **Accept:** payload round trips, output caps and cancellation
  hold; report compression ratio, CPU/query, WAN bytes and p99 under mixed sizes.

- [ ] **C-09 · P2 — controlled warming and refresh admission.** Warm known frequent
  reads by tenant/region only after schema/ownership/freshness state is valid. Apply
  concurrency/QPS/byte budgets, TTL jitter, failure backoff and a kill switch. Protect
  surviving primaries during regional cold starts. Evaluate frequency-based admission
  (e.g. TinyLFU) against scan pollution before replacing LRU. **Accept:** rolling
  restart improves time-to-hit-rate without exceeding backend or tail-latency budgets;
  one-off scans do not evict the entire hot working set.

## Distributed journal/cache layer and global sharded PG wire

- [ ] **D-01 · P1 design, staged implementation — separate control authority from data.**
  An optional small consensus-backed control plane should own shard maps, writer
  epochs, membership, tenant placement and policy versions. Region-local proxies
  consume immutable snapshots. Keep reads off the consensus path; fail closed for
  writes when authority expires. Cache versions should be `(cluster, shard,
  authority_epoch, commit_position)`, not last-writer-wins wall clocks. HLC is useful
  for causal ordering but not a substitute for quorum/fencing. **Accept:** rolling
  config changes and partition healing cannot route a transaction across epochs.

- [ ] **D-02 · P1 design — durable replay/outcome journal with explicit guarantees.**
  Define stable request and transaction IDs, ordered parameterized operations,
  observed-result hashes, source epoch and lifecycle states (received, dispatched,
  outcome unknown, committed/aborted, acknowledged). Use segmented checksummed logs,
  group commit, bounded retention/compaction, encryption and tenant access control.
  Region-local quorum and asynchronous geographic replication can be defaults;
  cross-region synchronous durability must be a distinct latency/RPO choice.
  **Accept:** proxy crash at every lifecycle transition yields a resolvable outcome
  or an honest unknown response; never silent loss/duplicate replay.

  **Critical design boundary:** an external distributed journal cannot atomically
  commit PostgreSQL effects and its own outcome record. Database participation
  (transactional dedup/outcome rows or a supported extension/engine API) is needed
  to close that uncertainty window. An `08007` result remains necessary otherwise.
  Likewise a journal cannot preserve a client TCP connection when the proxy process
  dies; cross-proxy continuation needs a reconnect/resume protocol or transport
  mechanism, with explicitly supported client behavior.

- [ ] **D-03 · P1 — backend capabilities and a versioned shard map.** PG-wire support
  alone says nothing about replication, snapshots, shard metadata, promotion,
  deduplication or distributed transactions. Define a provider contract for those
  capabilities. Start with transaction-pinned single-shard routing using extracted
  Bind values and stable hashes; send unsupported cross-shard work to the database
  coordinator or reject it explicitly. **Accept:** prepared statements, resharding,
  stale maps and rebalance preserve ownership and transaction placement; every
  backend adapter advertises and tests its supported capability set.

- [ ] **D-04 · P2 — region-aware placement and bounded fan-out.** Route reads to the
  closest sufficiently fresh replica, retain a home region for writes, honor tenant
  residency, and apply hedging only to proven side-effect-free reads within a
  duplicate-work budget. Prefer a database coordinator for cross-shard SQL unless
  HeliosProxy intentionally owns distributed SQL execution. **Accept:** latency,
  stale-replica and region-loss scenarios preserve consistency/residency; scatter
  queries have bounded concurrency/bytes and cancel all children on failure. AVG,
  ORDER BY/LIMIT, collations and joins require semantic tests before proxy fan-out.

- [ ] **D-05 · P1 — independent feature profiles without `ha-tr`.** Keep connection
  pooling, topology, fair admission, shard routing and cache/control-plane support
  usable without journal/admin-replay modules. Clarify the existing unconditional
  `tr_mode` behavior instead of assuming the flag disables it. Add a capabilities
  manifest showing compiled/enabled/wired/unsupported features; reject enabled
  unavailable features and unknown nested settings in an opt-in strict config mode.
  **Accept:** CI exercises default, no-default, replay, routing/cache without ha-tr,
  and complete bundles; settings cannot silently promise absent behavior.

## Performance and validation gates

- [ ] **P-01 · P1 — measure user-path success and tails, not a mean of microbenchmarks.**
  Use the existing 107-case baseline for attribution, then compare identical direct
  PG, HAProxy+Patroni, and HeliosProxy deployments under identical backend capacity,
  TLS, durability, pooling, timeouts and hardware. If adding PgBouncer to the baseline,
  label that combination separately. Report useful committed TPS, p50/p95/p99/p999,
  first-row latency, client error/unknown rates, RSS, CPU and WAN bytes. Retain the
  session's 3% cumulative budget but also gate critical paths individually; no
  unrelated nanosecond improvement should hide a replay/pool regression. **Accept:**
  same-window baseline/candidate results and correctness counters for normal load,
  burst, failover and recovery. “Best” applies only to measured workload dimensions.

- [ ] **P-02 · P2 — measure hot-key and multi-key lock contention before optimizing.**
  Edge map and L1 hit paths update LRU under exclusive locks. O(1) removes scans,
  not contention. Compare sharded maps, admission/sampling or segmented policies
  with byte accounting and reverse-index costs included. Journal contention and
  one-consumer analytics queues need separate overload measurements. **Accept:**
  concurrency at 1/16/64 clients, skewed/uniform keys and mixed reads/writes improves
  tail latency/CPU without incorrect eviction or lost invalidations. Do not raise
  host load beyond approved baseline sizes to chase a headline.

- [ ] **P-03 · P1 — operational observability for continuity and cache trust.** Add
  histograms for detection/reconnect/restore/replay time, queue waits and regional
  latency; counters for replay refusals by reason, uncertain outcomes, journal
  durability lag, cache gap recovery, coalescer leaders/followers, retained bytes and
  invalidation backlog. Existing cache hit/miss/eviction and TR counters remain.
  Export region/backend/policy labels with bounded cardinality, never raw SQL,
  parameters, session IDs or secrets as metric labels. **Accept:** a fault run can
  be explained from metrics, and alerting detects an edge serving outside its SLA.

- [ ] **V-01 · P1 — turn advertised features into reachable acceptance tests.**
  Maintain claim → config/CLI/API → runtime call → actual assertion → environment
  mapping. Surface early-return integration skips as skips; fail a live-required
  job if backend variables are absent. Cover protocol differences and combinations
  (TR × pooling × cache × prepared statements × auth × topology). Update stale TR,
  architecture, feature and demo claims, including library-only modules. **Accept:**
  every advertised daemon capability has a test that fails when its runtime hook is
  removed, rather than merely constructing its configuration struct.

- [ ] **V-02 · P1 — isolated HA/chaos and scale-out laboratory.** Use disposable
  primary/replicas and at least two proxies; preserve a committed-transfer ledger
  and check client-visible results as well as final balances. Test real promotion,
  async WAL loss, synchronous durability, uncertain COMMITs, process death, slow
  clients, TLS/auth rotation, rolling drain, stale configs, control/data partitions,
  regional cold cache and resharding. **Accept:** bounded runtime/resources and
  deterministic cleanup; publish supported failure guarantees plus unmet cases.
  Production-like existing containers/data directories are not this laboratory.

## What to reuse from Lite and Full

Inspection is of local source; these are candidate designs, not independently
certified implementations or a reason to couple Proxy to an entire database build.

| Source | Useful idea | Qualification before reuse |
|---|---|---|
| `../Lite/src/proxy.rs` | Topology/LSN export adapter and backwards-compatible facade | It re-exports the Proxy crate. Lite's DistribCache tests largely exercise this same code, not an independent stronger cache |
| `../Lite/src/replication/shard_router.rs` | Typed shard keys, shard map decisions, coordinator fallback and aggregation model | Reuse routing contracts; validate schema/Bind extraction and transaction ownership at the wire layer |
| `../Full/heliosdb-metadata/src/raft_storage.rs` | Persistent Raft state/log, snapshots and compaction patterns | Audit crash/fsync/recovery and membership semantics before extracting a small control-plane dependency |
| `../Full/heliosdb-cache/src/tiered_cache.rs` | Tier budgets and TinyLFU-inspired admission | Benchmark with PG response payloads and tenant isolation; don't port an entire cache stack blindly |
| `../Full/heliosdb-cache/src/stampede_protection.rs` | Leader/follower miss coordination and cancellation/timeout design | Validate against the C-04 contract; source/test existence is not proof of live proxy coalescing |
| `../Full/heliosdb-cache/src/distributed_sync.rs` | Version vectors, explicit consistency policy and partition vocabulary | Its transport is labeled simulated pub/sub and uses an unbounded in-process channel. Do **not** treat it as an already proven distributed consensus cache |

## Carried over from the 2026-07 next-batch audit

That tracker (`docs/perf-2026-07/NEXT-BATCH-audit.md`) was closed on 2026-09-11
after re-checking every item against the code. Most of it shipped; these survived
and keep their original identifiers so the history stays traceable. Two of its
items need no new entry: **M1/M2** (gateways bypass policy and dial a fresh
authenticated backend per request) are exactly H-06, and the missing per-gateway
connection cap belongs to H-05's admission bounds.

- [ ] **O-01 · P0 — `migration_ready` must not mask apply errors.** `mirror::status`
  computes `lag = enqueued - mirrored - errors` and then reports
  `migration_ready: lag == 0 && dropped == 0` (`src/mirror.rs:65`, `:75`), so a
  mirrored write that failed to apply cancels itself out of the lag and the
  endpoint declares the target safe to cut over. Report readiness only with zero
  errors and zero drops, and expose the error and drop counts in the same payload.
  **Accept:** a run with deliberately failing applies never reports
  `migration_ready: true`; the operator sees why.

- [ ] **O-02 · P1 — one query-timeout contract for the management backend client.**
  `BackendClient::run_query` calls `stream_query_timeout()`, which returns a
  hardcoded 30 s (`src/backend/client.rs:324`), while `BackendConfig.query_timeout`
  is set by every caller (branch clone, mirror, replay, upgrade, the gateways) and
  never read. This is also a gate-5 violation: a timeout that is not tunable.
  **Accept:** the configured value is honored on every management query; a caller
  that sets 5 s times out at 5 s; the default remains 30 s where nothing is set.

- [ ] **O-03 · P1 — bound and actually parallelize shadow execution.**
  `shadow_execute` buffers both backends' complete result sets with no ceiling,
  and despite the doc comment it awaits the primary before starting the shadow
  (`src/shadow_execute/mod.rs:68`), so the shadow adds its full latency to the
  request. Compare under a configurable row/byte budget, degrade to a digest
  beyond it, and run the two concurrently. **Accept:** a large result set compares
  within the budget without unbounded RSS; shadow latency overlaps the primary.

- [ ] **O-04 · P2 — operator replay: a deadline and a lock-free window scan.**
  `TransactionJournal::entries_in_window` clones and sorts every matching entry
  while holding the journal read lock (`src/transaction_journal.rs:414`), and the
  replay driver has no overall deadline — only per-query timeouts. **Accept:** a
  large window neither stalls live journal writers nor runs unbounded; a replay
  that exceeds its deadline stops and reports partial progress.

- [ ] **O-05 · P3 — extended-batch tracking and error-frame allocation.**
  `batch_refs`/`batch_defines` are cleared only when a Sync ends the cycle
  (`src/server.rs:3982`), so a client that never Syncs grows them without bound
  and the re-prepare filter is quadratic over that growth; and
  `create_severity_response` builds a `HashMap` plus four `String`s per error frame
  (`src/server.rs:7576`). Both are cold or adversarial paths, not hot-path costs.
  **Accept:** a never-Sync client has bounded per-session memory; error-frame
  construction allocates once.

## Delivery sequence

1. **Done (1.6.1 + 1.7.0).** TR-01/02/03/05 are closed and session state is bounded
   and restorable (TR-04); replay observations are verified under one recovery
   deadline (TR-06). The boundary probes are green against real PostgreSQL and the
   support matrix in `docs/transaction-replay.md` states what replay does and does
   not promise. What remains in this group is TR-07 (journal and operator replay
   tooling), and the TR-08/TR-09 availability residuals.
2. Wire H-01/02/03/04 and apply admission/observability. Establish real promotion,
   fencing and load-balancing acceptance with and without `ha-tr`.
3. Address C-01/02/03/06, then active coalescing and freshness policy. Optimize
   compression, fine invalidation and warming against measured p99/CPU/WAN costs.
4. Build D-01/02/03 as optional interoperable components with honest consistency and
   resume contracts. Validate multiple proxies/regions/shards before global claims.

This preserves the product's focus: an open-source PG-aware proxy whose continuity,
pooling, routing and cache behavior can be demonstrated under failure. A distributed
journal/cache layer strengthens that design only when it preserves backend commit
truth and bounded resource use.
