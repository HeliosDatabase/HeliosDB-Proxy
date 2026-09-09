# HeliosProxy feature audit — 2026-09-08

**Verdict: 1.6.0 has real in-session replay, but the advertised feature set is not
fully honored by the daemon. Replay has reproducible safety defects. Global HA,
automatic promotion, and general sharded routing are not established capabilities.**

Audited source: `1dca229` (`heliosdb-proxy` 1.6.0). This is an audit and a prioritized
implementation backlog, not a claim that the defects below have been fixed.
See [IMPROVEMENTS.md](IMPROVEMENTS.md) for proposed changes and acceptance criteria,
including the review of Sonnet's six suggestions.

Implementation has started with [TR-01 commit-outcome safeguards](TR-01.md).
The evidence below remains the original baseline; candidate results are separate.

## Handoff reconciled

Read Claude Code's latest Proxy transcript and `/home/gpc/HDB/sprint/status/Proxy.md`.
The last substantive code/validation handoff was September 4; the September 6 update
confirmed Lite's 1.6.0 dependency pin. The transcript file's newer modification date
does not mean a newer implementation was delivered.

Claude reported the September performance/stability batch and F3 in-session replay
shipped in 1.6.0, 77 live replay checks passed, and the September 4 Criterion run
matched 92 previous cases with mean median delta +1.59%; 107 cases were recorded.
The source, changelog, and `benches/BASELINE.md` contain these changes/results.
These are historical measurements, not a new performance result from this audit.

The important qualifications are:

- `scripts/regress/tr-failover-test.sh` kills a TCP relay in front of the **same**
  PostgreSQL database used as the replacement route. It tests connection loss and
  recovery, not replica catch-up, real promotion, fencing, or a regional partition.
- `src/server.rs::TrSession`, `tr_decide`, and `tr_handle_fault` are unconditional.
  In-session `tr_mode` recovery works without `ha-tr`, and `tr_enabled = false`
  does not disable it. The flag gates separate journal/replay/migration modules.
- `docs/transaction-replay.md` describes the pre-F3 implementation. README's TR row
  is newer, but several neighboring HA/routing rows describe library components as
  though the standalone daemon invokes them.
- A single mean across microbenchmarks can conceal critical-path regressions.
  Claude's baseline itself records localized regressions and host variance.

## Verification performed

All builds/tests ran sequentially under the fleet build lock and a systemd scope
with `MemoryMax=24G`, `MemorySwapMax=0`; two build jobs and two test threads.
The existing integration target directory was used only from this checkout under
that lock. Fresh daemon binaries were copied out for independent wire probes.

| Command (all Cargo commands used `--locked`) | Result | Library tests |
|---|---|---:|
| `cargo test` | Pass | 404 |
| `cargo test --features ha-tr` | Pass | 490 |
| `cargo test --features all-features` | Pass | 1698 |
| `cargo test --features all-features,postgres-topology` | Pass | 1699 |
| `cargo test --no-default-features` | Pass, existing `BackendConn::dirty` dead-code warning | 323 |
| `cargo fmt --check` | Pass | — |
| `cargo clippy --features all-features -- -D warnings` | Pass | — |
| `cargo +1.86 check --features msrv-features` | Pass | — |

The integration target reports 45 passing and three ignored cases per configuration.
**Do not interpret those 45 as 45 live feature validations:** without
`HELIOS_TEST_PG_HOST`, several tests return early; others test configuration/types.
The ignored cases require Kubernetes, Terraform, and Pulumi. Feature-dependent
documentation examples are also ignored (12 with all-features, one with defaults,
none with no defaults). The all-features configurations run five WASM E2E tests.
No skips were added by this audit.

The ordinary `all-features` bundle is not Cargo's `--all-features`: it excludes
topology providers and `observability`. This audit tested the documented CI matrix
plus no-default-features; it does not certify every possible feature combination.

Raw Cargo logs: `/tmp/proxy-audit-20260908/`. A compact durable record lives in
[evidence/gates.json](evidence/gates.json). No runtime Rust code was changed, and no
new throughput benchmark was run. Existing databases/services were not modified.

### New adversarial wire checks

[tr-boundary-test.py](../../../scripts/regress/tr-boundary-test.py) runs a real proxy
against two disposable loopback PG-wire fixtures. It captures client frames and
the exact SQL sent to the replacement. Fixtures do not implement a database:
these are protocol/orchestration reproductions, **not measured duplicate database
commits or a replication test**. The unsafe repeated dispatch is directly observed.

```sh
python3 scripts/regress/tr-boundary-test.py /path/to/heliosdb-proxy --output /tmp/tr-boundaries.json
```

Exit 1 means a safety assertion failed; exit 2 means the harness could not run.
These failing assertions are retained as an explicit regression target, not hidden
behind expected-failure annotations or added to CI as a green test.

| Probe | Observed behavior | Safety invariant |
|---|---|---|
| Plain `COMMIT`, response lost | `08007`; no replay | Pass |
| `/* audit */ COMMIT`, response lost | Replacement receives `BEGIN`, prior INSERT, commented COMMIT; client receives success | **Fail** |
| Extended batch `SELECT 1` then `COMMIT`, response lost | Replacement receives BEGIN, prior INSERT, SELECT, COMMIT; client receives success | **Fail** |
| SELECT fails after RowDescription + one DataRow | Client frame tags `TDTDDCZ`: two descriptions and three rows for a two-row replacement response | **Fail** |
| `SELECT audit_side_effect()` loses response | SELECT is dispatched again | **Fail**: unproven function safety |
| SET tracking cap = 1; two successful SETs | Replacement restores only the older SET and remains usable | **Fail** |
| SET after SAVEPOINT, then ROLLBACK TO and COMMIT | Rolled-back SET is restored after failover | **Fail** |
| Transactional RESET ALL followed by ROLLBACK | Previously established SET is missing from restoration | **Fail** |
| `tr_mode = none` control | `57P01`; no replay | Pass |
| Ordinary explicit transaction recovery control | BEGIN, INSERT, interrupted SELECT sent to replacement | Pass |

Results and binary SHA-256 values are recorded for
[default](evidence/tr-boundaries-default.json),
[all-features + postgres-topology](evidence/tr-boundaries-all-pg.json), and
[no-default-features](evidence/tr-boundaries-no-default.json).
The pre-existing release binary was also probed; its result is saved separately as
[historical-binary evidence](evidence/tr-boundaries-existing-release.json), without
substituting its provenance for the fresh builds.

## Findings that block a stronger replay guarantee

**TR-01 — unknown COMMIT protection has lexical and batch bypasses (P0).**
`server.rs::tr_classify` examines the leading keyword without stripping SQL comments.
`tr_extended_sql` resolves only the first SQL in an extended batch. Consequently,
both cases above miss `StmtKind::Commit`. `tr_decide` then permits replay of the
explicit transaction. PostgreSQL treats comments as whitespace, so the first case
is valid ordinary COMMIT syntax. Fix the full request classifier and preserve the
unknown-outcome response across all SQL spellings and every Execute in a batch.
[PostgreSQL lexical rules](https://www.postgresql.org/docs/current/sql-syntax-lexical.html).

**TR-02 — already-delivered results are not accounted for (P0).**
`stream_until_ready` and its capture variant forward complete frames immediately.
The fault handed to `tr_handle_fault` carries no count of already-visible response
frames. Its Flush-cycle guard does not cover the demonstrated simple-query case.
Resending a result prefix cannot be transparent to a streaming client.

**TR-03 — SELECT is not proof of repeatability or absence of side effects (P0).**
`tr_classify` excludes some built-ins (`nextval`, `setval`) in leading SELECTs,
but accepts arbitrary functions and accepts read-leading WITH without that check.
A volatile PostgreSQL function can modify data; an unknown-outcome autocommit call
can therefore duplicate effects. A function-safety contract must precede retry.
[PostgreSQL volatility rules](https://www.postgresql.org/docs/current/xfunc-volatility.html).

**TR-04 — SET tracking does not reproduce transaction semantics (P1).**
`tr_after_response` appends transaction SETs to `pending_tx_gucs` without savepoint
rollback bookkeeping and clears prior GUCs immediately on RESET ALL. Extended SETs
are explicitly excluded. The cap logs incomplete restoration but recovery proceeds.
Moreover, `pending_tx_gucs` is not bounded by the session SET cap: a long explicit
transaction can accumulate it even in default session mode. The wire probes cover
the first two cases and cap behavior; the unbounded list and extended exclusion are
source findings.

**TR-05 — partial extended writes need an uncertainty state (P0, source finding).**
`forward_extended_batch` classifies `write_all(batch)` failure as `NotDelivered`.
Earlier complete Parse/Bind/Execute messages in a partially written batch may have
executed. A complete-frame Query has different semantics; do not blindly assign
the same delivery rule to both. Add byte/frame-boundary fault injection before
changing this classification.

**TR-06 — replay does not verify observations already used by the client (P1).**
`tr_replay_transaction` discards prior responses and checks rejection/failed state,
not equality with prior rows, command tags, or affected-row counts. Even syntactically
deterministic SQL can observe different committed data after reconnecting. The
record also lacks a verified source timeline/LSN and a proof of old-writer fencing.
The current opt-in qualification is necessary but insufficient for a general
application-continuity guarantee.

**TR-07 — the three replay mechanisms have different guarantees (P1).**
In-session replay uses `ClientSession::tx_state`. `journal_write` is a separate,
post-response, SQL-only best-effort record with a synthetic node id and LSN zero;
the shared journal is not a durable write-ahead commit log. `/api/replay` iterates
timestamp-window entries on one target connection and continues after errors; it
does not reconstruct committed transactions or identify rolled-back source writes.
The library `FailoverReplay` opens a connection per statement, skips transaction
control, and succeeds without execution if a backend is missing. Its orchestration
must not be advertised as equivalent to the new live-session mechanism.

## Advertised feature reachability

“Wired” below means a daemon construction/call path exists. It does not certify
every interaction, engine, workload, or failure mode. Library tests alone cannot
establish daemon behavior.

| Capability | Current daemon reality / evidence | Audit disposition |
|---|---|---|
| PG-wire relay, extended batches, COPY | `handle_client`, forwarding/stream functions; many unit and historical live checks | Wired; new replay stream/batch defects above |
| Session/transaction/statement pooling | `pool-modes`, `ensure_conn`, lease return/reset paths | Wired; fairness and fleet-wide connection budgets still need load gates |
| Read/write split | `choose_target_node`, `select_read_node` | Wired |
| Least-connections / latency read strategies | Daemon `select_read_node` unconditionally uses an RR counter; `load_balancer.read_strategy` is not read in `server.rs` | **Advertised configuration not honored**; library strategies exist |
| Health query / recovery threshold | Daemon sends SSLRequest; does not run `check_query`; a single successful probe marks healthy regardless of `success_threshold` | **Partial configuration contract** |
| Automatic primary promotion / candidate ranking | `FailoverController` has no daemon construction site | Library capability; **not automatic daemon promotion** |
| PostgreSQL/Helios topology discovery | Providers exist in `primary_tracker.rs`; daemon still selects configured roles | **Not connected to daemon routing** |
| Lag-based eligibility | Daemon health `replication_lag_bytes` stays None; unknown lag is allowed | Threshold lacks a live measurement source |
| Read-your-writes | A timed primary-stickiness window is wired | Not a cross-proxy LSN causal token or a global freshness guarantee |
| In-session replay | Unconditional core path | Works on controls; seven failing safety probes |
| Session migration / cursors / planned switchover | Separate library APIs; live replay restores a limited SET log and lazy prepares | Full session/cursor restoration not wired to daemon failover |
| Time-window replay / shadow | `ha-tr` admin handlers and backend clients | Wired operator tools; distinct from HA guarantees |
| INSERT coalescing | `InsertBatcher` has no daemon construction site | Library facility; README overstates automatic behavior |
| Query cache | Simple-query L1/L2 path wired; L3/advanced config differs from library surface | Needs commit-aware invalidation, context identity and active miss coalescing |
| Edge cache | Real PG forwarding and SSE invalidation; O(1) LRU, reverse table index, home counter/epoch | Works as best-effort TTL cache; no consensus, aggregate byte budget or gap replay |
| DistribCache | Optional library with TCP peers; no daemon configuration/construction | Experimental library, already labeled in README |
| Routing hints / rewrite | Runtime simple-query hooks; some extended hints | Per-hint/per-protocol parity must be explicit |
| Schema routing / sharding | Daemon uses analyzer to choose configured analytics node | General shard routing/fan-out is not wired |
| Rate limit / circuit / analytics / anomaly / tenancy | Feature-conditional construction and query hooks | Wired subsets; cross-interface/protocol equivalence not established |
| Auth / TLS / HBA | Runtime wire paths, including fresh backend SCRAM | Auth-proxy library's JWT/OAuth/LDAP surfaces are not all equivalent to PG login |
| WASM / plugin registry | Runtime simple-query hooks, install/signature tooling and tests | Wired subset; Branch route explicitly falls back; extended parity needs validation |
| HTTP SQL / GraphQL / MCP | Runtime listeners; direct BackendClient execution | Real endpoints, but shared pooling/TR/policy parity is not established |
| Mirror / migration / branch / admin / drain | Runtime endpoints and tasks; historical regress scripts | Present; require isolated end-to-end acceptance for each promise |
| Distributed/multi-DC HA and sharded PG wire | No daemon shard-map/authority epoch/causal-token protocol | Proposed direction, not shipped proof |

## Boundaries of this audit

This pass independently validates compilation, existing tests, source reachability,
and adversarial wire recovery. It does not re-run the historical live SQL battery,
kill a database, promote a real standby, exercise managed PG vendors, or certify
geographic scale. Those need the isolated acceptance environment in the backlog.
The documented live harnesses target existing services and require explicit
operator approval under the local host rules; they were not needed to reproduce
the protocol defects and were not run.

“Best open-source all-in-one” remains a measurable product objective. Today it
would be inaccurate to claim verified replacement of HAProxy plus Patroni:
HAProxy documents active checks/load balancing, and Patroni documents a DCS-based
multi-DC design and watchdog fencing. The proxy currently does not wire comparable
promotion authority into its daemon. Its differentiator should be **safe PG-aware
continuity plus efficient routing/cache/pooling on top of explicit HA authority**.
[HAProxy checks](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/reliability/health-checks/),
[HAProxy balancing](https://www.haproxy.com/documentation/haproxy-configuration-tutorials/proxying-essentials/configuration-basics/backends/),
[Patroni multi-DC design](https://patroni.readthedocs.io/en/latest/ha_multi_dc.html),
[Patroni watchdog fencing](https://patroni.readthedocs.io/en/latest/watchdog.html).
