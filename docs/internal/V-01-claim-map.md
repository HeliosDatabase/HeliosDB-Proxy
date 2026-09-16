# V-01 — advertised capabilities and their acceptance evidence

**Status:** first slice 2026-09-15. Companion to `GET /capabilities` (D-05): the manifest
says what is compiled/enabled/wired; this file records *where each claim is actually
exercised*, so a claim without a test is visible.

| Capability | Runtime hook asserted by | Behavioural coverage |
|---|---|---|
| `pool-modes` | `server::tests::enabled_capabilities_are_actually_wired` (+ `pool_manager` presence) | `pool::*` unit tests; `scripts/regress/pool-modes-test.sh` (live, PG; Nano SKIPs capabilities) |
| `query-cache` | same test (`state.query_cache`) | `cache::*` unit tests; `server::tests::query_cache::*` (incl. C-02 commit-aware plan) |
| `routing-hints` | same test (`state.hint_parser`) | `routing::hint_parser` tests; `routing` Criterion bench |
| `rate-limiting` | same test (`state.rate_limiter`) | `rate_limit::*` tests |
| `circuit-breaker` | same test (`state.circuit_breaker`) | `circuit_breaker::*` tests; `/api/circuit` admin test |
| `query-analytics` | same test (`state.analytics`) | `analytics::*` tests; `scripts/regress/analytics-test.sh` (live; slow-query sub-check SKIPs without `pg_sleep`) |
| `lag-routing` | policy path (`lag_excludes_standby`, `advance_health`) | `server::tests::lag_policy_*`, `lag_exclusion_thresholds`; `lag::*` unit tests |
| `multi-tenancy` | construction tests | `multi_tenancy::*` tests |
| `auth-proxy` / TR auth boundary | construction tests | `auth_scram`/`auth` tests |
| `query-rewriting` | construction tests | `rewriter::*` tests |
| `wasm-plugins` | construction tests + E2E | `tests/wasm_plugin_e2e.rs` (5 cases under all-features) |
| `graphql-gateway` | gateway construction tests | `graphql::*` tests |
| `schema-routing` | construction tests | `schema_routing::*` tests |
| `edge-proxy` | construction tests | `edge::*` tests; `/api/edge` admin tests |
| `postgres-topology` | `build_primary_tracker` wiring test | `primary_tracker::*` (lease/epoch) + `server::tests::authoritative_leader_*` |
| `mcp` / `http-gateway` | config-gated spawn + gateway tests | `mcp` dispatch tests; `gateway_pool` pool tests |
| `observability` | manifest reports `wired: false` | n/a by design (no-op); `/metrics` covered by admin tests |
| `distribcache` / `ldap-auth` / `heliosdb-topology` | manifest reports `wired: false` | library tests only; documented as not daemon-wired |

## Live-test skips are explicit

`tests/integration/fixture.rs` returns `None` when the backend env vars are unset
(so a developer without a backend still gets a green unit run). Setting
`HELIOS_REQUIRE_LIVE=1` makes a missing `HELIOS_TEST_PG_HOST` / `HELIOS_TEST_STANDBY_HOST`
a hard panic, so a "live validation" CI job cannot pass by skipping everything.

## Remaining V-01 work

- Turn the table into a machine-checked map (each `wired: true` capability must name a
  test target) rather than prose.
- Combination coverage (TR × pooling × cache × prepared statements × auth × topology) is
  still partial; the isolated laboratory is V-02.
- Several construction-only rows above prove reachability, not behavior under fault;
  V-02 extends them.
