# Nano v3.57 — opt-in options to speed up the Proxy workload

**To:** HeliosDB-Nano release owner · **From:** HeliosProxy (audit-2026-06 candidate)
· **Date:** 2026-06-14

Cross-team reply. **Constraint honored:** every item below is *additive + opt-in*
(a `SET helios.*` session param or a gated cargo feature), defaults **OFF =
today's behavior**, and touches only the extended-protocol / pipeline / COPY /
pooled-connection paths — **not** the single-row simple-`Query` OLTP path that
`pg35_benchmark` measures. Nothing changes Nano defaults or OLTP numbers.

Grounded in (1) the 2×2 scalability matrix just run (proxy baseline vs candidate
× Nano 3.57 vs 3.37, see `SCALABILITY-MATRIX.md`) and (2) exactly what the Proxy
issues against Nano on its hot path.

---

## (a) Where Proxy-driven time goes on the Nano side — measured

At c16 through the candidate proxy → Nano 3.57:

| Proxy issues… | tps | vs simple | what costs the delta on Nano |
|---|---:|---:|---|
| simple `Query` (one round-trip) | 93,293 | — | parse+plan+exec+**text-encode** per query |
| extended `Parse/Bind/Describe/Execute/Sync` | 69,658 | **−25%** | **per-statement Parse+plan** (unnamed → re-planned every exec) + more frames |
| prepared (named statement, parsed once) | 84,887 | −9% | parse-once recovers most of it; residual = extra Bind/Execute/Sync frames |

So, prioritized, Proxy→Nano time goes to:
1. **Per-statement parse + plan on the extended path.** The `extended < prepared`
   gap (69.7k → 84.9k) *is* the cost of re-planning. The proxy issues a `Parse`
   for every unique statement; unnamed extended statements (very common) make Nano
   re-parse + re-plan on every execution.
2. **Extra wire frames** in extended vs simple. The proxy already coalesces the
   whole `Parse…Execute` batch into **one network round-trip** at `Sync` (proxy
   Batch B), so the residual `prepared < simple` gap (84.9k → 93.3k) is Nano-side
   per-frame handling, not network.
3. **Connection + session setup.** Each new client = a new backend connection +
   auth handshake + startup-parameter exchange. Dominates under connection churn.
4. **Text result encoding.** Nano text-encodes every column; meaningful for
   numeric/timestamp-heavy analytic (HTAP) result sets. The proxy forwards verbatim.
5. **Per-statement implicit-transaction overhead** for standalone read statements.

The proxy's **health probe is already cheap** (TCP-connect only, every 5s/node —
no query), and the per-query relay is now streaming + allocation-light (Batches
A–E), so the remaining latency is genuinely inside Nano's parse/plan/encode/txn
machinery, which is what these knobs target.

## (b)+(c) Nano knobs/features I wish existed — prioritized

| # | Feature (opt-in) | Est. Proxy speedup | Ties to what the Proxy issues | Wire gap to confirm |
|---|---|---|---|---|
| **P1** | **Server-side prepared-statement plan cache** (`SET helios.plan_cache=on` or gated feature). Cache parse+plan keyed by statement text / named-stmt id. | **+15–25%** on extended-heavy workloads | Proxy sends `Parse` per unique stmt; lets **unnamed** extended stmts reach *prepared* throughput, and shared plans once the proxy pools statements | — (internal) |
| **P2** | **Pipelined extended execution without intermediate flush** — N `Bind/Execute` before one `Sync`, executed in order, one `ReadyForQuery`. | **+15–40%** on batch/ORM-flush/multi-row workloads | Proxy already batches `Parse…Execute` to one `Sync` (Batch B); with a guaranteed ordered no-flush pipeline the proxy can merge client pipelines into one round-trip | Confirm multiple `Execute` before `Sync` with no intermediate RFQ |
| **P3** | **COPY (CopyIn/CopyOut) wire support.** | **2–10× on bulk ingest** vs row-by-row INSERT | Proxy already relays `CopyData/CopyDone` and handles `CopyInResponse/CopyBothResponse` (Batch B). **Critical for the PG→Nano migration mirror (Proxy Batch G2)** which replays high write volume | Confirm Nano speaks `CopyInResponse`/`CopyOutResponse`/`CopyData`/`CopyDone` |
| **P4** | **Cheap session-state reset** (fast `DISCARD ALL`-equivalent / `helios.reset_session()`). | Unlocks **N-clients-over-M-connections pooling** (connection-storm / serverless) | Proxy cross-client pooling (Batch F.3b/C) recycles a backend conn between clients and must reset session state per lease release; a lightweight reset avoids a full-round-trip penalty | — (internal) |
| **P5** | **Binary result-format toggle** (negotiated per portal). | **+5–15%** on wide numeric/temporal result sets (HTAP analytics) | Extended protocol can request binary columns; proxy can pass `result_format=binary` through and skip text encode | Confirm binary encode for int/bigint/float/timestamp/uuid + correct `RowDescription` OIDs (proxy then needs a small binary-DataRow follow-up) |
| **P6** | **Per-session autocommit / implicit-txn fast path** (`SET helios.fast_autocommit=on`, default off). | **+3–8%** on point-read HTAP/OLTP-ish reads | Skips txn bookkeeping for read-only single statements | OLTP-sensitive → strictly opt-in/default-off |
| **P7** | **Faster/cacheable connection setup.** | **+2–5%** on short-lived-connection workloads | Reduces per-connection auth + parameter-exchange cost | — |

### Wire-level capabilities the Proxy needs that Nano may not yet support (verify)
- **Binary format** for params *and* results in the extended protocol (P5); correct `Describe` → `ParameterDescription`/`RowDescription` with binary OIDs.
- **COPY** sub-protocol (P3) — proxy is already wired to relay it.
- **Pipelining**: multiple `Execute` before `Sync` with no intermediate `ReadyForQuery` (P2).
- **Portal `max_rows` / partial fetch** with `PortalSuspended` — proxy can stream; confirm Nano honors a row limit.
- **`ParameterStatus` for `helios.*` GUCs** so the proxy and clients can discover which opt-in features are active.

## Bottom line (what to build first)
1. **P1 plan cache** — highest leverage for the proxy's measured extended/prepared gap; compounds with proxy statement pooling.
2. **P3 COPY** — highest leverage for the upcoming **PG→Nano migration mirror (Proxy Batch G2)**; also the biggest ingest win generally.
3. **P4 cheap reset** — the unlock for cross-client connection pooling.

All opt-in, all default-off = current behavior, none touches the simple-`Query`
single-row OLTP path. Happy to co-design the `helios.*` session-param surface and
wire a proxy capability probe (`ParameterStatus`) so the proxy auto-enables each
feature only when the connected Nano advertises it.
