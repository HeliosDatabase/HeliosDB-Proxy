# BATCH F — Table-stakes features (adoption blockers)

> Generated from the 2026-06-10 deep audit (77-agent workflow, adversarially verified). 
> Raw data: `docs/audit-2026-06/audit-result.json`.

**Goal:** Capabilities every serious deployment requires before HeliosProxy can sit in a regulated or driver-diverse data path. Judge-ranked from a 30-proposal, 4-lens ideation panel; none exist in-tree today (verified in-source).

**Parallel-execution compatibility:** PARALLEL-friendly across items (different modules), except F4 after B.

**Files touched:** new modules + `src/server.rs` startup path + `src/auth/`

**Conflicts with:** F items touch the startup/TLS path — independent of A–D except F4 (extended prepared statements) which REQUIRES BATCH B.

**Acceptance criteria:**

- Each feature ships with a demo under demos/, docs, and integration tests, per repo convention.

---

## Items (judge score in brackets)

### [95] Client-Facing TLS Termination + mTLS/SPIFFE Identity

- **Effort:** M  |  **Lens:** competitive + enterprise-ops (merged)
- **Pitch:** Terminate rustls TLS on the client listener (SNI, hot cert reload) and authenticate workloads by mTLS/SPIFFE SVID, with a FIPS-capable build flag as the enterprise tail.
- **Why it wins:** Verified hard blocker: src/server.rs:905-908 rejects every SSLRequest with 'N', so no compliance review or managed-PG migration can pass — every competitor terminates client TLS. Feasibility is exceptional because rustls/tokio-rustls already ship and src/backend/tls.rs has a working TLS client; it is wiring, not new dependency work. It also unlocks the entire regulated-buyer segment that makes every other enterprise feature sellable, and SPIFFE/cert-to-role mapping compounds straight into auth-proxy and multi-tenancy.
- **Builds on:** src/backend/tls.rs, src/server.rs startup phase, rustls/tokio-rustls deps, src/auth/role_mapper.rs, multi-tenancy identification, plugins/hot_reload.rs watcher pattern for cert rotation

### [91] SCRAM Verifier Auth + auth_query + pg_hba Rules

- **Effort:** M  |  **Lens:** competitive
- **Pitch:** Proxy-terminated SCRAM-SHA-256 with verifiers fetched via auth_query plus pg_hba-style host/db/user rules — so transaction pooling works against real production auth instead of trust-like passthrough.
- **Why it wins:** SCRAM passthrough fundamentally breaks transaction pooling (the proxy cannot mint backend connections without credentials), making this the second hard adoption gate after TLS. The hard crypto already exists — a tested SCRAM state machine with PBKDF2/HMAC in src/backend/auth.rs that inverts into the server-side verifier — and src/auth/ supplies the rule-engine scaffolding. PgBouncer/PgCat/Odyssey/Supavisor all have this; without it HeliosProxy loses every pooler bake-off at the first step.
- **Builds on:** src/backend/auth.rs (SCRAM state machine), src/auth/{credentials,role_mapper,handler}.rs, multi-tenancy tenant identification

### [88] Query Cancellation Forwarding

- **Effort:** S  |  **Lens:** competitive
- **Pitch:** Issue proxy-generated BackendKeyData, keep a key-to-backend map, and forward client CancelRequest to the right node — so Ctrl-C actually kills the runaway query.
- **Why it wins:** Verified gap: src/server.rs:936 silently closes the connection on CancelRequest, so driver timeouts and psql Ctrl-C do nothing while the runaway query burns a pooled backend. Half the plumbing exists (src/backend/client.rs already caches BackendKeyData for exactly this purpose). Smallest effort on the board with disproportionate credibility — every pooler evaluation tests cancellation, and Supavisor markets it as a headline feature. The portfolio's quick win.
- **Builds on:** src/backend/client.rs BackendKeyData cache, src/protocol.rs CancelRequest decode, connection pool session registry

<details><summary>Full original rationale (competitive lens)</summary>

src/server.rs:936 currently handles CancelRequest with 'just close connection' — a client pressing Ctrl-C in psql, or any driver timeout-triggered cancel, does nothing, and the runaway query keeps burning the pooled backend. Cancellation is so table-stakes that Supavisor lists 'query cancellation' as a headline feature, PgBouncer has carried it since inception (and gated it behind auth checks after the 1.25.x KILL_CLIENT CVE-2026-6667 review), and Odyssey/pgpool support it. Half the plumbing exists: src/backend/client.rs already caches BackendKeyData 'for potential cancel requests' (line 65). Needed: issue proxy-generated keys to clients, keep a key→backend map, and open a cancel connection to the right node. Small effort, disproportionate credibility — every pooler bake-off tests Ctrl-C.

</details>

### [87] Extended-Protocol Prepared Statements + Binary Format Fidelity

- **Effort:** L  |  **Lens:** competitive
- **Pitch:** Protocol-level named prepared-statement remapping across backend swaps in transaction mode, plus binary Parse/Bind/DataRow end-to-end — the bar PgBouncer 1.25 now enables by default.
- **Why it wins:** Every ORM (Npgsql, JDBC, asyncpg) uses extended protocol by default, and PgBouncer made transaction-mode prepared statements default-on — this is now table stakes for any real workload. The compounding is unusually deep: src/backend/mod.rs:17 confirms text-only I/O, which silently undercuts flagship features — pgvector embeddings travel binary (the pgvector-router market) and ha-tr replay cannot faithfully journal binary params. Fixing it hardens replay, shadow-diff, cache, and the vector story simultaneously; pin-cause diagnostics via query-analytics beat RDS Proxy's notorious pinning opacity.
- **Builds on:** src/pool/prepared.rs tracker, src/pipeline.rs, src/protocol.rs Parse/Bind/Execute codec, src/backend/client.rs, ha-tr transaction journal, query-analytics

<details><summary>Full original rationale (competitive lens)</summary>

PgBouncer 1.21 made protocol-level prepared statements in transaction mode its most-requested feature ever (15-250% throughput gains), and as of 1.25.x it is ON by default (max_prepared_statements=200); Supavisor ships named prepared statement support too — so any ORM workload (Npgsql, JDBC, asyncpg all use extended protocol by default) now expects this to just work. HeliosProxy's PreparedStatementTracker (src/pool/prepared.rs) tracks name/SQL/param-OIDs for re-creation but there is no client-visible name remapping or cross-backend Parse replay at the protocol level. Worse, src/backend/mod.rs:17 documents the hand-rolled backend as 'Text format only' — binary Bind parameters and binary result rows are unsupported in the replay/shadow/cache paths. That directly undercuts flagship features: pgvector embeddings (the pgvector-router plugin's whole market) are typically transmitted binary, and ha-tr transaction replay cannot faithfully journal binary params. RDS Proxy's notorious pinning pain (any >16KB statement pins; no pinning filters for PG) is the adoption wedge: do this right with pin-cause diagnostics from query-analytics and HeliosProxy beats the managed incumbent.

</details>
