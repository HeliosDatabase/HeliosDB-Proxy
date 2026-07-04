# Group 2C — Conditional reset (skip DISCARD ALL when provably clean)

The correct replacement for the abandoned async-park idea (M2b). M2b tried to
move the `DISCARD ALL` off the critical path by parking the connection *before*
the reset finished — which destroyed connection reuse (the next query dialed a
fresh backend: TCP + startup + SCRAM, far costlier than the reset it skipped)
and cascaded to `No healthy nodes` at c=64. See the M2b memory note.

**This group removes the reset cost the right way: don't move the reset — skip
it entirely when the connection is provably clean, keeping reuse intact.**

## Design

Opt-in via `pool_mode.skip_clean_reset` (default **false** — exact current
behaviour unless enabled). When on, Transaction/Statement pooling parks a
connection *without* running `reset_query` **iff it is provably clean**:
`!dirty && prepared.is_empty() && unnamed_sig.is_none()`.

- **`BackendConn.dirty`** is set when a forwarded simple-query statement is not
  provably session-neutral, by the conservative classifier
  `stmt_leaves_session_state(sql)`.
- **Extended protocol** (any `Parse`) always leaves `prepared`/`unnamed_sig`
  set, so it is never clean-skipped — conservative by construction. The win is
  on simple-protocol autocommit traffic (which is what `pgbench -S` and many
  read-heavy apps use).

### The classifier (safety-critical)

Biased **hard toward "dirty"** — a false "clean" leaks session state to the next
borrower (a security bug); a false "dirty" merely costs an unnecessary reset.
A statement is clean only if:
1. it is a single statement (any non-trailing `;` → dirty), AND
2. its leading keyword is provably session-neutral (`SELECT`/`INSERT`/`UPDATE`/
   `DELETE`/`WITH`/`VALUES`/`TABLE`/`SHOW`/`EXPLAIN`/`FETCH`/tx-control), AND
3. it contains no session-state constructs: `SELECT … INTO` (whole-word `INTO`
   on SELECT/WITH leads only — `INSERT INTO` is fine), `set_config`,
   `pg_advisory*`, `nextval`/`setval`.

Everything else (`SET`, `CREATE TEMP`, `PREPARE`, `DECLARE`, `LISTEN`,
`DISCARD`, DDL, `COPY`, multi-statement, `;`-in-literal, …) → dirty → full reset.

**Documented limitation:** a `SELECT` that calls a user-defined function which
internally runs `set_config(..., is_local => false)` or takes an advisory lock
via an aliased path is not detectable from SQL text (the direct forms are). This
is why the flag is opt-in and intended for autocommit/simple-protocol workloads.
It is still strictly safer than pgbouncer's transaction-mode default, which runs
no reset at all.

### Invariant

Every parked connection is clean — either reset (dirty path) or skip-parked
(clean path). A reused connection (`BackendConn::new`) starts `dirty=false`.

## Results

- **Correctness:** `stmt_classifier_is_conservative` unit test (30+ cases, every
  state category dirty). Live `conditional-reset-test.sh` 6/6: clean SELECTs
  skip the reset; temp table / prepared statement / GUC / LISTEN state created
  by statement 1 is cleared before the reused connection serves statement 2 (a
  classifier miss would leak it — the test discriminates).
- **Performance (transaction mode, pgbench -S, skip on vs baseline always-reset):**
  c=16 **+48%** (28.5k → 42.1k tps), c=64 **+31%** (40.9k → 53.7k tps) — now
  matching/beating session mode.
- **No regression:** default build byte-identical (flag off compiles the
  classification out of the hot path); default regression 9/9; pool-modes 4/4;
  copy-poolmode 4/4.

## Deferred
Extended-protocol clean-skip (needs per-statement dealloc tracking); surfacing
`resets_skipped` via `/api/pools`.
