# Proxy completion before 3.0

Owner objective (2026-11-07): continue the open items and finalize everything
before starting 3.0. Starting main: `93fb08f`. Working branch:
`fix/pre3-followups-20261107`. No 3.0 API/module retirement is included here.

## Order and acceptance

1. Reconcile umbrella `ff8db286f8a7` and fix correctness/security gaps first.
2. Correct shipped examples and website configuration; complete missing CI legs.
3. Complete cache interfaces, tenant-budget boundaries and remaining follow-ups.
4. Implement P4 peer cache (`81e181bdd7c3`) after its correctness prerequisites,
   with logical invalidation, authenticated bounded transport and TLS.
5. Complete Nano/Lite acceptance and release 2.5.0 after the existing Nano hold.

Every code item needs its regression, relevant feature matrix, lint/fmt/MSRV,
live integration when applicable, and Criterion comparison. Relay/pool/write-path
changes and release additionally need the live user-path gate. Heavy jobs run
sequentially under the fleet lock with a 24 GiB/no-swap scope. Existing failures
and skipped backend coverage remain explicit; no static-only completion claims.

## Umbrella reconciliation

| Topic | Evidence at 93fb08f | Remaining work |
|---|---|---|
| Cache refresh | P3 `918edffa62e1` done; final batch gate | None for the existing P3 scope |
| Per-tier metrics, optional compression deps | P1 `7c860e8a2e14` done; final batch gate | None for P1 scope |
| Gateway tenant caps | `1d73ba306c92` done; final batch gate | None for that item's scope |
| Gateway writes / EXPLAIN ANALYZE | `6a0c6c34d27a`, `73f84de3ded3` done | None for those scopes |
| Extended-protocol read cache | docs/configuration.md explicitly bypasses it | Implement safely with parameter and result-format identity |
| Gateway SQL result cache | Tool cache exists; SQL gateways still bypass result cache | Design and validate identity/policy parity |
| cache_tables / cache=l1,l2,all | src/cache/hints.rs explicitly says ignored | Honor safe tier selection and additive dependencies |
| Dependency extraction | Quote-aware scanners landed; arbitrary views/functions need backend dependencies | Live dependency drill and safe handling |
| Legacy FunctionCall / catch-all | src/server/client.rs forwards raw messages outside query policy | Close policy/admission/cache/replay bypass; raw-wire regression |
| Semaphore cap validation | Node/reconnect/pool cap can exceed Tokio bound | `cb4a60eb2d87`: reproduced startup panic, fix awaiting gates |
| Tenant SIGHUP caps | src/server/mod.rs intentionally preserves startup settings | Implement shared cap resizing/debt semantics, or explicit owner disposition |
| Extended 2PC exemption | tenant_gate_exempt handles simple protocol only | Add cycle-aware exemption without admitting other work |
| COPY stall deadline | Idle deadlines explicitly exclude streaming COPY | Add configurable stall timeout and teardown coverage |
| Bare idle Flush | Already consumed before forwarding for every session; existing capped/uncapped/timeout regressions in tenant_budgets.rs | Re-run existing regressions; no code change required |
| Journal memory per tenant | Replay-byte budget excludes recovery journal | Add journal memory accounting/bounds |
| Multi-tenant examples | Prior release binary fails parsing tenants array-of-tables | `83438f1cbfa2`: corrected examples + parser coverage, awaiting gates |
| Extra CI lint legs | msrv-features and wasm-only absent | `7eaf74978b2e`: added, awaiting exact lint runs |
| Reprepare clone-collect | src/server/client.rs still clones batch_refs | Optimize ownership without changing prepared-statement replay |
| Old distribcache / public API cleanup | Explicit 3.0 work | Deferred to 3.0, `e174b742a1ae` |

## Dependencies and other open items

- Website `781d033cf154`: live page still has stale config examples. No local source
  found; requested its source location. Prepare a patch before publication.
- Host `c9ad6dee52fa`: owner restored Docker chains, backend network arrangement and
  recurrence guard remain. No daemon/network changes during other sessions' gates.
- Nano `9be5815eb534`: branch count(*) issue is OPEN in Sprinter. Release remains held.
- Nano `6bc4a4040eb7`: branch isolation IN_PROGRESS; branch acceptance needs B1+.
- Lite `702736ebceac`: separate process, remove Rust dependency, use GET /api/stats.
- Lite `a7bc90bc2ac2`: recovery/WAL functions and promotion pair drill.
- Final batch performance evidence has attribution exceptions; repeat full Criterion
  and user-path gates on the final pre-release tree, retaining all raw samples.

## Current evidence

`/home/gpc/HDB/sprint/baselines/proxy/pre3-20261107/`

- `example-original-runtime.log`: original multi-tenant config rejected by release
  candidate before startup (`invalid type: map, expected a string`).
- `oversized-node-before.log`: prior candidate panics in Tokio semaphore construction.
- Build/test run queued on the fleet lock while Nano validation owns it.

This is an active checklist, not a completion report. Sprinter remains the status
of record; append gate evidence there before moving any item to fixed/done.

## GitHub backlog reconciliation

GitHub has one open issue, #54, the September audit umbrella. Its 2026-10-15
comment explicitly retains C-04/C-05/C-06/C-07..C-09, P-02 and D-01..D-04.
The checkboxes in IMPROVEMENTS.md are older than multiple shipped fixes; reconcile
against runtime evidence instead of treating every unchecked line as unimplemented.

C-06 is now independently tracked as `5e59bf2492c5`: cache identity must isolate
startup/session/security settings or bypass unknown contexts. It is a prerequisite
for the extended/gateway/peer cache work. Design notes (not validation) are in
`pre3-20261107/C06-DESIGN-NOTES.md` in the evidence directory. P4 clock/transport
prerequisites are in `P4-PREREQUISITES.md`.

The owner was asked whether the entirely new D-01..D-04 architecture projects are
part of this pre-3.0 implementation scope, or separately scheduled proposals.
Concrete existing-product fixes continue while that optional scope answer is pending.

COPY work is prepared separately on `fix/pre3-copy-stall-20261107`, with an
unfixed control worktree carrying only its wire regressions. Its focused gates
are queued under the fleet lock; per-tenant recovery-journal accounting remains open.

## Validation progress (2026-11-07, still unaccepted)

First batch frozen diff SHA256:
`fa88a0daad9f11c9b3f4a95224dc49838fadcdc34248b4e00faef9d6f680b079`.
Targeted tests 1061/0; matrix no-default 860/0, default 948/0, ha-tr
948/0, all-features 2752/0, all-features+postgres-topology 2757/0.
Seven Clippy profiles, fmt, diff check, MSRV 1.86 and benchmark compilation pass.
Plain release control/candidate binaries built; live integration, live drills,
simple/prepared user-path and full Criterion gates remain queued/running. No merge.

Website: eight corrected replacement blocks pass the real strict config loader.
Offline HTML and patch are ready in the evidence directory; deployment source is
still unknown, and no live page has been changed.

C-06: native PostgreSQL comparisons reproduced four wrong-result cases, each
with a matching uncached proxy control (role/RLS, tenant GUC, runtime search_path,
startup options). Separate candidate namespaces shared caches by all startup
parameters and permanently bypasses result caching after context-changing or
opaque statements. Focused gates queued; no fix acceptance yet. Hidden catalog
function/view dependencies remain separately open.

COPY: r1/r2 focused runs caught missing handling of delayed extended COPY
responses. r3 observes CopyIn/CopyBoth in the common frame path and re-arms the
client deadline on a COPY state transition. Focused gates queued; journal-memory
bounds remain open. Raw failed runs are retained.

Single-feature lexer cfg cleanup is tracked as `316065dd80fa`; query-cache-only,
edge-only, tenancy-only, routing-only and no-default test/lint checks are queued
on its isolated worktree. This does not change the frozen first batch.

## Native results and combined validation candidate

- First batch: live PostgreSQL integration 51/0; all 12 existing standalone live
  regression drills pass. Simple/prepared throughput and Criterion remain pending.
- C-06: exact `93fb08f` base returns wrong rows in 7/7 native identity cases;
  the isolated candidate matches PostgreSQL in all seven and still records a real
  neutral-read cache hit. Cache/server focused suite 366/0, Clippy/fmt pass.
  Evidence `pre3-cache-identity-native-20261107-r4/`. Opaque catalog dependencies
  remain open; this is not full release acceptance.
- SQL-feature lint cleanup: all five isolated Clippy profiles pass; every enabled
  lexer feature runs nonzero tests. Evidence `pre3-subset-lints-20261107-r2/`.
- COPY native checks prove rollback/backend exit/capacity recovery for all four
  stall variants and ongoing-input progress. Backend cancellation exposed the
  existing deliberate mid-COPY watch suppression; the opt-in timeout must watch
  and settle the backend error/RFQ instead of calling it stalled client input.
- `49f43bff8122`: separately tracks journal completion metadata lost across Flush
  and COPY yields, and stale execute positions after read-only cycles. New old-API
  wire regression is queued on both an unfixed control and a candidate. Metadata
  prefixes use the existing max_pending_bytes cap; tenant-wide journal accounting
  is still open under `74b9dcc82bba`.

A cumulative candidate is being assembled separately at
`/home/gpc/HDB/worktrees/proxy-pre3-integrated-20261107` on
`fix/pre3-integrated-20261107` (base `93fb08f`), leaving the first batch frozen.
It combines the first batch, cache identity, COPY, feature guards, journal fixes,
and admin policy counters without changing the public metrics snapshot shape.
It is not accepted, committed, merged or published. Its full gates still need to run.

## Continuation update (2026-11-08, cumulative acceptance still open)

The integrated candidate remains at base `93fb08f`; its current source diff
SHA256 is `43d21d8b5ce60ce35e048110cfdf3fb3cf1a70c69b646a316360711b99e1656b`.
Default library validation passed 905/0 and all-features library validation
passed 2769/0. `cargo fmt --all --check` and `git diff --check` passed. These
results were appended as partial GATE-RECORDs to Sprinter items
`4fc4d25adde5`, `83438f1cbfa2`, and `cb4a60eb2d87`; all remain in_progress.

The no-default matrix was queued but did not acquire the shared build lock.
At the last live process check, Nano gate `nano-fk-r2-fix-native-20261108-v1`
still held the lock and its cargo test process remained live. No Proxy build
was allowed to overlap it.

Simple-Query R1 preserved a real five-red/two-positive runtime baseline, but
its native-before fixture failed before issuing a case because it created
`audit/audit` while the referenced cache identity helper connects as
`bench/benchdb`. R1 is retained unchanged. R2 was copied with only the native
fixture user/database aligned to `bench/benchdb`; the candidate diff remains
`c44d45da4405c54204f80b84f2c030d0dbe55ef9490b037d73a65b52654e7464`.
R2 has not yet run and is not accepted.

The expected protected-archive SHA256 from prior continuation notes could not
be located among current archive files. The dated preservation bundle at
`/home/gpc/hdb-preservation-20261107T103541Z/build-provenance.tar.gz` hashes to
`82f01db09b4cf94ea085dd9fa4f8d228ade61d3e4e6f675afd1d5cc2ea0748c9`, so it
does not verify the expected archive. This discrepancy remains unresolved.
No merge, push, release or issue closure was performed.

Correction to archive-path search: the protected archive is
`/home/gpc/HDB/Proxy/heliosdb-issues.tar.gz` (also copied under
`target/package/heliosdb-proxy-2.1.0/`); both copies currently hash to the
expected `399391dc4de895c32e49b3964ad8f5c087d0f981f61c48353ebf6ac5746168e9`.
The different preservation `build-provenance.tar.gz` is a separate artifact.

The queued no-default cumulative library gate has now run under the fleet lock
and 24 GiB/no-swap scope on the same candidate diff: `cargo test --locked --lib
--no-default-features` passed 817/0 (no ignored tests). The remaining cumulative
feature profiles, lint/MSRV, native and performance acceptance remain open.

The `ha-tr` cumulative library profile then passed 905/0 under the same
fleet-lock bounded scope and candidate hash. The four required core profiles
now have results: no-default 817/0, default 905/0, ha-tr 905/0, and
all-features 2769/0. `all-features,postgres-topology`, Clippy, MSRV, native and
performance acceptance are still open.

The five-profile cumulative Clippy matrix also completed on the unchanged
candidate with `--lib --tests -- -D warnings`. Evidence logs and per-profile
exit files are in
`/home/gpc/HDB/sprint/baselines/proxy/pre3-integrated-validation-20261108/`;
no-default, default, ha-tr, all-features, and all-features+postgres-topology
each exited 0. MSRV 1.86, benchmark compilation, native drills, paired
performance acceptance, and remaining backlog item acceptance are still open.

The final required library profile, `cargo test --locked --lib --features
all-features,postgres-topology`, passed 2769/0 under the same fleet-lock
24 GiB/no-swap scope and unchanged candidate hash. The five-profile library
matrix is now complete; Clippy, MSRV, benchmark/native and performance gates
remain pending.

## Live pre-3.0 backlog order (Sprinter, refreshed 2026-11-08)

Proxy in-progress items sorted by current Sprinter priority:

1. `4fc4d25adde5` FunctionCall/unexpected frontend-message refusal (28).
2. `83438f1cbfa2` shipped multi-tenant example/config repair (25).
3. `cb4a60eb2d87` semaphore-cap validation (25).
4. `49f43bff8122` TR-07 completion/error metadata across Flush/COPY (18.7).
5. `5e59bf2492c5` C-06 cache isolation by startup/session/security context (18.7).
6. `a12be2681910` strict simple Query CString validation (15).
7. `28f0809d934a` function-call identity for read safety (14).
8. `e50b5d4789f4` extended transaction-end proof before rate-cap exemption (14).
9. `a27f98399245` prepared user-path p99 regression review (12).
10. `a85dcd54df24` edge reconnect cursor acknowledgement (12).

Remaining in-progress items are website sample correction `781d033cf154` (10),
MSRV/wasm lint profiles `7eaf74978b2e` (9), PG session-context preservation
`9b98cf163c2f` (8.4), cache hints `83758239caef` (8.3), prepared transaction
budget exemption `8d424cd3df17` (8.3), subset-lint cleanup `316065dd80fa` (6),
tenant COPY/journal memory budgets `74b9dcc82bba` (5), and live tenant-cap reload
`583cf348d707` (3). The lower-priority extended/HTTP/MCP cache item
`a7918a2e5b35` and peer-cache P4 `81e181bdd7c3` remain open; P4 is explicitly
backlog work. Public API lint suppressions `e174b742a1ae` are explicitly 3.0.

Current release dependencies remain outside this Proxy tree: Nano branch
`6bc4a4040eb7` and count(*) `9be5815eb534`, plus Lite-owned
`702736ebceac` and `a7bc90bc2ac2`. The API-stats contract item `c3397d898b25`
is done on Proxy main; its remaining work belongs to Lite. Current dependency
snapshot: `/home/gpc/HDB/sprint/baselines/proxy/pre3-integration-ledger-20261108/dependencies-refreshed-1928.json`.

### Simple-Query native-fixture correction (supersedes the earlier diagnosis)

The earlier statement that R1 failed because its `audit/audit` container did
not match the native client was incorrect. R1 explicitly connected both clients
as `audit/audit`, and its cleanup helper also connects as `audit/audit`; its
owned PostgreSQL identity check passed. R1 reported a bare `AssertionError()`
after proxy startup with no stage, so the failing assertion is not identified
and must not be attributed to a specific ownership or query/cache check.

R2 changed the container and native client to `bench/benchdb` but left the
shared cleanup helper using `audit/audit`. Its `native-before` therefore failed
at the backend identity check with `EOFError('connection closed')`, before the
proxy started. R2 is retained as a failed harness revision and is not accepted.

R3 is prepared from the unchanged five-red/two-positive source snapshot. It
restores consistent `audit/audit`, uses the common trust-auth init arguments,
adds stage-specific named assertions for proxy process/binary/listener/backend
ownership and early malformed-query/cache steps, and captures backend logs
before fixture cleanup. R3 frozen inputs and Python syntax both pass read-only
preflight; source diff remains
`c44d45da4405c54204f80b84f2c030d0dbe55ef9490b037d73a65b52654e7464`.
No R3 runtime, native, after-fix, or cargo gate has run yet.

An unsafe preflight attempt was also recorded: importing `gate.py` via
`runpy.run_path` executed its top-level runner instead of only checking its
manifest. The R2 runner stopped in `before-native` with the fixture mismatch
above, before any cargo command or candidate patch. Its owned disposable
container cleanup reports `removed: true`, and a fresh Docker label query finds
no leftover container. No database outside that disposable fixture was used.
Future preflight reads use AST/hash checks only; R3 still awaits a correctly
serialized run under the fleet lock.

Correction to that unsafe-preflight record: the R2 top-level runner first did
execute its focused `unit-before` cargo test before reaching `before-native`.
That run produced the exact expected five runtime reds and two positive
controls (1.16 s, 1617 filtered), with the frozen source diff unchanged. This
single focused command ran outside the fleet lock due to the same `runpy`
mistake; it is preserved as diagnostic/baseline evidence, not counted as a
properly serialized acceptance gate. Native-before then failed at the
`audit/audit` cleanup identity mismatch; no fix was applied and no later cargo
phase ran. Re-run the causal pipeline under the lock before accepting it.

## Validation continuation (2026-11-08)

The integrated candidate completed the full five-profile unit matrix:
no-default 817/0, default 905/0, ha-tr 905/0, all-features 2769/0, and
all-features+postgres-topology 2769/0. The last profile ran after the physical
Nano gate released the shared lock. Logs for the topology run are available in
the captured tool session; the five-profile Clippy logs and exit receipts are
at `/home/gpc/HDB/sprint/baselines/proxy/pre3-integrated-validation-20261108/`.
All Clippy profiles (no-default, default, ha-tr, all-features,
all-features+postgres-topology) exited 0 under `--lib --tests -- -D warnings`.

`cargo +1.86 check --locked --features msrv-features` also passed on the
unchanged candidate using a dedicated target directory and the fleet lock.
All five library profiles, all five Clippy profiles, formatting, diff checks,
and MSRV 1.86 are now green. Benchmark compilation, cumulative native/live
drills, paired prepared/simple user-path and Criterion acceptance, unresolved
pre-3.0 backlog items, and external Nano/Lite release dependencies remain open.

The cumulative optimized benchmark compile then passed:
`cargo bench --locked --features all-features --no-run`. It produced the
library, plugin, binary, cache, pooling, protocol, relay, and routing benchmark
executables. Their paths and SHA256 receipts are in
`/home/gpc/HDB/sprint/baselines/proxy/pre3-integrated-validation-20261108/bench-no-run.json`.
This is compile-only; no Criterion timings or user-path performance acceptance
are claimed.


## Validation continuation (2026-11-08, Simple Query)

R8 under the shared fleet lock proved the unfixed defect against owned PostgreSQL 18.4: trailing-junk and double-NUL Query frames returned cached `SELECT 42` (`TDCZ`, exact DataRow, L1 hit), while native PostgreSQL returned `ERROR 08P01`. The proxy was therefore serving a valid prefix for structurally malformed requests. Empty-payload and missing-NUL frames were rejected by both paths; valid empty Query, LATIN1 byte response, and typed Parse controls matched byte-for-byte. R8 after-fix targeted tests passed 7/7 in feature, isolated-feature, and minimal profiles; the run then exposed a stale tenant-suite threshold (observed 89/0 vs expected >=90).

R10 resumed from the exact post-fix source hash `e1eb27a9c34f848a329e89ccf6035f8654fc5106c8056d9076c849d6f883b2f3`, without reapplying the patch. Protocol selector 21/0, three Clippy profiles, fmt, diff check, plain release build, and native-after passed. Owned PostgreSQL verified four malformed Query payloads each receive exactly one FATAL 08P01 E with no ReadyForQuery and no cache hit; the three empty/LATIN1/typed Parse controls matched expected wire bytes. Proxy reaped and owned PG container removed. Evidence: `/home/gpc/HDB/sprint/baselines/proxy/pre3-simple-query-frame-20261108-r10/`.

The verified Simple Query change was ported to the combined candidate without replacing its other edits. Combined all-features library suite then passed 2771/0, including all seven Simple Query parser/handler/cache/rewrite/direct-forward tests. Candidate base remains `93fb08f912f6d377430a9d72e86980ef6734ceaf`; current source diff SHA256 is `a5a5068e405049f79f13717f0ff3f22bb1ae38cbb2d92e054681014e38bd6613`. Archive SHA remains `399391dc4de895c32e49b3964ad8f5c087d0f981f61c48353ebf6ac5746168e9`. This item remains in progress: full cumulative matrix/lint/MSRV/performance after the port, broader plugin/transaction-abandonment coverage, integration, merge/release acceptance remain open.

## Validation continuation (2026-11-09, native commit-at-Flush edge event)

The cumulative candidate's commit-at-Flush unit coverage was extended with an
owned PostgreSQL 18.4 wire drill at
`tests/integration/commit_flush_edge_event.py`. Against the immutable candidate
binary SHA256 `d7333295091102de72bfb91abf3e58f0968794b6003881434f7968e774dbec32`,
the home SSE stream delivered the exact committed table invalidation while
Sync was withheld, the native backend showed the committed value, Sync returned
`ReadyForQuery(I)`, and no duplicate invalidation appeared after Sync. The
proxy was reaped and the exact labeled backend container removed. Evidence is
in `/home/gpc/HDB/sprint/baselines/proxy/pre3-integrated-native-20261109/commit-flush-edge-r4/`.
This closes only the native edge-event subgate; the relevant Sprinter items
remain `in_progress` pending the full release gates.

The current cumulative simple-protocol user-path comparison failed the 3%
committed-write budget: session/16 TPS -3.09%, transaction/16 TPS -4.20% and
p99 +8.52%, with zero failed transactions, unknown outcomes, or checkpoints.
The raw archive is `/home/gpc/HDB/sprint/baselines/proxy/pre3-integrated-native-20261109/usertp-simple/`;
the performance review item `a27f98399245` is triaged with `ESCALATE: fable`.
This is a retained gate failure, not accepted performance evidence. The full
Criterion run and prepared user-path acceptance remain open.

2026-11-09 Criterion diagnostics: the cache benchmark fixture now validates a
`cache_tables` declaration through `parsed.tables`, and the full cache bench
completed, including L2-only (3.83–3.88 us, `L2Warm`, no L1 map before lookup).
The relay bench completed against the clean base and integrated candidate in
the isolated target at
`/home/gpc/HDB/sprint/baselines/proxy/pre3-integrated-native-20261109/criterion/target`.
It used 10 samples, 1 s warmup, and 2 s measurement. The comparison is mixed,
with small regressions and high outlier rates (including +6.98% at
`switchover/drain/64`); treat it as diagnostic only. It neither attributes nor
waives the simple user-path failure. The complete consistent Criterion matrix
and prepared user-path acceptance are still required.

2026-11-09 isolated five-bench comparison completed after saving clean-base
measurements and comparing the integrated candidate in the same Criterion
target. Evidence summary:
`/home/gpc/HDB/sprint/baselines/proxy/pre3-integrated-native-20261109/criterion-common-r2-summary.json`;
raw Criterion baseline/new/change estimates are retained under the sibling
`criterion/target/criterion/`. Candidate product source corresponds to release
binary SHA256 `d7333295091102de72bfb91abf3e58f0968794b6003881434f7968e774dbec32`;
the worktree tracked diff at the bench run was
`b12693df6c7f23206f079275ad42d7d1dd34d603d7793bcef347496687b68677` after
candidate-only test/bench fixture changes (product code unchanged). Diagnostic
mean-time changes include protocol short Query +13.52% (95% CI +10.06% to
+16.62%), medium Query +3.72% (+2.62% to +4.90%), cache L1 hit +4.83% (+3.51%
to +6.15%), L2 hit +4.82% (+3.43% to +6.40%), miss/store +9.64% (+7.52% to
+11.81%), and short write-table invalidation +3.03% (+0.70% to +5.46%). Pool
creation of 10 improved 14.38% (-17.82% to -11.43%). The comparison is
sequential and uses short samples (some groups use 50); treat these as
performance-review signals, not final acceptance or causal attribution. The
simple and prepared user-path gates remain controlling and unresolved.

A focused 50-sample, 3 s warmup / 5 s measurement repeat of
`protocol/query_text/short_select` measured clean base at 11.896 ns and the
candidate at 12.369 ns (about +3.98%). A trial replacing byte iteration with
the existing `memchr` dependency measured 15.992 ns (+34.43% versus the saved
base); that edit was reverted. The helper source is unchanged. Full values and
confidence intervals are in `criterion-common-r2-summary.json`. This remains a
review signal rather than a causal explanation; the user-path regression still
needs independent attribution and a passing cumulative retest.

Correction to the earlier cache-benchmark note: the candidate-only
`query_hint_contract` Criterion group later produced inconsistent setup
assertions in the isolated optimized benchmark, while the exact L2-only and
declared-dependency public API regressions passed in debug and release test
profiles. Those candidate-only cases were removed from Criterion so the common
cache groups can be compared against the base. Their semantic contracts remain
covered by tests; no production cache change or performance waiver is claimed.


## Tenant journal budget runtime correction (2026-11-10)

A native PostgreSQL 18.4 runtime probe exposed that startup-resolved database tenants
were attached to `ClientSession.tenant_charge`, but journal source identity read only
`session.variables["tenant_id"]`. The live before-fix run therefore observed zero
usage and zero refusals despite a 1,200-byte configured cap. The fix makes journal
source identity prefer the canonical tenant on the session's already-attached
`TenantBudget`, falling back to the configured tenant variable only when no budget
identity is attached. Regression `journal_source_uses_startup_tenant_budget_identity`
passes 1/0.

After-fix native proof against PostgreSQL 18.4 passed on release binary SHA256
`d18381f7e1fd78834b0a252a582eb88e14d1e47185e2d8e59e18582ffcfb210f`. With a
1,200-byte cap, the transaction began at 890 bytes, the large INSERT raised one
budget refusal, retained usage fell to a 605-byte incomplete marker, and commit
retained 611 bytes. The backend executed the INSERT inside a temporary
`ON COMMIT DROP` table; the probe verified its row and the table's commit drop.
All cap/refusal/retention assertions passed; the proxy was terminated and reaped.
Evidence and exact before-fix failure: `/home/gpc/HDB/sprint/baselines/proxy/
pre3-integrated-native-20261110-r1/tenant-journal/` (`before-result.json`,
`result.json`, source script, configs, logs, and binary receipt). The remaining
full cumulative matrix/lint/MSRV, native/live suite, Criterion and quiet-window
user-path acceptance remain open; no merge or release is claimed.

## Current benchmark-build and CI-lint receipts (2026-11-10)

The current integrated candidate remains based on `93fb08f912f6d377430a9d72e86980ef6734ceaf`
with tracked diff SHA256
`a406b35d8f5703397de86a95de51175d7def23b62a705b31ba4354a6dbb1401f`.
Under the shared fleet lock and 24 GiB/no-swap scope,
`cargo bench --locked --features all-features --no-run` completed successfully
after 7m09s and produced all eight benchmark executables (library, plugin,
main, cache, pooling, protocol, relay and routing). Their individual SHA256s
and the command result are recorded on Sprinter item `a27f98399245`; this is
compile evidence only, not a Criterion comparison.

The two additional CI lint legs were run serially on the same unchanged source:
`cargo clippy --locked --lib --tests --features msrv-features -- -D warnings`
and `cargo clippy --locked --lib --tests --no-default-features --features
wasm-plugins -- -D warnings`; both passed. Logs are under
`/home/gpc/HDB/sprint/baselines/proxy/pre3-extra-clippy-current-20261110/`.
Sprinter item `7eaf74978b2e` is marked fixed (unmerged).

Criterion comparison and final simple/prepared user-path gates remain open.
At the latest check, host load was 2.85/4.63/6.64, but the backup transfer and
PostgreSQL recovery processes remained live. Do not treat that as an
acceptance-quality quiet window. The candidate remains uncommitted and no
merge or release is claimed.

## Takeover on gpc001ca as `admin`: gate, attribution, fixes (2026-11-15 / 2026-11-16)

The candidate was continued in place under `/home/admin/HDB` (the trees were relocated
from `/home/gpc/HDB`; paths in the sections above are the old ones). Still based on
`93fb08f912f6d377430a9d72e86980ef6734ceaf`, uncommitted; tracked diff SHA256 at the last
gate `095fe57787d66f95…` (68 dirty paths). Patch snapshots taken before each sync are
in `/home/admin/HDB/sprint/baselines/proxy/pre3-admin-toolchain-20261115/`.

**Environment.** Rust is user-local for `admin` (1.95.0, and 1.86.0 for MSRV). The fleet
lock is `/home/admin/HDB/sprint/coordination/build.lock`; `scripts/bench-gate.sh` takes
`LOCK=` and `OUT=` for it. The five `proxy-pre3-*` worktree links were repaired with
`git worktree repair` (owner-approved); the `proxy-nano-*` links and the stale
registrations were left alone and nothing was pruned.

**Criterion gate 1 — `pre3-criterion-integrated-admin-20261115-r1`: FAIL.** Clean
baseline `proxy-pre3-criterion-base-20261109-r2`, all-features, five suites, three
interleaved rounds: 167 matched cases, mean −0.39 %, median +0.13 %, 19 separated
regressions over 2 %, 18 separated improvements. Its `ATTRIBUTION.md` names every one:

- 11 are placement: four on executables whose function bodies are all identical to the
  baseline's, seven on functions whose own body is in the baseline executable
  (`prepared_parse/deallocate/named` was −23 % with the arms the other way round on
  2026-11-10).
- `cache/l3/store_full_5000` +6.1 % and `store_lookup_contended_8` +21.2 % were real: the
  cache-namespace field grew an L3 slot from 256 to 288 bytes, so a bucket's first block
  of four passed glibc's per-thread cache limit. Fixed: the slot holds namespace and
  context strings behind one shared pointer (216 bytes, compile-time bound).
- `pool_mode/txn_event_detect/savepoint` +23.1 % and `rollback_to` +8.7 % were real
  code generation on unchanged source (a chunked compare inlined into
  `TransactionEvent::detect`). Fixed by removing the redundant 17-byte prefix test.
- `cache/l2/compress_lz4_1m` and `compress_zstd_4k` are heap-state noise (the changed
  step measures 22–36 ns in isolation against deltas of 543 ns and 47 µs).
- `cache/query/l2_hit_1k` +3.3 % and `l1_due_claim_held_1k` +2.1 % were profiled (the host
  allows `perf_event_open`; tools in the gate's `attribution/`): 10–20 ns per lookup is
  the cost of the namespace, tier and dependency checks; the rest sits in lexer functions
  with unchanged bodies. The same profile found `memchr::memmem::find` building a
  searcher per call in the cache hint parser and the lexer (34 ns of an L1 hit, 84 ns of
  an L2 hit, 174 ns of a miss and store, in the baseline too). Fixed: static searchers.
- New case `journal/append_entry/after/1024` measured a reallocation of the bench's own
  making; the bench now reserves in its setup.

**SIGHUP tenant caps.** Implemented as live resize (`src/tenant_slots.rs`,
`TenantSlots::reload_caps`): a reload that only changes the size of caps or byte budgets
tenants already run under applies to the running process. A raised cap admits waiters in
FIFO order; a lowered cap ends nothing and admits nobody new until the tenant's holders
are back under it. Any other change to `[multi_tenancy]`, and a cap a tenant would gain
or lose, still refuses the whole section with a warning naming the reason. Tests in
`src/server/tests/multi_tenancy.rs` and the module; documented in
`docs/configuration.md` and `CHANGELOG.md`.

**Criterion gate 2 — `pre3-criterion-integrated-admin-20261116-r2`: FAIL by the tool.**
The candidate of gate 1 plus the resize, the three fixes above and the static searchers:
167 matched cases, mean −1.21 %, median −1.15 %, 22 separated regressions over 2 %, 36
separated improvements. Every real regression of gate 1 was at or below the baseline
(`store_full_5000` −0.5 %, `savepoint` −13.8 %, `rollback_to` −9.8 %, `l2_hit_1k` −1.0 %,
`l1_due_claim_held_1k` −5.1 %). Its `ATTRIBUTION.md`: 18 of the 22 are placement or noise
on functions whose body is in the baseline executable, among them the seven
`routing/write_detect` cases (+37..+70 %): `QueryRouter::is_write_query`, unchanged in
source and in machine code, moved 224 bytes and ran 2.5× slower, reproduced in a second
build of both trees. Two are on allocator-bound cases and could not be separated from
allocator noise (`journal/entries_in_window/scan_500`, `journal/rollback_to_savepoint/16`).
Two were real and small: `cache/classify/select_only` +6.2 % (12 ns, a byte loop in the
journal classifier's new call-free shortcut) and `pool_mode/txn_event_detect/commit`
+4.7 % (0.5 ns).

**After gate 2.** `QueryRouter::is_write_query` no longer upper-cases the whole statement
into a new `String`: it reads an ASCII first word in place and keeps the old path for
text with a non-ASCII byte before the end of that word (reference test against the old
form over 6 144 generated texts). `journal_capture::classify_single` searches for `(`
with `memchr::memchr`.

**Validation of the candidate as gated below** (dev copy identical to the worktree, each
step under the fleet lock, 24 GiB / no swap; logs copied to
`/home/admin/HDB/sprint/baselines/proxy/pre3-admin-toolchain-20261115/validate-20261116T115337Z/`):
`cargo fmt --check`; clippy `-D warnings` for all-features `--all-targets`, default,
`ha-tr`, `all-features,postgres-topology`, `msrv-features` and `--no-default-features
--features wasm-plugins` (`--tests`); tests default 1005/0 (4 ignored), `ha-tr` 1005/0
(4), all-features 2916/0 (15), `all-features,postgres-topology` 2921/0 (15); `cargo
+1.86.0 check --locked --features msrv-features`; `cargo bench --features all-features
--no-run`. All passed.

**Criterion gate 3 (last) — `pre3-criterion-integrated-admin-20261116-r3`: FAIL by the tool.**
167 matched cases, mean −4.69 %, median −1.01 %, 9 separated regressions over 2 %, 44
separated improvements. The seven `routing/write_detect` cases are −45..−65 % (29–48 ns
to 14–18 ns); `cache/classify/select_only` is −4.75 %; four of gate 2's 22 separated cases are
separated again (`prepare/named`, `short_select`, `rollback_to_savepoint/16`,
`manager_acquire_release/8`). Its
`ATTRIBUTION.md`, with a second symbols-kept build and a profile of every case:

- 7 are placement or noise on functions whose body is in the baseline executable:
  `pool_mode/manager_acquire_release/8` +19.3 % (reversed, −12 % and −20 %, in the second
  build; the baseline arm alone moved between 43 and 63 µs across the three gates),
  `protocol/query_text/short_select` +12.05 % and `protocol/encode_message/kilobyte_in_list`
  +3.8 % (body-identical executable, byte-identical to both earlier gates'),
  `pool_mode/prepared_parse/prepare/named` +9.0 % and `prepare/typed` +6.6 %
  (`src/pool/prepared.rs` unchanged; the difference is in `malloc`, `free` and `core`
  string functions), `pool/node_endpoint/create` +4.9 % and
  `pool/throughput/sequential_acquire/1` +3.0 % (no source change; within ±0.5 % and
  −0.5..−2.8 % in the second build).
- `journal/rollback_to_savepoint/16` +6.75 % (89 ns) is not attributed to source: the
  changed function's own samples are 34 ns in the baseline and 31 ns in the candidate and
  the second build measures +7.5 % and −7.0 %, but two gates in a row separated it, so a
  cost of up to 90 ns per 16-entry rollback is not excluded.
- `cache/classify/begin` +4.5 % (5 ns) is real: the journal classifier's retry-safety
  change costs 5–12 ns per classified statement.

New cases `journal/append_entry/after/16` and `/1024` are recorded in
`benches/BASELINE.md` (136.7 ns and 220.1 ns). No further gate was run: every gate of a
new build draws a new set of placement cases on this CPU (AMD EPYC 7371), which the
tool's rule cannot pass, and the remaining real costs are the ones listed under Open.
After the gate only `benches/BASELINE.md` and this document changed (tracked diff
SHA256 `7618dffea2456c4e54a4ef2f2734e2aaacb3af4cb4e8e9b882a6281d89d58855`).

**Open** (as of 2026-11-16; superseded by the next section).

- User-path gate (simple and prepared, live PostgreSQL): not run. `admin` has no Docker
  access on this host and each run needs the owner's approval.
- Legacy FunctionCall (`F`) is refused with a fatal `0A000`. Clients that use the
  fastpath large-object API (libpq `lo_*`, JDBC `LargeObject`, `pg_dump` with large
  objects) will fail through the proxy; owner decision.
- Extended-protocol read cache: not started.
- Accepted per-call costs, all measured: journal classification 5–12 ns per statement;
  `normalize_with_dependencies` 6–12 ns per L2 lookup, store and refresh (an extra move
  of the normalized query); pool-mode detection of `COMMIT` 0.5 ns.
- `journal/rollback_to_savepoint/16`: see gate 3.
- `"START\tTRANSACTION"` is classified as a plain statement by pool-mode detection
  (as before this work).
- Nothing is committed, merged or released.

## Open items of the takeover closed, user-path gate, cache-on finding (2026-11-16 / 2026-11-17)

Owner instruction of 2026-11-16: "Do the "still open" topics, add admin to Docker
group". Same worktree, still based on `93fb08f912f6d377430a9d72e86980ef6734ceaf`,
uncommitted: 74 dirty paths. Tracked diff SHA256: `41949aeebf8dd3b6…` when validated and
when the release binary was built; `f02d3e4f2f941c4b…` at Criterion gate 4 (adds
`CHANGELOG.md`, `docs/configuration.md` and a comment and a warning in
`scripts/regress/bench-usertp.sh`); `5c321c4080d8de94…` now (adds the wording of two code
comments and one changelog entry after gate 4). A release build of the final tree is
byte-identical to the binary that was drilled and gated (SHA256 `68a576f9f03357c4…`;
`pre3-admin-toolchain-20261115/final-check-20261117/`, with a clippy all-features
`--all-targets` re-run, clean).

**Docker.** `admin` is in the `docker` group (`sudo usermod -aG docker admin`, the only
use of `sudo`). Live harnesses ran one at a time under the fleet lock against the
disposable backend `proxy-pre3-pgbench-20261110`; record with the owner's approval
verbatim, binaries' hashes and every result:
`/home/admin/HDB/sprint/baselines/proxy/pre3-live-drills-admin-20261116/GATE-RECORD.md`.

**Legacy FunctionCall (`F`).** No longer refused wholesale. New top-level key
`function_call`: `"large-objects"` (default) forwards PostgreSQL's built-in large-object
functions, by catalog OID (checked against `pg_proc` of 18.4), as admitted requests on
the primary (tenant permit, rate limiter, circuit breaker) and refuses every other
function with a non-fatal `0A000` and ReadyForQuery; `"any"` forwards every OID and
treats each call as a write of unknown scope (cached results retired, reads pinned to
the primary, permit kept as for a session lock); `"refuse"` refuses all. A transaction
that ran a forwarded call is never replayed after a failover; a journaled one that ran a
writing call is marked incomplete. A malformed `F` ends the session with `08P01`.
`src/server/fastpath.rs`; the three values are the owner's to revisit, in particular the
default. Limits: plugins, rewrite rules, tenant row filters, anomaly detection and
analytics do not see these calls, which carry no SQL.

**Pool-mode transaction detection.** `TransactionEvent::detect` reads words separated by
any SQL whitespace instead of testing fixed prefixes: `START\tTRANSACTION`,
`BEGIN\n…`, `COMMIT AND CHAIN` (stays in a transaction), `ROLLBACK\tTO` are classified
as PostgreSQL parses them. `START` begins a transaction only before the word
`TRANSACTION` (the old two-prefix test also took `START WORK TRANSACTION`). Reference
test against the prefix form on plain statements.

**Per-call costs.** `QueryCache` adds a hint's tables to the normalized query in place
(`add_dependencies`), and the journal classifier's call scan is out of line
(`read_unless_it_calls`). Neither showed in gate 4 (below); the comments and the
changelog entry say so. The costs they were aimed at stand.

**Extended-protocol read cache** (`[cache] extended_protocol`; default `"hint"` since
2026-11-20, owner decision after the 3.0.0 user-path gate: statements opt in with
`/*helios:cache=on*/`, `"on"` is the former default `true`).
`src/server/ext_cache.rs`. Served shapes: `Parse("")`/`Bind`/[`Describe` portal]/
`Execute`/`Sync` where the unnamed `Parse` leads the batch (needs
`optimize_unnamed_parse`, the default), and `Bind`/[`Describe` portal]/`Execute`/`Sync`
of a registered named statement; unnamed portal, no row limit. The key is the statement
text plus a namespace hashing declared parameter types, bound values, parameter and
result format codes and the batch shape. Conditions are those of a simple query's
lookup. A hit leaves the backend holding the unnamed statement the client believes it
parsed. Limits: L1 is not used (L2/L3 only); other batch shapes, transactions, sessions
that have written or changed context, and the `[edge]` cache are not served; no
Criterion case reaches it (its functions are private to `server`).

**Promotion bug found on the way (pre-existing).** An unnamed `Parse` that was not the
first message of its batch left the proxy believing the backend still held the
previously promoted unnamed statement; a later `Bind` without `Parse` would have run the
wrong statement text on that backend connection. Fixed (`unnamed_untracked`), with a
harness test that fails without the fix.

**Worktree links.** The four `proxy-nano-*` links were repaired with
`git worktree repair` (branches resolve, dirty paths preserved: 16 and 15 in the two
auth-terminal trees). Nothing was pruned; about 115 stale registrations are untouched.
Evidence: `pre3-admin-toolchain-20261115/worktree-repair-20261116/`.

**Validation** (dev copy identical to the worktree at `41949aee…`, each step under the
fleet lock, 24 GiB / no swap; logs in
`pre3-admin-toolchain-20261115/validate-20261116T215340Z/`): `cargo fmt --check`;
clippy `-D warnings` for all-features `--all-targets`, default, `ha-tr`,
`all-features,postgres-topology`, `msrv-features` and `--no-default-features --features
wasm-plugins` (`--tests`); tests default 1014/0 (4 ignored), `ha-tr` 1014/0 (4),
all-features 2939/0 (15), `all-features,postgres-topology` 2944/0 (15); `cargo +1.86.0
check --locked --features msrv-features`; `cargo bench --features all-features
--no-run`. All passed.

**Live drills** (candidate release binary SHA256 `68a576f9f03357c4…`, PostgreSQL 18.4):

- `tests/integration/fastpath_extended_cache.py` (new): PASS. A 3 000-byte large object
  through the fastpath protocol under the default; `"any"` and `"refuse"` behave as
  documented; extended cache 8 hits / 7 stores, responses byte-identical to the
  backend's, refill after another session's write, `Bind` without `Parse` after a hit.
- `BK=pg scripts/regress/run.sh`: 10 PASS, 0 FAIL.

**User-path gate** (`scripts/perf-gate-userpath.sh`, base `a3a4e2c70d55e64d…` against
the candidate, 3 % budget on committed-write TPS and p99, cache off as shipped):

- Prepared protocol (`pre3-userpath-admin-20261116-prepared-r1`): **PASS**. Committed
  writes +0.6 %, +0.4 %, +0.2 %, −0.0 %.
- Simple protocol (`pre3-userpath-admin-20261116-simple-r1`): **FAIL by the tool** on one
  cell, transaction mode at 64 clients: −5.3 % TPS, p99 +5.4 %. The other three gated
  cells: −1.2 %, −0.5 %, +1.9 %.
- That cell alone, five passes (`pre3-userpath-admin-20261116-simple-txn64-r2`):
  **PASS**, −0.5 % TPS, p99 +2.2 %. The −5.3 % did not reproduce. Over all eight
  interleaved pairs the candidate's mean is −1.6 % and it is the lower arm in seven of
  them, so a cost of 1–2 % on this cell is likely; it is inside the budget and not
  attributed.
- Ungated read cells outside 3 %: simple session/16 −3.6 %, prepared transaction/16
  −3.7 %; simple transaction/16 was +4.0 % in the same run.
- Not run: `TR_MODE=transaction`, a `[multi_tenancy]` config, `PGBENCH_PROTOCOL=extended`.

**Finding: the per-read settings proof makes an enabled query cache about 16 times
slower than no cache on cheap reads.** The gate above runs with the cache off. With
`[cache] enabled = true` and `trust_backend_read_effects = true` on both arms
(diagnostic runs, pgbench select-only, random keys, about 70 % misses):

| protocol | proof | base reads/s | candidate reads/s | |
|---|---|---|---|---|
| simple, session/16 | per read (default) | 68 754 | 4 227 | −93.9 % |
| prepared, session/16 | per read (default) | 80 460 | 4 385 | −94.6 % |
| prepared, session/64, transaction/16, transaction/64 | per read (default) | 103 012, 38 735, 49 487 | 4 530, 4 060, 4 298 | −95.6 %, −89.5 %, −91.3 % |
| simple, session/16 | `backend_context_immutable = true` | 67 734 | 65 899 | −2.7 % |
| prepared, session/16 | `backend_context_immutable = true` | 80 883 | 84 088 | +4.0 % |

The cost is the `SHOW ALL` proof before each lookup and each store
(`src/server/cache_scope.rs`), added to the simple-protocol cache earlier in this
candidate; it had not been measured. The extended-protocol cache inherits it and is
otherwise free on this workload. The backend alone answers `SHOW ALL` 29 700 times per
second at 16 clients, so a two-proof read is bounded near 15 000 per second even with a
free proxy. Documented with these figures in `docs/configuration.md` and
`CHANGELOG.md`; the mechanism was not changed. Owner decision, see Open.

**Criterion gate 4 — `pre3-criterion-integrated-admin-20261117-r4`: FAIL by the tool.**
167 matched cases, mean −4.66 %, median −0.92 %, 21 separated regressions over 2 % (9 in
gate 3), 37 separated improvements. `TransactionEvent::detect`: plain statements −43 %
(14.9 → 8.5 ns), `START TRANSACTION` −34 %, `ROLLBACK` −25 %, `RELEASE` −20 %,
`ROLLBACK TO` −11.5 %; `routing/write_detect` unchanged from gate 3 (−44..−64 %). Its
`ATTRIBUTION.md`, with a second symbols-kept build and a profile of every separated
relay, cache and pooling case:

- 17 are placement or noise on code that did not change: `journal/total_size/{1,16,128}`
  +8..+21.5 % (−6.5..−14 %, separated the other way, in gate 3 on the same source;
  −0.3..−11.6 % in the second build), four `journal/statement_type` cases +2.8..+4.3 %
  (function unchanged from the baseline; −5..−16.5 % in gates 2 and 3), two protocol
  cases on the body-identical executable that is byte-identical to gate 3's,
  `prepare/typed` and `prepare/named` (unchanged file), `cache/l2/compress_lz4_1m` +7.9 %
  (entirely inside an identical `lz4_flex` function on identical input; positive in
  every measurement so far, no source cost found), `compress_zstd_4k`,
  `cache/write_tables/insert_history`, `switchover/buffer_query/enqueue_one`,
  `pool/acquire_release/contention/2`, `pool/throughput/sequential_acquire/10`.
- `journal/entries_in_window/scan_500` +7.2 % is not excluded: −23.8 % in the second
  build, but positive in all four gates, and it clones a changed `JournalEntry`. Up to
  5 ns per returned entry.
- Three are real and small: `cache/classify/begin` +8.0 % (5–10 ns per classified
  statement, the retry-safety change; the out-of-line scan did not help),
  `journal/manager/begin_log_commit` +4.8 % (10–37 ns per journaled transaction, the
  per-tenant accounting in `after_commit` and `push_committed`; +1.0..+1.8 % in gates
  1–3, first separated here), `pool_mode/txn_event_detect/savepoint` +8.2 % (at most
  1.2 ns per `SAVEPOINT`, from the rewrite that saves 2.5–13.7 ns on five other kinds).

**Open.**

- **Cache with the default per-read proof**: −94 % on cheap reads against the
  baseline, both protocols. Options: (a) ship as is with the documented cost and
  `backend_context_immutable` as the throughput setting; (b) make the proof cheaper per
  read, e.g. a full `SHOW ALL` once per backend connection and a one-row check of
  `pg_conf_load_time()` per read (the assumption that a reload is the only way an existing session's
  settings change without a statement the proxy sees is not verified, and this is not
  designed in detail); (c) only reduce the proxy's handling of the reply (bounded gain: the
  backend caps it near 15 000 reads per second). Until decided, `extended_protocol =
  true` extends the same cost to prepared reads of anyone who enables the cache.
- Simple protocol, transaction mode, 64 clients: a likely 1–2 % committed-write cost,
  inside the budget, not attributed.
- User-path gate variants not run: `TR_MODE=transaction`, `[multi_tenancy]`, extended
  protocol.
- `function_call` default and the non-fatal refusal are a choice made here.
- Per-call costs measured by gate 4 and kept: journal classification 5–10 ns per
  statement; 10–37 ns per journaled transaction; at most 1.2 ns per `SAVEPOINT` in
  pool-mode detection; `journal/entries_in_window` possibly 5 ns per entry.
- The Criterion tool's verdict is FAIL in all four gates; each new build separates a
  different set of unchanged-code cases on this CPU. Passing it needs either a
  different host or a rule that discounts body-identical and sign-flipping cases.
- The extended-protocol cache and FunctionCall forwarding have no Criterion case and
  no cache-on user-path acceptance run, only the diagnostics above.
- Nothing is committed, merged or released.
